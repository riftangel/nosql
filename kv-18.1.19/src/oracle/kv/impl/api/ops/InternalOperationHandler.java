/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.api.ops;

import static oracle.kv.Value.Format.isTableFormat;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.RecordVersion;
import com.sleepycat.je.utilint.VLSN;

import oracle.kv.Key;
import oracle.kv.UnauthorizedException;
import oracle.kv.Version;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestHandlerImpl.RequestContext;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.InternalOperationHandler.Keyspace.KeyAccessChecker;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.table.TimeToLive;

/**
 * Base class for implementing server-side handling of individual operations.
 * Each concrete instance executes operations for instances of a single
 * InternalOperation class.  Because a single handler is used to execute
 * multiple operations concurrently, handlers should not store per-operation
 * data.
 *
 * @param <T> the type of the associated internal operation
 */
public abstract class InternalOperationHandler<T extends InternalOperation> {

    /**
     * An empty, immutable privilege list, used when an operation is requested
     * that does not require authentication.
     */
    static final List<KVStorePrivilege> emptyPrivilegeList =
        Collections.emptyList();

    /** Used to avoid fetching data for a key-only operation. */
    static final DatabaseEntry NO_DATA = new DatabaseEntry();
    static {
        NO_DATA.setPartial(0, 0, true);
    }

    /** Used as an empty value DatabaseEntry */
    private static final DatabaseEntry EMPTY_DATA =
        new DatabaseEntry(new byte[0]);

    /** Same key comparator as used for KV keys */
    static final Comparator<byte[]> KEY_BYTES_COMPARATOR =
        new Key.BytesComparator();

    /**
     * The overall operation handler.
     */
    final OperationHandler operationHandler;

    /**
     * The associated opcode.
     */
    final OpCode opCode;

    /**
     * The type of the operation associated with this handler.
     */
    private final Class<T> operationType;

    InternalOperationHandler(OperationHandler operationHandler,
                             OpCode opCode,
                             Class<T> operationType) {
        this.operationHandler = operationHandler;
        this.opCode = opCode;
        this.operationType = operationType;
    }

    public OperationHandler getOperationHandler() {
        return operationHandler;
    }

    OpCode getOpCode() {
        return opCode;
    }

    Class<T> getOperationType() {
        return operationType;
    }

    /**
     * Execute the operation on the given repNode.
     *
     * @param op the operation to execute
     * @param txn the transaction to use for the operation
     * @param partitionId the partition ID of RN owning the request data
     * @return the result of execution
     * @throws UnauthorizedException if an attempt is made to access restricted
     * resources
     */
    abstract Result execute(T op,
                            Transaction txn,
                            PartitionId partitionId)
        throws UnauthorizedException;

    /**
     * Checks whether the input key has the server internal key prefix as
     * a prefix.  That is, does the key reference something that is definitely
     * within the server internal keyspace?
     */
    boolean isInternalRequestor() {
        final ExecutionContext currentContext = ExecutionContext.getCurrent();
        if (currentContext == null) {
            return true;
        }
        return currentContext.hasPrivilege(SystemPrivilege.INTLOPER);
    }

    /**
     * Returns an immutable list of privilege required to execute the
     * operation.  The required privileges depend on the operation and the
     * keyspace it accessing.
     *
     * @param op the operation
     * @return the list of privileges for the operation
     */
    abstract List<? extends KVStorePrivilege> getRequiredPrivileges(T op);

    Logger getLogger() {
        return operationHandler.getLogger();
    }

    public RepNode getRepNode() {
        return operationHandler.getRepNode();
    }

    Version getVersion(Cursor cursor) {
        return operationHandler.getVersion(cursor);
    }

    ResultValueVersion makeValueVersion(Cursor c,
                                        DatabaseEntry dataEntry,
                                        OperationResult result) {
        return operationHandler.makeValueVersion(c, dataEntry, result);
    }

    ResultValueVersion makeValueVersion(Cursor c, DatabaseEntry dataEntry) {
        return operationHandler.makeValueVersion(c, dataEntry);
    }

    protected TableImpl getAndCheckTable(final long tableId) {
        return operationHandler.getAndCheckTable(tableId);
    }

    TableImpl findTableByKeyBytes(byte[] keyBytes) {
        return operationHandler.findTableByKeyBytes(keyBytes);
    }

    /**
     * Creates JE write options with given TTL arguments.
     */
    com.sleepycat.je.WriteOptions makeOption(TimeToLive ttl, boolean updateTTL) {
        int ttlVal = ttl != null ? (int) ttl.getValue() : 0;
        TimeUnit ttlUnit = ttl != null ? ttl.getUnit() : null;
        return new com.sleepycat.je.WriteOptions()
            .setTTL(ttlVal, ttlUnit)
            .setUpdateTTL(updateTTL);
    }

    /**
     * Using the cursor at the prior version of the record and the data of the
     * prior version, return the requested previous value and/or version.
     * <p>
     * The {@link #getVersion(Cursor) access via cursor} for version data is
     * skipped for performance reason unless required by the given return
     * choice.
     * <p>
     * Expiration time is included if either version or value is requested but
     * not included for the choice of NONE.
     *
     * @param cursor cursor positioned on prior state of data
     * @param prevData previous data
     * @param prevValue choice of which state to attach and
     * the value+version to return as result
     * @param result result of looking up the previous state of the record.
     * Carries expiration time for the record. It is extremely unlikely to
     * be null, but handle that, just in case.
     */
    void getPrevValueVersion(Cursor cursor,
                             DatabaseEntry prevData,
                             ReturnResultValueVersion prevValue,
                             OperationResult result) {

        long expirationTime = (result != null ? result.getExpirationTime() : 0);
        switch (prevValue.getReturnChoice()) {
        case VALUE:
            assert !prevData.getPartial();
            prevValue.setValueVersion(prevData.getData(),
                                      null,
                                      expirationTime);
            break;
        case VERSION:
            prevValue.setValueVersion(null,
                                      getVersion(cursor),
                                      expirationTime);
            break;
        case ALL:
            assert !prevData.getPartial();
            prevValue.setValueVersion(prevData.getData(),
                                      getVersion(cursor),
                                      expirationTime);
            break;
        case NONE:
            prevValue.setValueVersion(null, null, 0L);
            break;
        default:
            throw new IllegalStateException
                (prevValue.getReturnChoice().toString());
        }
    }

    /**
     * Returns whether the Version of the record at the given cursor position
     * matches the given Version.
     */
    boolean versionMatches(Cursor cursor, Version matchVersion) {

        final RepNodeId repNodeId = getRepNode().getRepNodeId();
        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);

        /* First try without forcing an LN fetch. */
        RecordVersion recVersion =
            cursorImpl.getCurrentVersion(false /*fetchLN*/);

        /* The LSN may match, in which case we don't need the VLSN. */
        if (matchVersion.samePhysicalVersion(repNodeId, recVersion.getLSN())) {
            return true;
        }

        /* Try the VLSN if it is resident and available. */
        long vlsn = recVersion.getVLSN();
        if (!VLSN.isNull(vlsn)) {
            return matchVersion.sameLogicalVersion(vlsn);
        }

        /* The VLSN is not resident. Force a fetch and try again. */
        recVersion = cursorImpl.getCurrentVersion(true /*fetchLN*/);
        vlsn = recVersion.getVLSN();
        assert !VLSN.isNull(vlsn);
        return matchVersion.sameLogicalVersion(vlsn);
    }

    /**
     * Create a DatabaseEntry for the value bytes.  If the value is
     * empty, as indicated by a single zero or 1 byte in the array, use an empty
     * DatabaseEntry to allow JE to optimize the empty value.
     *
     * Both 0, 1 and 2 need to be accepted. See Value.writeFastExternal() where
     * it is writing with Format of NONE or TABLE, TABLE_V1
     */
    DatabaseEntry valueDatabaseEntry(byte[] value) {

        if (value.length == 1 && (value[0] == 0 || isTableFormat(value[0]))) {
            return EMPTY_DATA;
        }
        return new DatabaseEntry(value);
    }

    /**
     * As above, but reuse the supplied DatabaseEntry. Callers be sure to use
     * the returned value as it may not be the same as the one passed in.
     */
    DatabaseEntry valueDatabaseEntry(DatabaseEntry dbEntry,
                                     byte[] value) {

        if (value.length == 1 && (value[0] == 0 || isTableFormat(value[0]))) {
            return EMPTY_DATA;
        }

        dbEntry.setData(value);
        return dbEntry;
    }

    /**
     * Determine if current request can access system tables.
     */
    static boolean allowAccessSystemTables() {
        final ExecutionContext currentContext = ExecutionContext.getCurrent();
        if (currentContext == null) {
            return true;
        }
        final RequestContext reqContext =
             (RequestContext) currentContext.operationContext();
        final Request request = reqContext.getRequest();

        /* Only check write operation against system table */
        if (!request.isWrite()) {
            return true;
        }

        /* Check if request came from internal nodes */
        return currentContext.hasPrivilege(SystemPrivilege.INTLOPER);
    }

    public static final int MIN_READ = 1024;

    /**
     * Returns the estimated disk storage size for the record at the
     * specified cursor's current position.
     */
    public static int getStorageSize(Cursor cursor) {
        return DbInternal.getCursorImpl(cursor).getStorageSize();
    }

    /**
     * Returns the number of index writes performed when writing the record at
     * the specified cursor. This method assumes the cursor is at the position
     * of the last write.
     */
    static int getNIndexWrites(Cursor cursor) {
        return DbInternal.getCursorImpl(cursor).getNSecondaryWrites();
    }
    
    /**
     * An common interface for those operations need to access table keyspace,
     * which defining the privileges needed.
     */
    interface PrivilegedTableAccessor {
        /**
         * Returns the needed privileges accessing the table specified by the
         * id.
         *
         * @param tableId table id
         * @return a list of required privileges
         */
        List<? extends KVStorePrivilege> tableAccessPrivileges(long tableId);
    }

    /**
     * A class to help identify the keyspace which operation is accessing.
     */
    static class Keyspace {

        /**
         * The encoding of the prefix of the server-private keyspace(///),
         * which is a subset of the "internal" keyspace (//) used by the client
         * to store the Avro schema.
         */
        private static final byte[] PRIVATE_KEY_PREFIX =
            //new byte[] { Key.BINARY_COMP_DELIM, Key.BINARY_COMP_DELIM };
            new byte[] { 0, 0 };

        /**
         * The encoding of the prefix of the Avro schema keyspace(//sch/),
         * which is used by the client to store the Avro schema.
         */
        private static final byte[] AVRO_SCHEMA_KEY_PREFIX =
            new byte[] { 0, 0x73, 0x63, 0x68 }; /* Keybytes of "//sch" */

        static interface KeyAccessChecker {
            boolean allowAccess(byte[] key);
        }

        static final KeyAccessChecker privateKeyAccessChecker =
            new KeyAccessChecker() {
            @Override
            public boolean allowAccess(byte[] key) {
                return !Keyspace.isPrivateAccess(key);
            }
        };

        static final KeyAccessChecker schemaKeyAccessChecker =
            new KeyAccessChecker() {
            @Override
            public boolean allowAccess(byte[] key) {
                return !Keyspace.isSchemaAccess(key);
            }
        };

        enum KeyspaceType { PRIVATE, SCHEMA, GENERAL }

        static KeyspaceType identifyKeyspace(byte[] key) {
            if (key == null || key.length == 0) {
                throw new IllegalArgumentException(
                    "Key bytes may not be null or empty");
            }

            /* Quick return for non-internal keyspace */
            if (key[0] == 0) {
                if (isSchemaAccess(key)) {
                    return KeyspaceType.SCHEMA;
                }
                if (isPrivateAccess(key)) {
                    return KeyspaceType.PRIVATE;
                }
            }
            /* Other cases are regarded as general keyspace */
            return KeyspaceType.GENERAL;
        }

        /**
         * Checks whether the input key has the Avro schema keyspace as a
         * prefix. That is, does the key matches exactly the "//sch" or has the
         * major component of "//sch"?
         */
        static boolean isSchemaAccess(byte[] key) {
            if (key.length < AVRO_SCHEMA_KEY_PREFIX.length) {
                return false;
            }
            for (int i = 0; i < AVRO_SCHEMA_KEY_PREFIX.length; i++) {
                if (key[i] != AVRO_SCHEMA_KEY_PREFIX[i]) {
                    return false;
                }
            }
            /* Key is exactly the "//sch" */
            if (key.length == AVRO_SCHEMA_KEY_PREFIX.length) {
                return true;
            }
            /* Key has "//sch" as its partial or complete major component */
            final byte endingByte = key[AVRO_SCHEMA_KEY_PREFIX.length];
            return endingByte == (byte) 0xff /* Path delimiter */ ||
                   endingByte == (byte) 0x00; /* Component delimiter */
        }

        /**
         * Checks whether the input key is a prefix of the schema key space.
         * That is, does the key reference something that is may be within the
         * schema key space?
         */
        static boolean mayBeSchemaAccess(byte[] key) {
            if (key.length < AVRO_SCHEMA_KEY_PREFIX.length) {
                for (int i = 0; i < key.length; i++) {
                    if (key[i] != AVRO_SCHEMA_KEY_PREFIX[i]) {
                        return false;
                    }
                }
                return true;
            }
            for (int i = 0; i < AVRO_SCHEMA_KEY_PREFIX.length; i++) {
                if (key[i] != AVRO_SCHEMA_KEY_PREFIX[i]) {
                    return false;
                }
            }
            /* Key is exactly the "//sch" */
            if (key.length == AVRO_SCHEMA_KEY_PREFIX.length) {
                return true;
            }
            /* Key has "//sch" as its partial or complete major component */
            final byte endingByte = key[AVRO_SCHEMA_KEY_PREFIX.length];
            return endingByte == (byte) 0xff /* Path delimiter */ ||
                   endingByte == (byte) 0x00; /* Component delimiter */
        }

        /**
         * Checks whether the input key has the server private keyspace as a
         * prefix.  That is, does the key reference something that is
         * definitely within the server private keyspace?
         */
        static boolean isPrivateAccess(byte[] key) {
            if (key.length < PRIVATE_KEY_PREFIX.length) {
                return false;
            }
            for (int i = 0; i < PRIVATE_KEY_PREFIX.length; i++) {
                if (key[i] != PRIVATE_KEY_PREFIX[i]) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Checks whether the input key is a prefix of the server private key
         * space.  That is, does the key reference something that is may be
         * within the server private keyspace?
         */
        static boolean mayBePrivateAccess(byte[] key) {
            if (key.length < PRIVATE_KEY_PREFIX.length) {
                for (byte element : key) {
                    if (element != PRIVATE_KEY_PREFIX.length) {
                        return false;
                    }
                }
                return true;
            }
            for (int i = 0; i < PRIVATE_KEY_PREFIX.length; i++) {
                if (key[i] != PRIVATE_KEY_PREFIX.length) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Checks whether the input key is outside both the server private
         * keyspace and the schema keyspace.
         */
        static boolean isGeneralAccess(byte[] key) {
            return !isPrivateAccess(key) && !isSchemaAccess(key);
        }
    }

    /**
     * A per-key access checker for possible table keyspaces.  Access to a key
     * is allowed if and only if the key falls in a table's keyspace and the
     * current user has access privileges on that table.
     */
    static class TableAccessChecker implements KeyAccessChecker {

        private final PrivilegedTableAccessor tableAccessor;
        final OperationHandler operationHandler;

        /**
         * A set caching the tables which have been verified to be accessible.
         */
        private final Set<Long> accessibleTables = new HashSet<Long>();

        TableAccessChecker(OperationHandler operationHandler,
                           PrivilegedTableAccessor tableAccessor) {
            this.operationHandler = operationHandler;
            this.tableAccessor = tableAccessor;
        }

        @Override
        public boolean allowAccess(byte[] key) {
            if (!Keyspace.isGeneralAccess(key)) {
                return true;
            }

            final TableImpl possibleTable =
                operationHandler.findTableByKeyBytes(key);
            /* Not accessing table, returns false */
            if (possibleTable == null) {
                return false;
            }

            return internalCheckTableAccess(possibleTable);
        }

        boolean internalCheckTableAccess(TableImpl table) {
            final ExecutionContext exeCtx = ExecutionContext.getCurrent();
            if (exeCtx == null) {
                return true;
            }

            if (accessibleTables.contains(table.getId())) {
                return true;
            }

            if (!AccessCheckUtils.currentUserOwnsResource(table)) {
                if (!exeCtx.hasAllPrivileges(
                        tableAccessor.tableAccessPrivileges(table.getId()))) {
                    return false;
                }

                /* Ensure at least read privileges on all parent tables. */
                TableImpl parent = (TableImpl) table.getParent();
                while (parent != null) {
                    final long pTableId = parent.getId();
                    /*
                     * One of the parent table is accessible, exits the loop
                     * and returns true according to induction
                     */
                    if (accessibleTables.contains(pTableId)) {
                        break;
                    }

                    final TablePrivilege parentReadPriv =
                        new TablePrivilege.ReadTable(pTableId);
                    if (!exeCtx.hasPrivilege(parentReadPriv) &&
                        !exeCtx.hasAllPrivileges(
                            tableAccessor.tableAccessPrivileges(pTableId))) {
                        return false;
                    }
                    parent = (TableImpl) parent.getParent();
                }

                if (table.isSystemTable() && !allowAccessSystemTables()) {
                    return false;
                }
            }

            /* Caches the verified table */
            accessibleTables.add(table.getId());
            return true;
        }
    }

    /**
     * A per-key access checker for possible system tables. Client write access
     * to a key is not allowed if the key falls in a system table key space.
     */
    static class SysTableAccessChecker implements KeyAccessChecker {

        final OperationHandler operationHandler;

        SysTableAccessChecker(OperationHandler operationHandler) {
            this.operationHandler = operationHandler;
        }

        @Override
        public boolean allowAccess(byte[] key) {
            if (!Keyspace.isGeneralAccess(key)) {
                return true;
            }

            final ExecutionContext exeCtx = ExecutionContext.getCurrent();
            if (exeCtx == null) {
                return true;
            }

            final TableImpl table = operationHandler.findTableByKeyBytes(key);
            if (table != null && table.isSystemTable() &&
                !allowAccessSystemTables()) {
                return false;
            }
            return true;
        }
    }

    /**
     * A simple class to hold a combination of Version and expiration time for
     * use by put and delete operations when both are needed in a single return
     * value.
     */
    static final class VersionAndExpiration {
        private final Version version;
        private final long expirationTime;

        VersionAndExpiration(final Version version,
                             final OperationResult result) {
            this.version = version;
            this.expirationTime = result.getExpirationTime();
        }

        long getExpirationTime() {
            return expirationTime;
        }

        Version getVersion() {
            return version;
        }
    }
}

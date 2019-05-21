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

import java.util.Collections;
import java.util.List;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

import oracle.kv.FaultException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.InternalOperationHandler.PrivilegedTableAccessor;
import oracle.kv.impl.api.ops.MultiTableOperationHandler.TargetTableAccessChecker;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

/**
 * Base server handler for subclasses of {@link IndexOperation}.
 */
public abstract class IndexOperationHandler<T extends IndexOperation>
        extends InternalOperationHandler<T>
    implements PrivilegedTableAccessor {

    IndexOperationHandler(OperationHandler handler,
                          OpCode opCode,
                          Class<T> operationType) {
        super(handler, opCode, operationType);
    }

    public IndexScanner getIndexScanner(T op,
                                        Transaction txn,
                                        CursorConfig cursorConfig,
                                        LockMode lockMode,
                                        boolean keyOnly,
                                        boolean moveAfterResumeKey) {
        final Table table = getTable(op);
        return new IndexScanner(op, txn,
                                getSecondaryDatabase(op,
                                                     table.getNamespace(),
                                                     table.getFullName()),
                                getIndex(op,
                                         table.getNamespace(),
                                         table.getFullName()),
                                op.getIndexRange(),
                                op.getResumeSecondaryKey(),
                                op.getResumePrimaryKey(),
                                moveAfterResumeKey,
                                cursorConfig,
                                lockMode,
                                keyOnly);
    }

    String getTableName(T op) {
        long id = op.getTargetTables().getTargetTableId();
        Table table = getRepNode().getTable(id);
        if (table == null) {
            throw new MetadataNotFoundException
                ("Cannot access table.  It may not exist, id: " + id,
                 operationHandler.getTableMetadataSeqNum());
        }
        return table.getFullName();
    }

    Table getTable(T op) {
        long id = op.getTargetTables().getTargetTableId();
        Table table = getRepNode().getTable(id);
        if (table == null) {
            throw new MetadataNotFoundException
                ("Cannot access table.  It may not exist, id: " + id,
                 operationHandler.getTableMetadataSeqNum());
        }
        return table;
    }

    IndexImpl getIndex(T op, String namespace, String tableName) {
        final Index index =
            getRepNode().getIndex(namespace, op.getIndexName(), tableName);
        if (index == null) {
            throw new MetadataNotFoundException
                ("Cannot find index " + op.getIndexName() + " in table "
                 + tableName, operationHandler.getTableMetadataSeqNum());
        }

        reserializeOldKeys(op, (IndexImpl) index);

        return (IndexImpl) index;
    }

    /*
     * This method translates, if needed, older binary key format to newer
     * binary key format. It is used to handle the following 2 cases:
     *
     * (a) an IndexOperation is sent from a pre-4.2 client to a 4.4+ server and
     * the index was created by a 4.2+ server. In this case, the binary
     * start/stop index keys that are created by the client do not contain the
     * null or special-value indicator bytes that are normally expected by the
     * server.
     *
     * (b) an IndexOperation is sent from a 4.2 or 4.3 client to a 4.4+ server
     * and the index was created by the 4.4+ server. In this case, the value of
     * NULL_INDICATOR used by the client is different than the one used by the
     * server.
     *
     * To handle (a) and (b), the start/stop keys are deserialized at the server
     * by a version of IndexImpl.deserializeIndexKey() that uses the client
     * version to parse the start/stop keys correctly, and then reserialized to
     * add the indicator bytes that are expected by the server.
     */
    void reserializeOldKeys(T op, IndexImpl index) {

        int indexVersion = index.getIndexVersion();
        short opVersion = op.getOpSerialVersion();

        if (// client is pre-4.2 and index is 4.2+
            (opVersion < SerialVersion.V12 && index.supportsSpecialValues()) ||
            // client is 4.2 or 4.3 and index is 4.4+
            (indexVersion > 0 &&
             (opVersion == SerialVersion.V12 ||
              opVersion == SerialVersion.V13))) {

            op.getIndexRange().reserializeOldKeys(index, opVersion);
        }
    }

    SecondaryDatabase getSecondaryDatabase(T op,
                                           String namespace,
                                           String tableName) {
        final SecondaryDatabase db =
            getRepNode().getIndexDB(namespace,
                                    op.getIndexName(),
                                    tableName);
        if (db == null) {
            throw new MetadataNotFoundException("Cannot find index database: " +
                op.getIndexName() + ", " + tableName,
                operationHandler.getTableMetadataSeqNum());
        }
        return db;
    }

    public void verifyTableAccess(T op)
        throws UnauthorizedException, FaultException {

        if (ExecutionContext.getCurrent() == null) {
            return;
        }

        new TargetTableAccessChecker(operationHandler, this,
                                     op.getTargetTables())
            .checkAccess();
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(T op) {
        /*
         * Checks the basic privilege for authentication here, and leave the
         * the table access checking in {@code verifyTableAccess()}.
         */
        return SystemPrivilege.usrviewPrivList;
    }

    @Override
    public List<? extends KVStorePrivilege>
        tableAccessPrivileges(long tableId) {
        return Collections.singletonList(
            new TablePrivilege.ReadTable(tableId));
    }

    static boolean exceedsMaxReadKB(IndexOperation op) {
        if (op.getMaxReadKB() > 0) {
            return op.getReadKB() > op.getMaxReadKB();
        }
        return false;
    }
}

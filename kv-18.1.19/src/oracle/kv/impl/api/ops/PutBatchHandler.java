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

import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.kv.UnauthorizedException;
import oracle.kv.Version;
import oracle.kv.impl.api.bulk.BulkPut.KVPair;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.api.ops.Result.PutBatchResult;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link PutBatch}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |           0          |           0           |
 * | PutBatch (1)  |   N/A  |---+----------------------+-----------------------|
 * |               |        | A |           0          |    new record size    |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 *      (1) The calculation entry is per input record. The total throughput
 *          is the sum of the result from each input record.
 */
class PutBatchHandler extends MultiKeyOperationHandler<PutBatch> {

    /**
     * Whether the operation has implicit deletion because of its TTL setting.
     * This flag is reset for each operation in a batch, and if true, the
     * operation requires DELETE_TABLE privilege.
     */
    boolean hasValidTTLSetting = false;

    PutBatchHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_BATCH, PutBatch.class);
    }

    @Override
    Result execute(PutBatch op,
                   Transaction txn,
                   PartitionId partitionId)
        throws UnauthorizedException {

        checkTableExists(op);

        final KVAuthorizer kvAuth = checkPermission(op);

        final List<Integer> keysPresent =
            putIfAbsentBatch(op, txn, partitionId, op.getKvPairs(), kvAuth);

        return new PutBatchResult(op.getReadKB(), op.getWriteKB(),
                                  op.getKvPairs().size(), keysPresent);
    }

    private List<Integer> putIfAbsentBatch(PutBatch op,
                                           Transaction txn,
                                           PartitionId partitionId,
                                           List<KVPair> kvPairs,
                                           KVAuthorizer kvAuth) {

        final com.sleepycat.je.WriteOptions noExpiry =
            makeOption(TimeToLive.DO_NOT_EXPIRE, false);

        final List<Integer> keysPresent = new ArrayList<Integer>();

        final Database db = getRepNode().getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        /*
         * To return previous value/version, we have to either position on the
         * existing record and update it, or insert without overwriting.
         */
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        int i = -1;
        byte[] lastKey = null;

        try {
            for (KVPair e : kvPairs) {
                i++;
                if (lastKey != null && Arrays.equals(lastKey, e.getKey())) {
                    keysPresent.add(i);
                    continue;
                }
                lastKey = e.getKey();

                keyEntry.setData(e.getKey());
                /*
                 * The returned entry may be the same one passed in, but if the
                 * entry is empty, it'll be a static, shared value.
                 */
                DatabaseEntry dataEntryToUse =
                    valueDatabaseEntry(dataEntry, e.getValue());

                /*
                 * Check if this put has valid TTL setting. This must be done
                 * before the access check.
                 */
                hasValidTTLSetting = (e.getTTLVal() != 0);

                if (!kvAuth.allowAccess(keyEntry)) {
                    throw new UnauthorizedException("Insufficient access " +
                      "rights granted");
                }

                while (true) {
                    final com.sleepycat.je.WriteOptions jeOptions;
                    int ttlVal = e.getTTLVal();
                    if (ttlVal != 0) {
                        jeOptions = makeJEWriteOptions(ttlVal, e.getTTLUnit());
                    } else {
                        jeOptions = noExpiry;
                    }
                    final OperationResult result =
                            BasicPutHandler.putEntry(cursor, keyEntry,
                                                     dataEntryToUse,
                                                     Put.NO_OVERWRITE,
                                                     jeOptions);
                    if (result != null) {
                        op.addWriteBytes(getStorageSize(cursor),
                                         getNIndexWrites(cursor));
                        final Version v = getVersion(cursor);
                        MigrationStreamHandle.get().
                            addPut(keyEntry, dataEntryToUse,
                                   v.getVLSN(), result.getExpirationTime());
                        break;
                    }
                    /* Key already exists. */
                    keysPresent.add(i);
                    break;
                }
            }
        } finally {
            TxnUtil.close(cursor);
        }

        return keysPresent;
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(PutBatch op) {
        /*
         * Checks the basic privilege for authentication here, and leave the
         * keyspace checking and the table access checking in
         * {@code operationHandler.putIfAbsentBatch()}.
         */
        return SystemPrivilege.usrviewPrivList;
    }

    @Override
    List<? extends KVStorePrivilege> schemaAccessPrivileges() {
        return SystemPrivilege.schemaWritePrivList;
    }

    @Override
    List<? extends KVStorePrivilege> generalAccessPrivileges() {
        return SystemPrivilege.writeOnlyPrivList;
    }

    @Override
    public
    List<? extends KVStorePrivilege> tableAccessPrivileges(long tableId) {
        if (hasValidTTLSetting) {
            return Arrays.asList(new TablePrivilege.InsertTable(tableId),
                                 new TablePrivilege.DeleteTable(tableId));
        }
        return Collections.singletonList(
                   new TablePrivilege.InsertTable(tableId));
    }

    private void checkTableExists(PutBatch op) {
        if (op.getTableIds() != null) {
            for (long id : op.getTableIds()) {
                getAndCheckTable(id);
            }
        }
    }

    private com.sleepycat.je.WriteOptions makeJEWriteOptions(
        int ttlVal, TimeUnit ttlUnit) {

        return new com.sleepycat.je.WriteOptions()
            .setTTL(ttlVal, ttlUnit)
            .setUpdateTTL(false);
    }
}

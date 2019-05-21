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

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.TimeToLive;

/**
 * Server handler for {@link PutIfPresent}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * | PutIfPresent  | VERSION|---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    old record size   |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |           0           |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
class PutIfPresentHandler extends BasicPutHandler<PutIfPresent> {

    PutIfPresentHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_IF_PRESENT, PutIfPresent.class);
    }

    @Override
    Result execute(PutIfPresent op, Transaction txn, PartitionId partitionId) {

        verifyDataAccess(op);

        final ReturnResultValueVersion prevVal =
            new ReturnResultValueVersion(op.getReturnValueVersionChoice());

        final VersionAndExpiration result = putIfPresent(
            op, txn, partitionId, op.getKeyBytes(), op.getValueBytes(), prevVal,
            op.getTTL(), op.getUpdateTTL());

        reserializeResultValue(op, prevVal.getValueVersion());

        return new Result.PutResult(getOpCode(),
                                    op.getReadKB(), op.getWriteKB(),
                                    prevVal.getValueVersion(),
                                    result);
    }

    /**
     * Update a key/value pair.
     */
    private VersionAndExpiration putIfPresent(
        PutIfPresent op,
        Transaction txn,
        PartitionId partitionId,
        byte[] keyBytes,
        byte[] valueBytes,
        ReturnResultValueVersion prevValue,
        TimeToLive ttl,
        boolean updateTTL) {

        assert (keyBytes != null) && (valueBytes != null);

        final Database db = getRepNode().getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = valueDatabaseEntry(valueBytes);

        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        final WriteOptions jeOptions = makeOption(ttl, updateTTL);
        try {
            final DatabaseEntry prevData =
                prevValue.getReturnChoice().needValue() ?
                new DatabaseEntry() :
                NO_DATA;
            OperationResult prevResult =
                cursor.get(keyEntry, prevData,
                           Get.SEARCH, LockMode.RMW.toReadOptions());
            if (prevResult == null) {
                op.addReadBytes(MIN_READ);
                return null;
            }
            getPrevValueVersion(cursor, prevData, prevValue, prevResult);
            op.addWriteBytes(getStorageSize(cursor), 0); /* Old value */
            if (prevValue.getReturnChoice().needValue()) {
                op.addReadBytes(getStorageSize(cursor));
            } else {
                op.addReadBytes(MIN_READ);
            }
            OperationResult result = putEntry(cursor, null, dataEntry,
                                              Put.CURRENT, jeOptions);
            op.addWriteBytes(getStorageSize(cursor),
                             getNIndexWrites(cursor));
            final VersionAndExpiration v =
                new VersionAndExpiration(getVersion(cursor), result);
            MigrationStreamHandle.get().
                addPut(keyEntry, dataEntry,
                       v.getVersion().getVLSN(), result.getExpirationTime());
            return v;
        } finally {
            TxnUtil.close(cursor);
        }
    }
}

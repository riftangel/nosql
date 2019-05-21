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

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link Delete}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |      MIN_READ x 2    |  deleted record size  |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |      MIN_READ x 2    |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |      MIN_READ x 2    |  deleted record size  |
 * | Delete        | VERSION|---+----------------------+-----------------------|
 * |               |        | A |      MIN_READ x 2    |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    deleted record    |  deleted record size  |
 * |               |        |   |      size x 2        |                       |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |      MIN_READ x 2    |           0           |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
class DeleteHandler extends BasicDeleteHandler<Delete> {

    DeleteHandler(OperationHandler handler) {
        super(handler, OpCode.DELETE, Delete.class);
    }

    @Override
    Result execute(Delete op, Transaction txn, PartitionId partitionId) {
        verifyDataAccess(op);

        final ReturnResultValueVersion prevVal =
            new ReturnResultValueVersion(op.getReturnValueVersionChoice());

        final boolean result =
            delete(op, txn, partitionId, op.getKeyBytes(), prevVal);

        reserializeResultValue(op, prevVal.getValueVersion());
        return new Result.DeleteResult(getOpCode(),
                                       op.getReadKB(), op.getWriteKB(),
                                       prevVal.getValueVersion(),
                                       result);
    }

    /**
     * Delete the key/value pair associated with the key.
     */
    private boolean delete(Delete op, Transaction txn,
                           PartitionId partitionId,
                           byte[] keyBytes,
                           ReturnResultValueVersion prevValue) {

        assert (keyBytes != null);

        final Database db = getRepNode().getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);

        /*
         * To return previous value/version, we must first position on the
         * existing record and then delete it.
         */
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        try {
            final DatabaseEntry prevData =
                prevValue.getReturnChoice().needValue() ?
                new DatabaseEntry() :
                NO_DATA;
            final OperationResult result =
                cursor.get(keyEntry, prevData,
                           Get.SEARCH, LockMode.RMW.toReadOptions());
            if (result == null) {
                op.addReadBytes(MIN_READ);
                return false;
            }
            final int recordSize = getStorageSize(cursor);
            if (prevValue.getReturnChoice().needValueOrVersion()) {
                getPrevValueVersion(cursor, prevData, prevValue, result);
                if (prevValue.getReturnChoice().needValue()) {
                    op.addReadBytes(recordSize);
                } else {
                    op.addReadBytes(MIN_READ);
                }
            } else {
                op.addReadBytes(MIN_READ);
            }
            cursor.delete(null);
            op.addWriteBytes(recordSize, getNIndexWrites(cursor));
            MigrationStreamHandle.get().addDelete(keyEntry, cursor);
            return true;
        } finally {
            TxnUtil.close(cursor);
        }
    }
}

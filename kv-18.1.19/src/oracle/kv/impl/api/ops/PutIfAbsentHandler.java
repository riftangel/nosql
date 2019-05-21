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

import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.TimeToLive;

/**
 * Server handler for {@link PutIfAbsent}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |           0           |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |    new record size    |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |           0           |
 * | PutIfAbsent   | VERSION|---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |    new record size    |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    old record size   |           0           |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |    new record size    |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
class PutIfAbsentHandler extends BasicPutHandler<PutIfAbsent> {

    PutIfAbsentHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_IF_ABSENT, PutIfAbsent.class);
    }

    @Override
    Result execute(PutIfAbsent op, Transaction txn, PartitionId partitionId) {

        verifyDataAccess(op);

        final ReturnResultValueVersion prevVal =
            new ReturnResultValueVersion(op.getReturnValueVersionChoice());

        final VersionAndExpiration result = putIfAbsent(
            op, txn, partitionId, op.getKeyBytes(), op.getValueBytes(), prevVal,
            op.getTTL(), op.getUpdateTTL());

        reserializeResultValue(op, prevVal.getValueVersion());

        return new Result.PutResult(getOpCode(),
                                    op.getReadKB(), op.getWriteKB(),
                                    prevVal.getValueVersion(),
                                    result);
    }

    /**
     * Insert a key/value pair.
     */
    private VersionAndExpiration putIfAbsent(PutIfAbsent op,
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
        /*
         * To return previous value/version, we have to either position on the
         * existing record and update it, or insert without overwriting.
         */
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        try {
            WriteOptions jeOptions = makeOption(ttl, updateTTL);
            while (true) {
                OperationResult result = putEntry(cursor, keyEntry, dataEntry,
                        Put.NO_OVERWRITE, jeOptions);
                if (result != null) {
                    op.addReadBytes(MIN_READ);
                    op.addWriteBytes(getStorageSize(cursor),
                                     getNIndexWrites(cursor));
                    final VersionAndExpiration v =
                        new VersionAndExpiration(getVersion(cursor), result);
                    MigrationStreamHandle.get().
                        addPut(keyEntry, dataEntry,
                               v.getVersion().getVLSN(),
                               result.getExpirationTime());
                    return v;
                }
                /* Simple case: previous version and value are not returned. */
                if (prevValue.getReturnChoice() == Choice.NONE) {
                    op.addReadBytes(MIN_READ);
                    return null;
                }
                /* Get and return previous value/version. */
                final DatabaseEntry prevData =
                    prevValue.getReturnChoice().needValue() ?
                    new DatabaseEntry() :
                    NO_DATA;
                result = cursor.get(keyEntry,
                                    prevData,
                                    Get.SEARCH,
                                    LockMode.DEFAULT.toReadOptions());
                if (result != null) {
                    getPrevValueVersion(cursor, prevData, prevValue, result);
                    if (prevValue.getReturnChoice().needValue()) {
                        op.addReadBytes(getStorageSize(cursor));
                    } else {
                        op.addReadBytes(MIN_READ);
                    }
                    return null;
                }
                /* Another thread deleted the record.  Continue. */
            }
        } finally {
            TxnUtil.close(cursor);
        }
    }
}

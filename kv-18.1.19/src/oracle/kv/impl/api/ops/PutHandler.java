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

import static com.sleepycat.je.Put.CURRENT;
import static com.sleepycat.je.Put.NO_OVERWRITE;
import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;

import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.TimeToLive;

/**
 * Server handler for {@link Put}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |           0          |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |           0          |    new record size    |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * | Put           | VERSION|---+----------------------+-----------------------|
 * |               |        | A |           0          |    new record size    |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    old record size   |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |           0          |    new record size    |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
public class PutHandler extends BasicPutHandler<Put> {

    public PutHandler(OperationHandler handler) {
        super(handler, OpCode.PUT, Put.class);
    }

    @Override
    public Result execute(Put op, Transaction txn, PartitionId partitionId) {

        verifyDataAccess(op);

        final ReturnResultValueVersion prevVal =
            new ReturnResultValueVersion(op.getReturnValueVersionChoice());

        final VersionAndExpiration result = put(
            op, txn, partitionId, op.getKeyBytes(), op.getValueBytes(), prevVal,
            op.getTTL(), op.getUpdateTTL());

        reserializeResultValue(op, prevVal.getValueVersion());

        return new Result.PutResult(getOpCode(),
                                    op.getReadKB(), op.getWriteKB(),
                                    prevVal.getValueVersion(),
                                    result);
    }

    /**
     * Put a key/value pair. If the key exists, the associated value is
     * overwritten.
     */
    private VersionAndExpiration put(Put op, Transaction txn,
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

        final com.sleepycat.je.WriteOptions jeOptions =
                makeOption(ttl, updateTTL);

        /*
         * To return previous value/version, we have to either position on the
         * existing record and update it, or insert without overwriting.
         */
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        try {
            while (true) {
                OperationResult result = putEntry(cursor, keyEntry, dataEntry,
                                                  NO_OVERWRITE, jeOptions);
                if (result != null) {
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
                final Choice choice = prevValue.getReturnChoice();
                final DatabaseEntry prevData =
                            choice.needValue() ? new DatabaseEntry() : NO_DATA;
                result = cursor.get(keyEntry, prevData,
                                    Get.SEARCH, LockMode.RMW.toReadOptions());
                if (result != null) {
                    getPrevValueVersion(cursor, prevData, prevValue, result);
                    final int oldRecordSize = getStorageSize(cursor);
                    if (choice.needValue()) {
                        op.addReadBytes(oldRecordSize);
                    } else if (choice.needVersion()) {
                        op.addReadBytes(MIN_READ);
                    }
                    result = putEntry(cursor, null, dataEntry, CURRENT,
                                      jeOptions);
                    if (result.isUpdate()) {
                        /* Delete of the old */
                        op.addWriteBytes(oldRecordSize, 0);
                    }
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
                /* Another thread deleted the record.  Continue. */
            }
        } finally {
            TxnUtil.close(cursor);
        }
    }
}

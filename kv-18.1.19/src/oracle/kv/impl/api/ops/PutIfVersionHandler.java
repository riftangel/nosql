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

import oracle.kv.Version;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.TimeToLive;

/**
 * Server handler for {@link PutIfVersion}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | Y |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | N |        MIN_READ      |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | Y |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * | PutIfVersion  | VERSION|---+----------------------+-----------------------|
 * |               |        | N |        MIN_READ      |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | Y |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | N |    old record size   |           0           |
 * +---------------------------------------------------------------------------+
 *      # = Target record matches (Y) or not (N)
 */
class PutIfVersionHandler extends BasicPutHandler<PutIfVersion> {

    PutIfVersionHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_IF_VERSION, PutIfVersion.class);
    }

    @Override
    Result execute(PutIfVersion op, Transaction txn, PartitionId partitionId) {

        verifyDataAccess(op);

        final ReturnResultValueVersion prevVal =
            new ReturnResultValueVersion(op.getReturnValueVersionChoice());

        final VersionAndExpiration result = putIfVersion(
            op, txn, partitionId, op.getKeyBytes(), op.getValueBytes(),
            op.getMatchVersion(), prevVal, op.getTTL(), op.getUpdateTTL());

        reserializeResultValue(op, prevVal.getValueVersion());

        return new Result.PutResult(getOpCode(),
                                    op.getReadKB(), op.getWriteKB(),
                                    prevVal.getValueVersion(),
                                    result);
    }

    /**
     * Update a key/value pair, if the existing record has the given version.
     */
    private VersionAndExpiration putIfVersion(
        PutIfVersion op,
        Transaction txn,
        PartitionId partitionId,
        byte[] keyBytes,
        byte[] valueBytes,
        Version matchVersion,
        ReturnResultValueVersion prevValue,
        TimeToLive ttl,
        boolean updateTTL) {

        assert (keyBytes != null) && (valueBytes != null) &&
            (matchVersion != null);

        final Database db = getRepNode().getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = valueDatabaseEntry(valueBytes);
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        try {
            OperationResult result =
                    cursor.get(keyEntry, NO_DATA,
                               Get.SEARCH, LockMode.RMW.toReadOptions());
            if (result == null) {
                op.addReadBytes(MIN_READ);
                /* Not present, return null. */
                return null;
            }
            if (versionMatches(cursor, matchVersion)) {
                op.addReadBytes(MIN_READ);
                op.addWriteBytes(getStorageSize(cursor), 0); /* Old value */
                WriteOptions options = makeOption(ttl, updateTTL);
                /* Version matches, update and return new version. */
                result = putEntry(cursor, null, dataEntry, Put.CURRENT,
                                  options);
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
            /* No match, get previous value/version and return null. */
            final DatabaseEntry prevData;
            if (prevValue.getReturnChoice().needValue()) {
                prevData = new DatabaseEntry();
                result = cursor.get(keyEntry,
                                    prevData,
                                    Get.CURRENT,
                                    LockMode.RMW.toReadOptions());
                op.addReadBytes(getStorageSize(cursor));
            } else {
                prevData = NO_DATA;
                op.addReadBytes(MIN_READ);
            }
            getPrevValueVersion(cursor, prevData, prevValue, result);
            return null;
        } finally {
            TxnUtil.close(cursor);
        }
    }
}

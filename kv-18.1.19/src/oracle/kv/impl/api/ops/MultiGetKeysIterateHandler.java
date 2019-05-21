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

import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_READ_COMMITTED;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link MultiGetKeysIterate}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * | MultiGetKeysI.|  N/A   | - | sum of record sizes  |           0           |
 * +---------------------------------------------------------------------------+
 */
class MultiGetKeysIterateHandler
        extends MultiKeyIterateHandler<MultiGetKeysIterate> {

    MultiGetKeysIterateHandler(OperationHandler handler) {
        super(handler, OpCode.MULTI_GET_KEYS_ITERATE,
              MultiGetKeysIterate.class);
    }

    @Override
    Result execute(MultiGetKeysIterate op,
                   Transaction txn,
                   PartitionId partitionId) {

        final KVAuthorizer kvAuth = checkPermission(op);

        final List<ResultKey> results = new ArrayList<ResultKey>();

        final boolean moreElements = iterateKeys(op,
            txn, partitionId, op.getParentKey(), true /*majorPathComplete*/,
            op.getSubRange(), op.getDepth(), op.getDirection(),
            op.getBatchSize(), op.getResumeKey(), CURSOR_READ_COMMITTED,
            results, kvAuth);

        return new Result.KeysIterateResult(getOpCode(),
                                            op.getReadKB(), op.getWriteKB(),
                                            results, moreElements);
    }
}

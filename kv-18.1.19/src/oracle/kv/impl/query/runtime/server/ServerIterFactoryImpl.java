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

package oracle.kv.impl.query.runtime.server;

import com.sleepycat.je.Transaction;

import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.query.runtime.BaseTableIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.query.runtime.ServerIterFactory;
import oracle.kv.impl.query.runtime.UpdateRowIter;
import oracle.kv.impl.topo.PartitionId;

/**
 * An instance of ServerIterFactoryImpl is created during
 * TableQueryHandler.execute() and is stored in the RCB.
 */
public class ServerIterFactoryImpl implements ServerIterFactory {

    private final Transaction theTxn;

    private final PartitionId thePartitionId;

    private final OperationHandler theOperationHandler;

    public ServerIterFactoryImpl(
        Transaction txn,
        PartitionId partitionId,
        OperationHandler operationHandler) {

        theTxn = txn;
        thePartitionId = partitionId;
        theOperationHandler = operationHandler;
    }

    Transaction getTxn() {
        return theTxn;
    }

    PartitionId getPartitionId() {
        return thePartitionId;
    }

    OperationHandler getOperationHandler() {
        return theOperationHandler;
    }

    @Override
    public PlanIter createTableIter(
        RuntimeControlBlock rcb,
        BaseTableIter parent) {

        return new ServerTableIter(rcb, this, parent);
    }

    @Override
    public PlanIter createUpdateRowIter(UpdateRowIter parent) {
        return new ServerUpdateRowIter(parent);
    }
}

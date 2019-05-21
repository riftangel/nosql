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
package oracle.kv.impl.admin.plan.task;

import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.persist.model.Persistent;

/**
 * Start a repNode. Assumes the node has already been created.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Deprecated
@Persistent(version=1)
public class StartRepNode extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private StorageNodeId snId;
    private RepNodeId repNodeId;
    private boolean continuePastError;

    public StartRepNode(AbstractPlan plan,
                        StorageNodeId storageNodeId,
                        RepNodeId repNodeId,
                        boolean continuePastError) {
        super();
        this.plan = plan;
        this.snId = storageNodeId;
        this.repNodeId = repNodeId;
        this.continuePastError = continuePastError;
    }

    /* DPL */
    protected StartRepNode() {
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        Utils.startRN(plan, snId, repNodeId);
        return State.SUCCEEDED;
    }


    @Override
    public boolean continuePastError() {
        return continuePastError;
    }

    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(), repNodeId);
    }
}

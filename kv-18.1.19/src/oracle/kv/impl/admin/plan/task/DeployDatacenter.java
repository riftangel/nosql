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

import oracle.kv.impl.admin.plan.DeployDatacenterPlan;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for creating a Datacenter and registering it with the Topology.
 */
@Persistent(version=1)
public class DeployDatacenter extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DeployDatacenterPlan plan;

    public DeployDatacenter(DeployDatacenterPlan plan) {
        super();
        this.plan = plan;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private DeployDatacenter() {
    }

    @Override
    protected DeployDatacenterPlan getPlan() {
        return plan;
    }

    /**
     * Failure and interruption statuses are set by the PlanExecutor, to
     * generalize handling or exception cases.
     */
    @Override
    public State doWork()
        throws Exception {

        /* Nothing to do for now. */
        return State.SUCCEEDED;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return super.getName(sb).append(" zone=")
                                .append(plan.getDatacenterName());
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}

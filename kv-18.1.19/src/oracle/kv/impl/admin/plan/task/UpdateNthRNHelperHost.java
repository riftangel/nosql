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

import java.util.List;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a RepNode to update its helper hosts to include all its
 * peers.Because the topology is not written until task execution time, this
 * flavor of UpdateNthRNHelperHost must wait until task run time to know the
 * actual RepGroupId and RepNodeId to use.
 */
@Persistent
public class UpdateNthRNHelperHost extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DeployTopoPlan plan;

    private int planShardIdx;

    private int nthRN;

    public UpdateNthRNHelperHost(DeployTopoPlan plan,
                                 int planShardIdx,
                                 int nthRN) {

        super();
        this.plan = plan;
        this.planShardIdx = planShardIdx;
        this.nthRN = nthRN;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private UpdateNthRNHelperHost() {
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final Topology topo = admin.getCurrentTopology();
        final RepGroupId rgId = plan.getShardId(planShardIdx);
        final List<RepNodeId> rnList = topo.getSortedRepNodeIds(rgId);
        final RepNodeId rnId = rnList.get(nthRN);

        Utils.updateHelperHost(admin, topo, topo.get(rgId),
                               rnId, plan.getLogger());
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}

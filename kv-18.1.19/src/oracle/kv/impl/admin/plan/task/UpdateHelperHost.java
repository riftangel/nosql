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

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a RepNode to update its helper hosts to include all its
 * peers.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Deprecated
@Persistent(version=1)
public class UpdateHelperHost extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private RepNodeId rnId;
    private RepGroupId rgId;

    public UpdateHelperHost(AbstractPlan plan,
                            RepNodeId rnId,
                            RepGroupId rgId) {

        super();
        this.plan = plan;
        this.rnId = rnId;
        this.rgId = rgId;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private UpdateHelperHost() {
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {
        final Topology topo = plan.getAdmin().getCurrentTopology();
        Utils.updateHelperHost(plan.getAdmin(),
                               topo,
                               topo.get(rgId),
                               rnId,
                               plan.getLogger());
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}

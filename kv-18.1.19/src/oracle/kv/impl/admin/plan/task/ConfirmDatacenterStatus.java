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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Check if a given datacenter is empty.
 */
@Persistent
public class ConfirmDatacenterStatus extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DatacenterId targetId;
    private TopologyPlan plan;
    private String infoMsg;

    public ConfirmDatacenterStatus(TopologyPlan plan,
                                   DatacenterId targetId,
                                   String infoMsg) {
        super();
        this.targetId = targetId;
        this.plan = plan;
        this.infoMsg = infoMsg;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private ConfirmDatacenterStatus() {
    }

    @Override
    protected TopologyPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork() throws Exception {

        /* Determine if the target datacenter is empty. */
        final Topology topo = plan.getTopology();

        for (StorageNode sn: topo.getSortedStorageNodes()) {
            if (targetId.equals(sn.getDatacenterId())) {
                throw new IllegalCommandException(infoMsg);
            }
        }
        return Task.State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}

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
import java.util.logging.Level;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Send a simple newParameters call to the RepNodeAdminAPI to refresh its
 * parameters without a restart. Because the topology is not written until task
 * execution time, this flavor of NewRepNodeParameters must wait until task run
 * time to know the actual RepNodeId to use.
 */
public class NewNthANParameters extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final int nthAN;
    private final int planShardIdx;
    private final DeployTopoPlan plan;

    /**
     * We don't have an actual RepGroupId and ArbNodeId to use at construction
     * time. Look those ids up in the plan at run time.
     */
    public NewNthANParameters(DeployTopoPlan plan,
                              int planShardIdx,
                              int nthAN) {
        this.plan = plan;
        this.planShardIdx = planShardIdx;
        this.nthAN = nthAN;
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Topology topo = plan.getAdmin().getCurrentTopology();
        final RepGroupId rgId = plan.getShardId(planShardIdx);
        final List<ArbNodeId> anList = topo.getSortedArbNodeIds(rgId);
        final ArbNodeId targetANId = anList.get(nthAN);
        final StorageNodeId hostSNId = topo.get(targetANId).getStorageNodeId();
        plan.getLogger().log(Level.FINE, "{0} sending newParameters", this);

        final GlobalParams gp = plan.getAdmin().getParams().getGlobalParams();
        final StorageNodeParams snp =
                plan.getAdmin().getStorageNodeParams(hostSNId);
        final ArbNodeAdminAPI anai =
            RegistryUtils.getArbNodeAdmin(gp.getKVStoreName(),
                                          snp.getHostname(),
                                          snp.getRegistryPort(),
                                          targetANId,
                                          plan.getLoginManager());

        anai.newParameters();
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}

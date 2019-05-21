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

import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.ChangeAdminParamsPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a storage node to write a new configuration file to
 * include new AdminParams..
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class WriteNewAdminParams extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    private ChangeAdminParamsPlan plan;
    private ParameterMap newParams;
    private StorageNodeId targetSNId;
    private AdminId aid;

    WriteNewAdminParams() {
    }

    public WriteNewAdminParams(ChangeAdminParamsPlan plan,
                               ParameterMap newParams,
                               AdminId aid,
                               StorageNodeId targetSNId) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.aid = aid;
        this.targetSNId = targetSNId;
    }

    @Override
    protected ChangeAdminParamsPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        /*
         * Merge and store the changed rep node params in the admin db before
         * sending them to the SNA.
         */
        final Admin admin = plan.getAdmin();
        final Parameters parameters = admin.getCurrentParameters();
        final AdminParams current = parameters.get(aid);
        final ParameterMap currentMap = current.getMap();
        final ParameterMap diff = currentMap.diff(newParams, true);

        /*
         * Exit early if the parameters have not changed.
         *
         * Note that this check may result in the parameters being updated in
         * the Admin database but not on the SN/Admin. This can happen if the
         * plan fails after the admin.updateParams(). See SR #25621 for why
         * this is not a straightforward fix.
         */
        if (currentMap.merge(newParams, true) == 0) {
            plan.getLogger().log(Level.INFO,
                                 "{0} no difference in Admin parameters", this);
            return State.SUCCEEDED;
        }

        final StorageNodeAgentAPI sna =
                RegistryUtils.getStorageNodeAgent(parameters, targetSNId,
                                                  admin.getLoginManager());
        try {
            /* Check the parameters prior to writing them to the DB. */
            sna.checkParameters(currentMap, aid);
        } catch (UnsupportedOperationException ignore) {
            /*
             * If UOE, the SN is not yet upgraded to a version that
             * supports this check, so just ignore
             */
        }
        admin.updateParams(current);

        plan.getLogger().log(Level.INFO, "{0} changing params for {1}: {2}",
                             new Object[]{this, aid, diff});

        /* Ask the SNA to write a new configuration file. */
        sna.newAdminParameters(currentMap);

        /* Tell subsequent tasks that we did indeed change the parameters. */
        plan.getNeedsActionSet().add(aid);

        return State.SUCCEEDED;
    }


    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(), targetSNId);
        planner.lock(plan.getId(), plan.getName(), aid);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(aid);
    }
}

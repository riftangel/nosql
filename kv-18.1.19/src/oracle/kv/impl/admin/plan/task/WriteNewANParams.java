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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for asking a storage node to write a new configuration file.
 */
public class WriteNewANParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final ParameterMap newParams;
    private final StorageNodeId targetSNId;
    private final ArbNodeId anid;
    private final boolean continuePastError;
    private transient boolean currentContinuePastError;

    public WriteNewANParams(AbstractPlan plan,
                            ParameterMap newParams,
                            ArbNodeId anid,
                            StorageNodeId targetSNId,
                            boolean continuePastError) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.anid = anid;
        this.targetSNId = targetSNId;
        this.continuePastError = continuePastError;
        currentContinuePastError = continuePastError;
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
        currentContinuePastError = false;
        final Admin admin = plan.getAdmin();
        final ArbNodeParams anp = admin.getArbNodeParams(anid);
        final ParameterMap anMap = anp.getMap();
        final ArbNodeParams newAnp = new ArbNodeParams(newParams);
        newAnp.setArbNodeId(anid);
        final ParameterMap diff = anMap.diff(newParams, true);
        if (diff.isEmpty()) {
            return State.SUCCEEDED;
        }
        plan.getLogger().log(Level.INFO,
                            "{0} changing params for {1}: {2}",
                            new Object[]{plan, anid, diff});

        /*
         * Merge and store the changed rep node params in the admin db before
         * sending them to the SNA.
         */
        anMap.merge(newParams, true);
        admin.updateParams(anp);
        currentContinuePastError = continuePastError;

        /* Ask the SNA to write a new configuration file. */
        final Topology topology = admin.getCurrentTopology();
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, loginMgr);

        try {
            StorageNodeAgentAPI sna =
                registryUtils.getStorageNodeAgent(targetSNId);
            sna.newArbNodeParameters(anMap);
        } catch (RemoteException | NotBoundException e) {
            plan.getLogger().log(Level.WARNING,
                                 "Unable change parameters for {0}. " +
                                 "Could not access {1} due to exception {2}.",
                                 new Object[]{anid, targetSNId, e});
            if (!currentContinuePastError) {
                throw e;
            }
        }

        return State.SUCCEEDED;
    }

    /* Lock the target SN and AN */
    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(), targetSNId);
        planner.lock(plan.getId(), plan.getName(), anid);
    }

    @Override
    public boolean continuePastError() {
        return currentContinuePastError;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(anid);
    }
}

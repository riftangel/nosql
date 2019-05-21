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

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a storage node to write a new configuration file.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class WriteNewParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private ParameterMap newParams;
    private StorageNodeId targetSNId;
    private RepNodeId rnid;
    private boolean continuePastError;
    private transient boolean currentContinuePastError;

    public WriteNewParams(AbstractPlan plan,
                          ParameterMap newParams,
                          RepNodeId rnid,
                          StorageNodeId targetSNId,
                          boolean continuePastError) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.rnid = rnid;
        this.targetSNId = targetSNId;
        this.continuePastError = continuePastError;
        this.currentContinuePastError = continuePastError;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private WriteNewParams() {
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
        ParameterMap rnMap = writeToDB(plan, newParams, rnid, targetSNId);
        currentContinuePastError = continuePastError;
        if (rnMap == null) {
            return State.SUCCEEDED;
        }
        try {
            writeConfig(plan.getAdmin(), targetSNId, rnMap);
        } catch (RemoteException | NotBoundException e) {
            if (currentContinuePastError) {
                plan.getLogger().log(Level.WARNING, "Unable to access {0} " +
                                     "to change parameters for " +
                                     "{1} due to exception {2}",
                                     new Object[]{targetSNId, rnid, e});
            } else {
                throw e;
            }
        }
        return State.SUCCEEDED;
    }

    public static boolean writeNewParams(AbstractPlan plan,
                                         ParameterMap newParams,
                                         RepNodeId rnid,
                                         StorageNodeId targetSNId)
        throws Exception {
        ParameterMap rnMap =
            writeToDB(plan, newParams, rnid, targetSNId);
        if (rnMap == null) {
            return false;
        }
        writeConfig(plan.getAdmin(),targetSNId, rnMap);
        return true;
    }

    private static ParameterMap writeToDB(AbstractPlan plan,
                                          ParameterMap newParams,
                                          RepNodeId rnid,
                                          StorageNodeId targetSNId)
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();
        final RepNodeParams rnp = admin.getRepNodeParams(rnid);
        final ParameterMap rnMap = rnp.getMap();
        final RepNodeParams newRnp = new RepNodeParams(newParams);
        newRnp.setRepNodeId(rnid);
        final ParameterMap diff = rnMap.diff(newParams, true);
        plan.getLogger().log(Level.INFO, "{0} changing params for {1}: {2}",
                             new Object[]{plan, rnid, diff});

        /*
         * Merge and store the changed rep node params in the admin db before
         * sending them to the SNA.
         */
        if (rnMap.merge(newParams, true) <= 0) {
            return null;
        }
        /* Check the parameters prior to writing them to the DB. */
        ParameterMap snMap =
            admin.getStorageNodeParams(targetSNId).getMap();
        String dbVersion =
           snMap.get(ParameterState.SN_SOFTWARE_VERSION).asString();
        KVVersion snVersion =
            dbVersion == null ? null : KVVersion.parseVersion(dbVersion);
        if (snVersion != null &&
            snVersion.compareTo(KVVersion.CURRENT_VERSION) == 0 &&
            !StorageNodeAgent.isFileSystemCheckRequired(rnMap)) {
            StorageNodeAgent.checkSNParams(rnMap,
                                           admin.getGlobalParams().getMap());
        } else {
            RegistryUtils registryUtils =
                new RegistryUtils(admin.getCurrentTopology(),
                                  admin.getLoginManager());
            StorageNodeAgentAPI sna =
                registryUtils.getStorageNodeAgent(targetSNId);
            try {
                sna.checkParameters(rnMap, rnid);
            } catch (UnsupportedOperationException ignore) {
                /*
                 * If UOE, the SN is not yet upgraded to a version that
                 * supports this check, so just ignore
                */
            }
        }
        admin.updateParams(rnp);
        return rnMap;
    }

    private static void writeConfig(Admin admin,
                                    StorageNodeId targetSNId,
                                    ParameterMap rnMap)
        throws RemoteException, NotBoundException {

        RegistryUtils registryUtils =
            new RegistryUtils(admin.getCurrentTopology(),
                              admin.getLoginManager());
        StorageNodeAgentAPI sna =
            registryUtils.getStorageNodeAgent(targetSNId);
        /* Ask the SNA to write a new configuration file. */

        sna.newRepNodeParameters(rnMap);
    }

    /* Lock the target SN and RN */
    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(), targetSNId);
        planner.lock(plan.getId(), plan.getName(), rnid);
    }

    @Override
    public boolean continuePastError() {
        return currentContinuePastError;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(rnid);
    }
}

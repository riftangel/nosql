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

package oracle.kv.impl.admin;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

class SoftwareVersionUpdater implements ParameterListener  {

    private static final int MAX_PLAN_WAIT_MS = 5 * 60 * 1000;
    private static final int MIN_THREADS = 1;
    private static final String THREAD_NAME = "SoftwareVersionUpdater";
    private static final String UPDATE_VERSION_PLAN_NAME =
        "UpdateVersionMetadata";
    private static final String UPDATE_GLOBAL_VERSION_PLAN_NAME =
        "UpdateGlobalVersionMetadata";
    private final Admin admin;
    private final Logger logger;
    private final AtomicBoolean isShutdown;
    private final ScheduledThreadPoolExecutor executors;
    private final Map<String, AgentInfo> agents;
    private long pollIntervalMS;
    private KVVersion storeVersion = null;

    /**
     * A thread that periodically checks the SNs software version.
     * If needed, the value is updated in the admin database as a
     * parameter for the SN. May also update the store version which
     * is a global parameter.
     */
    SoftwareVersionUpdater(Admin admin,
                           long pollIntervalMS,
                           Logger logger) {
        String threadName =
            admin.getParams().getAdminParams().getAdminId() + "_" + THREAD_NAME;
        this.admin = admin;
        this.logger = logger;
        this.pollIntervalMS = pollIntervalMS;
        isShutdown = new AtomicBoolean(false);
        agents = new ConcurrentHashMap<String, AgentInfo>();
        executors =
            new ScheduledThreadPoolExecutor
                (MIN_THREADS, new KVThreadFactory(threadName, logger));

        AgentInfo agentInfo = new AgentInfo(admin.toString());
        setupFuture(agentInfo,  pollIntervalMS);
        agents.put(admin.toString(), agentInfo);
    }

    void shutdown() {
        logger.fine("Shutting down " + THREAD_NAME);
        isShutdown.set(true);
        unregisterAgent(admin.toString());
        executors.shutdown();

        /*
         * Best effort to shutdown. If the await returns false, we proceed
         * anyway.
         */
        try {
            executors.awaitTermination(1000,
                                       TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.info (THREAD_NAME + " interrupted during shutdown: " +
                         LoggerUtils.getStackTrace(e));
        }
    }

    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        DurationParameter pi =
            (DurationParameter)newMap.getOrDefault(
                ParameterState.AP_VERSION_CHECK_INTERVAL);
        if (pi.toMillis() != pollIntervalMS) {
            pollIntervalMS = pi.toMillis() ;
            resetAgents(pollIntervalMS);
        }
    }

    private void process() {
        Integer planId = null;
        HashMap<StorageNodeId, String> updates = findUpdates();

        if (updates.isEmpty()) {
            return;
        }
        if (!updates.isEmpty()) {
            logger.fine(THREAD_NAME + "Updating software version.");
            planId =
                admin.getPlanner().createUpdateSoftwareVersionPlan(
                    UPDATE_VERSION_PLAN_NAME, updates);
            try {
                admin.approvePlan(planId);
                admin.executePlan(planId, false);
                admin.awaitPlan(planId,
                                MAX_PLAN_WAIT_MS,  TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.log(Level.WARNING,
                           THREAD_NAME + "Encountered exception " +
                           "running update SN version plan.",
                           e);
                admin.cancelPlan(planId);
                throw e;
            }
        }

        /*
         * Check if we need to update the kvstore version.
         */
        if (storeVersion == null) {
            return;
        }

        final ParameterMap pm =
            new ParameterMap(ParameterState.GLOBAL_TYPE,
                             ParameterState.GLOBAL_TYPE);
        pm.setParameter(ParameterState.GP_STORE_VERSION,
                        storeVersion.getNumericVersionString());
        planId =
            admin.getPlanner().createChangeGlobalComponentsParamsPlan(
                UPDATE_GLOBAL_VERSION_PLAN_NAME, pm, true);
        try {
            admin.approvePlan(planId);
            admin.executePlan(planId, false);
            admin.awaitPlan(planId,
                            MAX_PLAN_WAIT_MS,  TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "THREAD_NAME " + "Encountered exception " +
                       "running update store version plan.",
                       e);
            admin.cancelPlan(planId);
            throw e;
        }
    }

    /**
     * Returns set of SN's and corresponding versions and may
     * set the storeVersion member if the information is available, otherwise
     * the storeVersion member is set to null.
     */
    private HashMap<StorageNodeId, String> findUpdates() {
        final HashMap<StorageNodeId, String> retVal =
            new HashMap<StorageNodeId, String>();
        final Topology topo = admin.getCurrentTopology();
        final Parameters params = admin.getCurrentParameters();
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, loginMgr);

        ParameterMap gp = params.getGlobalParams().getMap();
        storeVersion = null;

        KVVersion minVersion = null;
        boolean gotAll = true;

        for (StorageNodeParams tSNp : params.getStorageNodeParams()) {
            KVVersion snVersion = null;
            StorageNodeId snId = tSNp.getStorageNodeId();
            ParameterMap snParams = tSNp.getMap();
            KVVersion dbVersion =
                getVersion(
                snParams.get(ParameterState.SN_SOFTWARE_VERSION).asString());

            try {
                StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
                StorageNodeStatus sns = sna.ping();
                snVersion = sns.getKVVersion();

                if (!snVersion.equals(dbVersion)) {
                    retVal.put(snId, snVersion.getNumericVersionString());
                }
            } catch (NotBoundException | RemoteException e) {
            }
            if (snVersion == null && dbVersion == null) {
                gotAll = false;
            } else {
                KVVersion tMin = getMin(snVersion, dbVersion);
                if (minVersion == null) {
                    minVersion = tMin;
                } else if (minVersion.compareTo(tMin) > 0) {
                    minVersion = tMin;
                }
            }
        }
        if (gotAll) {
            KVVersion dbStoreVersion =
                getVersion(gp.get(ParameterState.GP_STORE_VERSION).asString());
            if (dbStoreVersion == null ||
                dbStoreVersion.compareTo(minVersion) < 0) {
                storeVersion = minVersion;
            }
        }
        return retVal;
    }

    private void setupFuture(AgentInfo info, long pollMillis) {
        Runnable pollTask = new PollTask(info);
        Future<?> future =
            executors.scheduleAtFixedRate(pollTask,
                                          pollMillis, // initial delay
                                          pollMillis,
                                          TimeUnit.MILLISECONDS);
        info.setFuture(future);
    }

    private synchronized void unregisterAgent(String name) {
        AgentInfo info = agents.remove(name);
        if (info == null) {
            /* Nothing to do. */
            return;
        }

        if (info.future == null) {
            return;
        }

        logger.fine("Removing " + name + " from executing");
        info.future.cancel(false);
    }

    private synchronized void resetAgents(long pollMillis) {
        logger.info
            (THREAD_NAME + ": resetting interval to: " + pollMillis +
             " milliseconds (" + agents.size() + " agents)");
        for (final String key : new ArrayList<>(agents.keySet())) {
            final AgentInfo info = agents.remove(key);
            if (info.future != null) {
                info.future.cancel(false);
            }
            setupFuture(info, pollMillis);
            agents.put(key, info);
        }
    }

    private KVVersion getVersion(String versionString) {
        KVVersion retVal = null;
        try {
            retVal = KVVersion.parseVersion(versionString);
        } catch (Exception e) {

        }
        return retVal;
    }

    private KVVersion getMin(KVVersion v1, KVVersion v2) {
        if (v1 == null ) {
            return v2;
        }
        if (v2 == null) {
            return v1;
        }
        if (v1.compareTo(v2) < 0) {
            return v1;
        }
        return v2;
    }

    private class PollTask implements Runnable {
        private final AgentInfo agentInfo;

        PollTask(AgentInfo agentInfo) {
            this.agentInfo = agentInfo;
        }

        @Override
        public void run() {

            try {
                if (isShutdown.get()) {
                    logger.fine("SoftwareVersionUpdater is shutdown");
                    return;
                }
                logger.fine(THREAD_NAME + " polling " + agentInfo);
                process();
            } catch (Exception e) {
            }
        }
    }

    private class AgentInfo {
        private final String name;
        private Future<?> future;

        AgentInfo(String name) {
            this.name = name;
        }

        void setFuture(Future<?> f) {
             this.future = f;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}

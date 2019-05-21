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

package oracle.kv.impl.arb;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.arb.MonitorAgentImpl;
import oracle.kv.impl.arb.admin.ArbNodeAdmin;
import oracle.kv.impl.arb.admin.ArbNodeAdminImpl;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.ServiceFaultHandler;
import oracle.kv.impl.fault.SystemFaultException;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterTracker;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * This is the "main" that represents the ArbNode. It handles startup and
 * houses all the pieces of software that share a JVM such as a request
 * handler, the administration support and the monitor agent.
 */
public class ArbNodeService
    implements ConfigurableService {

    private ArbNodeId arbNodeId;
    private Params params;

    /**
     * The components that make up the service.
     */
    private ArbNodeSecurity anSecurity = null;
    private ArbNodeAdminImpl admin = null;
    private MonitorAgentImpl monitorAgent = null;
    private ArbNode arbNode = null;

    /**
     *  The status of the service
     */
    private ServiceStatusTracker statusTracker;

    /**
     * Parameter change tracker
     */
    private final ParameterTracker parameterTracker;

    /**
     * Global parameter change tracker
     */
    private final ParameterTracker globalParameterTracker;

    private ArbStatsTracker arbStatsTracker;

    /**
     * The object used to coordinate concurrent request to start and stop the
     * rep node service.
     */
    private final Object startStopLock = new Object();

    /**
     * Whether the stop method has been called, even if the actual stop is
     * being delayed because of other operations.
     */
    private volatile boolean stopRequested;

    /**
     * The fault handler associated with the service.
     */
    private final ServiceFaultHandler faultHandler;

    /**
     * True if running in a thread context.
     */
    private final boolean usingThreads;
    protected Logger logger;

    public ArbNodeService(boolean usingThreads) {
        super();
        faultHandler = new ServiceFaultHandler(this, logger,
                                               ProcessExitCode.RESTART);
        this.usingThreads = usingThreads;
        parameterTracker = new ParameterTracker();
        globalParameterTracker = new ParameterTracker();
    }

    /**
     * Initialize a ArbNodeService.  This must be invoked before start() is
     * called.
     */
    public void initialize(SecurityParams securityParams,
                           ArbNodeParams arbNodeParams,
                           LoadParameters lp) {

        securityParams.initRMISocketPolicies();

        GlobalParams globalParams =
            new GlobalParams(lp.getMap(ParameterState.GLOBAL_TYPE));

        StorageNodeParams storageNodeParams =
            new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));

        /* construct the Params from its components */
        params = new Params(securityParams, globalParams,
                            storageNodeParams, arbNodeParams);

        arbNodeId = arbNodeParams.getArbNodeId();

        /*
         * The AgentRepository is the buffered monitor data and belongs to the
         * monitorAgent, but is instantiated outside to take care of
         * initialization dependencies. Don't instantiate any loggers before
         * this is in place, because constructing the AgentRepository registers
         * it in LoggerUtils, and ensures that loggers will tie into the
         * monitoring system.
         */
        AgentRepository monitorBuffer =
            new AgentRepository(globalParams.getKVStoreName(), arbNodeId);

        logger = LoggerUtils.getLogger(this.getClass(), params);
        faultHandler.setLogger(logger);

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        anSecurity = new ArbNodeSecurity(this, logger);

        statusTracker = new ServiceStatusTracker(logger, monitorBuffer);

        arbNode =  new ArbNode(params);
        addParameterListener(arbNode);

        arbStatsTracker =
            new ArbStatsTracker(this, params.getArbNodeParams().getMap(),
                                params.getGlobalParams().getMap(),
                                monitorBuffer);
        addParameterListener(arbStatsTracker.getARBParamsListener());
        addGlobalParameterListener(arbStatsTracker.getGlobalParamsListener());

        admin = new ArbNodeAdminImpl(this, arbNode);
        monitorAgent = new MonitorAgentImpl(this, monitorBuffer);

        addParameterListener(UserDataControl.getParamListener());

        final LoginUpdater loginUpdater = new LoginUpdater();

        loginUpdater.addServiceParamsUpdaters(anSecurity);
        loginUpdater.addGlobalParamsUpdaters(anSecurity);

        addParameterListener(loginUpdater.new ServiceParamsListener());
        addGlobalParameterListener(
            loginUpdater.new GlobalParamsListener());

        /* Initialize system properties. */
        File kvDir = FileNames.getKvDir(storageNodeParams.getRootDirPath(),
                                        globalParams.getKVStoreName());

        String policyFile;
        try {
            policyFile =
                FileNames.getSecurityPolicyFile(kvDir).getCanonicalPath();
        } catch (IOException e) {
            throw new SystemFaultException
                ("IO Exception trying to access security policy file: " +
                 FileNames.getSecurityPolicyFile(kvDir), e);
        }

        if (policyFile != null && new File(policyFile).exists()) {
            System.setProperty("java.security.policy", policyFile);
        }

        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }

        /* Sets the server hostname for remote objects */
        AsyncRegistryUtils.setServerHostName(storageNodeParams.getHostname());

        /* Disable to allow for faster timeouts on failed connections. */
        System.setProperty("java.rmi.server.disableHttp", "true");

        if (!usingThreads) {
            storageNodeParams.setRegistryCSF(securityParams);
        }
    }

    /**
     * Notification that there are modified service parameters.
     */
    synchronized public void newParameters() {
        ParameterMap newMap = null;
        ParameterMap oldMap = params.getArbNodeParams().getMap();
        StorageNodeParams snp = params.getStorageNodeParams();
        try {
            final StorageNodeId snid1 = snp.getStorageNodeId();
            StorageNodeAgentAPI snapi =
                RegistryUtils.getStorageNodeAgent(
                    snp.getHostname(),
                    snp,
                    snid1,
                    anSecurity.getLoginManager());

            LoadParameters lp = snapi.getParams();
            newMap = lp.getMap(oldMap.getName(), oldMap.getType());
        } catch (NotBoundException | RemoteException e) {
            /* Ignore exception, will read directly from file */
        }

        if (newMap == null) {
            newMap =
                ConfigUtils.getArbNodeMap(params.getStorageNodeParams(),
                                          params.getGlobalParams(),
                                          arbNodeId, logger);
        }

        /* Do nothing if maps are the same */
        if (oldMap.equals(newMap)) {
            logger.info("newParameters are identical to old parameters");
            return;
        }
        logger.info("newParameters: refreshing parameters");
        params.setArbNodeParams(new ArbNodeParams(newMap));
        parameterTracker.notifyListeners(oldMap, newMap);
    }

    /**
     * Notification that there are modified global parameters
     */
    synchronized public void newGlobalParameters() {
        ParameterMap oldMap = params.getGlobalParams().getMap();
        ParameterMap newMap =
            ConfigUtils.getGlobalMap(params.getStorageNodeParams(),
                                     params.getGlobalParams(),
                                     logger);

        /* Do nothing if maps are the same */
        if (oldMap.equals(newMap)) {
            logger.info(
                "newGlobalParameters are identical to old global parameters");
            return;
        }
        logger.info("newGlobalParameters: refreshing global parameters");
        params.setGlobalParams(new GlobalParams(newMap));
        globalParameterTracker.notifyListeners(oldMap, newMap);
    }

    public void addParameterListener(ParameterListener listener) {
        parameterTracker.addListener(listener);
    }

    public ArbStatsTracker getStatsTracker() {
        return arbStatsTracker;
    }

    Logger getLogger() {
        return logger;
    }

    private void addGlobalParameterListener(ParameterListener listener) {
        globalParameterTracker.addListener(listener);
    }

    /**
     * Starts the ArbNodeService.
     *
     */
    @Override
    public void start() {

        synchronized (startStopLock) {
            statusTracker.update(ServiceStatus.STARTING);

            try {

                logger.info("Starting ArbNodeService");

                /*
                 * Disable support for async requests on the server side in
                 * process mode since the arbiter service doesn't need this
                 * capability.
                 */
                if (!usingThreads) {
                    AsyncControl.serverUseAsync = false;
                }

                /*
                 * Start the monitor agent first, so the monitor can report
                 * state and events.
                 */
                monitorAgent.startup();
                checkStopRequestedDuringStart();

                /*
                 * Start up admin.  Most requests will fail until the arbNode
                 * is started but this is done early to allow ping() to
                 * function.
                 */
                admin.startup();
                checkStopRequestedDuringStart();

                /*
                 * Start up the arbNode so that it's available for admin and
                 * user requests.
                 */
                arbNode.startup();
                checkStopRequestedDuringStart();

                statusTracker.update(ServiceStatus.RUNNING);
                logger.info("Started ArbNodeService");
            } catch (RemoteException re) {
                statusTracker.update(ServiceStatus.ERROR_NO_RESTART);
                throw new IllegalStateException
                    ("ArbNodeService startup failed", re);
            }
        }
    }

    private void checkStopRequestedDuringStart() {
        if (stopRequested) {
            throw new IllegalStateException(
                "ArbNodeService startup failed because stop was requested");
        }
    }

    /**
     * Invokes the stop method on each of the components constituting the
     * service. Proper sequencing of the stop methods is important and is
     * explained in the embedded method comments.
     *
     * If the service has already been stopped the method simply returns.
     */
    @Override
    public void stop(boolean force) {
        stopRequested = true;
        synchronized (startStopLock) {
            if (statusTracker.getServiceStatus().isTerminalState()) {
                /* If the service has already been stopped. */
                return;
            }
            statusTracker.update(ServiceStatus.STOPPING);

            try {

                /*
                 * Push all stats out of the operation stats collector, to
                 * attempt to get them to the admin monitor or to the local log.
                 */
                arbStatsTracker.pushStats();

                /*
                 * Stop the admin. Note that an admin shutdown request may be
                 * in progress, but the request should not be impacted by
                 * stopping the admin service. Admin services may be using the
                 * environment, so stop them first.
                 */
                admin.stop();

                /*
                 * Stop the arb node next.
                 */
                arbNode.stop();

                /* Set the status to STOPPED */
                statusTracker.update(ServiceStatus.STOPPED);

                /*
                 * Shutdown the monitor last.
                 */
                monitorAgent.stop();
            } catch (RemoteException re) {
                statusTracker.update(ServiceStatus.ERROR_NO_RESTART);
                throw new IllegalStateException
                    ("ArbNodeService stop failed", re);
            } finally {
                if (!usingThreads) {
                    /* Flush any log output. */
                    LoggerUtils.closeHandlers
                        ((params != null) ?
                         params.getGlobalParams().getKVStoreName() :
                         "UNKNOWN");
                }
            }
        }
    }

    @Override
    public boolean stopRequested() {
        return stopRequested;
    }

    public ServiceStatusTracker getStatusTracker() {
        return statusTracker;
    }

    @Override
    public void update(ServiceStatus status) {
        statusTracker.update(status);
    }

    public ArbNodeService.Params getParams() {
        return params;
    }

    /**
     * Returns the ArbNodeId associated with the service
     */
    public ArbNodeId getArbNodeId() {
        return arbNodeId;
    }

    public StorageNodeId getStorageNodeId() {
        return params.getStorageNodeParams().getStorageNodeId();
    }

    public ArbNodeParams getArbNodeParams() {
        return params.getArbNodeParams();
    }

    /**
     * Returns the fault handler associated with the service
     */
    public ProcessFaultHandler getFaultHandler() {
       return faultHandler;
    }

    @Override
    public boolean getUsingThreads() {
        return usingThreads;
    }

    /**
     * Rebinds the remote component in the registry associated with this SN
     */
    public void rebind(Remote remoteComponent,
                       RegistryUtils.InterfaceType type,
                       ClientSocketFactory clientSocketFactory,
                       ServerSocketFactory serverSocketFactory)
        throws RemoteException {

        StorageNodeParams snp = params.getStorageNodeParams();
        RegistryUtils.rebind(snp.getHostname(), snp.getRegistryPort(),
                             params.getGlobalParams().getKVStoreName(),
                             getArbNodeId().getFullName(), type,
                             remoteComponent,
                             clientSocketFactory,
                             serverSocketFactory);
    }

    /**
     * Unbinds the remote component in the registry associated with this SN
     */
    public boolean unbind(Remote remoteComponent,
                          RegistryUtils.InterfaceType type)
        throws RemoteException {

        StorageNodeParams snp = params.getStorageNodeParams();

        return RegistryUtils.unbind
            (snp.getHostname(), snp.getRegistryPort(),
             params.getGlobalParams().getKVStoreName(),
             getArbNodeId().getFullName(), type, remoteComponent);
    }

    ArbNode getArbNode() {
        return arbNode;
    }

    public ArbNodeAdmin getArbNodeAdmin() {
        return admin;
    }

    public ArbNodeSecurity getArbNodeSecurity() {
        return anSecurity;
    }

    public LoginManager getLoginManager() {
        return anSecurity.getLoginManager();
    }

    /**
     * Issue a BDBJE update of the target node's HA address.
     *
     * @param groupName - Replication group name
     * @param targetNodeName - Node name
     * @param targetHelperHosts - Helper hosts used to access RepGroupAdmin
     * @param newNodeHostPort - List of new helpers, if NULL complete entry is
     *                          removed from the rep group.
     */
    public void updateMemberHAAddress(String groupName,
                                      String targetNodeName,
                                      String targetHelperHosts,
                                      String newNodeHostPort) {

        /*
         * Setup the helper hosts to use for finding the master to execute this
         * update.
         */
        Set<InetSocketAddress> helperSockets = new HashSet<InetSocketAddress>();
        StringTokenizer tokenizer =
                new StringTokenizer(targetHelperHosts,
                                    ParameterUtils.HELPER_HOST_SEPARATOR);
        while (tokenizer.hasMoreTokens()) {
            String helper = tokenizer.nextToken();
            helperSockets.add(HostPortPair.getSocket(helper));
        }

        /*
         * Check the target node's HA address. If it is already changed
         * and if that node is alive, don't make any further changes. If the
         * target still has its old HA address, attempt an update.
         */
        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(groupName, helperSockets,
                                      ((arbNode != null) ?
                                       arbNode.getRepNetConfig() : null));
        ReplicationGroup rg = rga.getGroup();
        com.sleepycat.je.rep.ReplicationNode jeRN =
            rg.getMember(targetNodeName);
        if (jeRN == null) {
            throw new IllegalStateException
                (targetNodeName + " does not exist in replication group " +
                 groupName);
        }

        if (newNodeHostPort == null) {
            rga.deleteMember(targetNodeName);
            return;
        }

        String newHostName = HostPortPair.getHostname(newNodeHostPort);
        int newPort = HostPortPair.getPort(newNodeHostPort);

        if ((jeRN.getHostName().equals(newHostName)) &&
            (jeRN.getPort() == newPort)) {

            /*
             * This node is already changed, nothing more to do. Do this
             * check in case the change has been made previously, and this
             * node is alive, as the updateAddress() call will incur an
             * exception if the node is alive.
             */
            return;
        }

        rga.updateAddress(targetNodeName, newHostName, newPort);
    }

    /**
     * A convenience class to package all the parameter components used by
     * the Arb Node service
     */
    public static class Params {
        private final SecurityParams securityParams;
        private volatile GlobalParams globalParams;
        private final StorageNodeParams storageNodeParams;
        private volatile ArbNodeParams arbNodeParams;

        public Params(SecurityParams securityParams,
                      GlobalParams globalParams,
                      StorageNodeParams storageNodeParams,
                      ArbNodeParams arbNodeParams) {
            super();
            this.securityParams = securityParams;
            this.globalParams = globalParams;
            this.storageNodeParams = storageNodeParams;
            this.arbNodeParams = arbNodeParams;
        }

        public SecurityParams getSecurityParams() {
            return securityParams;
        }

        public GlobalParams getGlobalParams() {
            return globalParams;
        }

        public StorageNodeParams getStorageNodeParams() {
            return storageNodeParams;
        }

        public ArbNodeParams getArbNodeParams() {
            return arbNodeParams;
        }

        public void setArbNodeParams(ArbNodeParams params) {
            arbNodeParams = params;
        }

        public void setGlobalParams(GlobalParams params) {
            globalParams = params;
        }
    }
}

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

import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.PlanProgress;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.client.admin.ClientAdminService;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.mgmt.AdminStatusReceiver;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.RoleResolver;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * AdminService houses the two services provided for administering a kv store
 * instance and providing clients with access to Admin API functions.
 */
public class AdminService implements ConfigurableService {

    private AdminServiceParams params;
    private Admin admin = null;           /* Admin can be null at bootstrap. */
    private CommandService commandService;
    private CommandService exportableCommandService;
    private LoginService loginService;
    private UserLogin exportableUL;
    private ClientAdminService clientService;
    private ClientAdminService exportableClientService;
    private AdminStatusReceiver statusReceiver = null;
    private ParameterListener parameterListener = null;
    private MgmtAgentPlanStateChangeTracker planStateListener = null;
    private MgmtAgentPlanProgressTracker planProgressListener = null;
    private AdminSecurity adminSecurity;

    private Logger logger;

    private AdminServiceFaultHandler faultHandler;
    private final boolean usingThreads;

    public static final int ADMIN_MIN_HEAP_MB = ParameterUtils.applyMinHeapMB(96);
    public static final int ADMIN_MAX_HEAP_MB = ParameterUtils.applyMinHeapMB(128);

    /* Default Java heap to 96M for now */
    public static final String DEFAULT_JAVA_ARGS =
        "-XX:+DisableExplicitGC " +
        "-Xms" + ADMIN_MIN_HEAP_MB + "M -Xmx" + ADMIN_MAX_HEAP_MB + "M " +
        "-server" +

        /*
         * Disable JE's requirement that helper host names be resolvable.  We
         * want nodes to be able to start up even if other nodes in the
         * replication group have been removed and no longer have DNS names.
         * [#23120]
         */
        " -Dje.rep.skipHelperHostResolution=true";

    /* For unit test support, to determine if the service is up or down. */
    private boolean active = false;

    /**
     * Whether the stop method has been called, even if the actual stop is
     * being delayed because of other operations.
     */
    private volatile boolean stopRequested;

    /**
     * Creates a non-bootstrap AdminService.  The initialize() method must
     * be called before start().
     */
    public AdminService(boolean usingThreads) {
        this.usingThreads = usingThreads;
        faultHandler = null;
    }

    /**
     * Creates a bootstrap AdminService.  The initialize() method should not
     * be called before start(). The bootstrap service is always created as
     * a PRIMARY Admin.
     */
    public AdminService(BootstrapParams bp,
                        SecurityParams sp,
                        boolean usingThreads) {
        this.usingThreads = usingThreads;
        deriveASParams(bp, sp);

        /*
         * No need to worry about RMI Socket Policies and Registry CSF here
         * because ManagedBootstrapAdmin takes care of that for us, if needed.
         */

        /* When kvStoreName is null, we are starting in bootstrap mode. */
        if (params.getGlobalParams().getKVStoreName() == null) {
            logger =
                LoggerUtils.getBootstrapLogger(bp.getRootdir(),
                                               FileNames.BOOTSTRAP_ADMIN_LOG,
                                               "BootstrapAdmin");
        } else {
            logger = LoggerUtils.getLogger(this.getClass(), params);
        }
        faultHandler = new AdminServiceFaultHandler(logger, this);
        adminSecurity = new AdminSecurity(this, logger);
    }

    /**
     * Initialize an AdminService.  This must be called before start() if not
     * created via the BootStrap constructor.
     */
    public void initialize(SecurityParams securityParams,
                           AdminParams adminParams,
                           LoadParameters lp) {

        securityParams.initRMISocketPolicies();

        GlobalParams globalParams =
            new GlobalParams(lp.getMap(ParameterState.GLOBAL_TYPE));
        StorageNodeParams storageNodeParams =
            new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        /*
         * Initializing storage node params with admin mount point before
         * start. If admin mount is not specified then kvroot directory will
         * host admin env files as per current design.
         * 
         * TODO: Need to check if admin directory should be part of AdminParams
         * like RN Log Directories are part of RepNodeParams.
         */
        storageNodeParams.setAdminDirMap(
            lp.getMap(ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS));

        params = new AdminServiceParams
            (securityParams, globalParams, storageNodeParams, adminParams);

        logger = LoggerUtils.getLogger(this.getClass(), params);
        if (faultHandler == null) {
            faultHandler = new AdminServiceFaultHandler(logger, this);
        }

        if (!usingThreads) {
            storageNodeParams.setRegistryCSF(securityParams);
        }

        adminSecurity = new AdminSecurity(this, logger);
    }

    /**
     * Start and stop are synchronized to avoid having stop() run before
     * start() is done.  This comes up in testing.
     */
    @Override
    public synchronized void start() {
        getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

                @Override
                public void execute() {
                    startInternal();
                }
             });
    }

    private void startInternal() {

        /*
         * Disable support for async requests on the server side in process
         * mode since the admin service doesn't need this capability.
         */
        if (!usingThreads) {
            AsyncControl.serverUseAsync = false;
        }

        final StorageNodeParams snParams = params.getStorageNodeParams();
        final String hostName = snParams.getHostname();

        /* Sets the server hostname for remote objects */
        AsyncRegistryUtils.setServerHostName(hostName);

        /* Disable to allow for faster timeouts on failed connections. */
        System.setProperty("java.rmi.server.disableHttp", "true");

        /*
         * If kvStoreName is null, then we are starting in bootstrap mode.
         * The Admin can't be created yet.
         */
        final String kvStoreName = params.getGlobalParams().getKVStoreName();
        if (kvStoreName == null) {
            logger.info("Starting in bootstrap mode");
        } else {
            logger.info("Starting AdminService");
            admin = new Admin(params, this);
        }

        /*
         * Create the UserLoginImpl instance and bind it in the registry if
         * security is enabled.
         */
        final boolean isSecure = params.getSecurityParams().isSecure();
        if (isSecure) {

            ServiceBinder<UserLogin, LoginService> serviceBinder =
                new ServiceBinder<UserLogin, LoginService>
                (GlobalParams.ADMIN_LOGIN_SERVICE_NAME, this) {

                @Override
                LoginService makeInsecureService() {
                    return new LoginService(aservice);
                }

                @Override
                UserLogin getRemote() {
                    return insecureService.getUserLogin();
                }
            };

            loginService = serviceBinder.getInsecureService();
            exportableUL = serviceBinder.getSecureService();

            /* Install the login updater now */
            if (admin != null) {
                admin.installSecurityUpdater();
            }
        }

        /* Create the CommandService instance and bind it in the registry */
        ServiceBinder<CommandService, CommandService> serviceBinder =
            new ServiceBinder<CommandService, CommandService>
            (GlobalParams.COMMAND_SERVICE_NAME, this) {

            @Override
            CommandServiceImpl makeInsecureService() {
                return new CommandServiceImpl(aservice);
            }

            @Override
            CommandService getRemote() {
                return insecureService;
            }
        };
        commandService = serviceBinder.getInsecureService();
        exportableCommandService = serviceBinder.getSecureService();

        /* Create the ClientAdminService for handling DDL and DML statements */
        ServiceBinder<ClientAdminService, ClientAdminService> stmtBinder =
            new ServiceBinder<ClientAdminService, ClientAdminService>
            (GlobalParams.CLIENT_ADMIN_SERVICE_NAME, this) {

            @Override
            ClientAdminServiceImpl makeInsecureService() {
                return new ClientAdminServiceImpl(aservice, logger);
            }

            @Override
            ClientAdminService getRemote() {
                return insecureService;
            }
        };
        clientService = stmtBinder.getInsecureService();
        exportableClientService = stmtBinder.getSecureService();

        synchronized (this) {
            active = true;
            this.notifyAll();
        }
        logger.info("Started AdminService");
    }

    @Override
    public void stop(boolean force) {
        stopRequested = true;
        synchronized (this) {
            logger.info("Shutting down AdminService instance" +
                        (force ? " (force)" : ""));

            update(ServiceStatus.STOPPING);
            String hostName = params.getStorageNodeParams().getHostname();
            if (commandService != null) {
                ((CommandServiceImpl) commandService).stopRemoteTestInterface(
                    logger);
                try {
                    logger.info("Unbinding CommandService");
                    RegistryUtils.unbind
                        (hostName,
                         params.getStorageNodeParams().getRegistryPort(),
                         GlobalParams.COMMAND_SERVICE_NAME,
                         exportableCommandService);
                    commandService = null;
                } catch (RemoteException re) {
                    String msg = "Can't unbind CommandService. ";
                    logger.severe(msg + LoggerUtils.getStackTrace(re));
                    throw new IllegalStateException(msg, re);
                }
            }

            if (clientService != null) {
                try {
                    logger.info("Unbinding ClientAdminService");
                    RegistryUtils.unbind
                        (hostName,
                         params.getStorageNodeParams().getRegistryPort(),
                         GlobalParams.CLIENT_ADMIN_SERVICE_NAME,
                     exportableClientService);
                    clientService = null;
                } catch (RemoteException re) {
                    String msg = "Can't unbind ClientAdminService. ";
                    logger.severe(msg + LoggerUtils.getStackTrace(re));
                    throw new IllegalStateException(msg, re);
                }
            }

            if (loginService != null) {
                try {
                    logger.info("Unbinding LoginService");
                    RegistryUtils.unbind
                        (hostName,
                         params.getStorageNodeParams().getRegistryPort(),
                         GlobalParams.ADMIN_LOGIN_SERVICE_NAME,
                         exportableUL);
                    loginService = null;
                } catch (RemoteException re) {
                    String msg = "Can't unbind LoginService. ";
                    logger.severe(msg + LoggerUtils.getStackTrace(re));
                    throw new IllegalStateException(msg, re);
                }
            }

            if (admin != null) {
                logger.info("Shutting down Admin");
                admin.shutdown(force);
                admin = null;
            }

            active = false;
            this.notifyAll();
        }
    }

    @Override
    public boolean stopRequested() {
        return stopRequested;
    }

    /**
     *  Wait for the service to be started or stopped.
     */
    public synchronized void waitForActive(boolean desiredState)
        throws InterruptedException {

        while (active != desiredState) {
            this.wait();
        }
    }

    /**
     * Accessor for admin, which can be null.
     */
    public Admin getAdmin() {
        return admin;
    }

    /**
     * Accessor for params
     */
    public AdminServiceParams getParams() {
        return params;
    }

    /**
     * Accessor for security info
     */
    public AdminSecurity getAdminSecurity() {
        return adminSecurity;
    }

    /**
     * Accessor for login services
     */
    public LoginService getLoginService() {
        return loginService;
    }

    /**
     * Accessor for internal login manager
     */
    public InternalLoginManager getLoginManager() {
        return (adminSecurity == null) ? null : adminSecurity.getLoginManager();
    }

    /**
     * Configure the store name and then create the Admin instance, which
     * creates the Admin database.  This method can be used only when the
     * AdminService is running in bootstrap/configuration mode.
     */
    public void configure(String storeName) {
        assert admin == null;
        params.getGlobalParams().setKVStoreName(storeName);

        /*
         * Since we are bootstrapping, there is a chicken-egg problem regarding
         * the HA service port.  The bootstrap parameters have an HA port range
         * configured for the SNA.  We know that none of these are in use at
         * this time, so we will commandeer the first port in this range for
         * now.  Later, when this #1 admin is officially deployed via the
         * deployment plan, we will note the use of this port in the Admin's
         * parameter record, thereby reserving its use with the PortTracker.
         */
        final StorageNodeParams snp = params.getStorageNodeParams();

        final int haPort = PortRange.getRange(snp.getHAPortRange()).get(0);
        AdminParams ap = params.getAdminParams();
        ap.setJEInfo(snp.getHAHostname(), haPort, snp.getHAHostname(), haPort);

        admin = new Admin(params, this);

        /* Now we can use the real log configuration. */
        logger.info("Changing log files to log directory for store " +
                    storeName);
        logger = LoggerUtils.getLogger(this.getClass(), params);
        faultHandler.setLogger(logger);
        adminSecurity.configure(storeName);
        if (loginService != null) {
            loginService.resetLogger(logger);
        }
        logger.info("Configured Admin for store: " + storeName);

        /* We can install the login updater now */
        final SecurityParams securityParams = params.getSecurityParams();
        if (securityParams != null) {
            admin.installSecurityUpdater();
        }
    }
    /**
     * Subordinate services (e.g. CommandService) use this
     * method when logging.  AdminService's logger can change during
     * bootstrap.
     */
    public Logger getLogger() {
        return logger;
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
     * Initialize our AdminServiceParams member based on the contents of a
     * BootParams instance.
     */
    private void deriveASParams(BootstrapParams bp, SecurityParams sp) {

        String storeName = bp.getStoreName();
        StorageNodeId snid =
            new StorageNodeId(storeName == null ? 1 : bp.getId());

        GlobalParams gp = new GlobalParams(storeName);

        /*
         * Pass user-defined external authentication method to global parameter
         * so that bootstrap admin can recognize this setting automatically.
         */
        final String externalAuths = bp.getUserExternalAuth();
        gp.setUserExternalAuthMethods(externalAuths);

        /*
         * In this release, once the IDCS OAuth authentication mechanism is
         * enabled, the whole store won't allow session extension.
         *
         * TODO: Remove this restriction by distinguishing sessions created
         * after successfully login using different authentication methods.
         */
        if (SecurityUtils.hasIDCSOAuth(externalAuths)) {
            gp.setSessionExtendAllow("false");
        }

        StorageNodeParams snp =
            new StorageNodeParams(snid, bp.getHostname(), bp.getRegistryPort(),
                                  "Admin Bootstrap");

        snp.setRootDirPath(bp.getRootdir());
        /*
         * Setting Admin Directory property for configure store command
         * support
         */
        snp.setAdminDirMap(bp.getAdminDirMap());
        snp.setHAHostname(bp.getHAHostname());
        snp.setHAPortRange(bp.getHAPortRange());
        snp.setServicePortRange(bp.getServicePortRange());

        final AdminParams ap = new AdminParams(new AdminId(1),
                                               snp.getStorageNodeId(),
                                               AdminType.PRIMARY);
        params = new AdminServiceParams(sp, gp, snp, ap);
    }

    /**
     * Issue a BDBJE update of the target node's HA address.
     */
    public void updateMemberHAAddress(AdminId targetId,
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

        String storeName = params.getGlobalParams().getKVStoreName();
        String groupName = Admin.getAdminRepGroupName(storeName);

        /* Change the target node's HA address. */
        logger.info("Updating rep group " + groupName + " using helpers " +
                    targetHelperHosts + " to change " + targetId + " to " +
                    newNodeHostPort);

        String targetNodeName = Admin.getAdminRepNodeName(targetId);

        /*
         * Figure out the right ReplicationNetworkConfig to use.  If there's
         * an admin present, we just use that config.  Otherwise (not sure
         * why it wouldn't be if we are part of a replicated config),
         * construct a DataChannelFactory from the SecurityParams, if
         * present.
         */
        final ReplicationNetworkConfig repNetConfig;
        if (admin != null) {
            repNetConfig = admin.getRepNetConfig();
        } else if (params.getSecurityParams() == null) {
            repNetConfig = null;
        } else {
            final Properties haProps =
                params.getSecurityParams().getJEHAProperties();
            logger.info("DataChannelFactory: " +
                        haProps.getProperty(
                            ReplicationNetworkConfig.CHANNEL_TYPE));
            repNetConfig = ReplicationNetworkConfig.create(haProps);
        }

        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(groupName, helperSockets, repNetConfig);

        ReplicationGroup rg = rga.getGroup();
        com.sleepycat.je.rep.ReplicationNode jeRN =
            rg.getMember(targetNodeName);
        if (jeRN == null) {
            throw new IllegalStateException
                (targetNodeName + " does not exist in replication group " +
                 groupName);
        }

        String newHostName =  HostPortPair.getHostname(newNodeHostPort);
        int newPort =  HostPortPair.getPort(newNodeHostPort);

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

    public void installStatusReceiver(AdminStatusReceiver asr) {

        statusReceiver = asr;

        if (admin == null) {
            /* We're unconfigured; report waiting for deployment. */
            update(ServiceStatus.WAITING_FOR_DEPLOY);
        } else {
            /* Otherwise, if we're up and servicing calls, we are running. */
            update(ServiceStatus.RUNNING);
        }
    }

    @Override
    public void update(ServiceStatus newStatus) {
       updateAdminStatus(admin, newStatus);
    }

    void updateAdminStatus(Admin a, ServiceStatus newStatus) {
        if (statusReceiver == null) {
            return;
        }

        /*
         * During bootstrapping, we have no Admin, so we can't reliably add the
         * ParameterListener when installing the receiver.  This method is
         * called at receiver installation time, and also when the Admin's
         * status changes.  When it changes from bootstrap to configured mode,
         * we can install the listener here.  Also, the Admin is given as an
         * argument to this method because, during bootstrapping, this method
         * is called before AdminService's admin instance variable is assigned.
         */
        if (a != null && parameterListener == null) {
            parameterListener = new ParameterChangeListener();
            a.addParameterListener(parameterListener);
            /* Prime the pump with the first newParameters call. */
            parameterListener.newParameters
                (null,
                 a.getParams().getAdminParams().getMap());
        }

        /*
         * Use the same pattern as ParameterListener and add a plan state plan
         * listener at the point when the Admin changes from bootstrap to
         * configured mode.
         */
        if (a != null && planStateListener == null) {
            planStateListener = new MgmtAgentPlanStateChangeTracker();
            a.getMonitor().trackPlanStateChange(planStateListener);
        }
        if (a != null && planProgressListener == null) {
            planProgressListener = new MgmtAgentPlanProgressTracker();
            a.getMonitor().trackPlanProgress(planProgressListener);
        }

        State adminState = (a == null ? null : a.getReplicationMode());

        boolean isMaster =
            (adminState == null || adminState != State.MASTER ? false : true);

        try {
            statusReceiver.updateAdminStatus
                (new ServiceStatusChange(newStatus), isMaster);
        } catch (RemoteException e) {
            /*
             * This should not prevent the admin from coming up.  Just log the
             * failure.
             */
            logger.log
                (Level.WARNING,
                 "Failed to send status updateof " + newStatus.toString() +
                 " to MgmtAgent", e);
        }
    }

    public RoleResolver getRoleResolver() {
        return (adminSecurity == null) ? null : adminSecurity.getRoleResolver();
    }

    /**
     * The plan state change tracker for updating the status receiver.
     */
    private class MgmtAgentPlanStateChangeTracker
        implements ViewListener<PlanStateChange> {

        @Override
        public void newInfo(ResourceId resourceId, PlanStateChange planState) {
            try {
                statusReceiver.updatePlanStatus(planState.toJsonString());
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log
                    (Level.WARNING,
                     "Failure to deliver plan state change to MgmtAgent", re);
                return;
            }
        }
    }

    /**
     * The plan progress tracker for updating the status receiver.
     */
    private class MgmtAgentPlanProgressTracker
        implements ViewListener<PlanProgress> {

        @Override
        public void newInfo(ResourceId resourceId, PlanProgress planProgress) {
            try {
                statusReceiver.updatePlanStatus(planProgress.toJsonString());
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log
                    (Level.WARNING,
                     "Failure to deliver plan progress to MgmtAgent", re);
                return;
            }
        }
    }

    /**
     * The parameter listener for updating the status receiver.
     */
    private class ParameterChangeListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            try {
                statusReceiver.receiveNewParams(newMap);
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log
                    (Level.WARNING,
                     "Failure to deliver parameter change to MgmtAgent", re);
                return;
            }
        }
    }

    /**
     * Helper class to create and then export a secured RMI service.
     */
    private abstract class ServiceBinder<S extends Remote, I> {
        private final String svcName;
        protected final AdminService aservice;
        protected I insecureService;
        private S secureService;

        /**
         * Instantiating a ServiceBinder will actually invoke the creation and
         * proxy wrapping of the targer service.
         *
         * @param svcName is the RMI service name.
         */
        ServiceBinder(String svcName, AdminService aservice) {
            this.svcName = svcName;
            this.aservice = aservice;

            /* create and wrap the RMI service */
            initService();
        }

        public S getSecureService() {
            return secureService;
        }

        public I getInsecureService() {
            return insecureService;
        }

        /** Instantiate an insecure version of the service */
        abstract I makeInsecureService();

        /** Return the Remote from the insecure version of the service. */
        abstract S getRemote();

        /**
         * Start the service, and wrap it with the appropriate security proxy
         */
        private void initService() {
            final StorageNodeParams snParams = params.getStorageNodeParams();
            final String hostName = snParams.getHostname();
            final int registryPort = snParams.getRegistryPort();
            final String kvStoreName =
                params.getGlobalParams().getKVStoreName();

            logger.info("Starting " + svcName + " on rmi://" + hostName + ":" +
                        registryPort + "/" + svcName);
            try {
                insecureService = makeInsecureService();

                /* Wrap the insecure service in a secure proxy */
                try {
                    secureService = SecureProxy.create
                        (getRemote(),
                         adminSecurity.getAccessChecker(),
                         faultHandler);
                    logger.info("Successfully created a secure proxy for " +
                                svcName);
                } catch (ConfigurationException ce) {
                    throw new IllegalStateException
                        ("Unable to create a secure proxy for " + svcName, ce);
                }

                RMISocketPolicy rmiSocketPolicy =
                    params.getSecurityParams().getRMISocketPolicy();
                final SocketFactoryPair sfp =
                    params.getStorageNodeParams().getAdminCommandServiceSFP
                    (rmiSocketPolicy, kvStoreName);

                RegistryUtils.rebind(hostName,
                                     registryPort,
                                     svcName,
                                     secureService,
                                     sfp.getClientFactory(),
                                     sfp.getServerFactory());

            } catch (RemoteException re) {
                String msg = "Starting " + svcName + " failed";
                logger.severe(msg + LoggerUtils.getStackTrace(re));
                throw new IllegalStateException(msg, re);
            }
        }
    }
}

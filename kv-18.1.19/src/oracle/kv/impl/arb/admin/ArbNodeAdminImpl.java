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

package oracle.kv.impl.arb.admin;

import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.arbiter.ArbiterStats;

import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.mgmt.ArbNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.arb.ArbNode;
import oracle.kv.impl.arb.ArbNodeService;
import oracle.kv.impl.arb.ArbStatsTracker;
import oracle.kv.impl.arb.admin.ArbNodeAdmin;
import oracle.kv.impl.arb.admin.ArbNodeAdminFaultHandler;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.server.LoggerUtils;

@SecureAPI
public class ArbNodeAdminImpl extends VersionedRemoteImpl
    implements ArbNodeAdmin {

    /**
     *  The arbNode being administered
     */
    private final ArbNode arbNode;
    private final ArbNodeService arbNodeService;

    /**
     * The fault handler associated with the service.
     */
    private final ArbNodeAdminFaultHandler faultHandler;
    private final Logger logger;

    /**
     * A conditional instance, created if the class can be found.
     */
    private RemoteTestInterface rti;

    /**
     * The exportable/bindable version of this object
     */
    private ArbNodeAdmin exportableArbNodeAdmin;

    private ArbNodeStatusReceiver statusReceiver;

    private static final String TEST_INTERFACE_NAME =
            "oracle.kv.impl.arb.ArbNodeTestInterface";

    public ArbNodeAdminImpl(ArbNodeService arbNodeService, ArbNode arbNode) {

        this.arbNodeService = arbNodeService;
        this.arbNode = arbNode;
        logger =
            LoggerUtils.getLogger(this.getClass(), arbNodeService.getParams());

        faultHandler = new ArbNodeAdminFaultHandler(arbNodeService,
                                                    logger,
                                                    ProcessExitCode.RESTART);
    }

    private void assertRunning() {
        ServiceStatus status =
            arbNodeService.getStatusTracker().getServiceStatus();
        if (status != ServiceStatus.RUNNING) {
            throw new IllegalArbNodeServiceStateException
                ("ArbNode is not RUNNING, current status is " + status);
        }
    }

    /**
     * Create the test interface if it can be found.
     */
    private void startTestInterface() {
        try {
            Class<?> cl = Class.forName(TEST_INTERFACE_NAME);
            Constructor<?> c = cl.getConstructor(arbNodeService.getClass());
            rti = (RemoteTestInterface) c.newInstance(arbNodeService);
            rti.start(SerialVersion.CURRENT);
        } catch (Exception ignored) {
        }
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void newParameters(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                assertRunning();
                arbNodeService.newParameters();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void newGlobalParameters(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                arbNodeService.newGlobalParameters();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public LoadParameters getParams(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<LoadParameters>() {

            @Override
            public LoadParameters execute() {
                return arbNode.getAllParams();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void shutdown(final boolean force, AuthContext authCtx, short serialVersion)
            throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                logger.log(Level.INFO, "ArbNodeAdmin shutdown({0})", force);
                arbNodeService.stop(force);
            }
        });
    }

    /**
     * Starts up the admin component, binding its stub in the registry, so that
     * it can start accepting remote admin requests.
     *
     * @throws RemoteException
     */
    public void startup()
        throws RemoteException {

        faultHandler.execute
        (new ProcessFaultHandler.Procedure<RemoteException>() {

            @Override
            public void execute() throws RemoteException {
                final String kvsName = arbNodeService.getParams().
                        getGlobalParams().getKVStoreName();
                final ArbNodeParams anp = arbNodeService.getArbNodeParams();
                final StorageNodeParams snp =
                    arbNodeService.getParams().getStorageNodeParams();

                final String csfName = ClientSocketFactory.
                        factoryName(kvsName,
                                    ArbNodeId.getPrefix(),
                                    InterfaceType.ADMIN.interfaceName());

                RMISocketPolicy rmiPolicy = arbNodeService.getParams().
                    getSecurityParams().getRMISocketPolicy();
                SocketFactoryPair sfp =
                    anp.getAdminSFP(rmiPolicy,
                                    snp.getServicePortRange(),
                                    csfName);

                if (sfp.getServerFactory() != null) {
                    sfp.getServerFactory().setConnectionLogger(logger);
                }
                initExportableArbNodeAdmin();
                arbNodeService.rebind(exportableArbNodeAdmin,
                                      InterfaceType.ADMIN,
                                      sfp.getClientFactory(),
                                      sfp.getServerFactory());

                logger.info("ArbNodeAdmin registered");
                startTestInterface();
            }
        });
    }

    private void initExportableArbNodeAdmin() {
        try {
            exportableArbNodeAdmin =
                SecureProxy.create(
                    ArbNodeAdminImpl.this,
                    arbNodeService.getArbNodeSecurity().getAccessChecker(),
                    faultHandler);
            logger.info(
                "Successfully created secure proxy for the arbnode admin");
        } catch (ConfigurationException ce) {
            logger.info("Unable to create proxy: " + ce + " : " +
                        ce.getMessage());
            throw new IllegalStateException("Unable to create proxy", ce);
        }
    }

    /**
     * Unbind the admin entry from the registry.
     *
     * If any exceptions are encountered, during the unbind, they are merely
     * logged and otherwise ignored, so that other components can continue
     * to be shut down.
     */
    public void stop() {
        try {
            arbNodeService.unbind(exportableArbNodeAdmin,
                                  RegistryUtils.InterfaceType.ADMIN);
            logger.info("ArbNodeAdmin stopping");
            if (rti != null) {
                rti.stop(SerialVersion.CURRENT);
            }

        } catch (RemoteException e) {
            logger.log(Level.INFO,
                       "Ignoring exception while stopping arbNodeAdmin", e);
            return;
        }
    }

    @Override
    @PublicMethod
    public ArbNodeStatus ping(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<ArbNodeStatus>() {

            @Override
            public ArbNodeStatus execute() {
                ServiceStatus status =
                    arbNodeService.getStatusTracker().getServiceStatus();
                State state = State.DETACHED;
                long currentVLSN = 0;

                try {
                    ArbiterStats sg = arbNode.getStats(false);
                    if (sg != null) {
                        currentVLSN = sg.getVLSN();
                        state = State.valueOf(sg.getState());
                    }
                } catch (EnvironmentFailureException ignored) {

                    /*
                     * The environment could be invalid.
                     */
                }
                String haHostPort =
                            arbNode.getArbNodeParams().getJENodeHostPort();
                return new ArbNodeStatus(status,
                                         currentVLSN, state, haHostPort);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public ArbNodeInfo getInfo(AuthContext authCtx, short serialVersion)
            throws RemoteException {
        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<ArbNodeInfo>() {

            @Override
            public ArbNodeInfo execute() {
                return new ArbNodeInfo(arbNode);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean updateMemberHAAddress(final String groupName,
                                         final String targetNodeName,
                                         final String targetHelperHosts,
                                         final String newNodeHostPort,
                                         AuthContext authCtx,
                                         short serialVersion)
                                         throws RemoteException {
        return faultHandler.execute
                (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {

                @Override
                public Boolean execute() {
                    assertRunning();
                    try {
                        arbNodeService.updateMemberHAAddress(groupName,
                                                             targetNodeName,
                                                             targetHelperHosts,
                                                             newNodeHostPort);
                        return true;
                    } catch (UnknownMasterException e) {
                        return false;
                    }
                }
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void installStatusReceiver(final ArbNodeStatusReceiver receiver,
            AuthContext authCtx, short serialVersion) throws RemoteException {
        statusReceiver = receiver;

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {

            @Override
            public void execute() throws RemoteException {
                ServiceStatusTracker anStatusTracker =
                    arbNodeService.getStatusTracker();
                anStatusTracker.addListener(new StatusListener(), receiver);

                ArbStatsTracker arbStatsTracker =
                    arbNodeService.getStatsTracker();
                arbStatsTracker.addListener(new ArbStatsListener());

                arbNodeService.addParameterListener
                    (new ParameterChangeListener());
            }
        });
    }

    /**
     * The status listener for updating the status receiver.
     */
    private class StatusListener implements ServiceStatusTracker.Listener {
        @Override
        public void update(ServiceStatusChange prevStatus,
                           ServiceStatusChange newStatus) {

            try {
                ArbNodeAdminImpl.this.statusReceiver.updateNodeStatus(
                    newStatus);
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log(Level.INFO,
                           "Failure to deliver status update to SNA", re);
                return;
            }
        }
    }

    /**
     * The perf stats listener for updating the status receiver.
     */
    private class ArbStatsListener implements ArbStatsTracker.Listener {
        @Override
        public void receiveStats(StatsPacket packet) {
            try {
                ArbNodeAdminImpl.this.statusReceiver.receiveStats(packet);
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log(Level.INFO,
                           "Failure to deliver perf stats to MgmtAgent: " +
                           re.getMessage());
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
                ArbNodeAdminImpl.this.statusReceiver.receiveNewParams(newMap);
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log(Level.INFO,
                           "Failure to deliver parameter change to MgmtAgent",
                           re);
                return;
            }
        }
    }
}

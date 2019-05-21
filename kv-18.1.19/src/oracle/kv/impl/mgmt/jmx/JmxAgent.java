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

package oracle.kv.impl.mgmt.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectInstance;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.mgmt.AgentInternal;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.sna.ServiceManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.ssl.SSLServerSocketFactory;

import com.sleepycat.je.rep.StateChangeEvent;

public class JmxAgent extends AgentInternal {

    final static String DOMAIN = "Oracle NoSQL Database";
    public final static String JMX_SSF_NAME = "jmxrmi";
    final static String JMX_CSF_NAME = "jmxrmi";

    private final MBeanServer server;
    private JMXConnectorServer connector;
    private final StorageNode snMBean;
    private final Map<RepNodeId, RepNode> rnMap = new HashMap<RepNodeId, RepNode>();
    private final Map<ArbNodeId, ArbNode>
        anMap = new HashMap<ArbNodeId, ArbNode>();
    private Admin admin;
    private static RMISocketPolicy jmxRMIPolicy;

    /**
     * The constructor is found by reflection and must match this signature.
     * However, the port and hostname arguments are not used by this
     * implementation.
     */
    public JmxAgent(StorageNodeAgent sna,
                    @SuppressWarnings("unused") int pollingPort,
                    @SuppressWarnings("unused") String trapHostName,
                    @SuppressWarnings("unused") int trapPort,
                    ServiceStatusTracker tracker) {

        super(sna);

        server = MBeanServerFactory.createMBeanServer();

        JMXServiceURL url = makeUrl();

        try {

            Map<String, Object> env = new HashMap<String, Object>();
            SocketFactoryPair jmxSFP = getJMXSFP();
            if (jmxSFP != null) {
                if (jmxSFP.getServerFactory() != null &&
                    jmxSFP.getClientFactory() != null) {

                    /*
                     * If using SSL, force the CSF to use the standard CSF
                     * class because jconsole won't have access to KVStore
                     * internal ones.
                     */
                    if (jmxSFP.getServerFactory().getClass() ==
                        SSLServerSocketFactory.class)  {
                        /*
                         * All SslRMIClientSocketFactory in the same JVM
                         * will use a single instance of SSLSocketFactory to
                         * create sockets. After here, JMXCollectorAgent also
                         * need a SslRMIClientSocketFactory, but must with a
                         * trust file. So we must set trust file property
                         * here, otherwise JMXCollectorAgent can only get a
                         * SslRMIClientSocketFactory without trust file.
                         */
                        final String trustFile = sna.getStoreTrustFile();
                        if (trustFile != null) {
                            System.setProperty("javax.net.ssl.trustStore",
                                               trustFile);
                        }
                        env.put(RMIConnectorServer.
                                RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                                new SslRMIClientSocketFactory());
                    }
                }
                if (jmxSFP.getServerFactory() != null) {
                    env.put(RMIConnectorServer.
                            RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                            jmxSFP.getServerFactory());

                    if (jmxSFP.getServerFactory().getClass() ==
                        SSLServerSocketFactory.class &&
                        jmxSFP.getClientFactory() != null) {

                        /*
                         * Needed so that we can bind in the SSL registry.
                         * Unfortunately, there doesn't appear to be a
                         * published mechanism for doing this.
                         */
                        env.put("com.sun.jndi.rmi.factory.socket",
                                jmxSFP.getClientFactory());
                    }
                }
            }
            connector = JMXConnectorServerFactory.newJMXConnectorServer
                (url, env, server);
            connector.start();

        } catch (IOException e) {
            throw new IllegalStateException
                ("Unexpected error creating JMX connector.", e);
        }

        addPlatformMBeans();

        snMBean = new StorageNode(this, server);

        setSnaStatusTracker(tracker);
    }

    /**
     * Set the SFP for JMX object exporting in preparation for construction
     * of JmxAgent instances.
     */
    public static void setRMISocketPolicy(RMISocketPolicy jmxRMIPolicy) {
        JmxAgent.jmxRMIPolicy = jmxRMIPolicy;
    }

    private SocketFactoryPair getJMXSFP() {
        if (jmxRMIPolicy == null) {
            return null;
        }

        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(JMX_SSF_NAME).setCsfName(JMX_CSF_NAME);
        return jmxRMIPolicy.getBindPair(args);
    }

    @Override
    public boolean checkParametersEqual(int pollp, String traph, int trapp) {
        /* JMX doesn't use these parameters, so always return true. */
        return true;
    }

    /**
     * Add the platform MBeans as proxies.  See [#22267].
     */
    private void addPlatformMBeans() {
        MBeanServer platformServer = ManagementFactory.getPlatformMBeanServer();

        Set<ObjectInstance> beans =
            platformServer.queryMBeans(null, null);

        for (ObjectInstance oi : beans) {

            try {
                Class<?> c =
                    getMBeanInterfaceClass(Class.forName(oi.getClassName()));
                /*
                 * If no complying MBean interface was found, skip this one.
                 * This is unexpected, but we should be able to carry on
                 * without it.
                 */
                if (c == null) {
                    sna.getLogger().warning
                        ("Unexpected non-compliant platform MBean impl " +
                         oi.getClassName() +
                         " found.  Forgoing proxy creation.");
                    continue;
                }

                /*
                 * If it is the MBeanServerDelegate, just skip it; we already
                 * have one of those.
                 */
                if (c.getName().equals
                    ("javax.management.MBeanServerDelegateMBean")) {
                    continue;
                }

                Object o = ManagementFactory.newPlatformMXBeanProxy
                    (platformServer, oi.getObjectName().toString(), c);

                server.registerMBean(o, oi.getObjectName());
            } catch (Exception e) {
                /*
                 * If anything goes wrong for one of these, just log
                 * the error and skip it.  For most purposes this will do.
                 */
                sna.getLogger().log
                    (Level.WARNING,
                     "Unexpected error creating platform mbean proxy for " +
                     oi.getClassName(), e);
            }
        }
    }

    /**
     * Find an MBean interface in this class's ancestry.
     */
    private static Class<?> getMBeanInterfaceClass(Class<?> c) {
        while (c != null) {
            String name = c.getName();
            if (name.endsWith("MBean") || name.endsWith("MXBean")) {
                return c;
            }
            Class<?>[] interfaces = c.getInterfaces();
            for (Class<?> i : interfaces) {
                Class<?> j = getMBeanInterfaceClass(i);
                if (j != null) {
                    return j;
                }
            }
            c = c.getSuperclass();
        }
        return null;
    }

    /* Construct a URL for the JMX service.  If the port range is restricted
     * grab a port from the range; otherwise use an anonymous port.
     */
    private JMXServiceURL makeUrl() {
        StringBuffer sb;
        if (!restrictPortRange()) {
            sb = new StringBuffer("service:jmx:rmi:///jndi/rmi://");
        } else {
            sb = new StringBuffer("service:jmx:rmi://");
            sb.append(getHostname());
            sb.append(":");
            sb.append(getFreePort());
            sb.append("/jndi/rmi://");
        }
        sb.append(getHostname());
        sb.append(":");
        sb.append(getRegistryPort());
        sb.append("/jmxrmi");      /* Use the standard JMX service name. */

        try {
            return new JMXServiceURL(sb.toString());
        } catch (MalformedURLException e) {
            throw new IllegalStateException
                ("Unexpected error constructing JMX service URL (" +
                 sb.toString(), e);
        }
    }

    @Override
    public void addRepNode(RepNodeParams rnp, ServiceManager mgr)
        throws Exception {

        final RepNodeId rnId = rnp.getRepNodeId();
        RepNode rn = new RepNode(rnp, server, snMBean);

        rnMap.put(rnId, rn);

        addServiceManagerListener(rnId, mgr);
    }

    @Override
    public void removeRepNode(RepNodeId rnid) {
        unexportStatusReceiver(rnid);
        RepNode rn = rnMap.get(rnid);
        if (rn != null) {
            rn.unregister();
        }
        rnMap.remove(rnid);
    }

    @Override
    public void addAdmin(AdminParams ap, ServiceManager mgr)
        throws Exception {

        admin = new Admin(ap, server, snMBean);
        addAdminServiceManagerListener(mgr);
    }

    @Override
    public void removeAdmin() {
        unexportAdminStatusReceiver();
        if (admin != null) {
            admin.unregister();
        }
        admin = null;
    }

    @Override
    public void shutdown() {

        super.shutdown();

        snMBean.unregister();

        for (RepNode rn : rnMap.values()) {
            rn.unregister();
        }
        rnMap.clear();

        if (admin != null) {
            admin.unregister();
            admin = null;
        }

        try {
            connector.stop();
        } catch (IOException e) {

            /*
             * This exception occurs when shutting down the StorageNodeAgent,
             * and here's why: Connector.stop() attempts to unregister
             * the connector from the RMI registry, but the registry has
             * already been cleaned up and unexported by the time we reach
             * here.
             *
             * The beneficial effect of calling stop() is to terminate the
             * connector's listener thread, which is all we are interested in
             * at this point.  The thread termination occurs before the
             * exception is thrown, so all is hunky dory.
             */
        }

        MBeanServerFactory.releaseMBeanServer(server);
    }

    @Override
    public void updateSNStatus(ServiceStatusChange p, ServiceStatusChange n) {
        snMBean.setServiceStatus(n.getStatus());
    }

    @Override
    protected void updateRepNodeStatus(RepNodeId which,
                                       ServiceStatusChange newStatus) {
        RepNode rn = rnMap.get(which);
        if (rn == null) {
            sna.getLogger().warning
                ("Updating service status, RepNode MBean not found for " +
                 which.getFullName());
            return;
        }

        sna.getLogger().info
            ("Updating service status, node: " + which.getFullName() +
             ", service status: " + newStatus.getStatus());
        rn.setServiceStatus(newStatus.getStatus());
    }

    @Override
    protected void updateReplicationState(RepNodeId which,
                                          StateChangeEvent sce) {
        RepNode rn = rnMap.get(which);
        if (rn == null) {
            sna.getLogger().warning
                ("Updating replication state, RepNode MBean not found for " +
                 which.getFullName());
            return;
        }

        sna.getLogger().info
            ("Updating replication state, node: " + which.getFullName() +
            ", replication state: " + sce.getState());
        rn.updateReplicationState(sce);
    }

    @Override
    protected void updateRepNodePerfStats(RepNodeId which, StatsPacket packet) {
        RepNode rn = rnMap.get(which);
        if (rn == null) {
            sna.getLogger().warning
                ("Updating perf stats, RepNode MBean not found for " +
                 which.getFullName());
            return;
        }

        rn.setPerfStats(packet);
    }

    @Override
    protected void updateRepNodeParameters(RepNodeId which, ParameterMap map) {
        RepNode rn = rnMap.get(which);
        if (rn == null) {
            sna.getLogger().warning
                ("Updating parameters, RepNode MBean not found for " +
                 which.getFullName());
            return;
        }

        RepNodeParams rnp = new RepNodeParams(map);
        rn.setParameters(rnp);
    }

    @Override
    protected void updateArbNodeStatus(ArbNodeId which,
                                       ServiceStatusChange newStatus) {
        ArbNode an = anMap.get(which);
        if (an == null) {
            sna.getLogger().warning
                ("Updating service status, ArbNode MBean not found for " +
                 which.getFullName());
            return;
        }

        an.setServiceStatus(newStatus.getStatus());
    }

    @Override
    protected void updateArbNodePerfStats(ArbNodeId which, StatsPacket packet) {
        ArbNode an = anMap.get(which);
        if (an == null) {
            sna.getLogger().warning
                ("Updating perf stats, ArbNode MBean not found for " +
                 which.getFullName());
            return;
        }

        an.setPerfStats(packet);
    }

    @Override
    protected void updateArbNodeParameters(ArbNodeId which, ParameterMap map) {
        ArbNode an = anMap.get(which);
        if (an == null) {
            sna.getLogger().warning
                ("Updating parameters, ArbNode MBean not found for " +
                 which.getFullName());
            return;
        }

        ArbNodeParams anp = new ArbNodeParams(map);
        an.setParameters(anp);
    }

    @Override
    public void updateAdminParameters(ParameterMap newMap) {
        AdminParams ap = new AdminParams(newMap);
        admin.setParameters(ap);
    }

    @Override
    public void updateAdminStatus(ServiceStatusChange newStatus,
                                  boolean isMaster) {

        admin.setServiceStatus(newStatus.getStatus(), isMaster);
    }

    @Override
    public void updatePlanStatus(String planStatus) {
        admin.updatePlanStatus(planStatus);
    }

    @Override
    public void addArbNode(ArbNodeParams anp, ServiceManager mgr)
        throws Exception {

        final ArbNodeId anId = anp.getArbNodeId();
        ArbNode an = new ArbNode(anp, server, snMBean);
        anMap.put(anId, an);
        addServiceManagerListener(anId, mgr);
    }

    @Override
    public void removeArbNode(ArbNodeId anid) {
        unexportStatusReceiver(anid);
        ArbNode an = anMap.get(anid);
        if (an != null) {
            an.unregister();
        }
        anMap.remove(anid);
    }
}

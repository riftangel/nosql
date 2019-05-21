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

package jmx;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import oracle.kv.mgmt.jmx.AdminMXBean;
import oracle.kv.mgmt.jmx.RepNodeMXBean;
import oracle.kv.mgmt.jmx.StorageNodeMXBean;

/**
 * SnaJmxClient is a JMX client that connects to a NoSQL Database
 * StorageNodeAgent's JMX service and displays properties of MXBeans found
 * there.  The connection can be made via regular RMI or via RMI over SSL,
 * depending on the command line arguments given to the program.
 *
 * Description of flags:
 *  -host    -- the host where a JMX-enabled StorageNodeAgent is running
 *  -port    -- the registry port belonging to the StorageNodeAgent
 *  -trust   -- optional pathname to the SNA's client.trust file.
 *              If -trust is given, then the JMX connection is via SSL.
 *  -verbose -- optional flag; if present, every property of the
 *              MXBeans is shown; otherwise only status is shown.
 *  -listen  -- optional flag; if present, the program listens indefinitely
 *              for events from the StorageNodeAgentMXBean.  If absent, the
 *              program exits after displaying properties.
 */

public class SnaJmxClient {

    final static String usage =
        "Usage: SnaJmxClient -host <host> -port <port> " +
        "[-trust <path>] [-verbose] [-listen]";

    final static ObjectName storageNodeMBeanName;
    final static ObjectName adminMBeanName;
    final static ObjectName repNodeMBeanWildcardName;
    static {
        try {
            storageNodeMBeanName =
                new ObjectName("Oracle NoSQL Database:type=StorageNode");
            adminMBeanName =
                new ObjectName("Oracle NoSQL Database:type=Admin");
            repNodeMBeanWildcardName =
                    new ObjectName("Oracle NoSQL Database:type=RepNode,id=*");
        } catch (MalformedObjectNameException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    final JMXConnector jmxc;
    final MBeanServerConnection mbsc;
    final boolean verbose;

    public static void main(String argv[]) throws Exception {

        String host = null;
        int port = 0;
        String trustStore = null;
        boolean verbose = false;
        boolean listen = false;

        for (int i = 0; i < argv.length; i++) {
            if ("-host".equals(argv[i])) {
                host = argv[++i];
            } else if ("-port".equals(argv[i])) {
                port = Integer.parseInt(argv[++i]);
            } else if ("-trust".equals(argv[i])) {
                trustStore = argv[++i];
            } else if ("-verbose".equals(argv[i])) {
                verbose = true;
            } else if ("-listen".equals(argv[i])) {
                listen = true;
            } else {
                System.err.println(usage);
                System.exit(1);
            }
        }

        if (host == null || port == 0) {
            System.err.println(usage);
            System.exit(1);
        }

        /* If we will use SSL, set the trustStore property */
        if (trustStore != null) {
            System.getProperties().setProperty
                ("javax.net.ssl.trustStore", trustStore);
        }

        SnaJmxClient client =
            new SnaJmxClient(host, port, trustStore != null, verbose);

        client.doStatusPoll();
        if (listen) {
            System.out.println("Waiting for notifications...");
            client.listen();
            /* The listener thread is a daemon, so we hang the main thread to
             * let the listener thread run forever, or until the user gets
             * tired and hits C-c.
             */
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    SnaJmxClient(String host, int port, boolean useSsl, boolean verbose)
        throws Exception {

        this.verbose = verbose;

        /* This URL is the standard JMX service descriptor. */
        JMXServiceURL url =
            new JMXServiceURL
            ("service:jmx:rmi:///jndi/rmi://" + host +":" +
             port + "/jmxrmi");

        /* If using SSL, set up the appropriate SocketFactories. */
        Map<String, Object> env = null;
        if (useSsl) {
            SslRMIClientSocketFactory csf = new SslRMIClientSocketFactory();
            env = new HashMap<String, Object>();
            env.put("com.sun.jndi.rmi.factory.socket", csf);
        }

        jmxc = JMXConnectorFactory.connect(url, env);
        mbsc = jmxc.getMBeanServerConnection();
    }

    private void doStatusPoll()
        throws Exception {

        /* There is always exactly one StorageNodeMXBean, because it represents
         * the service that provides the JMX interface.
         */
        StorageNodeMXBean snmb =
            JMX.newMBeanProxy(mbsc, storageNodeMBeanName,
                              StorageNodeMXBean.class, true);

        System.out.println("SNA sn" + snmb.getSnId() +
                           ", status is " + snmb.getServiceStatus());

        if (verbose) {
            dumpAllProperties(snmb);
        }

        /* There is either zero or one AdminMXBean.  We use the queryNames
         * interface with a non-wildcard ObjectName, so we can tell which.
         * The set returned will either be empty or have a single member.
         */
        Set<ObjectName> adminNames = mbsc.queryNames(adminMBeanName, null);
        if (adminNames.size() > 0) {
            AdminMXBean amb =
                JMX.newMBeanProxy(mbsc, adminMBeanName,
                                  AdminMXBean.class, true);
            System.out.println("Admin admin" + amb.getAdminId() +
                               ", status is " + amb.getServiceStatus());
            if (verbose) {
                dumpAllProperties(amb);
            }
        }

        /* There can be any number of RepNodeMBeans. */
        Set<ObjectName> repNodeNames =
            mbsc.queryNames(repNodeMBeanWildcardName, null);

        for (ObjectName on : repNodeNames) {
            RepNodeMXBean rnmb =
                JMX.newMBeanProxy(mbsc, on, RepNodeMXBean.class, true);

            System.out.println("RepNode " + rnmb.getRepNodeId() +
                               ", status is " + rnmb.getServiceStatus());
            if (verbose) {
                dumpAllProperties(rnmb);
            }
        }
    }

    /* Use Introspector to find all the property accessors in the MBean, and
     * invoke them.  I am not sure why notificationInfo is always here, but we
     * filter it out.
     */
    private void dumpAllProperties(Object mbean) throws Exception {
        BeanInfo info =
            Introspector.getBeanInfo(mbean.getClass(), Object.class);
        for(PropertyDescriptor pd : info.getPropertyDescriptors()) {
            if (pd == null || "notificationInfo".equals(pd.getName())) {
                continue;
            }
            Method m = pd.getReadMethod();
            System.out.println("    " + pd.getName() + ": " + m.invoke(mbean));
        }
    }

    /*
     * Add a listener for StorageNode events.  These include events from the
     * StorageNodeAgent itself along with those for all of the
     * StorageNodeAgent's managed service.  It is also possible to listen for
     * events for a single managed service, but this is simple and does what is
     * needed for demo purposes.
     */
    private void listen() throws Exception {
        mbsc.addNotificationListener
            (storageNodeMBeanName, new SNAListener(), null, null);
    }

    public class SNAListener implements NotificationListener {
        @Override
        public void handleNotification(Notification notif,
                                       Object handback) {

            System.out.println("Notification received: " + notif.getMessage());
        }
    }
}

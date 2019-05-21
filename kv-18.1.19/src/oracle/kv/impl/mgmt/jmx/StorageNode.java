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

import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.mgmt.jmx.StorageNodeMXBean;

public class StorageNode
    extends NotificationBroadcasterSupport
    implements StorageNodeMXBean {

    final private JmxAgent agent;
    final private MBeanServer server;
    private ObjectName oName;
    ServiceStatus status;
    long notifySequence = 1L;

    final private static String
        NOTIFY_SN_STATUS_CHANGE = "oracle.kv.storagenode.status";

    public StorageNode(JmxAgent agent, MBeanServer server) {
        this.agent =  agent;
        this.server = server;
        status = ServiceStatus.UNREACHABLE;
        register();
    }

    private void register() {

        /* There is only one StorageNode, so no need to identify it further. */
        StringBuffer buf = new StringBuffer(JmxAgent.DOMAIN);
        buf.append(":type=StorageNode");

        try {
            oName = new ObjectName(buf.toString());
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException
                ("Unexpected exception creating JMX ObjectName " +
                 buf.toString(), e);
        }

        try {
            server.registerMBean(this, oName);
        } catch (Exception e) {
            throw new IllegalStateException
                ("Unexpected exception registring MBean " + oName.toString(),
                 e);
        }
    }

    public void unregister() {
        if (oName != null) {
            try {
                server.unregisterMBean(oName);
            } catch (Exception e) {
                throw new IllegalStateException
                    ("Unexpected exception while unregistring MBean " +
                     oName.toString(), e);
            }
        }
    }

    public synchronized void setServiceStatus(ServiceStatus newStatus) {
        if (status.equals(newStatus)) {
            return;
        }

        Notification n = new Notification
            (NOTIFY_SN_STATUS_CHANGE, oName, notifySequence++,
             System.currentTimeMillis(),
             "The service status for the StorageNode has" +
             " changed to " + newStatus.toString() + ".");

        n.setUserData(newStatus.toString());

        sendNotification(n);

        status = newStatus;
    }

    /**
     * Send a RepNode's , ArbNode's or Admin's notifications
     * from the StorageNodeMBean.
     * This is convenient because RepNodes can come and go, and notification
     * subscriptions on Repnodes do not survive this death and resurrection.  A
     * client can subscribe to StorageNodeMBean events, and receive events for
     * all the RepNodes managed by this storageNode.
     */
    public synchronized void sendProxyNotification(Notification n) {

        sendNotification(n);
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {

        return new MBeanNotificationInfo[]
        {
            new MBeanNotificationInfo
                (new String[]{NOTIFY_SN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in this StorageNode's service status."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in a RepNode's service status."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_SINGLE_TFLOOR},
                 Notification.class.getName(),
                 "Single-operation throughput floor violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_SINGLE_LCEILING},
                 Notification.class.getName(),
                 "Single-operation latency ceiling violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_MULTI_TFLOOR},
                 Notification.class.getName(),
                 "Multi-operation throughput floor violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_MULTI_LCEILING},
                 Notification.class.getName(),
                 "Multi-operation latency ceiling violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_OP_METRIC},
                 Notification.class.getName(),
                 "New operation performance metrics are available."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_TABLE_METRIC},
                 Notification.class.getName(),
                 "New RepNode table metrics are available."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_JVM_STATS},
                 Notification.class.getName(),
                 "New RepNode JVM stats are available."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_EXCEPTION_METRIC},
                 Notification.class.getName(),
                 "New RepNode exception metrics are available."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_ENV_METRIC},
                 Notification.class.getName(),
                 "New statistics are available."),
            new MBeanNotificationInfo
                (new String[]{Admin.NOTIFY_ADMIN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in the Admin's service status."),
            new MBeanNotificationInfo
                (new String[]{ArbNode.NOTIFY_AN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in a ArbNode's service status."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_REPLICATION_STATE},
                 Notification.class.getName(),
                 "Announce a change in a RepNode's replication state."),
            new MBeanNotificationInfo
                (new String[]{Admin.NOTIFY_PLAN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a plan status change.")
        };
    }

    @Override
    public String getServiceStatus() {
        return status.toString();
    }

    @Override
    public boolean isHostingAdmin() {
        return agent.isHostingAdmin();
    }

    @Override
    public String getRootDirPath() {
        return agent.getRootDir();
    }

    @Override
    public String getStoreName() {
        return agent.getStoreName();
    }

    @Override
    public String getHostname() {
        return agent.getHostname();
    }

    @Override
    public int getRegistryPort() {
        return agent.getRegistryPort();
    }

    @Override
    public String getHAHostname() {
        return agent.getHAHostname();
    }

    @Override
    public int getCapacity() {
        return agent.getCapacity();
    }

    @Override
    public int getLogFileLimit() {
        return agent.getLogFileLimit();
    }

    @Override
    public int getLogFileCount() {
        return agent.getLogFileCount();
    }

    @Override
    public String getHaPortRange() {
        return agent.getSnHaPortRange();
    }

    @Override
    public int getSnId() {
        return agent.getSnId();
    }

    @Override
    public int getMemoryMB() {
        return agent.getMemoryMB();
    }

    @Override
    public int getNumCPUs() {
        return agent.getNumCpus();
    }

    @Override
    public String getMountPoints() {
        return agent.getMountPointsString();
    }

    @Override
    public String getRNLogMountPoints() {
        return agent.getRNLogMountPointsString();
    }

    @Override
    public String getAdminMountPoints() {
        return agent.getAdminMountPointsString();
    }

    @Override
    public long getCollectorInterval() {
        return agent.getCollectorInterval();
    }
}

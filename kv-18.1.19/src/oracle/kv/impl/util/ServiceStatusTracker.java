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

package oracle.kv.impl.util;

import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.mgmt.NodeStatusReceiver;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * Tracks service level state changes, so that it can be conveniently
 * monitored and tested.
 */
public class ServiceStatusTracker {

    private ServiceStatusChange prev = null;
    private ServiceStatusChange current =
        new ServiceStatusChange(ServiceStatus.STARTING);

    /**
     * The listeners that are invoked on each update.
     */
    private final List<Listener> listeners;

    /**
     * The logger used to log service state changes.
     */
    private Logger logger;

    /**
     * Create a ServiceStatusTracker when the MonitorAgent repository isn't
     * yet available.
     */
    public ServiceStatusTracker(Logger logger) {
        this.logger = logger;
        listeners = new LinkedList<Listener>();
    }

    /**
     * Create a ServiceStatusTracker and attach a MonitorAgent listener.
     */
    public ServiceStatusTracker
        (Logger logger, AgentRepository agentRepository) {
        this(logger);
        addListener(new StatusMonitor(agentRepository));
    }

    /**
     * Returns the instantaneous service status
     */
    public ServiceStatus getServiceStatus() {
        return current.getStatus();
    }

    /**
     * Reset logger
     */
    public synchronized void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Add a new listener
     */
    public synchronized void addListener(Listener listener) {
        listeners.add(listener);
    }

    /**
     * Add a listener that connects this status tracker to the monitor agent.
     */
    public synchronized void addListener(AgentRepository agentRepository) {
        listeners.add(new StatusMonitor(agentRepository));
    }

    /**
     * Add a new listener and send the current status to receiver.
     * @throws RemoteException 
     */
    public synchronized void addListener(Listener listener,
                                         NodeStatusReceiver receiver)
        throws RemoteException {

        ServiceStatusChange s = new ServiceStatusChange(current.getStatus());
        receiver.updateNodeStatus(s);
        listeners.add(listener);
    }

    /**
     * Updates the service status if it has changed. Upon a change it invokes
     * any listeners that may be registered to listen to such changes.
     */
    public synchronized void update(ServiceStatus newStatus) {
        if (current.getStatus().equals(newStatus)) {
            current.updateTime();
            return;
        }

        prev = current;
        current = new ServiceStatusChange(newStatus);

        logger.info("Service status changed from " + prev.getStatus() +
                    " to " + newStatus);

        for (Listener listener : listeners) {
            listener.update(prev, current);
        }

    }

    /**
     * A Listener used for monitoring and test purposes.
     */
    public interface Listener {
        void update(ServiceStatusChange prevStatus,
                    ServiceStatusChange newStatus);
    }

    /**
     * Sends status changes to the monitor agent.
     */
    private class StatusMonitor implements Listener {

        private final AgentRepository monitorAgentBuffer;

        StatusMonitor(AgentRepository monitorAgentBuffer) {
            this.monitorAgentBuffer = monitorAgentBuffer;
        }

        @Override
        public void update(ServiceStatusChange prevStatus,
                           ServiceStatusChange newStatus) {
            monitorAgentBuffer.add(newStatus);
        }
    }
}

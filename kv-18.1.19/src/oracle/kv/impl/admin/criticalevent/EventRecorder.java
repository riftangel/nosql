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

package oracle.kv.impl.admin.criticalevent;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.Tracker.EventHolder;
import oracle.kv.impl.monitor.TrackerListenerImpl;
import oracle.kv.impl.monitor.views.LogTracker;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.PerfTracker;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.monitor.views.ServiceStatusTracker;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.persist.model.Persistent;

public class EventRecorder {

    private final Admin admin;
    private final ServiceStatusTracker statusTracker;
    private final PerfTracker perfTracker;
    private final LogTracker logTracker;
    private final TrackerListenerImpl statusListener;
    private final TrackerListenerImpl perfListener;
    private final TrackerListenerImpl logListener;
    private Thread workerThread;
    private boolean workerThreadGo;
    private boolean isShutdown;
    private LatestEventTimestamps timestamps;
    private final List<SyncWaiter> syncWaiters = new ArrayList<>();

    public EventRecorder(final Admin admin) {

        this.admin = admin;

        /*
         * Listeners are created with the interesting time of 0; later, they
         * are given the real timestamps (from the admin database) in start().
         */
        final ServerSocketFactory ssf =
            admin.getParams().getStorageNodeParams().getAdminListenerSSF();

        try {
            statusListener = new Listener(ssf, 0L);
            perfListener = new Listener(ssf, 0L);
            logListener = new Listener(ssf, 0L);
        } catch (RemoteException re) {
            throw new IllegalStateException
                ("Creating an event listener failed. ", re);
        }

        isShutdown = false;
        /* Register our three listeners. */
        Monitor m = admin.getMonitor();
        statusTracker = m.getServiceChangeTracker();
        statusTracker.registerListener(statusListener);

        perfTracker = m.getPerfTracker();
        perfTracker.registerListener(perfListener);

        logTracker = m.getLogTracker();
        logTracker.registerListener(logListener);

        /* Spawn a thread to poll for events. */
        workerThread = new StoppableThread(null, admin.getExceptionHandler(),
                                           EventRecorder.class.getSimpleName())
        {
            @Override
            public void run() {
                try {
                    eventRecorderWorker();
                } catch (UnknownMasterException ume) {
                    /*
                     * Master state changed; can no longer persist events exit
                     * worker.
                     */
                    admin.getLogger().info("Master transition. " +
                                           "Exiting event recorder:" +
                                           ume.getMessage());
                } catch (ReplicaWriteException rwe) {
                    /* Handle as above. */
                    admin.getLogger().info("Master transition " +
                                           "Exiting event recorder:" +
                                           rwe.getMessage());
                } finally {
                    /* Clean up. */
                    workerThreadGo = false;
                    for (SyncWaiter s : syncWaiters) {
                        s.setNotified();
                    }
                }
            }
            @Override
            protected Logger getLogger() {
                return admin.getLogger();
            }
        };

        workerThreadGo = false;
    }

    public void start(LatestEventTimestamps let) {
        timestamps = let;
        statusListener.setInterestingTime(let.getStatusTimestamp());
        perfListener.setInterestingTime(let.getPerfTimestamp());
        logListener.setInterestingTime(let.getLogTimestamp());
        workerThreadGo = true;
        workerThread.start();
    }

    private void eventRecorderWorker() {

        while (true) {
            Tracker.RetrievedEvents<ServiceChange> statusEventsContainer;
            List<Tracker.EventHolder<ServiceChange>> statusEvents;
            Tracker.RetrievedEvents<PerfEvent> perfEventsContainer;
            List<Tracker.EventHolder<PerfEvent>> perfEvents;
            Tracker.RetrievedEvents<LogRecord> logEventsContainer;
            List<Tracker.EventHolder<LogRecord>> logEvents;

            synchronized(this) {
                while (true) {
                    /* If we're shutting down, exit the thread. */
                    if (!workerThreadGo) {
                        return;
                    }

                    /* Poll the three trackers for events of interest. */
                    int nEvents = 0;
                    statusEventsContainer = statusTracker.retrieveNewEvents
                        (timestamps.getStatusTimestamp());
                    nEvents += statusEventsContainer.size();

                    perfEventsContainer = perfTracker.retrieveNewEvents
                        (timestamps.getPerfTimestamp());
                    nEvents += perfEventsContainer.size();

                    logEventsContainer = logTracker.retrieveNewEvents
                        (timestamps.getLogTimestamp());
                    nEvents += logEventsContainer.size();

                    if (nEvents != 0) {
                        /*
                         * There is something to see; don't drop into wait.
                         */
                        break;
                    }

                    /*
                     * Before waiting, notify any sync waiters that we
                     * completed a cycle.
                     */
                    for (SyncWaiter s : syncWaiters) {
                        s.setNotified();
                    }

                    /*
                     * Wait for Listener.onNewEventsPresent.
                     */
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                    }
                }
            }

            if (statusEventsContainer.size() != 0) {
                /*
                 * Remember the timestamp of the last record we retrieved; this
                 * will become the "since" argument in the next request.  Also,
                 * the timestamps will be stored in the database for recovery's
                 * purposes.
                 */
                long statusSince =
                    statusEventsContainer.getLastSyntheticTimestamp();
                timestamps.setStatusTimestamp(statusSince);

                /*
                 * Move the interesting time up, so older items can be
                 * deleted.
                 */
                statusListener.setInterestingTime(statusSince);

                /*
                 * The number of recordable events is a subset of all events.
                 * There may be no recordable events at all.
                 */
                statusEvents = statusEventsContainer.getRecordableEvents();
                if (!statusEvents.isEmpty()) {
                    storeStatusEvents(statusEvents);
                }
            }

            /*
             * These next two blocks follow the pattern established above.
             */
            if (perfEventsContainer.size() != 0) {
                long perfSince =
                    perfEventsContainer.getLastSyntheticTimestamp();
                timestamps.setPerfTimestamp(perfSince);
                perfListener.setInterestingTime(perfSince);
                perfEvents = perfEventsContainer.getRecordableEvents();
                if (!perfEvents.isEmpty()) {
                    storePerfEvents(perfEvents);
                }
            }

            if (logEventsContainer.size() != 0) {
                long logSince =
                    logEventsContainer.getLastSyntheticTimestamp();
                timestamps.setLogTimestamp(logSince);
                logListener.setInterestingTime(logSince);
                logEvents = logEventsContainer.getRecordableEvents();
                if (!logEvents.isEmpty()) {
                    storeLogEvents(logEvents);
                }
            }
        }
    }

    private void storeStatusEvents
        (List<Tracker.EventHolder<ServiceChange>> statusEvents) {

        final List<CriticalEvent> pevents = new ArrayList<>();

        for (EventHolder<ServiceChange> eh : statusEvents) {
            pevents.add(new CriticalEvent(eh.getSyntheticTimestamp(),
                                          eh.getEvent()));
        }
        admin.storeEvents(pevents, timestamps);
    }

    private void storePerfEvents
        (List<Tracker.EventHolder<PerfEvent>> perfEvents) {

        final List<CriticalEvent> pevents = new ArrayList<>();

        for (EventHolder<PerfEvent> eh : perfEvents) {
            pevents.add(new CriticalEvent(eh.getSyntheticTimestamp(),
                                          eh.getEvent()));
        }
        admin.storeEvents(pevents, timestamps);
    }

    private void storeLogEvents
        (List<Tracker.EventHolder<LogRecord>> logEvents) {

        final List<CriticalEvent> pevents = new ArrayList<>();

        for (EventHolder<LogRecord> eh : logEvents) {
            pevents.add(new CriticalEvent(eh.getSyntheticTimestamp(),
                                          eh.getEvent()));
        }
        admin.storeEvents(pevents, timestamps);
    }

    /*
     * A simple class to be the lock object in the sync() method, below.
     */
    private static class SyncWaiter {
        public boolean notified = false;

        public synchronized void setNotified() {
            notified = true;
            this.notify();
        }
    }

    /**
     * Force the worker thread to execute a cycle.  Wait for it to complete
     * before returning.
     */
    public void sync() {
        SyncWaiter mySyncWaiter = new SyncWaiter();

        synchronized (this) {
            if (!workerThreadGo) {
                /* No point in trying to sync a recorder that is not running. */
                return;
            }

            syncWaiters.add(mySyncWaiter);
            this.notify(); /* wake up the worker thread */
        }

        synchronized (mySyncWaiter) {
            while (mySyncWaiter.notified == false) {
                try {
                    mySyncWaiter.wait();
                } catch (InterruptedException e) {
                }
            }
        }

        synchronized (this) {
            syncWaiters.remove(mySyncWaiter);
        }
    }

    public void shutdown() {
        if (isShutdown) {
            admin.getLogger().info("EventRecorder already shut down");
            return;
        }
        isShutdown = true;

        sync(); /* Drain pending events queued in the Trackers. */

        if (workerThreadGo) {
            workerThreadGo = false;
            workerThread.interrupt();
            try {
                workerThread.join();
            } catch (InterruptedException e) {
                admin.getLogger().warning
                    ("Interrupted while joining the worker thread.");
            }
        }
        statusTracker.removeListener(statusListener);
        perfTracker.removeListener(perfListener);
        logTracker.removeListener(logListener);

        /*
         * We aren't using RMI here, but we still must unexport the listener,
         * which is a UnicastRemoteObject, and exports itself on construction.
         */
        try {
            UnicastRemoteObject.unexportObject(statusListener, true);
            UnicastRemoteObject.unexportObject(perfListener, true);
            UnicastRemoteObject.unexportObject(logListener, true);
        } catch (NoSuchObjectException nsoe) {
            String msg = "Unexporting a listener failed.";
            admin.getLogger().warning(msg + LoggerUtils.getStackTrace(nsoe));
        }
    }

    private class Listener extends TrackerListenerImpl {

        private static final long serialVersionUID = 1L;

        Listener(ServerSocketFactory ssf,
                 long interestingTime)
            throws RemoteException {

            super(ssf, interestingTime);
        }

        @Override
        public void notifyOfNewEvents() {
            synchronized(EventRecorder.this) {
                EventRecorder.this.notify();
            }
        }
    }

    /**
     * Bundle the interesting timestamps into a single object, which can be
     * stored in the database to record them.  On recovery or failover we
     * restart the eventrecorder with the last saved timestamps.
     */
    @Persistent
    public static class LatestEventTimestamps implements Serializable {
        private static final long serialVersionUID = 1L;

        long latestStatusEventTimestamp;
        long latestPerfEventTimestamp;
        long latestLogEventTimestamp;

        public LatestEventTimestamps(long s, long p, long l) {
            latestStatusEventTimestamp = s;
            latestPerfEventTimestamp = p;
            latestLogEventTimestamp = l;
        }

        public LatestEventTimestamps() {
        }

        public long getStatusTimestamp() {
            return latestStatusEventTimestamp;
        }

        public long getPerfTimestamp() {
            return latestPerfEventTimestamp;
        }

        public long getLogTimestamp() {
            return latestLogEventTimestamp;
        }

        public void setStatusTimestamp(long statusTimestamp) {
            this.latestStatusEventTimestamp = statusTimestamp;
        }

        public void setPerfTimestamp(long perfTimestamp) {
            this.latestPerfEventTimestamp = perfTimestamp;
        }

        public void setLogTimestamp(long logTimestamp) {
            this.latestLogEventTimestamp = logTimestamp;
        }
    }
}

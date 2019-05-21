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

package oracle.kv.impl.admin.client;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.logging.LogRecord;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.TrackerListenerImpl;
import oracle.kv.impl.util.LogFormatter;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

class LogtailCommand extends ShellCommand {
    private RemoteException threadException = null;
    private boolean logTailThreadGo;
    private LogListener lh;
    CommandServiceAPI cs;
    CommandShell cmd;

    LogtailCommand() {
        super("logtail", 4);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if (args.length != 1) {
            shell.badArgCount(this);
        }
        cmd = (CommandShell) shell;
        cs = cmd.getAdmin();
        try {
            shell.getOutput().println("(Press <enter> to interrupt.)");
            /*
             * Run the tail in a separate thread, so that the current thread
             * can terminate it when required.
             */
            Thread t = new Thread("logtail") {
                @Override
                public void run() {
                    try {
                        logtailWorker();
                    } catch (RemoteException e) {
                        threadException = e;
                    }
                }
            };

            threadException = null;
            logTailThreadGo = true;
            t.start();

            /* Wait for the user to type <enter>. */
            shell.getInput().readLine();
            synchronized (this) {
                logTailThreadGo = false;
                t.interrupt();
            }
            t.join();
            if (threadException != null) {
                return "Exception from logtail: " +
                    threadException.getMessage();
            }
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        } catch (IOException ioe) {
            return "Exception reading input during logtail";
        } catch (InterruptedException ignored) {
        }
        return "";
    }

    @Override
    public String getCommandDescription() {
        return "Monitors the store-wide log file until interrupted by an " +
               "\"enter\"" + eolt + "keypress.";
    }

    private static class LogListener extends TrackerListenerImpl {

        private static final long serialVersionUID = 1L;

        final Object lockObject;

        LogListener(long interestingTime, Object lockObject)
            throws RemoteException {

            super(interestingTime);
            this.lockObject = lockObject;
        }

        @Override
                public void notifyOfNewEvents() {
            synchronized (lockObject) {
                lockObject.notify();
            }
        }
    }

    private void logtailWorker()
        throws RemoteException {

        long logSince = 0;
        boolean registered = false;
        try {
            lh = new LogListener(logSince, this);
            cs.registerLogTrackerListener(lh);
            LogFormatter lf = new LogFormatter(null);
            registered = true;

            while (true) {
                Tracker.RetrievedEvents<LogRecord> logEventsContainer;
                List<LogRecord> logEvents;

                synchronized (this) {
                    while (true) {
                        /* If we're exiting, stop the listener. */
                        if (logTailThreadGo == false) {
                            try {
                                cs.removeLogTrackerListener(lh);
                                registered = false;
                            } finally {
                                UnicastRemoteObject.unexportObject(lh, true);
                                lh = null;
                            }
                            return;
                        }

                        logEventsContainer = cs.getLogSince(logSince);
                        logEvents = logEventsContainer.getEvents();
                        if (logEvents.size() != 0) {
                            break;
                        }
                        /* Wait for LogListener.onNewEventsPresent. */
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                        }
                    }
                    /*
                     * Remember the timestamp of the last record we retrieved;
                     * this will become the "since" argument in the next
                     * request.
                     */
                    logSince =
                        logEventsContainer.getLastSyntheticTimestamp();
                    /*
                     * Move the interesting time up, so older items can be
                     * deleted.
                     */
                    lh.setInterestingTime(logSince);
                }

                /* Print the records we got. */

                for (LogRecord lr : logEvents) {
                    cmd.getOutput().print(lf.format(lr));
                }
            }
        } finally {

            /*
             * Guard against exception paths that don't unregister. Failing
             * to unregister the listener will make the log queue continue to
             * grow.
             */
            if (registered) {
                cs.removeLogTrackerListener(lh);
            }

            if (lh != null) {
                 /* Remove listener object if worker exit unexpectedly.*/
                UnicastRemoteObject.unexportObject(lh, true);
                lh = null;
            }
        }
    }

    /**
     * Used for unit test.
     */
    protected LogListener getLogListener() {
        return this.lh;
    }
}

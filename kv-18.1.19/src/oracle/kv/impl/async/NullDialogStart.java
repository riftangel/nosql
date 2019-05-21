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

package oracle.kv.impl.async;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Helper class to quickly fail a dialog upon its start.
 */
public class NullDialogStart {

    private static final ScheduledExecutorService nullExecutor =
        new NullExecutor();
    private static final NullContext nullContext = new NullContext();

    /**
     * Fails the dialog.
     *
     * @param handler the dialog handler
     * @param cause the cause of the fail
     */
    public static void fail(DialogHandler handler,
                            Throwable cause) {
        try {
            handler.onStart(nullContext, true);
        } finally {
            try {
                handler.onAbort(nullContext, cause);
            } catch (Throwable t) {
                /*
                 * There is nothing we can do here. If the problem is
                 * persistent, it will be logged when the dialog is actually
                 * started.
                 */
            }
        }
    }

    /**
     * A null context provided to the failed dialog.
     */
    private static class NullContext implements DialogContext {

        @Override
        public boolean write(MessageOutput mesg, boolean finished) {
            return false;
        }

        @Override
        public MessageInput read() {
            return null;
        }

        @Override
        public long getDialogId() {
            return 0;
        }

        @Override
        public long getConnectionId() {
            return 0;
        }

        @Override
        public NetworkAddress getRemoteAddress() {
            return null;
        }

        @Override
        public ScheduledExecutorService getSchedExecService() {
            return nullExecutor;
        }

        @Override
        public String toString() {
            return "NullContext";
        }
    }

    /**
     * A null scheduled executor service provided to the null context.
     */
    private static class NullExecutor
            extends AbstractExecutorService
            implements ScheduledExecutorService {

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public boolean isShutdown() {
            return true;
        }

        @Override
        public boolean isTerminated() {
            return true;
        }

        @Override
        public void shutdown() {
            return;
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public void execute(Runnable command) {
            throw new RejectedExecutionException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable,
                                               long delay,
                                               TimeUnit unit) {
            throw new RejectedExecutionException();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command,
                                           long delay,
                                           TimeUnit unit) {
            throw new RejectedExecutionException();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                      long initialDelay,
                                                      long period,
                                                      TimeUnit unit) {
            throw new RejectedExecutionException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                         long initialDelay,
                                                         long delay,
                                                         TimeUnit unit) {
            throw new RejectedExecutionException();
        }
    }
}

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

package oracle.kv.impl.async.dialog.nio;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * A single thread scheduled executor for nio selectable channels.
 *
 * The executor serves two purposes:
 * (1) runs the select-process loop for a selector and channels registered;
 * (2) runs tasks as a scheduled executor.
 * Since the two functionality competes for execution cycles, we must ensure
 * some kind of fairness between them. This requires thread-safety concerns
 * such that the actions of one functionality does not block the other
 * indefinetly. This also invovles some fairness strategy, although currently
 * we only implement the most simple one.
 *
 * TODO: Each connection is associated with one and only one executor. Multiple
 * connections can associated with one executor. This causes a load-balancing
 * problem, e.g., when one connection is doing heavy-IO, other connections
 * associated with the same executor will be affected while some other
 * executors are idle. To solve this, two techniques are necessary: detection
 * and migration. We need to be able to detect such situation, identify the
 * problematic connection and migrate connections to create a balanced load for
 * all executors.
 */
public class NioChannelExecutor
    extends AbstractExecutorService implements Runnable, ChannelExecutor {

    private final Logger logger;

    private final NioChannelThreadPool parent;
    private final int childId;
    private final String id;

    /* Sequence number to break scheduling ties, and in turn to guarantee FIFO
     * order among tied entries. */
    private final static AtomicLong sequencer = new AtomicLong();

    /* Start time of this executor */
    private final long startTimeNs = System.nanoTime();

    /* Queue for register operations */
    private final Queue<RegisterEntry> registerQueue =
        new ConcurrentLinkedQueue<RegisterEntry>();

    /* Tasks accounting for select deadline */
    private final Queue<Runnable> taskQueue =
        new ConcurrentLinkedQueue<Runnable>();
    private final PriorityQueue<ScheduledFutureTask<?>> delayedQueue =
        new PriorityQueue<ScheduledFutureTask<?>>();
    private final List<Runnable> remainingTasks = new ArrayList<Runnable>();

    /* A reference to the thread powering the executor. */
    private volatile Thread thread;

    /*
     * Current time. Updated from time to time since System.nanoTime() is
     * relatively expensive comparing to submitted tasks that are possibly
     * small. It is only accessed inside the channel executor thread; no
     * thread-safety concerns.
     */
    private long currTimeNs = nanoTime();

    /*
     * Update interval. Number of submitted tasks executed before we update the
     * current time.
     */
    private final static int UPDATE_INTERVAL = 64;
    /*
     * A count down starting from UPDATE_INTERVAL. It is only accessed inside
     * the channel executor thread; no thread safety concerns.
     */
    private int updateCountdown = 0;

    private final Selector selector;
    private final AtomicBoolean pendingWakeup = new AtomicBoolean(false);

    /*
     * The default calculator that returns a constant 100 micro-seconds as the
     * maximum execution time for non-IO tasks after each IO processing.
     */
    private final TaskTimeCalculator taskTimeCalculator =
        new DefaultTaskTimeCalculator(100000L);

    enum State {
        /*
         * Accept new tasks, select and process queued tasks. Transit to
         * SHUTDOWN or STOP.
         */
        RUNNING,

        /*
         * Accept new tasks, select and process queued tasks. No new channel
         * can be registered. Transit to SHUTDOWN if no registered channel.
         */
        SHUTTINGDOWN,

        /*
         * Don't accept new tasks, don't select, process immediate one-shot
         * tasks, don't process delayed tasks, don't process periodic tasks.
         * Transit to STOP.
         */
        SHUTDOWN,

        /*
         * Don't accept new tasks, don't select, don't process tasks. Transit
         * to TERMINATED.
         */
        STOP,

        /* Cleaned up and terminated */
        TERMINATED,
    }

    /*
     * State of the executor. Updates should be inside a synchronization block
     * of this object. Volatile for read access.
     */
    private volatile State state = State.RUNNING;

    public NioChannelExecutor(Logger logger,
                              NioChannelThreadPool parent,
                              int childId)
        throws IOException {

        super();
        this.logger = logger;
        this.parent = parent;
        this.childId = childId;
        this.id = String.format("%x#%x", parent.getId(), childId);
        this.selector = Selector.open();
    }

    /**
     * Returns the id in the form of parentId#childId.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the childId.
     */
    public int getChildId() {
        return childId;
    }


    /* Implements ScheduledExecutorService */

    @Override
    public void execute(Runnable command) {
        if (command == null)
            throw new NullPointerException();
        if (isShutdownOrAfter()) {
            throw new RejectedExecutionException("Executor is shut down");
        }
        FutureTask<Void> task = new PrintableFutureTask(command);
        taskQueue.add(task);
        wakeup();
        /*
         * Cancel the task if the executor is shutting down while the task is
         * being added
         */
        if (isShutdownOrAfter()) {
            if (task.cancel(false)) {
                throw new RejectedExecutionException("Executor is shut down");
            }
        }
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command,
            long delay, TimeUnit unit) {
        if (command == null || unit == null)
            throw new NullPointerException();
        ScheduledFutureTask<Void> task = new ScheduledFutureTask<Void>(
                command, null, getDelayedTime(delay, unit));
        addDelayedTask(task);
        return task;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable,
            long delay, TimeUnit unit) {
        if (callable == null || unit == null)
            throw new NullPointerException();
        ScheduledFutureTask<V> task = new ScheduledFutureTask<V>(
                callable, getDelayedTime(delay, unit));
        addDelayedTask(task);
        return task;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
            long initialDelay, long period, TimeUnit unit) {
        if (command == null || unit == null)
            throw new NullPointerException();
        if (period <= 0L)
            throw new IllegalArgumentException();
        ScheduledFutureTask<Void> task = new ScheduledFutureTask<Void>(
                command, null, getDelayedTime(initialDelay, unit),
                unit.toNanos(period));
        addDelayedTask(task);
        return task;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
            long initialDelay, long delay, TimeUnit unit) {
        if (command == null || unit == null)
            throw new NullPointerException();
        if (delay <= 0L)
            throw new IllegalArgumentException();
        ScheduledFutureTask<Void> task = new ScheduledFutureTask<Void>(
                command, null, getDelayedTime(initialDelay, unit),
                -unit.toNanos(delay));
        addDelayedTask(task);
        return task;

    }

    /*
     * Implements the method of ExecutorService, although the upper layer is
     * not expected to call this method directly. Shutting down is expected to
     * be done with NioChannelThreadPool#shutdown. We are not restarting the
     * executor if it is shutdown.
     */
    @Override
    public void shutdown() {
        synchronized(this) {
            if (isShuttingDownOrAfter()) {
                return;
            }
            state = State.SHUTTINGDOWN;
        }
        wakeup();
    }

    /*
     * Note that the returned tasks may contain those that are already
     * cancelled.
     *
     * Implements the method of ExecutorService, although the upper layer is
     * not expected to call this method directly. Shutting down is expected to
     * be done with NioChannelThreadPool#shutdown. We are not restarting the
     * executor if it is shutdown.
     */
    @Override
    public List<Runnable> shutdownNow() {
        /* Transit to stop */
        synchronized(this) {
            if (isTerminated()) {
                return remainingTasks;
            }
            state = State.STOP;
        }
        /* Terminate if in executor thread. */
        if (inExecutorThread()) {
            terminate();
            return remainingTasks;
        }
        /*
         * Wake up from select and wait for the executor to terminate.
         */
        wakeup();
        /*
         * We should not wait long, so just wait for 10 secodns and if it is
         * not terminated, something is wrong.
         */
        try {
            awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            /* do nothing */
        }
        if (!isTerminated()) {
            throw new IllegalStateException(
                    "The executor is not terminated after 10 seconds; " +
                    "something is wrong");
        }
        return remainingTasks;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
        throws InterruptedException {

        if (inExecutorThread()) {
            throw new IllegalStateException(
                    "Cannot wait for termination inside executor thread");
        }

        if (!isShuttingDownOrAfter()) {
            throw new IllegalStateException(
                    "Should only call awaitTermination " +
                    "after executor is shutting down");
        }

        final long timeoutMillis =
            TimeUnit.MILLISECONDS.convert(timeout, unit);
        synchronized(this) {
            final boolean done = isTerminated();
            if (done) {
                return done;
            }
            wait(timeoutMillis);
        }
        return isTerminated();
    }

    @Override
    public boolean isShutdown() {
        return isShuttingDownOrAfter();
    }


    @Override
    public boolean isTerminated() {
        return state == State.TERMINATED;
    }

    /**
     * Returns {@code true} when executed in the thread of the executor.
     *
     * @return {@code true} if in executor thread
     */
    @Override
    public boolean inExecutorThread() {
        return (Thread.currentThread() == thread);
    }

    /**
     * Registers for interested only in accepting a connection.
     *
     * If the method is executed inside the executor, the operation is done
     * when the method returns ; otherwise, the operation is queued and
     * executed in order at a later time (since {@link
     * SelectableChannel#register} blocks when selector is selecting).
     */
    @Override
    public void registerAccept(ServerSocketChannel channel,
                               ChannelAccepter handler) throws IOException {

        if (isShuttingDownOrAfter()) {
            throw new IllegalStateException("Executor is shut down");
        }
        channel.configureBlocking(false);
        if (!inExecutorThread()) {
            registerQueue.add(
                    new RegisterEntry(
                        channel, SelectionKey.OP_ACCEPT, handler));
            wakeup();
        } else {
            if (!registerQueue.isEmpty()) {
                /* Make sure to do all operations submitted before this. */
                doRegister();
            }
            registerChannel(channel, SelectionKey.OP_ACCEPT, handler);
        }
    }

    /**
     * Registers for interested only in finishing a connection.
     *
     * If the method is executed inside the executor, the operation is done
     * when the method returns ; otherwise, the operation is queued and
     * executed in order at a later time (since {@link
     * SelectableChannel#register} blocks when selector is selecting).
     */
    @Override
    public void registerConnect(SocketChannel channel,
                                ChannelHandler handler) throws IOException {

        if (isShuttingDownOrAfter()) {
            throw new IllegalStateException("Executor is shut down");
        }
        channel.configureBlocking(false);
        if (!inExecutorThread()) {
            registerQueue.add(
                    new RegisterEntry(
                        channel, SelectionKey.OP_CONNECT, handler));
            wakeup();
        } else {
            if (!registerQueue.isEmpty()) {
                /* Make sure to do all operations submitted before this. */
                doRegister();
            }
            registerChannel(channel, SelectionKey.OP_CONNECT, handler);
        }
    }

    /**
     * Registers for interested in reading data from a channel.
     *
     * If the method is executed inside the executor, the operation is done
     * when the method returns ; otherwise, the operation is queued and
     * executed in order at a later time (since {@link
     * SelectableChannel#register} blocks when selector is selecting).
     */
    @Override
    public void registerRead(SocketChannel channel,
                             ChannelHandler handler) throws IOException {

        if (isShuttingDownOrAfter()) {
            throw new IllegalStateException("Executor is shut down");
        }
        channel.configureBlocking(false);
        if (!inExecutorThread()) {
            registerQueue.add(
                    new RegisterEntry(
                        channel, SelectionKey.OP_READ, handler));
            wakeup();
        } else {
            if (!registerQueue.isEmpty()) {
                /* Make sure to do all operations submitted before this. */
                doRegister();
            }
            registerChannel(channel, SelectionKey.OP_READ, handler);
        }
    }

    /**
     * Registers for interested only in reading and writing on a channel.
     *
     * If the method is executed inside the executor, the operation is done
     * when the method returns ; otherwise, the operation is queued and
     * executed in order at a later time (since {@link
     * SelectableChannel#register} blocks when selector is selecting).
     */
    @Override
    public void registerReadWrite(SocketChannel channel,
                                  ChannelHandler handler) throws IOException {

        if (isShuttingDownOrAfter()) {
            throw new IllegalStateException("Executor is shut down");
        }
        channel.configureBlocking(false);
        if (!inExecutorThread()) {
            registerQueue.add(
                    new RegisterEntry(
                        channel,
                        SelectionKey.OP_READ | SelectionKey.OP_WRITE,
                        handler));
            wakeup();
        } else {
            if (!registerQueue.isEmpty()) {
                /* Make sure to do all operations submitted before this. */
                doRegister();
            }
            registerChannel(channel,
                    SelectionKey.OP_READ | SelectionKey.OP_WRITE,
                    handler);
        }
    }

    /**
     * Removes any registered interest of the channel.
     *
     * If the method is executed inside the executor, the operation is done
     * when the method returns ; otherwise, the operation is queued and
     * executed in order at a later time (to maintain the order w.r.t. other
     * operations).
     */
    @Override
    public void deregister(SelectableChannel channel) throws IOException {
        if (!inExecutorThread()) {
            registerQueue.add(
                    new RegisterEntry(channel, 0, null));
            wakeup();
        } else {
            if (!registerQueue.isEmpty()) {
                /* Make sure to do all operations submitted before this. */
                doRegister();
            }
            deregisterChannel(channel);
        }
    }

    /**
     * Checks if the channel is interested in write.
     */
    @Override
    public boolean writeInterested(SelectableChannel channel) {

        try {
            SelectionKey key = channel.keyFor(selector);
            if (key == null) {
                return false;
            }
            return (key.interestOps() & SelectionKey.OP_WRITE) != 0;
        } catch (CancelledKeyException e) {
            return false;
        }
    }

    /**
     * Checks if the channel is deregistered.
     *
     * @param channel the socket channel to check
     * @return {@code true} if deregistered
     */
    @Override
    public boolean deregistered(SelectableChannel channel) {
        SelectionKey key = channel.keyFor(selector);
        if (key == null) {
            return true;
        }
        return false;
    }

    /* Implements Runnable */

    /**
     * Run the executor.
     */
    @Override
    public void run() {
        thread = Thread.currentThread();

        logger.log(Level.FINE, "Executor starts running: {0}", this);
        scheduleAtFixedRate(new DumpTask(), 0, 10, TimeUnit.SECONDS);
        while (!isShutdownOrAfter()) {
            try {
                runOnce();
                if (isShuttingDownOrAfter()) {
                    /*
                     * Transit to SHUTDOWN if we are in SHUTTINGDOWN and we do
                     * not have any channel registered.
                     */
                    if (selector.keys().isEmpty()) {
                        synchronized(this) {
                            if (state == State.SHUTTINGDOWN) {
                                state = State.SHUTDOWN;
                            }
                        }
                        break;
                    }
                }
            } catch (Throwable cause) {
                /*
                 * We encountered some exception that are not from the
                 * submitted tasks, but from our executor framework.
                 *
                 * The only exception we currently know is
                 * InterruptedException and the most likely cause of the
                 * exception is Future#cancel. Generally, we think we can
                 * continue the executor for InterruptedException.
                 *
                 * For all the other unknown exceptions, there are three
                 * approaches:
                 * - treats it as fatal and shuts down the JVM
                 * - shuts down this executor and starts another one to
                 *   take over
                 * - continue
                 * We currently adopts the third approach since it involves
                 * less work and we do not know if restarting things can
                 * solve the problem.
                 */
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO,
                            "Unexpected exception during execution: {0}",
                            new Object[] {
                                CommonLoggerUtils.getStackTrace(cause)});
                }
                continue;
            }
        }
        if (!isStoppedOrAfter()) {
            runTasks(Integer.MAX_VALUE);
        }
        terminate();
    }

    /**
     * Run once for the select, process and run task.
     *
     * This method can be used when upper layers want to control the select
     * control flow, e.g., to run multiple select loops in a single thread.
     */
    public void runOnce() throws IOException, InterruptedException {
        long procTime = selectAndProcess();
        long taskTime = taskTimeCalculator.getTaskTimeNanos(procTime);
        runTasks(getDelayedTime(taskTime));
    }

    /* NioChannelExecutor specific */

    public boolean isShuttingDownOrAfter() {
        return state.compareTo(State.SHUTTINGDOWN) >= 0;
    }

    public boolean isShutdownOrAfter() {
        return state.compareTo(State.SHUTDOWN) >= 0;
    }

    public boolean isStoppedOrAfter() {
        return state.compareTo(State.STOP) >= 0;
    }

    /**
     * A calculator to compute the maximum execution time for running non-IO
     * tasks after IO processing.
     */
    public interface TaskTimeCalculator {
        /**
         * Gets the maximum execution time for non-IO tasks, in nanoseconds,
         * according to the IO processing time.
         *
         * @param procTimeNs the IO processing time in nano seconds
         * @return the execution time for non-IO tasks
         */
        long getTaskTimeNanos(long procTimeNs);
    }

    /**
     * Returns the nano time with respect to the start time.
     */
    public long nanoTime() {
         return System.nanoTime() - startTimeNs;
    }

    /**
     * Return the string that represent the status of this executor.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(getClass().getSimpleName());
        builder.append(": parent=")
            .append(Integer.toHexString(parent.getId()));
        builder.append(" childId=").append(Integer.toHexString(childId));
        builder.append("\n");
        builder.append("\tstartTimeNs=").append(startTimeNs);
        builder.append(" currTimeNs=").append(nanoTime());
        builder.append("\n");
        builder.append("\tthread=").append(thread);
        builder.append(" selector=").append(selector);
        builder.append("\n");
        builder.append("\tstate=").append(state);
        builder.append(" #taskQueue=").append(taskQueue.size());
        builder.append(" #delayedQueue=").append(delayedQueue.size());
        builder.append(" #remainingTasks=").append(remainingTasks.size());
        builder.append("\n");
        builder.append("\tnext task: ").append(taskQueue.peek()).append("\n");
        builder.append("\tnext delayed: ").
            append(delayedQueue.peek()).append("\n");
        builder.append("\tselecting channels:\n");
        for (SelectionKey key : selector.keys()) {
            try {
                builder.append("\t\tch=").append(key.channel()).
                    append("\tops=").
                    append(Integer.toBinaryString(key.interestOps())).
                    append("\n");
            } catch (CancelledKeyException cke) {
                builder.append("\t\tch=").append(key.channel()).
                    append(" closed and key cancelled");
            }
        }
        return builder.toString();
    }

    public String tasksToString() {
        StringBuilder builder = new StringBuilder(getClass().getSimpleName());
        builder.append("taskQueue=[");
        for (Runnable task : taskQueue) {
            builder.append(task.toString());
            builder.append(", ");
        }
        builder.append("]");
        builder.append("delayedQueue=[");
        for (Runnable task : delayedQueue) {
            builder.append(task.toString());
            builder.append(", ");
        }
        builder.append("]");
        builder.append("remainingTasks=[");
        for (Runnable task : remainingTasks) {
            builder.append(task.toString());
            builder.append(", ");
        }
        builder.append("]");
        return builder.toString();
    }


    /* Implementations */

    /**
     * Returns a delayed time in the future, in nano-seconds, relative to the
     * current time.
     */
    private long getDelayedTime(long delay, TimeUnit unit) {
        return getDelayedTime(unit.toNanos((delay < 0) ? 0 : delay));
    }

    private long getDelayedTime(long delayNs) {
        final long curr = nanoTime();
        long result = curr + delayNs;
        /*
         * If result is less than the curr, overflow, just set it to maximum.
         */
        result = (result < curr) ? Long.MAX_VALUE : result;
        return result;
    }

    private class ScheduledFutureTask<V>
            extends FutureTask<V> implements RunnableScheduledFuture<V> {

        private final Object innerTask;
        private volatile long execTimeNanos;
        private final long seqno;

        /*
         * A positive value indicates fixed-rate execution.
         * A negative value indicates fixed-delay execution.
         * A value of 0 indicates a non-repeating (one-shot) task.
         */
        private final long period;

        ScheduledFutureTask(Callable<V> c, long execTimeNanos) {
            super(c);
            this.innerTask = c;
            this.execTimeNanos = execTimeNanos;
            this.period = 0;
            this.seqno = sequencer.getAndIncrement();
        }

        ScheduledFutureTask(Runnable r, V result, long execTimeNanos) {
            super(r, result);
            this.innerTask = r;
            this.execTimeNanos = execTimeNanos;
            this.period = 0;
            this.seqno = sequencer.getAndIncrement();
        }

        ScheduledFutureTask(Runnable r,
                            V result,
                            long execTimeNanos,
                            long period) {
            super(r, result);
            this.innerTask = r;
            this.execTimeNanos = execTimeNanos;
            this.period = period;
            this.seqno = sequencer.getAndIncrement();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(execTimeNanos - nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed delayed) {
            if (delayed == this) {
                return 0;
            }
            if (delayed instanceof ScheduledFutureTask) {
                ScheduledFutureTask<?> that = (ScheduledFutureTask<?>) delayed;
                long diff = this.execTimeNanos - that.execTimeNanos;
                if (diff < 0) {
                    return -1;
                }
                if (diff > 0) {
                    return 1;
                }
                if (this.seqno < that.seqno) {
                    return -1;
                }
                return 1;
            }
            long diff = getDelay(TimeUnit.NANOSECONDS) -
                delayed.getDelay(TimeUnit.NANOSECONDS);
            return Long.signum(diff);
        }

        @Override
        public boolean isPeriodic() {
            return period != 0;
        }

        @Override
        public void run() {
            final boolean periodic = isPeriodic();
            if (!canRunTask(periodic)) {
                cancel(false);
            } else if (!periodic) {
                super.run();
            } else if (super.runAndReset()) {
                setNextTime();
                delayedQueue.add(this);
            }
        }

        @Override
        public void done() {
            /*
             * We need to report the exception if there is any. Just get the
             * result and rethrow if there is any exception and runTasks method
             * will catch it.
             */
            if (!isCancelled()) {
                try {
                    get();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(
                        "Unexpected exception: " + e, e);
                }
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) {
                if (inExecutorThread()) {
                    delayedQueue.remove(this);
                } else {
                    execute(new PrintableFutureTask(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        delayedQueue.remove(this);
                                    }

                                    @Override
                                    public String toString() {
                                        return "Removing " + innerTask;
                                    }
                                }));
                }
            }
            return cancelled;
        }

        private void setNextTime() {
            if (period > 0) {
                execTimeNanos += period;
            } else if (period < 0) {
                execTimeNanos = getDelayedTime(-period);
            }
        }

        private boolean canRunTask(boolean periodic) {
            if (state == State.RUNNING) {
                return true;
            }
            if ((state == State.SHUTDOWN) && (!periodic)) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder builder =
                new StringBuilder(getClass().getSimpleName());
            builder.append(": seqno=").append(seqno).
                    append(" execTimeNanos=").append(execTimeNanos).
                    append(" period=").append(period).
                    append(" task=").append(innerTask);
            return builder.toString();
        }
    }

    private void addDelayedTask(final ScheduledFutureTask<?> task) {
        if (isShutdownOrAfter()) {
            throw new RejectedExecutionException("Executor is shut down");
        }
        if (inExecutorThread()) {
            delayedQueue.add(task);
        } else {
            execute(new Runnable() {
                @Override
                public void run() {
                    delayedQueue.add(task);
                }

                @Override
                public String toString() {
                    return "Adding to delayed queue: " + task;
                }
            });
            wakeup();
        }
    }

    /**
     * Wake up the selector.
     *
     * Newly submitted tasks can be run and the selector can select on new
     * keys.
     *
     * selector.wakeup() is a relatively expensive operation; use the
     * pendingWakeup variable for optimization.
     */
    private void wakeup() {
        if (!inExecutorThread() && pendingWakeup.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    /**
     * An entry for the register operation.
     */
    private class RegisterEntry {
        final SelectableChannel channel;
        final int ops;
        final Object attach;

        RegisterEntry(SelectableChannel channel, int ops, Object attach) {
            this.channel = channel;
            this.ops = ops;
            this.attach = attach;
        }
    }

    /**
     * Executes the select and the IO processing and returns the duration of IO
     * processing in nanos.
     */
    private long selectAndProcess()
        throws InterruptedException, IOException {

        assert inExecutorThread();

        if (isShutdownOrAfter()) {
            return 0;
        }

        doSelect();

        final long start = nanoTime();
        doProcess();
        currTimeNs = nanoTime();

        return currTimeNs - start;
    }

    /**
     * Do a select call.
     *
     * Block until but not past the next deadline.
     *
     * To ensure that we do not block past the deadline, we do two things:
     * (I) never block when there is a task in taskQueue;
     * (II) set pendingWakeup to false at the end of every select call.
     *
     * With these two actions, we will never block past the deadline. To
     * demonstrate, let's assume that a select interval SelectInterval_i:
     * (SelectStart_i, SelectEnd_i) missed the deadline of task T_j. The task
     * T_j has three steps before its execution:
     * Step1 taskQueue.add(new Runnable() { delayedQueue.add(T_j) })
     * Step2 wakeup()
     * Step3 delayedQueue.add(T_j)
     * For the interval to miss T_j, it can only happen under the following
     * conditions:
     * (1) Step3 is after SelectStart_i, otherwise, SelectStart_i will see the
     *     deadline and should not miss it.
     * (2) Step1 is after SelectStart_i, otherwise, because of (1), taskQueue
     *     will not be empty, and with (I) SelectStart_i will be non-blocking
     * (3) the compareAndSet in wakeup() call fails (i.e., pendingWakeup is
     *     already true), otherwise, because of (1) and (2), the wakeup should
     *     wake up the select interval
     * (4) According to (3) and (II), the pendingWakeup can be only set to true
     *     after the SelectEnd_i-1, therefore, it should wake up
     *     SelectInterval_i before Step2 and thus the interval should not miss
     *     the deadline of T_j
     */
    private void doSelect()
        throws InterruptedException, IOException {

        try {
            long timeoutMillis = (getUntilDeadline() - 100000L) / 1000000L;

            /*
             * Never block if we have some no-delay task or the next delayed
             * deadline is within 1ms
             */
            if (!taskQueue.isEmpty() || (timeoutMillis <= 0)) {
                selector.selectNow();
                /* Set the pendingWakeup to false */
                pendingWakeup.set(false);
                return;
            }

            while (true) {
                /* Do register operations if there is any. */
                doRegister();

                int selectedKeys = selector.select(timeoutMillis);

                /* Set the pendingWakeup to false */
                pendingWakeup.set(false);

                /**
                 * Unblocked from select for one of the following reasons:
                 * - Selected something
                 * - Thread interrupted
                 * - Woke up
                 * - Select timeout
                 * - Sporadic wake up
                 */

                /* selected something */
                if (selectedKeys != 0) {
                    return;
                }

                /* thread interrupted */
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                /* woke up by no delay task */
                if (!taskQueue.isEmpty()) {
                    return;
                }

                /* woke up by delay task or select timeout */
                timeoutMillis = (getUntilDeadline() - 100000L) / 1000000L;
                if (timeoutMillis <= 0) {
                    return;
                }

                /* woke up because of shut down */
                if (isShuttingDownOrAfter()) {
                    return;
                }

                /* woke up because new keys are registered to the selector or
                 * sporadic woke up, just continue */
            }
        } catch (CancelledKeyException e) {
            logger.log(Level.FINE,
                    "CancelledKeyException during select: {0}", e);
        }
    }

    /**
     * Do register operation before select.
     */
    private void doRegister() {

        while (!registerQueue.isEmpty()) {
            RegisterEntry entry = registerQueue.remove();
            if (entry.attach == null) {
                deregisterChannel(entry.channel);
                continue;
            }
            try {
                entry.channel.configureBlocking(false);
                registerChannel(entry.channel, entry.ops, entry.attach);
            } catch (IOException ioe) {
                logAndHandleError(
                        (NioHandler) entry.attach, ioe, entry.channel);
            }
        }

        /* Make sure new register can wake up the select. */
        pendingWakeup.set(false);

        /*
         * If someone register after while loop before pendingWakeup is set, to
         * make sure their operation is not missed, we simply wakeup our
         * selector here.
         */
        if ((!registerQueue.isEmpty()) && (!pendingWakeup.get())) {
            selector.wakeup();
        }
    }

    private void registerChannel(SelectableChannel channel,
                                 int ops,
                                 Object att) throws IOException {
        channel.register(selector, ops, att);
    }

    private void deregisterChannel(SelectableChannel channel) {

        SelectionKey key = channel.keyFor(selector);
        if (key != null) {
            key.cancel();
        }
    }

    /**
     * Returns the delay, in nano seconds, until the next delayed task due
     * time, zero if the task is overdue.
     */
    private long getUntilDeadline() {
        currTimeNs = nanoTime();
        ScheduledFutureTask<?> task = delayedQueue.peek();
        if (task == null) {
            /*
             * We do not have a task, returns the maximum value possible. It
             * seems on some machine, select throws IllegalArgumentException on
             * Long.MAX_VALUE. Hence use Integer.MAX_VALUE here.
             */
            return Integer.MAX_VALUE;
        }
        long diff = Math.min(Integer.MAX_VALUE, task.execTimeNanos - currTimeNs);
        return (diff < 0) ? 0 : diff;
    }

    /**
     * Process selected keys
     */
    private void doProcess() {
        Set<SelectionKey> selectedKeys = selector.selectedKeys();

        if (selectedKeys.isEmpty()) {
            return;
        }

        Iterator<SelectionKey> iter = selectedKeys.iterator();
        while (iter.hasNext()) {
            final SelectionKey key = iter.next();
            final Object attach = key.attachment();
            iter.remove();

            int readyOps;
            try {
                readyOps = key.readyOps();
            } catch (CancelledKeyException e) {
                /*
                 * We may concurrently cancelled the key. It should be
                 * harmless, just continue.
                 */
                continue;
            }

            if ((readyOps & SelectionKey.OP_ACCEPT) != 0) {
                ServerSocketChannel serverSocketChannel =
                    (ServerSocketChannel) key.channel();
                assert !serverSocketChannel.isBlocking();
                ChannelAccepter handler = (ChannelAccepter) attach;
                handleAccept(serverSocketChannel, handler);
            }

            if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                ChannelHandler handler = (ChannelHandler) attach;
                handleConnect(socketChannel, handler);
            }

            if ((readyOps & SelectionKey.OP_READ) != 0) {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                ChannelHandler handler = (ChannelHandler) attach;
                handleRead(socketChannel, handler);
            }

            if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                ChannelHandler handler = (ChannelHandler) attach;
                handleWrite(socketChannel, handler);
            }
        }
    }

    /**
     * A default time calculator that returns a constant time for the maximum
     * execution time for non-IO tasks after each IO processing.
     */
    private class DefaultTaskTimeCalculator implements TaskTimeCalculator {

        private final long timeToRunTask;

        public DefaultTaskTimeCalculator(long timeToRunTask) {
            this.timeToRunTask = timeToRunTask;
        }

        @Override
        public long getTaskTimeNanos(long procTime) {
            return timeToRunTask;
        }
    }

    private void runTasks(long until) {
        assert inExecutorThread();

        while (true) {
            fetchScheduledTasks();

            if (taskQueue.isEmpty()) {
                return;
            }

            while (!taskQueue.isEmpty()) {
                Runnable task = taskQueue.poll();
                try {
                    task.run();
                } catch (Throwable t) {
                    if (logger.isLoggable(Level.INFO)) {
                        logger.log(Level.INFO,
                                "An error occurs to the task {0}: {1}",
                                new Object[] {
                                    task,
                                    CommonLoggerUtils.getStackTrace(t) });
                    }
                }
                updateCurrTime();
                if (currTimeNs >= until) {
                    return;
                }
                assert state != State.TERMINATED;
                if (state == State.STOP) {
                    return;
                }
            }
        }
    }

    private void fetchScheduledTasks() {
        currTimeNs = nanoTime();

        while (true) {
            ScheduledFutureTask<?> task = delayedQueue.peek();
            if (task == null) {
                break;
            }

            if (task.execTimeNanos > currTimeNs) {
                break;
            }

            task = delayedQueue.poll();
            taskQueue.add(task);
        }
    }

    /**
     * Update the current time if necessary.
     *
     * The updates are relatively expensive, so only update once a while.
     */
    private void updateCurrTime() {
        if (updateCountdown == 0) {
            currTimeNs = nanoTime();
            updateCountdown = UPDATE_INTERVAL;
        }
        updateCountdown --;
    }

    /**
     * Clean up and terminate the executor.
     */
    private void terminate() {
        assert inExecutorThread();
        if (isTerminated()) {
            return;
        }
        synchronized(this) {
            if (isTerminated()) {
                return;
            }
            state = State.STOP;
        }

        remainingTasks.addAll(taskQueue);
        remainingTasks.addAll(delayedQueue);

        closeSelector();

        synchronized(this) {
            state = State.TERMINATED;
            notifyAll();
        }

        logger.log(Level.FINE, "Executor terminated: {0}", this);
    }

    /**
     * Cancel selection keys and close selector.
     */
    private void closeSelector() {
        if (isTerminated()) {
            return;
        }

        for (SelectionKey key : selector.keys()) {
            key.cancel();
            final Object attach = key.attachment();
            NioHandler handler = (NioHandler) attach;
            try {
                handler.onClosing();
            } catch (Throwable throwable) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO,
                            "Executor encountered error " +
                            "when calling handler onClosing, " +
                            "handler={0}: {1}",
                            new Object[] {
                                handler,
                                CommonLoggerUtils.getStackTrace(throwable)});
                }
            }
        }

        try {
            selector.close();
        } catch (IOException e) {
            logger.log(Level.FINE, "Error close selector: {0}", e);
        }
    }

    private void handleAccept(ServerSocketChannel serverSocketChannel,
                              ChannelAccepter handler) {
        SocketChannel socketChannel = null;
        try {
            socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                handler.onAccept(socketChannel);
            }
        } catch (Throwable t) {
            try {
                /*
                 * Always close the socketChannel (if any) if an exception occurs.
                 */
                if (socketChannel != null) {
                    try {
                        socketChannel.close();
                    } catch (Throwable t1) {
                        /* ignore */
                    }
                }

            } finally {
                logAndHandleError(handler, t, serverSocketChannel);
            }
        }
    }

    private void handleConnect(SocketChannel socketChannel,
                               ChannelHandler handler) {
        try {
            if (socketChannel.finishConnect()) {
                handler.onConnected();
            }
        } catch (Throwable t) {
            logAndHandleError(handler, t, socketChannel);
        }

    }

    private void handleRead(SocketChannel socketChannel,
                            ChannelHandler handler) {
        try {
            handler.onRead();
        } catch (Throwable t) {
            logAndHandleError(handler, t, socketChannel);
        }
    }

    private void handleWrite(SocketChannel socketChannel,
                             ChannelHandler handler) {
        try {
            handler.onWrite();
        } catch (Throwable t) {
            logAndHandleError(handler, t, socketChannel);
        }
    }

    private void logAndHandleError(NioHandler handler,
                                   Throwable t,
                                   SelectableChannel channel) {
        /* Logging with FINE for IOException, INFO for others */
        Level level = (t instanceof IOException) ? Level.FINE : Level.INFO;
        if (logger.isLoggable(level)) {
            logger.log(level,
                    "Executor encountered error, " +
                    "channel={0}, handler={1}: {2}",
                    new Object[] {
                        channel, handler,
                        CommonLoggerUtils.getStackTrace(t) });
        }
        try {
            handler.onError(t, channel);
        } catch (Throwable throwable) {
            if (logger.isLoggable(Level.INFO)) {
                logger.log(Level.INFO,
                        "Executor encountered error " +
                        "when calling handler onError, " +
                        "handler={0}: {1}",
                        new Object[] {
                            handler,
                            CommonLoggerUtils.getStackTrace(throwable)});
            }
        }
    }

    public class DumpTask implements Runnable {
        @Override
        public void run() {
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST,
                        "[Dump]" + NioChannelExecutor.this.toString());
            }
        }

        @Override
        public String toString() {
            return "Dump task";
        }
    }

    public class PrintableFutureTask extends FutureTask<Void> {

        private final Runnable r;

        public PrintableFutureTask(Runnable r) {
            super(r, null);
            this.r = r;
        }

        @Override
        public String toString() {
            return r.toString();
        }
    }
}

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

package oracle.kv.impl.async.dialog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.logging.Level;

import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.NullDialogStart;
import oracle.kv.impl.async.exception.ConnectionException;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.async.exception.ConnectionIOException;
import oracle.kv.impl.async.exception.ConnectionIdleException;
import oracle.kv.impl.async.exception.ConnectionIncompatibleException;
import oracle.kv.impl.async.exception.ConnectionTimeoutException;
import oracle.kv.impl.async.exception.ConnectionUnknownException;
import oracle.kv.impl.async.exception.DialogLimitExceededException;
import oracle.kv.impl.async.exception.InitialConnectIOException;
import oracle.kv.impl.async.perf.EndpointHandlerPerf;
import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * Abstract dialog endpoint handler that manages dialog contexts and protocol
 * events.
 */
public abstract class AbstractDialogEndpointHandler
    implements DialogEndpointHandler {

    private final static SecureRandom random = new SecureRandom();

    private final Logger logger;
    private final EndpointHandlerManager parent;
    private final boolean isCreator;
    private final NetworkAddress remoteAddress;

    private final AtomicLong sequencer = new AtomicLong(0);

    /* The uuid for the handler. */
    private final long uuid;
    /*
     * The uuid for the connection. It equals to the uuid field if the handler
     * is of a creator endpoint. It equals to 0 and are assigned during
     * handshake if the handler is of responder endpoint.
     */
    private volatile long connid;

    /* Options */

    /* maxDialogs for locally-started dialogs*/
    private volatile int localMaxDlgs;
    /* maxLen for outgoing protocol messages */
    private volatile int localMaxLen;
    /* max data size for outgoing dialog messages */
    private volatile int localMaxTotLen;
    /* maxDialogs for remotely-started dialogs */
    private final int remoteMaxDlgs;
    /* maxLen for incoming protocol messages */
    private final int remoteMaxLen;
    /* max data size for incoming dialog messages */
    private final int remoteMaxTotLen;
    /* the connect timeout */
    private final int connectTimeout;
    /* the heartbeat timeout */
    private final int heartbeatTimeout;
    /* the idle timeout */
    private final int idleTimeout;
    /* the heartbeat interval */
    private volatile int heartbeatInterval;
    /* the max number of dialog context we poll for each batch when we flush */
    private final int flushBatchNumContexts;
    /* the max number of batches when we flush */
    private final int flushNumBatches;

    private enum State {

        /*
         * The executor is not ready; the state is to ensure we call the
         * onExecutorReady method after construction.
         */
        NEED_EXECUTOR,

        /*
         * The channel is connecting to the remote; dialog activities are
         * allowed; but channel read/write not possible.
         */
        CONNECTING,

        /*
         * The connection established; the endpoint is doing handshaking step1;
         * dialog activities are allowed; but channel read/write for only
         * handshake message.
         */
        HANDSHAKING_STEP1,

        /*
         * The connection established; the endpoint is doing handshaking step2;
         * dialog activities are allowed, but channel read/write for only
         * handshake message.
         */
        HANDSHAKING_STEP2,

        /*
         * The handshake done; dialog activities allowed; channel read/write dialog
         * data allowed.
         */
        NORMAL,

        /*
         * Existing dialogs can finish up; new dialogs are not allowed; channel
         * read/write still allowed.
         */
        SHUTTINGDOWN,

        /*
         * A moment before TERMINATED. The channel is closing. No dialog or
         * channel read/write allowed.
         */
        TERMINATING,

        /*
         * All dialogs are finished; channel is closed. The handler is in its
         * final state.
         */
        TERMINATED,
    }

    /*
     * State of the endpoint. Updates must be inside the synchronization block
     * of this object. Volatile for read access.
     */
    private volatile State state = State.NEED_EXECUTOR;
    /* Dialog factories this endpoint support */
    private final Map<Integer, DialogHandlerFactory> dialogHandlerFactories;
    /*
     * Latest started dialogId by this endpoint. For a dialog context, the
     * dialogId is obtained upon first write. We acquire the dialogLock to
     * increase the value. Volatile for read thread-safety.
     */
    private volatile long latestLocalStartedDialogId = 0;
    private final ReentrantLock dialogLock = new ReentrantLock();
    private final Semaphore localDialogResource = new Semaphore(0);
    /*
     * Latest started dialogId by the remote. It will only be accessed inside the
     * single excutor executor thread.
     */
    private long latestRemoteStartedDialogId = 0;
    private final Semaphore remoteDialogResource = new Semaphore(0);

    /*
     * Contexts for dialogs with a dialogId. Use concurrent hash map for
     * get/put thread safety. Iterations are not thread-safe. Therefore, when
     * we are terminating and aborting all contexts, some contexts may be
     * missed. Hence, after each put we should check if we are terminating and
     * if yes, we should abort the contexts after.
     */
    private final Map<Long, DialogContextImpl> dialogContexts =
        new ConcurrentHashMap<Long, DialogContextImpl>();
    /*
     * Contexts that are submitted before handshake is done (and therefore,
     * onStart is not called yet). Use synchronization block on this list for
     * all access.
     */
    private final List<DialogContextImpl> preHandshakeContexts =
        new ArrayList<DialogContextImpl>();
    /*
     * Contexts for dialogs that the dialogId is yet to be assigned.
     * Thread-safety issues are the same with dialogContexts.
     */
    private final Set<DialogContextImpl> pendingDialogContexts =
        Collections.newSetFromMap(
                new ConcurrentHashMap<DialogContextImpl, Boolean>());
    /*
     * Contexts that have some messages to write. Acquire the writing lock
     * before add, remove or iteration.
     */
    private final Set<DialogContextImpl> writingContexts =
        new LinkedHashSet<DialogContextImpl>();
    private final ReentrantLock writingLock = new ReentrantLock();

    /*
     * Indicating we have a pending shutdown when we shut down gracefully
     * before handshake is done.
     */
    private volatile boolean pendingShutdown = false;
    /*
     * Information of the termination. There are two kinds of termination: a
     * graceful shutdown (ShutdownInfo) or an abrupt abort (AbortInfo). A
     * shutdown info will be replaced by an abort info, but not a shut down
     * info; an abort info will not be replaced. Write should be inside a
     * synchronization block of this object. Volatile for read thread safety.
     */
    private volatile TerminationInfo terminationInfo = null;
    /* Atomic flag to make sure we only write connection abort message once. */
    private final AtomicBoolean connectionAbortWritten =
        new AtomicBoolean(false);

    /* Pending ping operations. */
    private final Map<Long, Runnable> pendingPings =
        new ConcurrentHashMap<Long, Runnable>();

    /*
     * The flag to indicate if last flush sends all the data to the transport.
     * The flag is accessed when acquiring the flushLock.
     */
    private boolean lastFlushFinished = true;
    private ReentrantLock flushLock = new ReentrantLock();

    /* Timeout and heartbeat tasks and variables. */
    private final ConnectTimeoutTask connectTimeoutTask = new ConnectTimeoutTask();
    private volatile boolean noReadLastInterval = true;
    private volatile boolean noDialogFlushLastInterval = true;
    private volatile boolean noDialogActive = true;
    private final List<Future<?>> scheduledTasks = new ArrayList<Future<?>>();

    /* Perf */
    private final EndpointHandlerPerf endpointPerf;

    public AbstractDialogEndpointHandler(
            Logger logger,
            EndpointHandlerManager parent,
            EndpointConfig endpointConfig,
            boolean isCreator,
            NetworkAddress remoteAddress,
            Map<Integer, DialogHandlerFactory> dialogHandlerFactories) {

        /*
         * Initialize the local value according to endpointConfig. It will be
         * updated after handshake. The updated value will be the min of the
         * set value and the value that the remote tells us.
         */
        this.localMaxDlgs =
            endpointConfig.getOption(AsyncOption.DLG_LOCAL_MAXDLGS);
        this.localMaxLen =
            endpointConfig.getOption(AsyncOption.DLG_LOCAL_MAXLEN);
        this.localMaxTotLen =
            endpointConfig.getOption(AsyncOption.DLG_LOCAL_MAXTOTLEN);
        /* Sets the remote value according to endpointConfig. */
        this.remoteMaxDlgs =
            endpointConfig.getOption(AsyncOption.DLG_REMOTE_MAXDLGS);
        this.remoteMaxLen =
            endpointConfig.getOption(AsyncOption.DLG_REMOTE_MAXLEN);
        this.remoteMaxTotLen =
            endpointConfig.getOption(AsyncOption.DLG_REMOTE_MAXTOTLEN);
        /* Sets the timeout */
        this.connectTimeout =
            endpointConfig.getOption(AsyncOption.DLG_CONNECT_TIMEOUT);
        this.heartbeatTimeout =
            endpointConfig.getOption(AsyncOption.DLG_HEARTBEAT_TIMEOUT);
        /*
         * The heartbeatInterval will be updated after handshake. The updated
         * value will be the max of the set value and the value that the remote
         * tells us.
         */
        this.heartbeatInterval =
            endpointConfig.getOption(AsyncOption.DLG_HEARTBEAT_INTERVAL);
        this.idleTimeout =
            endpointConfig.getOption(AsyncOption.DLG_IDLE_TIMEOUT);
        /* Sets the flush batch endpointConfig */
        this.flushBatchNumContexts =
            endpointConfig.getOption(AsyncOption.DLG_FLUSH_BATCHSZ);
        this.flushNumBatches =
            endpointConfig.getOption(AsyncOption.DLG_FLUSH_NBATCH);

        this.logger = logger;
        this.parent = parent;
        this.uuid = getNonZeroLongUUID();
        this.connid = (isCreator) ? uuid : 0;
        this.isCreator = isCreator;
        this.remoteAddress = remoteAddress;
        this.dialogHandlerFactories = dialogHandlerFactories;
        this.remoteDialogResource.release(remoteMaxDlgs);
        this.endpointPerf = new EndpointHandlerPerf(logger);
    }

    /* Implements the EndpointHandler interface */

    /**
     * Returns the logger.
     */
    @Override
    public Logger getLogger() {
        return logger;
    }

    /**
     * Returns the network address of the remote endpoint.
     */
    @Override
    public NetworkAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Get the UUID for this handler.
     */
    @Override
    public long getUUID() {
        return uuid;
    }

    /**
     * Get the UUID for the connection.
     */
    @Override
    public long getConnID() {
        return connid;
    }

    /**
     * Returns a string format of the ID.
     */
    @Override
    public String getStringID() {
        if (isCreator) {
            return String.format("%x", uuid);
        }
        return String.format("%x:%x", connid, uuid);
    }

    /**
     * Called when {@code DialogContextImpl} is finished normally or aborted.
     *
     * The DialogContextImpl should guarantee this method is called only once.
     *
     * Remove the context from dialogContexts and pendingDialogContexts. Do not
     * remove context from writingContexts since we may be iterating on it. The
     * context will be removed during the iteration.
     */
    @Override
    public void onContextDone(DialogContextImpl context) {
        long dialogId = context.getDialogId();
        boolean local =
            (dialogId == 0) ||
            (isCreator && (dialogId > 0)) ||
            (!isCreator) && (dialogId < 0);
        if (local) {
            localDialogResource.release();
        } else {
            remoteDialogResource.release();
        }
        if (dialogId != 0) {
            dialogContexts.remove(dialogId);
        } else {
            /*
             * Acquire dialogLock to protect race against
             * writeDialogStartForContext which moves a context from
             * pendingDialogContexts to dialogContexts.
             */
            dialogLock.lock();
            try {
                pendingDialogContexts.remove(context);
                dialogId = context.getDialogId();
                /*
                 * Also remove the dialogId from dialogContexts. The
                 * ConcurrentHashMap#remove does nothing if the key is not in
                 * the map.
                 */
                dialogContexts.remove(dialogId);
            } finally {
                dialogLock.unlock();
            }
        }
        if (context.isFin()) {
            endpointPerf.onDialogFinished(context.getPerf());
        } else {
            endpointPerf.onDialogAborted(context.getPerf());
        }
        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST,
                    "Endpoint handler done with dialog: " +
                    "dialogId={0}, " +
                    "localActive={1}, remoteActive={2}",
                    new Object[] {
                        context.getStringID(),
                        localMaxDlgs -
                            localDialogResource.availablePermits(),
                        remoteMaxDlgs -
                            remoteDialogResource.availablePermits() });
        }
    }

    /**
     * Called when {@code DialogContextImpl} has new message to write.
     */
    @Override
    public void onContextNewWrite(DialogContextImpl context) {

        assert context.getDialogId() != 0;
        writingLock.lock();
        try {
            writingContexts.add(context);
        } finally {
            writingLock.unlock();
        }
        flushOrTerminate();
    }

    /**
     * Gets the max value of the size of a {@link MessageInput}.
     *
     * @return the max value
     */
    @Override
    public int getMaxInputTotLen() {
        return remoteMaxTotLen;
    }

    /**
     * Gets the max value of the size of a {@link MessageOutput}.
     *
     * @return the max value
     */
    @Override
    public int getMaxOutputTotLen() {
        return localMaxTotLen;
    }

    /**
     * Gets the max length for incoming protocol message.
     */
    @Override
    public int getMaxInputProtocolMesgLen() {
        return remoteMaxLen;
    }

    /**
     * Gets the max length for outgoing protocol message.
     */
    @Override
    public int getMaxOutputProtocolMesgLen() {
        return localMaxLen;
    }


    /**
     * Writes a DialogStart message for {@code DialogContextImpl}.
     */
    @Override
    public long writeDialogStartForContext(boolean finish,
                                           boolean cont,
                                           int typeno,
                                           long timeoutMillis,
                                           List<ByteBuffer> frame,
                                           DialogContextImpl context) {

        if (context.getDialogId() != 0) {
            throw new IllegalStateException(String.format(
                        "Writing dialog start " +
                        "when it already has a dialogId, " +
                        "context=%s", this));
        }

        long dialogId;
        dialogLock.lock();
        try {
            dialogId = isCreator ?
                ++latestLocalStartedDialogId : --latestLocalStartedDialogId;
            if (pendingDialogContexts.remove(context)) {
                dialogContexts.put(dialogId, context);
            } else {
                if (!context.isAborted()) {
                    throw new IllegalStateException(
                            String.format(
                                "Context not in the pending map " +
                                "while writing dialog start: %s",
                                context.toString()));
                }
            }
            /*
             * Write the dialog start after we move context from
             * pendingDialogContexts to dialogContexts, otherwise, race can
             * occur that when we get a response for the dialog, it is not in
             * the dialogContexts.
             */
            getProtocolWriter().writeDialogStart(
                    context.getPerf().isSampled(),
                    finish, cont, typeno, dialogId, timeoutMillis, frame);
        } finally {
            dialogLock.unlock();
        }

        return dialogId;
    }

    /**
     * Starts a dialog.
     */
    @Override
    public void startDialog(int dialogType,
                            DialogHandler dialogHandler,
                            long timeoutMillis) {

        if (timeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "Time out value must large than zero");
        }

        if (timeoutMillis == 0) {
            timeoutMillis = Integer.MAX_VALUE;
        }

        if (isShuttingDownOrAfter() || pendingShutdown) {
            dropDialog(dialogHandler,
                    terminationInfo.exception().getDialogException(false));
            return;
        }

        final DialogContextImpl context = new DialogContextImpl(
                this, dialogHandler,
                sequencer.incrementAndGet(), 0,
                dialogType, timeoutMillis);

        if (!isNormalOrAfter()) {
            synchronized(preHandshakeContexts) {
                if (!isNormalOrAfter()) {
                    if (preHandshakeContexts.size() > localMaxDlgs) {
                        dropDialog(context.getDialogHandler(),
                                new DialogLimitExceededException(localMaxDlgs));
                    } else {
                        preHandshakeContexts.add(context);
                    }
                    return;
                }
            }
        }

        tryStartDialog(context, false);
        /*
         * Set noDialogActive after tryStartDialog() to avoid a race. See
         * IdleTimeoutTask#run.
         */
        noDialogActive = false;

        /* Avoid race with abortDialogs */
        if (isShuttingDownOrAfter() || pendingShutdown) {
            abortContextAfterShuttingDown(context);
        }
    }

    /**
     * Returns the limit on the number of dialogs this endpoint can
     * concurrently start.
     */
    @Override
    public int getNumDialogsLimit() {
        if (isNormalOrAfter()) {
            return localMaxDlgs;
        }
        return -1;
    }

    /**
     * Shuts down the handler.
     *
     * If force is false, we only need to set the state and the flush method
     * will terminate the handler if there is nothing to do; otherwise, we do
     * the termination procedure.
     */
    @Override
    public void shutdown(String detail, boolean force) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                    "Endpoint handler shutting down, detail=[{0}], {1}",
                    new Object[] { detail, toString() });
        }
        if (force) {
            markTerminating(
                    new ConnectionEndpointShutdownException(
                        false,
                        String.format(
                            "Shut down with force, detail=[%s]", detail)));
            terminate();
        } else {
            markShuttingDown(detail);
            terminateIfNotActive();
        }
    }

    /**
     * Awaits the handler to terminate.
     */
    public void awaitTermination(long timeoutMillis)
        throws InterruptedException {

        if (isTerminated()) {
            return;
        }
        if (timeoutMillis <= 0) {
            return;
        }

        final long ts = System.currentTimeMillis();
        final long te = ts + timeoutMillis;
        while (true) {
            if (isTerminated()) {
                break;
            }
            final long curr = System.currentTimeMillis();
            final long waitTime = te - curr;
            if (waitTime <= 0) {
                break;
            }
            synchronized(this) {
                if (!isTerminated()) {
                    wait(waitTime);
                }
            }
        }
    }

    /**
     * Awaits the handler to finish handshake for testing.
     */
    public void awaitHandshakeDone() {
        while (true) {
            try {
                synchronized(this) {
                    if (!isNormalOrAfter()) {
                        wait();
                    } else {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new Error(e);
            }
        }
    }

    /**
     * Flush the data.
     */
    public void flush() throws IOException {
        final int written = writeDialogFrameBatches();
        if (written != 0) {
            noDialogFlushLastInterval = false;
        }
        if (flushLock.tryLock()) {
            try {
                lastFlushFinished = flushInternal(hasContextsToWrite());
            } finally {
                flushLock.unlock();
            }
        }
    }

    /**
     * Flush the data, terminate if IO exception or not active.
     */
    public void flushOrTerminate() {
        try {
            flush();
        } catch (Throwable t) {
            markTerminating(t);
        }
        terminateIfNotActive();
    }

    /**
     * Ping the remote.
     */
    public void ping(long cookie, Runnable callback) {
        if (isTerminatingOrAfter()) {
            throw new IllegalStateException(terminationInfo.exception());
        }
        pendingPings.put(cookie, callback);
        getProtocolWriter().writePing(cookie);
        flushOrTerminate();
    }

    /**
     * Is the endpoint a creator endpoint?
     */
    public boolean isCreator() {
        return isCreator;
    }

    /* Checks for the state */

    public boolean isNormal() {
        return (state == State.NORMAL);
    }

    public boolean isNormalOrAfter() {
        return State.NORMAL.compareTo(state) <= 0;
    }

    public boolean isShuttingDown() {
        return state == State.SHUTTINGDOWN;
    }

    public boolean isShuttingDownOrAfter() {
        return State.SHUTTINGDOWN.compareTo(state) <= 0;
    }

    public boolean isTerminatingOrAfter() {
        return State.TERMINATING.compareTo(state) <= 0;
    }

    public boolean isTerminated() {
        return (state == State.TERMINATED);
    }

    /**
     * Called when the channel is ready for our own protocol read/write after
     * connected and pre-read done.
     */
    public void onChannelReady() {
        logger.log(Level.FINE, "Endpoint handler channel ready: {0}", this);

        assertInExecutorThread();

        if (!transitStateOrShuttingDownOrDie(
                    State.CONNECTING, State.HANDSHAKING_STEP1)) {
            return;
        }
        if (isCreator) {
            getProtocolWriter().
                writeProtocolVersion(ProtocolMesg.CURRENT_VERSION);
        }
        flushOrTerminate();
    }

    /**
     * Returns the cause of the termination.
     */
    public ConnectionException getTerminationCause() {
        if (terminationInfo == null) {
            return null;
        }
        return terminationInfo.exception();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("(").append(getStringID()).append(")");
        sb.append("{");
        sb.append(" isCreator=").append(isCreator);
        sb.append(" remoteAddress=").append(remoteAddress);
        sb.append(" state=").append(state);
        sb.append(" pendingShutdown=").append(pendingShutdown);
        sb.append(" #dialogFactories=").append(dialogHandlerFactories.size());
        synchronized(this) {
            sb.append(" latestLocalStartedDialogId=")
                .append(Long.toString(latestLocalStartedDialogId, 16));
            sb.append(" latestRemoteStartedDialogId=")
                .append(Long.toString(latestRemoteStartedDialogId, 16));
        }
        sb.append(" approx#dialogContexts=").append(dialogContexts.size());
        sb.append(" approx#pendingDialogContexts=")
            .append(pendingDialogContexts.size());
        writingLock.lock();
        try {
            sb.append(" #writingContexts=").append(writingContexts.size());
        } finally {
            writingLock.unlock();
        }
        sb.append(" terminationInfo=").append(terminationInfo);
        sb.append("}");
        return sb.toString();
    }

    /**
     * Flush the channel output.
     *
     * @param writeHasRemaining {@code true} if have more data to write after
     * this flush
     * @return {@code true} if all data flushed
     */
    protected abstract boolean flushInternal(boolean writeHasRemaining)
        throws IOException;

    /**
     * Cleans up resources of the endpoint handler.
     */
    protected abstract void cleanup() throws IOException;

    /**
     * Called when the executor is ready.
     */
    protected void onExecutorReady() {
        synchronized(this) {
            if (state != State.NEED_EXECUTOR) {
                throw new IllegalStateException(
                        "onExecutorReady must be called first");
            }
            state = State.CONNECTING;
        }
        /*
         * Dialogs submitted before executor is ready does not have their
         * timeout task scheduled properly because the executor is not
         * associated with this handler yet. Start the timout task here.
         */
        synchronized(preHandshakeContexts) {
            for (DialogContextImpl context : preHandshakeContexts) {
                context.startTimeout();
            }
        }
        /* Start our connection timeout task. */
        this.connectTimeoutTask.schedule();
    }

    /**
     * Writes batches of dialog frames.
     *
     * @return the number of written frames
     */
    protected int writeDialogFrameBatches() {
        int written = 0;
        for (int i = 0; i < flushNumBatches; ++i) {
            final int n = writeOneDialogFrameBatch();
            if (n <= 0) {
                break;
            }
            written += n;
        }
        return written;
    }

    /**
     * Called when channel input is ready for new protocol messages.
     */
    protected void onChannelInputRead() {
        assertInExecutorThread();

        noReadLastInterval = false;
        try {
            while(true) {
                ProtocolMesg mesg = getProtocolReader().read();
                if (mesg == null) {
                    return;
                }
                onMessageReady(mesg);
            }
        } catch (Throwable t) {
            markTerminating(t);
            terminate();
        }
    }

    /**
     * Write a ConnectionAbort message.
     */
    protected void writeConnectionAbort() {
        if (!isShuttingDownOrAfter()) {
            throw new AssertionError();
        }
        if (isCreator) {
            /*
             * Do not write if this is the creator endpoint. The creator side
             * has some expectation of quality of service from the responder,
             * so the responder provides the connection abort message to
             * explain why it stopped responding.  The responder does not care
             * if the creator goes away, so no need to send the message.
             */
            return;
        }
        if (terminationInfo.fromRemote()) {
            /* Do not write if already aborted by remote. */
            return;
        }
        /* Only write one ConnectionAbort */
        if (connectionAbortWritten.compareAndSet(false, true)) {
            final ConnectionException e = terminationInfo.exception();
            getProtocolWriter().
                writeConnectionAbort(exceptionToCause(e), e.toString());
        }
    }

    /**
     * Marks the endpoint handler as shutting down.
     */
    protected synchronized void markShuttingDown(String detail) {
        if (isShuttingDownOrAfter()) {
            return;
        }
        setTerminationInfo(new ShutdownInfo(detail));
        if (!isNormalOrAfter()) {
            pendingShutdown = true;
            return;
        }
        state = State.SHUTTINGDOWN;
        pendingShutdown = false;
    }

    /**
     * Mark the endpoint handler as terminating.
     */
    protected synchronized void markTerminating(Throwable t) {
        if (isTerminatingOrAfter()) {
            return;
        }
        ConnectionException wrapped;
        if (t instanceof IOException) {
            wrapped = (state.compareTo(State.CONNECTING) <= 0) ?
                new InitialConnectIOException((IOException) t, remoteAddress) :
                new ConnectionIOException((IOException) t, remoteAddress);
        } else if (t instanceof ConnectionException) {
            wrapped = (ConnectionException) t;
        } else {
            wrapped = new ConnectionUnknownException(t);
        }
        setTerminationInfo(new AbortInfo(wrapped));
        state = State.TERMINATING;
    }

    protected synchronized
        void markTerminating(ProtocolMesg.ConnectionAbort.Cause cause,
                             String detail) {
        if (isTerminatingOrAfter()) {
            return;
        }
        setTerminationInfo(new AbortInfo(cause, detail));
        state = State.TERMINATING;
    }

    protected synchronized void markTerminated() {
        if (isTerminated()) {
            return;
        }
        state = State.TERMINATED;
    }

    /**
     * Terminate the handler.
     */
    protected void terminate() {
        if (isTerminated()) {
            return;
        }
        if (!isShuttingDownOrAfter()) {
            /*
             * Should call markShuttingDown or markTerminating before calling
             * terminate.
             */
            throw new IllegalStateException(
                    "The method terminate() is called " +
                    "before transiting to a required state " +
                    "(SHUTTINGDOWN or after). " +
                    "This is a coding error.");
        }

        logger.log(Level.FINE, "Endpoint handler terminating: {0}", this);

        abortDialogs();
        cancelScheduledTasks();
        writeConnectionAbort();
        try {
            flush();
        } catch (Throwable t) {
            /*
             * We expect the flush to throw exceptions since we are terminating
             * (probably because of some error in the first place). It will not
             * cause any more harm if we cannot flush. There is also no need
             * for logging since the root cause should already be logged. Just
             * do nothing.
             */
        } finally {
            try {
                cleanup();
            } catch (Throwable t) {
                logger.log(Level.FINE,
                        "Error cleaning up for endpoint handler: {0}",
                        new Object[] { CommonLoggerUtils.getStackTrace(t) });
            }
        }
        endpointPerf.close();
        parent.onHandlerShutdown(this);
        markTerminated();
        /* Notify the threads calling awaitTermination. */
        synchronized(this) {
            notifyAll();
        }
        logger.log(Level.FINE, "Endpoint handler terminated: {0}", this);
    }

    /**
     * Sets the termination info.
     */
    private synchronized void setTerminationInfo(TerminationInfo info) {
        if (terminationInfo instanceof AbortInfo) {
            return;
        }
        if ((terminationInfo instanceof ShutdownInfo) &&
            (info instanceof ShutdownInfo)) {
            return;
        }
        terminationInfo = info;
    }

    /**
     * Terminate the handler if it is not active.
     */
    private void terminateIfNotActive() {
        /*
         * If we are shutting down (and NOT terminating) and have nothing more
         * to do, then we can terminate.
         */
        if (lastFlushFinished() && isShuttingDown() && !hasActiveDialogs()) {
            markTerminating(
                    new ConnectionEndpointShutdownException(
                        false,
                        "Shut down gracefully"));
            terminate();
        } else if (isTerminatingOrAfter()) {
            terminate();
        }
    }

    /**
     * Check if last flush finished.
     */
    private boolean lastFlushFinished() {
        if (flushLock.tryLock()) {
            try {
                return lastFlushFinished;
            } finally {
                flushLock.unlock();
            }
        }
        return false;
    }

    /**
     * Returns a non-zero UUID.
     */
    private long getNonZeroLongUUID() {
        while (true) {
            long result = random.nextLong();
            if (result != 0) {
                return result;
            }
        }
    }

    /**
     * Quickly aborts the dialog with NullDialogStart.
     */
    private void dropDialog(DialogHandler handler, Throwable cause) {
        NullDialogStart.fail(handler, cause);
        endpointPerf.onDialogDropped();
    }

    /**
     * Try to start the dialog.
     */
    private void tryStartDialog(DialogContextImpl context,
                                boolean inExecutorThread) {

        if (!localDialogResource.tryAcquire()) {
            dropDialog(context.getDialogHandler(),
                    new DialogLimitExceededException(localMaxDlgs));
            return;
        }
        pendingDialogContexts.add(context);
        if (endpointPerf.onDialogStarted()) {
            context.getPerf().startSampling();
        }
        context.startTimeout();
        context.callOnStart(inExecutorThread);
    }

    /**
     * Aborts DialogContextImpl after channel handler is about to close.
     */
    private void abortContextAfterShuttingDown(DialogContextImpl context) {
        if (!isShuttingDownOrAfter()) {
            throw new AssertionError();
        }
        context.onLocalAbortConnectionException(terminationInfo.exception());
    }

    /**
     * Returns {@code true} if there is still active dialogs.
     *
     * Need extra caution when calling this method as dialogs may be added
     * during the execution of the method.
     */
    private boolean hasActiveDialogs() {
        if (!dialogContexts.isEmpty()) {
            return true;
        }
        if (!pendingDialogContexts.isEmpty()) {
            return true;
        }
        if (dialogLock.tryLock()) {
            try {
                if ((!dialogContexts.isEmpty()) ||
                    (!pendingDialogContexts.isEmpty())) {
                    return true;
                }
            } finally {
                dialogLock.unlock();
            }
        } else {
            return true;
        }
        synchronized (preHandshakeContexts) {
            if (!preHandshakeContexts.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Writes one batch of dialog frames.
     *
     * @return the number of written frames, -1 if cannot acquire lock
     */
    private int writeOneDialogFrameBatch() {
        int written = 0;
        /*
         * Try acquire the lock. If someone else is trying to add or iterate on
         * writingContexts, i.e., they are writing or will write, let them do
         * their work.
         */
        if (writingLock.tryLock()) {
            try {
                Iterator<DialogContextImpl> iter = writingContexts.iterator();
                while (iter.hasNext()) {
                    DialogContextImpl context = iter.next();
                    if (context.isDone()) {
                        iter.remove();
                    }
                    if (context.onWriteDialogFrame()) {
                        iter.remove();
                    }
                    written ++;
                    if (written >= flushBatchNumContexts) {
                        break;
                    }
                }
            } finally {
                writingLock.unlock();
            }
        } else {
            return -1;
        }

        return written;
    }

    /**
     * Returns {@code true} if we have some dialog contexts to write.
     */
    private boolean hasContextsToWrite() {
        if (writingLock.tryLock()) {
            try {
                return !writingContexts.isEmpty();
            } finally {
                writingLock.unlock();
            }
        }
        return true;
    }

    /**
     * Called a protocol message is ready.
     */
    private void onMessageReady(ProtocolMesg mesg) {
        switch(mesg.type()) {
        case ProtocolMesg.PROTOCOL_VERSION_MESG:
            onReadProtocolVersion(
                    (ProtocolMesg.ProtocolVersion) mesg);
            break;
        case ProtocolMesg.PROTOCOL_VERSION_RESPONSE_MESG:
            onReadProtocolVersionResponse(
                    (ProtocolMesg.ProtocolVersionResponse) mesg);
            break;
        case ProtocolMesg.CONNECTION_CONFIG_MESG:
            onReadConnectionConfig(
                    (ProtocolMesg.ConnectionConfig) mesg);
            break;
        case ProtocolMesg.CONNECTION_CONFIG_RESPONSE_MESG:
            onReadConnectionConfigResponse(
                    (ProtocolMesg.ConnectionConfigResponse) mesg);
            break;
        case ProtocolMesg.NO_OPERATION_MESG:
            onReadNoOperation();
            break;
        case ProtocolMesg.CONNECTION_ABORT_MESG:
            onReadConnectionAbort((ProtocolMesg.ConnectionAbort) mesg);
            break;
        case ProtocolMesg.PING_MESG:
            onReadPing((ProtocolMesg.Ping) mesg);
            break;
        case ProtocolMesg.PINGACK_MESG:
            onReadPingAck((ProtocolMesg.PingAck) mesg);
            break;
        case ProtocolMesg.DIALOG_START_MESG:
            onReadDialogStart((ProtocolMesg.DialogStart) mesg);
            break;
        case ProtocolMesg.DIALOG_FRAME_MESG:
            onReadDialogFrame((ProtocolMesg.DialogFrame) mesg);
            break;
        case ProtocolMesg.DIALOG_ABORT_MESG:
            onReadDialogAbort((ProtocolMesg.DialogAbort) mesg);
            break;
        default:
            throw new IllegalArgumentException(
                    String.format(
                        "Unexpected message type: %s", mesg.type()));
        }
    }

    private void onReadProtocolVersion(ProtocolMesg.ProtocolVersion mesg) {
        ensureStateOrShuttingDownOrDie(State.HANDSHAKING_STEP1);
        if (isCreator) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_HANDLER_STATE +
                    "Received ProtocolVersion on creator endpoint");
        }
        if (mesg.version != ProtocolMesg.CURRENT_VERSION) {
            throw new ConnectionIncompatibleException(
                    false,
                    String.format(
                        "Incompatible version error: " +
                        "supported=%d, got=%d",
                        ProtocolMesg.CURRENT_VERSION, mesg.version));
        }
        if (transitStateOrShuttingDownOrDie(
                    State.HANDSHAKING_STEP1, State.HANDSHAKING_STEP2)) {
            getProtocolWriter().writeProtocolVersionResponse(1);
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                    "Endpoint handler got protocol version v={0}: {1}",
                    new Object[] { mesg.version, this });
        }
    }

    private void onReadProtocolVersionResponse(
            ProtocolMesg.ProtocolVersionResponse mesg) {
        ensureStateOrShuttingDownOrDie(State.HANDSHAKING_STEP1);
        if (!isCreator) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_HANDLER_STATE +
                    "Received ProtocolVersion on responder endpoint");
        }
        if (mesg.version != ProtocolMesg.CURRENT_VERSION) {
            throw new ConnectionIncompatibleException(
                    false,
                    String.format(
                        "Incompatible version error: " +
                        "supported=%d, got=%d",
                        ProtocolMesg.CURRENT_VERSION,
                        mesg.version));
        }
        if (transitStateOrShuttingDownOrDie(
                    State.HANDSHAKING_STEP1, State.HANDSHAKING_STEP2)) {
            getProtocolWriter().writeConnectionConfig(
                    connid, remoteMaxDlgs, remoteMaxLen, remoteMaxTotLen,
                    heartbeatInterval);
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                    "Endpoint handler got protocol version response " +
                    "v={0}: {1}",
                    new Object[] { mesg.version, this });

        }
    }

    private void onReadConnectionConfig(ProtocolMesg.ConnectionConfig mesg) {
        ensureStateOrShuttingDownOrDie(State.HANDSHAKING_STEP2);
        if (isCreator) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_HANDLER_STATE +
                    "Received ConnectionConfig on creator endpoint");
        }

        connid = mesg.uuid;

        setConfiguration(mesg.maxDialogs, mesg.maxLength, mesg.maxTotLen,
                mesg.heartbeatInterval);

        getProtocolWriter().writeConnectionConfigResponse(
                remoteMaxDlgs, remoteMaxLen, remoteMaxTotLen,
                heartbeatInterval);
        logger.log(Level.FINE, "Endpoint handler handshake done: {0}", this);
        onHandshakeDone();
    }

    private void onReadConnectionConfigResponse(
            ProtocolMesg.ConnectionConfigResponse mesg) {
        ensureStateOrShuttingDownOrDie(State.HANDSHAKING_STEP2);
        if (!isCreator) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_HANDLER_STATE +
                    "Received ConnectionConfig on responder endpoint");
        }

        setConfiguration(mesg.maxDialogs, mesg.maxLength, mesg.maxTotLen,
                mesg.heartbeatInterval);

        logger.log(Level.FINE, "Endpoint handler handshake done: {0}", this);
        onHandshakeDone();
    }

    private void onReadNoOperation() {
        ensureStateOrShuttingDownOrDie(State.NORMAL);
    }

    private void onReadConnectionAbort(ProtocolMesg.ConnectionAbort mesg) {
        markTerminating(mesg.cause, mesg.detail);
        terminate();
    }

    private void onReadPing(ProtocolMesg.Ping mesg) {
        ensureStateOrShuttingDownOrDie(State.NORMAL);
        getProtocolWriter().writePingAck(mesg.cookie);
    }

    private void onReadPingAck(ProtocolMesg.PingAck mesg) {
        ensureStateOrShuttingDownOrDie(State.NORMAL);
        Runnable callback = pendingPings.get(mesg.cookie);
        if (callback == null) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_HANDLER_STATE +
                    String.format("Wrong cookie for PingAck: cookie=%d",
                        mesg.cookie));
        }
        try {
            callback.run();
        } catch (Throwable t) {
            logger.log(Level.WARNING,
                    "Ping ack callback got exception: {0}",
                    CommonLoggerUtils.getStackTrace(t));
        }
    }

    private void onReadDialogStart(ProtocolMesg.DialogStart mesg) {
        ensureStateOrShuttingDownOrDie(State.NORMAL);

        long dialogId = mesg.dialogId;
        ensureDialogStartIdValidOrDie(dialogId);
        latestRemoteStartedDialogId = dialogId;

        if (isShuttingDownOrAfter()) {
            final String detail = String.format("cause=[%s]",
                    terminationInfo.exception().getMessage());
            getProtocolWriter().writeDialogAbort(
                    ProtocolMesg.DialogAbort.Cause.ENDPOINT_SHUTTINGDOWN,
                    dialogId,
                    String.format(
                        "Dialog rejected because " +
                        "endpoint is shutting down, %s",
                        detail));
        }

        int dialogType = mesg.typeno;
        DialogHandlerFactory factory = dialogHandlerFactories.get(dialogType);
        if (factory == null) {
            StringBuilder sb = new StringBuilder("known type numbers:");
            for (int knownType : dialogHandlerFactories.keySet()) {
                sb.append(knownType);
                sb.append(",");
            }
            if (logger.isLoggable(Level.INFO)) {
                logger.log(Level.INFO,
                        "Endpoint handler encounters that " +
                        "remote wants to start unknown dialog type: {0}; {1}",
                        new Object[] { mesg, sb});
            }
            getProtocolWriter().writeDialogAbort(
                    ProtocolMesg.DialogAbort.Cause.UNKNOWN_TYPE,
                    dialogId, sb.toString());
            return;
        }
        DialogHandler handler = factory.create();

        if (!remoteDialogResource.tryAcquire()) {
            String detail =
                ProtocolViolationException.ERROR_MAX_DIALOGS +
                String.format("limit=%d #active=%d",
                        remoteMaxDlgs,
                        (remoteMaxDlgs -
                         remoteDialogResource.availablePermits()));
            throw new ProtocolViolationException(false, detail);
        }

        noDialogActive = false;
        DialogContextImpl context = new DialogContextImpl(
                this, handler,
                sequencer.incrementAndGet(),
                dialogId, dialogType, mesg.timeoutMillis);
        dialogContexts.put(dialogId, context);

        if (isShuttingDownOrAfter()) {
            abortContextAfterShuttingDown(context);
            return;
        }

        endpointPerf.onDialogStarted();
        if (mesg.sampled) {
            context.getPerf().startSampling();
        }

        context.callOnStart(true);
        context.startTimeout();
        context.onReadDialogFrame(mesg.finish, mesg.cont, mesg.frame);

    }

    private void onReadDialogFrame(ProtocolMesg.DialogFrame mesg) {
        ensureStateOrShuttingDownOrDie(State.NORMAL);

        long dialogId = mesg.dialogId;
        ensureDialogFrameAbortIdValidOrDie(dialogId);

        DialogContextImpl context = dialogContexts.get(dialogId);
        if (context == null) {
            /*
             * This is allowed since the dialog might be aborted. It could also
             * be a protocol violation when the dialog was never started, but
             * this seems harmless. In either case, just ignore.
             */
            return;
        }
        context.onReadDialogFrame(mesg.finish, mesg.cont, mesg.frame);
    }

    private void onReadDialogAbort(ProtocolMesg.DialogAbort mesg) {
        ensureStateOrShuttingDownOrDie(State.NORMAL);

        long dialogId = mesg.dialogId;
        ensureDialogFrameAbortIdValidOrDie(dialogId);

        DialogContextImpl context = dialogContexts.get(dialogId);
        if (context == null) {
            /* Same with onReadDialogFrame */
            return;
        }
        context.onReadDialogAbort(mesg.cause, mesg.detail);
    }

    private synchronized void ensureStateOrShuttingDownOrDie(State target) {
        if (state == target) {
            return;
        }
        if (isShuttingDownOrAfter()) {
            return;
        }
        throw new ProtocolViolationException(
                false,
                ProtocolViolationException.
                ERROR_INVALID_HANDLER_STATE +
                String.format("expected=%s, got=%s", target, state));
    }

    private synchronized boolean
        transitStateOrShuttingDownOrDie(State from, State to) {

        if (state == from) {
            state = to;
            return true;
        } else if (isShuttingDownOrAfter()) {
            return false;
        } else {
            throw new IllegalStateException(
                    String.format(
                        "Trying to transit from %s, got %s",
                        from, state));
        }

    }

    private void setConfiguration(long maxDialogs,
                                  long maxLength,
                                  long maxTotLen,
                                  long interval) {
        localMaxDlgs = Math.min(localMaxDlgs, (int) maxDialogs);
        localMaxLen =  Math.min(localMaxLen, (int) maxLength);
        localMaxTotLen =  Math.min(localMaxTotLen, (int) maxTotLen);
        heartbeatInterval =  Math.max(heartbeatInterval, (int) interval);
    }

    private void onHandshakeDone() {
        /* update configurations */
        getProtocolWriter().setMaxLength(localMaxLen);
        localDialogResource.release(localMaxDlgs);
        /* transit state */
        if (transitStateOrShuttingDownOrDie(
                    State.HANDSHAKING_STEP2, State.NORMAL)) {
            synchronized(preHandshakeContexts) {
                for (DialogContextImpl context : preHandshakeContexts) {
                    tryStartDialog(context, true);
                }
                preHandshakeContexts.clear();
            }
        }
        /* Notify awaited threads. */
        synchronized(this) {
            notifyAll();
        }
        /*
         * Deal with pending shutdown. We only need to mark shutting down, and
         * let the already submitted dialogs keep running.
         */
        if (pendingShutdown) {
            markShuttingDown(terminationInfo.exception().getMessage());
        }
        /* Cancel and schedule tasks. */
        connectTimeoutTask.cancel();
        (new HeartbeatTimeoutTask()).schedule();
        (new HeartbeatTask()).schedule();
        (new IdleTimeoutTask()).schedule();
        endpointPerf.schedule(getSchedExecService());
    }

    private void ensureDialogStartIdValidOrDie(long dialogId) {
        boolean valid = isCreator ?
            (dialogId < latestRemoteStartedDialogId) :
            (dialogId > latestRemoteStartedDialogId);
        if (!valid) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_DIALOG_STATE +
                    String.format(
                        "Received DialogStart, isCreator=%s " +
                        "latestId=%s got=%s",
                        isCreator,
                        Long.toString(latestRemoteStartedDialogId, 16),
                        Long.toString(dialogId, 16)));
        }
    }

    private void ensureDialogFrameAbortIdValidOrDie(long dialogId) {
        boolean validId =
            ((isCreator && (dialogId > 0) &&
              (dialogId <= latestLocalStartedDialogId)) ||
             (isCreator && (dialogId < 0) &&
              (dialogId >= latestRemoteStartedDialogId)) ||
             (!isCreator && (dialogId > 0) &&
              (dialogId <= latestRemoteStartedDialogId)) ||
             (!isCreator && (dialogId < 0) &&
              (dialogId >= latestLocalStartedDialogId)));
        boolean local = ((isCreator && (dialogId > 0)) ||
                         (!isCreator && (dialogId < 0)));
        long latest = (local) ? latestLocalStartedDialogId :
            latestRemoteStartedDialogId;
        if (!validId) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_DIALOG_STATE +
                    String.format(
                        "Received DialogFrame/DialogAbort, " +
                        "isCreator=%s dialogLocal=%s " +
                        "latestId=%s got=%s",
                        isCreator, local,
                        Long.toString(latest, 16),
                        Long.toString(dialogId, 16)));
        }

    }

    /**
     * Abort dialogs in the context maps when terminating.
     *
     * We need to ensure that every active dialog is notified and aborted.
     * There are two races that will cause a context to miss this:
     * - Newly added dialog context may not be seen during the iteration. These
     *   new contexts will be aborted by these adding methods by checking our
     *   state after the context is added.
     * - The writeDialogStartForContext will move context from
     *   pendingDialogContexts to dialogContexts, creating a moment when
     *   neither of the collections holds this context. We grab the
     *   dialogLock when we iterating through the maps to avoid missing that.
     */
    private void abortDialogs() {
        if (!isTerminatingOrAfter()) {
            throw new IllegalStateException(
                    "Abort dialogs should only happen " +
                    "after handler is terminating");
            /*
             * This method is always called after the endpoint handler is
             * marked terminating which creates a happen-before relationship
             * between isTerminatingOrAfter() == true and the following
             * operations. Any operations seeing isTerminatingOrAfter() ==
             * false happens before the following operations.
             *
             * Specifically, in startDialog, if, before exiting, the method
             * sees isShuttingDownOrAfter() == false, the adding operation
             * happens before we enter this method and hence dialog will be
             * seen in one of the context maps and will be aborted here if not
             * finished earlier. Otherwise, the dialog will be aborted in the
             * startDialog method.
             */
        }

        /*
         * Lock the dialogLock so that we will not see an intermediate state
         * when we move contexts from pendingDialogContexts to dialogContexts.
         */
        dialogLock.lock();
        try {
            /* Abort all dialogs in pendingDialogContexts. */
            while (!pendingDialogContexts.isEmpty()) {
                Iterator<DialogContextImpl> iter =
                    pendingDialogContexts.iterator();
                while (iter.hasNext()) {
                    DialogContextImpl context = iter.next();
                    abortContextAfterShuttingDown(context);
                    iter.remove();
                }
            }

            /* Abort all dialogs in dialogContexts. */
            while (!dialogContexts.isEmpty()) {
                Iterator<Map.Entry<Long, DialogContextImpl>> iter =
                    dialogContexts.entrySet().iterator();
                while (iter.hasNext()) {
                    DialogContextImpl context = iter.next().getValue();
                    abortContextAfterShuttingDown(context);
                    iter.remove();
                }
            }
        } finally {
            dialogLock.unlock();
        }

        /* Abort all dialogs in preHandshakeContexts. */
        synchronized(preHandshakeContexts) {
            for (DialogContextImpl context : preHandshakeContexts) {
                NullDialogStart.fail(
                        context.getDialogHandler(),
                        terminationInfo.exception().getDialogException(false));
            }
            preHandshakeContexts.clear();
        }
        /* Also clear up writing contexts. */
        writingLock.lock();
        try {
            writingContexts.clear();
        } finally {
            writingLock.unlock();
        }
    }

    private void cancelScheduledTasks() {
        for (Future<?> future : scheduledTasks) {
            future.cancel(false);
        }
        connectTimeoutTask.cancel();
    }

    /* Information of the termination. */
    private class TerminationInfo {
        private final State preState;
        private final ConnectionException exception;
        private final String stackTrace;

        TerminationInfo(ConnectionException exception) {
            this.preState = state;
            this.exception = exception;
            this.stackTrace = CommonLoggerUtils.getStackTrace(exception);
        }

        boolean fromRemote() {
            return exception.fromRemote();
        }

        ConnectionException exception() {
            return exception;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("(");
            sb.append("preState=").append(preState);
            sb.append(" exception=").append(exception);
            sb.append(" fromRemote=").append(fromRemote());
            sb.append(" stackTrace=").append(stackTrace);
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Information for local graceful shutdown.
     */
    private class ShutdownInfo extends TerminationInfo {

        ShutdownInfo(String detail) {
            super(new ConnectionEndpointShutdownException(false, detail));
        }
    }

    /**
     * Information for abrupt abort, local or remote.
     */
    private class AbortInfo extends TerminationInfo {

        /**
         * Constructs an info that represents an abort by remote.
         */
        AbortInfo(ProtocolMesg.ConnectionAbort.Cause protocolCause,
                  String detail) {
            super(causeToException(protocolCause, detail));
        }

        /**
         * Info represents an abort by remote.
         */
        AbortInfo(ConnectionException exception) {
            super(exception);
        }

    }

    private ConnectionException causeToException(
            ProtocolMesg.ConnectionAbort.Cause cause,
            String detail)  {
        switch (cause) {
        case UNKNOWN_REASON:
            return new ConnectionUnknownException(detail);
        case ENDPOINT_SHUTDOWN:
            return new ConnectionEndpointShutdownException(true, detail);
        case HEARTBEAT_TIMEOUT:
            return new ConnectionTimeoutException(true, false, detail);
        case IDLE_TIMEOUT:
            return new ConnectionIdleException(true, detail);
        case INCOMPATIBLE_ERROR:
            return new ConnectionIncompatibleException(true, detail);
        case PROTOCOL_VIOLATION:
            return new ProtocolViolationException(true, detail);
        default:
            throw new IllegalArgumentException();
        }
    }

    private ProtocolMesg.ConnectionAbort.Cause exceptionToCause(
            ConnectionException e) {

        if (e instanceof ConnectionUnknownException) {
            return ProtocolMesg.ConnectionAbort.Cause.UNKNOWN_REASON;
        } else if (e instanceof ConnectionEndpointShutdownException) {
            return ProtocolMesg.ConnectionAbort.Cause.ENDPOINT_SHUTDOWN;
        } else if (e instanceof ConnectionTimeoutException) {
            return ProtocolMesg.ConnectionAbort.Cause.HEARTBEAT_TIMEOUT;
        } else if (e instanceof ConnectionIdleException) {
            return ProtocolMesg.ConnectionAbort.Cause.IDLE_TIMEOUT;
        } else if (e instanceof ConnectionIncompatibleException) {
            return ProtocolMesg.ConnectionAbort.Cause.INCOMPATIBLE_ERROR;
        } else if (e instanceof ProtocolViolationException) {
            return ProtocolMesg.ConnectionAbort.Cause.PROTOCOL_VIOLATION;
        } else if (e instanceof ConnectionIOException) {
            /*
             * We have an IOException here. The protocol cause does not
             * matter since we will not be able to write the cause anyway.
             * Returns unknown reason.
             */
            return ProtocolMesg.ConnectionAbort.Cause.UNKNOWN_REASON;
        } else {
            throw new IllegalArgumentException();
        }
    }

    private class ConnectTimeoutTask implements Runnable {

        private Future<?> future = null;

        @Override
        public void run() {
            markTerminating(
                    new ConnectionTimeoutException(
                        false, /* fromRemote */
                        true, /* shouldBackoff */
                        String.format(
                            "Connect timeout, " +
                            "handshake is not done within %d ms " +
                            "since the endpoint handler is created, %s",
                            connectTimeout,
                            AbstractDialogEndpointHandler.this)));
            terminate();
        }

        synchronized void schedule() {
            if (future != null) {
                throw new AssertionError();
            }
            try {
                future = getSchedExecService().schedule(
                        this, connectTimeout, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                markTerminating(t);
                terminate();
            }
        }

        synchronized void cancel() {
            if (future == null) {
                /*
                 * It is possible that the endpoint handler is terminated
                 * before this task is scheduled. In that case just return.
                 */
                return;
            }
            future.cancel(false);
        }
    }

    private class HeartbeatTimeoutTask implements Runnable {

        @Override
        public void run() {
            if (noReadLastInterval) {
                markTerminating(
                        new ConnectionTimeoutException(
                            false, /* fromRemote */
                            false, /* shouldBackoff */
                            String.format(
                                "Heartbeat timeout, " +
                                "no read event during last %d ms, %s",
                                heartbeatTimeout * heartbeatInterval,
                                this)));
                terminate();
            }
            noReadLastInterval = true;
        }

        void schedule() {
            try {
                scheduledTasks.add(
                        getSchedExecService().scheduleAtFixedRate(
                            this, 0, heartbeatTimeout * heartbeatInterval,
                            TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
                markTerminating(t);
                terminate();
            }
        }
    }

    private class HeartbeatTask implements Runnable {

        @Override
        public void run() {
            if (noDialogFlushLastInterval) {
                /*
                 * Send noOp when there is no dialog frame flushed to the
                 * transport during last interval.
                 */
                getProtocolWriter().writeNoOperation();
                flushOrTerminate();
            }
            noDialogFlushLastInterval = true;
        }

        void schedule() {
            try {
                scheduledTasks.add(
                        getSchedExecService().scheduleAtFixedRate(
                            this, 0, heartbeatInterval,
                            TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
                markTerminating(t);
                terminate();
            }
        }
    }

    private class IdleTimeoutTask implements Runnable {

        @Override
        public void run() {
            /*
             * Terminate the endpoint when no dialog active. Note that the
             * following code can create cases when noDialogActive is checked
             * as true, but then some dialogs are added before we terminate,
             * causing these dialogs to be aborted immediately. There is no
             * correctness issue since the startDialog method will see the
             * state change and abort these dialogs. The issue is with
             * performance.
             *
             * To avoid this issue, we need to add a lock inside startDialog,
             * and since it is a rare case for this to happen while it is a
             * common routine for the startDialog method to add new context,
             * it seems not worth to solve the issue.
             */
            if (noDialogActive) {
                markTerminating(
                        new ConnectionIdleException(
                            false,
                            String.format(
                                "Idle timeout, " +
                                "connection is idle during last %s ms",
                                idleTimeout)));
                terminate();
            }

            /*
             * We must be careful about a race that a concurrent startDialog
             * set noDialogActive to false, but hasActiveDialogs does not see
             * the new dialog, returning false and overwriting noDialogActive
             * to true. This can terminate the endpoint when the connection is
             * actually not idle.
             *
             * The following code solve this since in startDialog, the context
             * is added to pendingDialogContexts before noDialogActive is set.
             * If noDialogActive is incorrectly overwritten, the
             * hasActiveDialogs method here will see the context in
             * pendingDialogContexts which will set noDialogActive correctly.
             */
            noDialogActive = true;
            if (hasActiveDialogs()) {
                noDialogActive = false;
            }
        }

        void schedule() {
            try {
                scheduledTasks.add(
                        getSchedExecService().scheduleAtFixedRate(
                            this, idleTimeout, idleTimeout,
                            TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
                markTerminating(t);
                terminate();
            }
        }
    }

}

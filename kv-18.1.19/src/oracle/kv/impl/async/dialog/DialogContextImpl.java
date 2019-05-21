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

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.async.BytesInput;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.async.exception.ConnectionException;
import oracle.kv.impl.async.exception.ConnectionUnknownException;
import oracle.kv.impl.async.exception.ContextWriteExceedsLimitException;
import oracle.kv.impl.async.exception.ContextWriteFinException;
import oracle.kv.impl.async.exception.DialogNoSuchTypeException;
import oracle.kv.impl.async.exception.DialogUnknownException;
import oracle.kv.impl.async.perf.DialogPerf;
import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * Implements {@link DialogContext}.
 *
 * Notes for the implementation:
 * - DialogHandler methods must be called inside the executor thread.
 * - onStart method must be the first to be called. The write method should
 *   throw exception and read method return null before onStart is called.
 * - After onAbort is called, onCanRead/onCanWrite should not be called; the
 *   write should fail; read should return null. Write may fail before onAbort
 *   is called.
 * - After DialogAbort is sent, we should not send DialogStart or DialogFrame.
 * - Endpoint handler methods may acquire locks, therefore, to prevent deadlock,
 *   these methods should not appear inside a synchronization block.
 */
class DialogContextImpl implements DialogContext {

    public static final String TIMEOUT_INFO_PREFIX = "Dialog timed out: ";

    enum State {
        /* Inited, need to actually start the dialog */
        INITED_NEED_DIALOGSTART,

        /* Started */
        STARTED,

        /* Received last message, but not all messages are polled yet */
        READ_FIN0,

        /* Received last message, all messages are polled */
        READ_FIN,

        /* Wrote last message, but not yet sent out */
        WRITE_FIN0,

        /* Sent last message */
        WRITE_FIN,

        /* READ_FIN0 and WRITE_FIN0 */
        READ_FIN0_WRITE_FIN0,

        /* READ_FIN and WRITE_FIN0 */
        READ_FIN_WRITE_FIN0,

        /* READ_FIN0 and WRITE_FIN */
        READ_FIN0_WRITE_FIN,

        /* Finished normally */
        FIN,

        /* Aborted */
        ABORTED,
    }

    private final DialogEndpointHandler endpointHandler;
    private final Logger logger;
    private final ProtocolWriter protocolWriter;
    private final DialogHandler dialogHandler;
    private final boolean onCreatingSide;

    private final long contextId;
    private volatile long dialogId;
    private final int typeno;
    private final long timeoutMillis;

    /*
     * Dialog state. State transitions are all in synchroned block of the
     * context. To prevent deadlock, we guarantee not to acquire locks inside
     * the state synchronization block.
     */
    private volatile State state;
    /* The flag indicating we have a pending outgoing message */
    private static final int WS_PENDINGMESG = 0x01;
    /* The flag indiciating we should call onCanWrite when pending message is sent. */
    private static final int WS_CALLBACK = 0x02;
    /* The flag indicating we have an outstanding message. */
    private final AtomicInteger writeState = new AtomicInteger(0);
    /*
     * We have written the last message. The flag will be flipped to true when
     * we write the last message, it will never be false again.
     */
    private volatile boolean lastMesgWritten = false;
    /*
     * The frames to send. All access must be inside the context
     * synchronization block.
     */
    private Queue<List<ByteBuffer>> outputFrames = null;
    /* An object to sync on to prevent writing DialogFrame after DialogAbort */
    private final Object frameAndAbortSync = new Object();
    /*
     * The complete messages received. All accesses are inside the synchronized
     * block of this object.
     */
    private Queue<MessageInput> inputMessages = new LinkedList<MessageInput>();
    /*
     * The still-to-complete receiving message. All accesses must be inside
     * context synchronization block.
     */
    private MessageInput messageReceiving = new MessageInput();
    /*
     * The size of the current receiving message. All accesses are in one
     * single thread of the executor.
     */
    private int sizeOfMessageReceiving = 0;

    /* Timeout task */
    private final DialogTimeoutTask timeoutTask = new DialogTimeoutTask();

    private final AtomicBoolean onStartCalled = new AtomicBoolean(false);
    private final Runnable onStartTask = new OnStartTask();
    /* Information of abort, modified inside context synchronization block. */
    private volatile AbortInfo abortInfo = null;
    private final AtomicBoolean onAbortCalled = new AtomicBoolean(false);
    private final AtomicBoolean dialogAbortWritten = new AtomicBoolean(false);
    private final Runnable onAbortTask = new OnAbortTask();
    /* Dialog done flag */
    private final AtomicBoolean isDone = new AtomicBoolean(false);
    /* Perf */
    private final DialogPerf perf = new DialogPerf(this);

    DialogContextImpl(DialogEndpointHandler endpointHandler,
                      DialogHandler dialogHandler,
                      long contextId,
                      long dialogId,
                      int typeno,
                      long timeoutMillis) {
        this.endpointHandler = endpointHandler;
        this.logger = endpointHandler.getLogger();
        this.protocolWriter = endpointHandler.getProtocolWriter();
        this.dialogHandler = dialogHandler;
        this.onCreatingSide = (dialogId == 0);
        this.contextId = contextId;
        this.dialogId = dialogId;
        this.typeno = typeno;
        this.timeoutMillis = timeoutMillis;
        this.state = (dialogId == 0) ?
            State.INITED_NEED_DIALOGSTART : State.STARTED;

        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "Created dialog context: {0}", this);
        }
    }

    /**
     * Writes a new message output for the dialog.
     *
     * This method may be called by multiple threads.
     */
    @Override
    public boolean write(MessageOutput mesg, boolean finished) {
        final int mesgSize = mesg.size();
        final int maxTotLen = endpointHandler.getMaxOutputTotLen();
        if (mesgSize > maxTotLen) {
            throw new ContextWriteExceedsLimitException(mesgSize, maxTotLen);
        }

        final Queue<List<ByteBuffer>> frames =
            mesg.pollFrames(endpointHandler.getMaxOutputProtocolMesgLen());

        State prev;
        synchronized(this) {
            /* Quick fail for the exception cases. */
            switch (state) {
            case WRITE_FIN0:
            case WRITE_FIN:
            case READ_FIN0_WRITE_FIN0:
            case READ_FIN_WRITE_FIN0:
            case READ_FIN0_WRITE_FIN:
            case FIN:
                throw new ContextWriteFinException(
                        perf.getNumOutputsWritten());
            case ABORTED:
                return false;
            default:
                break;
            }
            /* Return false if there is a pending outgoing message. */
            if (!setWriteState()) {
                return false;
            }

            prev = state;
            switch (state) {
            case INITED_NEED_DIALOGSTART:
            case STARTED:
                if (finished) {
                    state = State.WRITE_FIN0;
                } else {
                    state = State.STARTED;
                }
                break;
            case READ_FIN0:
                if (finished) {
                    state = State.READ_FIN0_WRITE_FIN0;
                }
                break;
            case READ_FIN:
                if (finished) {
                    state = State.READ_FIN_WRITE_FIN0;
                }
                break;
            default:
                throw new AssertionError();
            }

            if (finished) {
                lastMesgWritten = true;
            }
            outputFrames = frames;
        }

        perf.onEvent(DialogPerf.Event.WRITE);

        /*
         * Only one thread will arrive here since all concurrent writes will
         * return after we cas on within the setWriteState method.
         */

        /*
         * Use local variable frames instead of outputFrames since it might be
         * cleaned up to null.
         */
        if ((prev == State.INITED_NEED_DIALOGSTART) &&
            (!frames.isEmpty())) {
            assert dialogId == 0;
            List<ByteBuffer> frame = frames.poll();
            boolean cont = !frames.isEmpty();
            boolean last = finished && !cont;
            dialogId = endpointHandler.writeDialogStartForContext(
                    last, cont, typeno, timeoutMillis, frame, this);

            perf.onEvent(DialogPerf.Event.SEND);

            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST,
                        "Dialog Id assigned to the context: {0}", this);
            }
        }

        /*
         * If we are aborted here, we need to write a DialogAbort since when
         * onLocalAbort is called the dialogId may not be assigned yet.
         */
        if (isAborted()) {
            writeDialogAbort();
            return false;
        }

        /* Notify the endpoint handler that we have frames to write. */
        endpointHandler.onContextNewWrite(this);

        if (isFin()) {
            cleanupContext();
        }
        return true;
    }

    /**
     * Reads a new message input for the dialog.
     */
    @Override
    public MessageInput read() {
        MessageInput input;
        synchronized(this) {
            if (inputMessages == null) {
                return null;
            }
            input = inputMessages.poll();
            if (input == null) {
                return null;
            }
            if (inputMessages.isEmpty()) {
                switch(state) {
                case INITED_NEED_DIALOGSTART:
                case STARTED:
                    break;
                case READ_FIN0:
                    state = State.READ_FIN;
                    break;
                case READ_FIN:
                case WRITE_FIN0:
                case WRITE_FIN:
                    break;
                case READ_FIN0_WRITE_FIN0:
                    state = State.READ_FIN_WRITE_FIN0;
                    break;
                case READ_FIN_WRITE_FIN0:
                    break;
                case READ_FIN0_WRITE_FIN:
                    state = State.FIN;
                    break;
                case FIN:
                    throw new IllegalStateException();
                case ABORTED:
                    return null;
                default:
                    throw new AssertionError();
                }
            }
        }
        perf.onEvent(DialogPerf.Event.READ);
        if (isFin()) {
            cleanupContext();
        }

        return input;
    }

    /**
     * Returns the dialogId of this context.
     */
    @Override
    public long getDialogId() {
        return dialogId;
    }

    /**
     * Returns the connection ID.
     */
    @Override
    public long getConnectionId() {
        return endpointHandler.getConnID();
    }

    /**
     * Returns the network address of the remote handler.
     */
    @Override
    public NetworkAddress getRemoteAddress() {
        return endpointHandler.getRemoteAddress();
    }

    /**
     * Returns the executor service associated with this context.
     */
    @Override
    public ScheduledExecutorService getSchedExecService() {
        return endpointHandler.getSchedExecService();
    }

    public boolean isDone() {
        return isDone.get();
    }

    public boolean isFin() {
        return state == State.FIN;
    }

    public boolean isAborted() {
        return state == State.ABORTED;
    }

    public DialogHandler getDialogHandler() {
        return dialogHandler;
    }

    public DialogPerf getPerf() {
        return perf;
    }

    /**
     * Starts the timeout for the dialog.
     */
    void startTimeout() {
        timeoutTask.schedule();
    }

    /**
     * Calls the onStart method.
     */
    void callOnStart(boolean inExecutorThread) {
        if (inExecutorThread) {
            onStartTask.run();
        } else {
            try {
                getSchedExecService().execute(onStartTask);
            } catch (RejectedExecutionException e) {
                /*
                 * The executor is rejecting tasks, the connection must be shut
                 * down if not yet. Just do nothing here. Someone will try to
                 * abort us and onStart will be called in the onAbortTask.
                 */
            }
        }
    }

    /**
     * Called by the endpoint handler when reading a DialogFrame.
     *
     * This method will only be called inside the executor thread.
     */
    void onReadDialogFrame(boolean finish,
                           boolean cont,
                           BytesInput frame) {
        endpointHandler.assertInExecutorThread();

        int readableBytes = frame.remaining();
        int size = sizeOfMessageReceiving + readableBytes;
        final int maxTotLen = endpointHandler.getMaxInputTotLen();
        if (size > maxTotLen) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_MAX_TOTLEN_EXCEEDED +
                    String.format(
                        "Received DialogFrame, limit=%d, mesgTotLen=%d, " +
                        "context=%s",
                        maxTotLen, size, this));
        }

        synchronized(this) {
            /*
             * Update the accounting first so that the sizeOfMessageReceiving
             * is always updated even if we are aborted and return early.
             */
            if (!cont) {
                sizeOfMessageReceiving = 0;
            } else {
                sizeOfMessageReceiving += readableBytes;
            }

            switch(state) {
            case INITED_NEED_DIALOGSTART:
                /*
                 * The endpoint handler should already see this as invalid
                 * since we don't have a dialogId yet.
                 */
                throw new AssertionError();
            case STARTED:
                if (finish) {
                    state = State.READ_FIN0;
                }
                break;
            case READ_FIN0:
            case READ_FIN:
                throw new ProtocolViolationException(
                        false,
                        ProtocolViolationException.
                        ERROR_INVALID_DIALOG_STATE +
                        String.format(
                            "Received DialogFrame, context=%s", this));
            case WRITE_FIN0:
                if (finish) {
                    state = State.READ_FIN0_WRITE_FIN0;
                }
                break;
            case WRITE_FIN:
                if (finish) {
                    state = State.READ_FIN0_WRITE_FIN;
                }
                break;
            case READ_FIN0_WRITE_FIN0:
            case READ_FIN_WRITE_FIN0:
            case READ_FIN0_WRITE_FIN:
            case FIN:
                throw new ProtocolViolationException(
                        false,
                        ProtocolViolationException.
                        ERROR_INVALID_DIALOG_STATE +
                        String.format(
                            "Received DialogFrame, context=%s", this));
            case ABORTED:
                /* We were somehow aborted, just ignore the frame */
                return;
            default:
                throw new AssertionError();
            }

            messageReceiving.add(frame);
            perf.onEvent(DialogPerf.Event.RECV);

            if (cont) {
                return;
            }

            inputMessages.add(messageReceiving);
            messageReceiving = new MessageInput();
        }

        try {
            dialogHandler.onCanRead(this, finish);
        } catch (Throwable t) {
            if (logger.isLoggable(Level.INFO)) {
                logger.log(Level.INFO,
                        "Encountered error with dialog handler: " +
                        "context={0}, error={1}",
                        new Object[] {
                            this, CommonLoggerUtils.getStackTrace(t) });
            }
            /*
             * It needs extra lockings to make sure about the side effect.
             * Seems not worth it. Just set it to true.
             */
            onLocalAbortHandlerError(t);
            return;
        }

        if (isFin()) {
            cleanupContext();
        }
    }

    /**
     * Called by the endpoint handler for the context to write next frame.
     *
     * The endpoint handler should ensure that only one thread is calling this
     * method.
     *
     * @return {@code true} if all frames are written
     */
    boolean onWriteDialogFrame() {
        List<ByteBuffer> frame = null;
        boolean lastFrame;

        synchronized(this) {
            if (isAborted()) {
                return true;
            }

            if (outputFrames == null) {
                lastFrame = true;
            } else {
                frame = outputFrames.poll();
                lastFrame = outputFrames.isEmpty();
            }

            if (lastFrame) {
                switch (state) {
                case INITED_NEED_DIALOGSTART:
                    /* Should already inited and written the DialogStart */
                    throw new AssertionError();
                case STARTED:
                case READ_FIN0:
                case READ_FIN:
                    break;
                case WRITE_FIN0:
                    state = State.WRITE_FIN;
                    break;
                case WRITE_FIN:
                    break;
                case READ_FIN0_WRITE_FIN0:
                    state = State.READ_FIN0_WRITE_FIN;
                    break;
                case READ_FIN_WRITE_FIN0:
                    state = State.FIN;
                    break;
                case READ_FIN0_WRITE_FIN:
                case FIN:
                    /* We already sent all the frames. */
                    throw new AssertionError();
                case ABORTED:
                    break;
                default:
                    throw new AssertionError();
                }

                outputFrames = null;
            }
        }

        if (frame != null) {
            /* Prevent write after a DialogAbort with a sync block. */
            synchronized(frameAndAbortSync) {
                if (state != State.ABORTED) {
                    protocolWriter.writeDialogFrame(
                            (lastMesgWritten && lastFrame), !lastFrame,
                            dialogId, frame);
                    perf.onEvent(DialogPerf.Event.SEND);
                }
            }
        }

        if (lastFrame) {
            onWrittenLastFrame();
        }

        if (isFin()) {
            cleanupContext();
        }

        return lastFrame;
    }

    /**
     * Called by the endpoint handler when reading a DialogAbort.
     *
     * This method will only be called inside the executor thread.
     */
    void onReadDialogAbort(ProtocolMesg.DialogAbort.Cause cause,
                           String detail) {
        endpointHandler.assertInExecutorThread();
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            /*
             * Creates abortInfo before assign state to ABORTED so that seeing
             * ABORTED state will guarantee to see abortInfo.
             */
            abortInfo = new AbortInfo(cause, detail);
            state = State.ABORTED;
        }
        cleanupContext();
        onAbortTask.run();
    }

    /**
     * Called when aborted locally because of timeout.
     *
     * This method will only be called inside the executor thread.
     */
    void onLocalAbortTimeout() {
        endpointHandler.assertInExecutorThread();
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            abortInfo = new AbortInfo(
                    new RequestTimeoutException(
                        (int) timeoutMillis,
                        TIMEOUT_INFO_PREFIX + toString(),
                        null,
                        false),
                    ProtocolMesg.DialogAbort.Cause.TIMED_OUT);
            state = State.ABORTED;
        }
        writeDialogAbort();
        cleanupContext();
        onAbortTask.run();
    }

    /**
     * Called when aborted locally because of a dialog exception.
     *
     * This method will only be called inside the executor thread.
     */
    void onLocalAbortHandlerError(Throwable cause) {
        endpointHandler.assertInExecutorThread();
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            abortInfo = new AbortInfo(
                    new DialogUnknownException(
                        hasSideEffect(),
                        false /* not from remote */,
                        cause.getMessage(),
                        cause),
                    ProtocolMesg.DialogAbort.Cause.UNKNOWN_REASON);
            state = State.ABORTED;
        }
        writeDialogAbort();
        cleanupContext();
        onAbortTask.run();
    }

    /**
     * Called when aborted locally because of a connection exception.
     */
    void onLocalAbortConnectionException(ConnectionException cause) {
        synchronized(this) {
            if (isAborted() || isFin()) {
                return;
            }
            abortInfo = new AbortInfo(
                    cause.getDialogException(hasSideEffect()),
                    ProtocolMesg.DialogAbort.Cause.CONNECTION_ABORT);
            state = State.ABORTED;
        }
        writeDialogAbort();
        cleanupContext();
        try {
            getSchedExecService().execute(onAbortTask);
        } catch (RejectedExecutionException e) {
            /* The executor is not working any more, just run the task here. */
            onAbortTask.run();
        }
    }

    /**
     * Returns {@code true} if may have side effect on the peer endpoint.
     *
     * We can decide whether this dialog may have any side effect on the peer
     * endpoint by checking if we have sent out any message.
     *
     * This side effect flag is only useful for creating side of a dialog since
     * the responding side usually does not care about whether the dialog has
     * side effect on the creating side.
     *
     * TODO: we can have the responding side send a side effect flag on the
     * dialog abort message to get a more accurate view of this flag, but not
     * sure if worth the trouble.
     *
     * The method should be called inside a synchronization block of this object.
     */
    private boolean hasSideEffect() {
        if (!onCreatingSide) {
            /*
             * Always mark as having side effect if we are the responding side.
             */
            return true;
        }
        return state != State.INITED_NEED_DIALOGSTART;
    }

    /**
     * Called when we finished writing all frames of the outstanding message.
     */
    private void onWrittenLastFrame() {
        final boolean callOnCanWrite = clearWriteState();
        if (!callOnCanWrite) {
            return;
        }

        getSchedExecService().execute(new Runnable() {
            @Override
            public void run() {
                if ((state != State.ABORTED) && (!lastMesgWritten)) {
                    try {
                        dialogHandler.onCanWrite(DialogContextImpl.this);
                    } catch (Throwable cause) {
                        if (logger.isLoggable(Level.INFO)) {
                            logger.log(Level.INFO,
                                    "Encountered error with dialog handler: " +
                                    "context={0}, error={1}",
                                    new Object[] {
                                        this,
                                        CommonLoggerUtils.getStackTrace(
                                                cause) });
                        }
                        /*
                         * We can make sure if there is side effect by
                         * recording if this is the first message, but it does
                         * not seem to worth the trouble.
                         */
                        onLocalAbortHandlerError(cause);
                    }
                }
            }
        });
    }

    /**
     * Transits the write state to indicate we have a pending outgoing message.
     *
     * Returns {@code false} if there is already a pending outgoing message.
     */
    private boolean setWriteState() {
        while (true) {
            final int curr = writeState.get();
            if ((curr & WS_PENDINGMESG) != 0) {
                /* already have pending outgoing message */
                if ((curr & WS_CALLBACK) != 0) {
                    /* already need call onCanWrite */
                    return false;
                }
                final int next = (curr | WS_CALLBACK);
                if (writeState.compareAndSet(curr, next)) {
                    return false;
                }
                continue;
            }

            /* no pending outgoing message */
            if ((curr & WS_CALLBACK) != 0) {
                throw new IllegalStateException();
            }

            final int next = curr | WS_PENDINGMESG;
            if (writeState.compareAndSet(curr, next)) {
                return true;
            }
        }
    }

    /**
     * Transit the write state to indicate we no longer have a pending outgoing
     * message.
     *
     * Clear the callback flag as well and returns {@code true} if need to call
     * onCanWrite.
     */
    private boolean clearWriteState() {
        while (true) {
            final int curr = writeState.get();
            if ((curr & WS_PENDINGMESG) == 0) {
                throw new IllegalStateException();
            }

            final int next = 0;
            if (writeState.compareAndSet(curr, next)) {
                return (curr & WS_CALLBACK) != 0;
            }
        }
    }

    /* Information of the dialog abort. */
    private class AbortInfo {
        private final State preState;
        private final ProtocolMesg.DialogAbort.Cause cause;
        private final String detail;
        private final Throwable throwable;
        private final boolean fromRemote;

        AbortInfo(ProtocolMesg.DialogAbort.Cause cause,
                  String detail) {
            this.preState = state;
            this.cause = cause;
            this.detail = getDetail(detail);
            this.throwable = causeToThrowable(cause, detail);
            this.fromRemote = true;
        }

        AbortInfo(Throwable throwable,
                  ProtocolMesg.DialogAbort.Cause cause) {
            this.preState = state;
            this.cause = cause;
            this.detail = getDetail(throwable.getMessage());
            this.throwable = throwable;
            this.fromRemote = false;
        }

        String getDetail(String s) {
            return (s == null) ? "null" : s;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("(");
            sb.append("preState=").append(preState);
            sb.append(" causeToSend=").append(cause);
            sb.append(" detail=").append(detail);
            sb.append(" throwable=").append(throwable);
            sb.append(" fromRemote=").append(fromRemote);
            sb.append(")");
            return sb.toString();
        }
    }

    private Throwable causeToThrowable(ProtocolMesg.DialogAbort.Cause c,
                                       String d)  {
        switch(c) {
        case UNKNOWN_REASON:
            return new DialogUnknownException(
                    hasSideEffect(),
                    true /* from remote */,
                    d, null /* no cause */);
        case CONNECTION_ABORT:
            return (new ConnectionUnknownException(d)).
                getDialogException(hasSideEffect());
        case ENDPOINT_SHUTTINGDOWN:
            /*
             * The dialog was rejected before doing anything, so no side
             * effect
             */
            return (new ConnectionEndpointShutdownException(true, d)).
                getDialogException(false);
        case TIMED_OUT:
            return new RequestTimeoutException(
                    (int) timeoutMillis,
                    TIMEOUT_INFO_PREFIX + toString(),
                    null, true);
        case UNKNOWN_TYPE:
            return new DialogNoSuchTypeException(d);
        default:
            throw new IllegalArgumentException(
                    String.format("Unknown dialog abort cause: %s", c));
        }
    }

    /**
     * Context is done, clean up.
     */
    private void cleanupContext() {
        if (isDone.compareAndSet(false, true)) {
            synchronized(this) {
                if (outputFrames != null) {
                    outputFrames.clear();
                    outputFrames = null;
                }
                if (inputMessages != null) {
                    inputMessages.clear();
                    inputMessages = null;
                }
                messageReceiving = null;
            }
            timeoutTask.cancel();
            if (isAborted()) {
                perf.onEvent(DialogPerf.Event.ABORT);
            } else {
                perf.onEvent(DialogPerf.Event.FIN);
            }
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "Dialog context done: {0}", this);
            }
            endpointHandler.onContextDone(this);
        }
    }

    /**
     * The task that calls the onStart method.
     */
    private class OnStartTask implements Runnable {
        @Override
        public void run() {
            /* Ensure onStart will only be called once. */
            if (onStartCalled.compareAndSet(false, true)) {
                try {
                    dialogHandler.onStart(DialogContextImpl.this, isAborted());
                } catch (Throwable t) {
                    onLocalAbortHandlerError(t);
                }
            }
        }
    }

    /**
     * The task that calls the onAbort method.
     */
    private class OnAbortTask implements Runnable {
        @Override
        public void run() {
            if (abortInfo == null) {
                throw new AssertionError();
            }
            /* Ensure we called onStart first */
            onStartTask.run();
            /* Ensure onAbort will only be called once. */
            if (onAbortCalled.compareAndSet(false, true)) {
                try {
                    dialogHandler.onAbort(
                            DialogContextImpl.this, abortInfo.throwable);
                } catch (Throwable t) {
                    /*
                     * The onAbort method is faulty. At this point, we
                     * cannot abort the dialog again. Just log.
                     */
                    logger.log(Level.WARNING,
                            "Exception in dialog handler: {0}",
                            new Object[] {CommonLoggerUtils.getStackTrace(t)});
                    return;
                }
            }
        }
    }

    /**
     * Write DialogAbort if we should.
     */
    private void writeDialogAbort() {
        if (abortInfo == null) {
            return;
        }
        if (abortInfo.fromRemote) {
            /* Don't write a DialogAbort if it is aborted by remote. */
            return;
        }
        if (dialogId == 0) {
            /*
             * Don't write DialogAbort when the dialogId is not assigned. We
             * may still need to write the message since the dialogId
             * assignment is not in the synchronization block of the context.
             * The write method will follow up on this case.
             */
            return;
        }
        /* Only write DialogAbort once. */
        if (dialogAbortWritten.compareAndSet(false, true)) {
            /* Prevent write before a DialogFrame with sync block. */
            synchronized(frameAndAbortSync) {
                protocolWriter.writeDialogAbort(
                        abortInfo.cause, dialogId, abortInfo.detail);
            }
        }
    }

    private class DialogTimeoutTask implements Runnable {

        private volatile Future<?> future = null;

        @Override
        public void run() {
            onLocalAbortTimeout();
        }

        void schedule() {
            if (future != null) {
                return;
            }
            final ScheduledExecutorService executor = getSchedExecService();
            if (executor == null) {
                /*
                 * This can happen when the dialog is submitted before the
                 * endpoint handler is initialized properly and associated with
                 * an executor. Just return, the task will be scheduled again.
                 */
                return;
            }
            try {
                future = executor.schedule(
                        DialogTimeoutTask.this, timeoutMillis,
                        TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                /*
                 * The executor is shutting down, therefore the connection must
                 * be aborted. Just abort the dialog here.
                 */
                onLocalAbortConnectionException(
                        new ConnectionUnknownException(e));
            }
        }

        void cancel() {
            if (future != null) {
                future.cancel(false);
            }
        }

    }

    public String getStringID() {
        return String.format("%s:%s",
                Long.toString(dialogId, 16),
                endpointHandler.getStringID());
    }

    @Override
    public synchronized String toString() {
        StringBuilder builder = new StringBuilder("DialogContext");
        builder.append("[");
        builder.append(" dialogId=").append(getStringID());
        builder.append(" contextId=").append(Long.toString(contextId, 16));
        builder.append(" dialogType=").append(typeno);
        builder.append(" dialogHandler=").append(dialogHandler);
        builder.append(" onCreatingEndpoint=").append(onCreatingSide);
        builder.append(" timeout=").append(timeoutMillis);
        builder.append(" state=").append(state);
        builder.append(" writeState=").append(writeState.get());
        builder.append(" abortInfo=").append(abortInfo);
        builder.append(" perf=").append(perf);
        builder.append("]");
        return builder.toString();
    }

}

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

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.async.AsyncVersionedRemote.ResponseType;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializeExceptionUtil;

/**
 * Base class used to implement remote calls by responders (servers) for
 * asynchronous service interfaces.  Subclasses should implement {@link
 * #getMethodOp} to identify the method and {@link #handleRequest
 * handleRequest} to read requests from the input stream, and should also
 * create subclasses of {@link AsyncVersionedRemoteDialogResultHandler} to
 * write responses to the output stream.
 *
 * @see AsyncVersionedRemote
 * @see AsyncVersionedRemoteDialogResultHandler
 */
public abstract class AsyncVersionedRemoteDialogResponder
        implements DialogHandler {

    private final DialogTypeFamily dialogTypeFamily;
    protected final Logger logger;
    private volatile DialogContext savedContext;
    private volatile MessageOutput savedResponse;

    /**
     * A dialog result handler that sends a response message containing the
     * supplied serial version.
     */
    private class SerialVersionDialogResultHandler
            extends AsyncVersionedRemoteDialogResultHandler<Short> {
        public SerialVersionDialogResultHandler() {
            super(SerialVersion.STD_UTF8_VERSION,
                  AsyncVersionedRemoteDialogResponder.this);
        }
        @Override
        protected void writeResult(Short result,
                                   MessageOutput response) {
            response.writeShort(result);
        }
        @Override
        public String toString() {
            return AsyncVersionedRemoteDialogResponder.this.toString();
        }
    }

    /** Creates an instance of this class. */
    public AsyncVersionedRemoteDialogResponder(
        DialogTypeFamily dialogTypeFamily, Logger logger) {

        this.dialogTypeFamily =
            checkNull("dialogTypeFamily", dialogTypeFamily);
        this.logger = checkNull("logger", logger);
    }

    /**
     * Returns the {@link MethodOp} associated with an integer value.
     *
     * @param methodOpValue the integer value for the method operation
     * @return the method op
     * @throws IllegalArgumentException if the value is not found
     */
    protected abstract MethodOp getMethodOp(int methodOpValue);

    /**
     * Reads the arguments for a request with the specified method op, returned
     * from a call to {@link #getMethodOp}, and write the response.
     *
     * @param methodOp the method op
     * @param in the input stream
     * @param context the dialog context for the call
     */
    protected abstract void handleRequest(MethodOp methodOp,
                                          MessageInput in,
                                          DialogContext context);

    /* -- From DialogHandler -- */

    @Override
    public void onStart(DialogContext context, boolean aborted) {
        savedContext = context;
    }

    @Override
    public void onCanWrite(DialogContext context) {
        write();
    }

    @Override
    public void onCanRead(DialogContext context, boolean finished) {
        final MessageInput request = context.read();
        if (request == null) {
            return;
        }
        if (!finished) {
            sendException(new IllegalArgumentException(
                              "Expected request to be finished"),
                          SerialVersion.STD_UTF8_VERSION);
            return;
        }
        final int methodOpVal;
        try {
            methodOpVal = readPackedInt(request);
        } catch (IOException e) {
            sendException(
                new RuntimeException(
                    "Unexpected problem reading method op: " + e, e),
                SerialVersion.STD_UTF8_VERSION);
            return;
        }
        final MethodOp methodOp;
        try {
            methodOp = getMethodOp(methodOpVal);
        } catch (IllegalArgumentException e) {
            sendException(e, SerialVersion.STD_UTF8_VERSION);
            return;
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(
                String.format("Call responder started" +
                              " %s" +
                              " methodOp=%s" +
                              " dialogId=%x:%x" +
                              " peer=%s",
                              this,
                              methodOp,
                              context.getDialogId(),
                              context.getConnectionId(),
                              context.getRemoteAddress()));
        }
        handleRequest(methodOp, request, context);
    }

    @Override
    public void onAbort(DialogContext context, Throwable cause) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                       String.format("Call responder aborted" +
                                     " %s" +
                                     " dialogId=%x:%x" +
                                     " peer=%s",
                                     this,
                                     context.getDialogId(),
                                     context.getConnectionId(),
                                     context.getRemoteAddress()),
                       cause);
        }
    }

    /* -- Other methods -- */

    /**
     * Writes the specified response as a successful result.
     *
     * @param response the response
     * @throws RuntimeException if there has already been an attempt to
     * write a response
     */
    void write(MessageOutput response) {
        if (savedResponse != null) {
            throw new RuntimeException(
                "Unexpected repeated attempt to write a response");
        }
        savedResponse = response;
        write();
    }

    /** Writes the saved response, if any. */
    private void write() {
        if (savedResponse != null) {
            if (savedContext.write(savedResponse, true /* finished */)) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(
                        String.format(
                            "Call responder completed" +
                            " %s" +
                            " dialogId=%x:%x" +
                            " peer=%s",
                            this,
                            savedContext.getDialogId(),
                            savedContext.getConnectionId(),
                            savedContext.getRemoteAddress()));
                }
                savedResponse = null;
            }
        }
    }

    /**
     * Sends an exception response.
     *
     * @param exception the exception
     * @param serialVersion the serial version to use for communications
     */
    protected void sendException(Throwable exception, short serialVersion) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                       String.format(
                           "Call responder failed" +
                           " %s" +
                           " dialogId=%x:%x" +
                           " peer=%s",
                           this,
                           ((savedContext != null) ?
                            savedContext.getDialogId() :
                            -1),
                           ((savedContext != null) ?
                            savedContext.getConnectionId() :
                            -1),
                           ((savedContext != null) ?
                            savedContext.getRemoteAddress() :
                            "")),
                       exception);
        }
        final MessageOutput out = new MessageOutput();
        try {
            ResponseType.FAILURE.writeFastExternal(out, serialVersion);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        out.writeShort(serialVersion);
        writeException(exception, out, serialVersion);
        write(out);
    }

    /**
     * Helper method to supply the local serial version in response to a remote
     * call to getSerialVersion.  Requests the serial version from the server
     * and provides a result handler to deliver the result back to the remote
     * caller.
     *
     * @param serialVersion the serial version of the initiator
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param server the local server implementation
     */
    protected void supplySerialVersion(short serialVersion,
                                       long timeoutMillis,
                                       AsyncVersionedRemote server) {
        final ResultHandler<Short> resultHandler =
            new SerialVersionDialogResultHandler();
        if (serialVersion < SerialVersion.MINIMUM) {
            resultHandler.onResult(
                null,
                SerialVersion.clientUnsupportedException(
                    serialVersion, SerialVersion.MINIMUM));
        } else {
            server.getSerialVersion(serialVersion, timeoutMillis,
                                    resultHandler);
        }
    }

    /**
     * Serializes an exception.
     *
     * @param exception the exception
     * @param response the output stream
     * @param serialVersion the serial version to use for communications
     */
    protected void writeException(Throwable exception,
                                  MessageOutput response,
                                  short serialVersion) {
        try {
            SerializeExceptionUtil.writeException(
                exception, response, serialVersion);
        } catch (IOException e) {
            throw new RuntimeException("Problem writing exception: " + e, e);
        }
    }

    /**
     * Returns the dialog context saved from the call to {@link #onStart}.  The
     * value should be non-null within the context of any callback method after
     * onStart.
     */
    protected DialogContext getSavedDialogContext() {
        return savedContext;
    }

    @Override
    public String toString() {
        return "Handler[dialogTypeFamily=" + dialogTypeFamily + "]";
    }
}

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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.AsyncVersionedRemote.ResponseType;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializeExceptionUtil;

/**
 * Base class for dialog handlers used by initiators (clients) for asynchronous
 * service interfaces.  Subclasses should implement {@link #readResult
 * readResult} to read the result from the input stream.
 *
 * @param <R> the type of the dialog result
 * @see AsyncVersionedRemote
 * @see AsyncVersionedRemoteInitiator
 */
public abstract class AsyncVersionedRemoteDialogInitiator<R>
        implements DialogHandler {

    private final MessageOutput request;
    private final Logger logger;
    private final ResultHandler<R> handler;
    private volatile State state = State.BEFORE_ON_START;

    enum State { BEFORE_ON_START, BEFORE_WRITE, BEFORE_READ, DONE; }

    /**
     * Creates an instance of this class.  The request parameter should already
     * have the data for the method invocation written to it.
     *
     * @param request the output stream for the request
     * @param logger for debug logging
     * @param handler the result handler
     */
    protected AsyncVersionedRemoteDialogInitiator(MessageOutput request,
                                                  Logger logger,
                                                  ResultHandler<R> handler) {
        this.request = checkNull("request", request);
        this.logger = checkNull("logger", logger);
        this.handler = checkNull("handler", handler);
    }

    /**
     * Reads the result from the input stream.
     *
     * @param response the input stream
     * @param serialVersion the serial version to use for communication
     * @return the result
     * @throws IOException if there is a problem with the format of the input
     */
    protected abstract R readResult(MessageInput response, short serialVersion)
        throws IOException;

    /* -- From DialogHandler -- */

    @Override
    public void onStart(DialogContext context, boolean aborted) {
        if (aborted) {
            return;
        }
        if (state != State.BEFORE_ON_START) {
            throw new IllegalStateException(
                "Expected state BEFORE_ON_START, was " + state);
        }
        state = State.BEFORE_WRITE;
        if (context.write(request, true /* finished */)) {
            state = State.BEFORE_READ;
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(String.format("Call initiator started" +
                                      " %s" +
                                      " dialogId=%x:%x" +
                                      " peer=%s" +
                                      " state=%s",
                                      this,
                                      context.getDialogId(),
                                      context.getConnectionId(),
                                      context.getRemoteAddress(),
                                      state));
        }
    }

    @Override
    public void onCanWrite(DialogContext context) {
        if (state != State.BEFORE_WRITE) {
            throw new IllegalStateException(
                "Expected state BEFORE_WRITE, was " + state);
        }
        if (context.write(request, true /* finished */)) {
            state = State.BEFORE_READ;
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(String.format("Call initiator write" +
                                      " %s" +
                                      " dialogId=%x:%x" +
                                      " peer=%s" +
                                      " state=%s",
                                      this,
                                      context.getDialogId(),
                                      context.getConnectionId(),
                                      context.getRemoteAddress(),
                                      state));
        }
    }

    @Override
    public void onCanRead(DialogContext context, boolean finished) {
        if (state != State.BEFORE_READ) {
            handler.onResult(
                null,
                new IllegalStateException(
                    "Expected state BEFORE_READ, was " + state));
            return;
        }
        if (!finished) {
            handler.onResult(
                null,
                new IllegalStateException("Only expect one response"));
            return;
        }
        final MessageInput response = context.read();
        if (response == null) {
            throw new IllegalStateException("Unexpected null read");
        }
        state = State.DONE;
        try {
            switch (ResponseType.readFastExternal(
                        response, SerialVersion.STD_UTF8_VERSION)) {
            case SUCCESS: {
                final short serialVersion = response.readShort();
                final R result = readResult(response, serialVersion);
                handler.onResult(result, null);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(String.format("Call initiator completed" +
                                              " %s" +
                                              " dialogId=%x:%x" +
                                              " peer=%s" +
                                              " result=%s",
                                              this,
                                              context.getDialogId(),
                                              context.getConnectionId(),
                                              context.getRemoteAddress(),
                                              result));
                }
                break;
            }
            case FAILURE: {
                final short serialVersion = response.readShort();
                final Throwable exception =
                    SerializeExceptionUtil.readException(response,
                                                         serialVersion);
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(String.format("Call initiator failed" +
                                              " %s" +
                                              " dialogId=%x:%x" +
                                              " peer=%s" +
                                              " exception=%s",
                                              this,
                                              context.getDialogId(),
                                              context.getConnectionId(),
                                              context.getRemoteAddress(),
                                              exception));
                }
                handler.onResult(null, exception);
                break;
            }
            default:
                throw new AssertionError();
            }
        } catch (Throwable e) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE,
                           String.format("Call initiator unexpected" +
                                         " exception" +
                                         " %s" +
                                         " dialogId=%x:%x" +
                                         " peer=%s",
                                         handler,
                                         context.getDialogId(),
                                         context.getConnectionId(),
                                         context.getRemoteAddress()),
                           e);
            }
            handler.onResult(
                null,
                new RuntimeException(
                    "Unexpected problem reading result: " + e, e));
        }
    }

    @Override
    public void onAbort(DialogContext context, Throwable exception) {
        state = State.DONE;
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                       String.format("Call initiator aborted" +
                                     " %s" +
                                     " dialogId=%x:%x" +
                                     " peer=%s",
                                     handler,
                                     context.getDialogId(),
                                     context.getConnectionId(),
                                     context.getRemoteAddress()),
                       exception);
        }
        handler.onResult(null, exception);
    }
}

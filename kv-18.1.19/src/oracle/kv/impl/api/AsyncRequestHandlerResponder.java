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

package oracle.kv.impl.api;

import static oracle.kv.impl.util.SerializationUtil.readPackedLong;

import java.io.IOException;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.api.AsyncRequestHandler.RequestMethodOp;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResultHandler;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.StandardDialogTypeFamily;
import oracle.kv.impl.util.SerialVersion;

/**
 * A responder (server-side) dialog handler for {@link AsyncRequestHandler}
 * dialogs.
 *
 * @see AsyncRequestHandler
 */
class AsyncRequestHandlerResponder
        extends AsyncVersionedRemoteDialogResponder {

    private final AsyncRequestHandler server;

    AsyncRequestHandlerResponder(AsyncRequestHandler server, Logger logger) {
        super(StandardDialogTypeFamily.ASYNC_REQUEST_HANDLER, logger);
        this.server = server;
    }

    @Override
    protected MethodOp getMethodOp(int methodOpVal) {
        return RequestMethodOp.valueOf(methodOpVal);
    }

    @Override
    protected void handleRequest(MethodOp methodOp,
                                 MessageInput request,
                                 DialogContext context) {
        final short serialVersion;
        try {
            serialVersion = request.readShort();
        } catch (IOException e) {
            sendException(
                new IllegalStateException(
                    "Problem deserializing request: " + e, e),
                SerialVersion.STD_UTF8_VERSION);
            return;
        }
        switch ((RequestMethodOp) methodOp) {
        case GET_SERIAL_VERSION:
            final long timeoutMillis;
            try {
                timeoutMillis = readPackedLong(request);
            } catch (IOException e) {
                sendException(
                    new IllegalStateException(
                        "Problem deserializing timeout for" +
                        " GET_SERIAL_VERSION: " + e,
                        e),
                    serialVersion);
                return;
            }
            supplySerialVersion(serialVersion, timeoutMillis, server);
            break;
        case EXECUTE:
            execute(serialVersion, request, context);
            break;
        default:
            throw new AssertionError();
        }
    }

    private void execute(short serialVersion,
                         MessageInput in,
                         DialogContext context) {
        final Request request;
        try {
            request = new Request(in);
        } catch (IOException e) {
            sendException(
                new IllegalStateException(
                    "Problem deserializing request: " + e, e),
                serialVersion);
            return;
        }
        server.execute(request, request.getTimeout(),
                       new AsyncRequestResultHandler(this, serialVersion,
                                                     context));
    }

    /**
     * Returns the dialog context associated with the asynchronous execute
     * request whose results are being returned to the specified handler, or
     * null if the handler is not associated with an asynchronous execute
     * request.
     *
     * @param handler the result handler
     * @return the dialog context or null
     */
    static DialogContext getDialogContext(ResultHandler<Response> handler) {
        if (handler instanceof AsyncRequestResultHandler) {
            return ((AsyncRequestResultHandler) handler).context;
        }
        return null;
    }

    private static class AsyncRequestResultHandler
            extends AsyncVersionedRemoteDialogResultHandler<Response> {
        private final DialogContext context;
        private AsyncRequestResultHandler(
            AsyncRequestHandlerResponder responder,
            short serialVersion,
            DialogContext context) {

            super(serialVersion, responder);
            this.context = context;
        }
        @Override
        protected void writeResult(Response result, MessageOutput out)
            throws IOException {

            result.writeFastExternal(out, serialVersion);
        }
    }
}

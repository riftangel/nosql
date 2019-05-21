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

import java.io.IOException;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogInitiator;
import oracle.kv.impl.async.AsyncVersionedRemoteInitiator;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.StandardDialogTypeFamily;

/**
 * An initiator (client-side) implementation of {@link AsyncRequestHandler}.
 *
 * @see AsyncRequestHandlerAPI
 */
class AsyncRequestHandlerInitiator extends AsyncVersionedRemoteInitiator
    implements AsyncRequestHandler {

    AsyncRequestHandlerInitiator(CreatorEndpoint endpoint,
                                 DialogType dialogType,
                                 Logger logger) {
        super(endpoint, dialogType, logger);
        if (dialogType.getDialogTypeFamily() !=
            StandardDialogTypeFamily.ASYNC_REQUEST_HANDLER) {
            throw new IllegalArgumentException(
                "Dialog type should have dialog type family" +
                " ASYNC_REQUEST_HANDLER, found: " +
                dialogType.getDialogTypeFamily());
        }
    }

    @Override
    public void getSerialVersion(short serialVersion,
                                 long timeoutMillis,
                                 ResultHandler<Short> handler) {
        getSerialVersion(serialVersion,
                         RequestMethodOp.GET_SERIAL_VERSION,
                         timeoutMillis,
                         handler);
    }

    @Override
    public void execute(final Request request,
                        final long timeoutMillis,
                        final ResultHandler<Response> resultHandler) {
        final short serialVersion = request.getSerialVersion();
        final MessageOutput out =
            startRequest(RequestMethodOp.EXECUTE, serialVersion);
        try {
            request.writeFastExternal(out, serialVersion);
        } catch (IOException e) {
            resultHandler.onResult(
                null,
                new RuntimeException(
                    "Unexpected problem writing request: " + e, e));
        }
        class ExecuteDialogHandler
            extends AsyncVersionedRemoteDialogInitiator<Response> {
            ExecuteDialogHandler() {
                super(out, logger, resultHandler);
            }
            @Override
            public Response readResult(MessageInput in,
                                       short responseSerialVersion)
                throws IOException {

                return new Response(in, responseSerialVersion);
            }
            @Override
            public String toString() {
                return "Handler[dialogType=" + dialogType +
                    " methodOp=" + RequestMethodOp.EXECUTE +
                    " request=(" + request + ")" +
                    " timeoutMillis=" + timeoutMillis + "]";
            }
        }
        startDialog(new ExecuteDialogHandler(), timeoutMillis);
    }
}

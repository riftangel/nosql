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

import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.api.AsyncRequestHandler.RequestMethodOp;
import oracle.kv.impl.async.AsyncVersionedRemoteAPI;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;

/**
 * API to make an asynchronous request on an RN.
 *
 * @see RequestHandlerAPI
 * @see AsyncRequestHandler
 */
public class AsyncRequestHandlerAPI extends AsyncVersionedRemoteAPI {

    private final AsyncRequestHandler remote;

    private AsyncRequestHandlerAPI(AsyncRequestHandler remote,
                                   short serialVersion) {
        super(serialVersion);
        this.remote = remote;
    }

    /**
     * Makes an asynchronous request to create an instance of this class,
     * returning the result to {@code rh}.
     *
     * @param endpoint the remote endpoint representing the server
     * @param dialogType the dialog type
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param logger for debug logging
     * @param rh the result handler
     */
    public static void wrap(CreatorEndpoint endpoint,
                            final DialogType dialogType,
                            long timeoutMillis,
                            Logger logger,
                            final ResultHandler<AsyncRequestHandlerAPI> rh) {
        final AsyncRequestHandler initiator =
            new AsyncRequestHandlerInitiator(endpoint, dialogType, logger);
        class GetSerialVersionResultHandler implements ResultHandler<Short> {
            @Override
            public void onResult(Short version, Throwable except) {
                if (except != null) {
                    rh.onResult(null, except);
                } else {
                    rh.onResult(new AsyncRequestHandlerAPI(initiator, version),
                                null);
                }
            }
            @Override
            public String toString() {
                return "Handler[dialogType=" + dialogType +
                    " methodOp=" + RequestMethodOp.GET_SERIAL_VERSION + "]";
            }
        }
        computeSerialVersion(initiator, timeoutMillis,
                             new GetSerialVersionResultHandler());
    }

    /**
     * Executes the request.
     *
     * @param request the request to be executed
     * @param timeoutMillis the remote execution timeout in milliseconds
     * @param handler the result handler
     * @see AsyncRequestHandler#execute
     */
    public void execute(Request request,
                        long timeoutMillis,
                        ResultHandler<Response> handler) {
        remote.execute(request, timeoutMillis, handler);
    }
}

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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.ConnectException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.ConsistencyException;
import oracle.kv.FaultException;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ResultHandler;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.BlockingResultHandler;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.exception.ConnectionIOException;
import oracle.kv.impl.async.exception.ConnectionTimeoutException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.exception.DialogUnknownException;
import oracle.kv.impl.async.exception.InitialConnectIOException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * An implementation of RequestDispatcher that supports asynchronous
 * operations.
 */
public class AsyncRequestDispatcherImpl extends RequestDispatcherImpl {

    /**
     * The amount of time in milliseconds to allow for a single roundtrip
     * network communication with the server.
     */
    private final long networkRoundtripTimeout;

    /**
     * Creates RequestDispatcher for a KVStore client. As part of the creation
     * of the client side RequestDispatcher, it contacts one or more SNs from
     * the list of SNs identified by their <code>registryHostport</code>.
     *
     * @param config the KVStore configuration
     *
     * @param clientId the unique clientId associated with the KVS client
     *
     * @param loginMgr a login manager used to authenticate metadata access.
     *
     * @param exceptionHandler the handler to be associated with the state
     * update thread
     *
     * @param logger a Logger
     *
     * @throws IllegalArgumentException if the configuration specifies read
     * zones that are not found in the topology
     *
     * @throws KVStoreException if an RN could not be contacted to obtain the
     * Topology associated with the KVStore
     */
    public AsyncRequestDispatcherImpl(
        KVStoreConfig config,
        ClientId clientId,
        LoginManager loginMgr,
        UncaughtExceptionHandler exceptionHandler,
        Logger logger)
        throws KVStoreException {

        super(config, clientId, loginMgr, exceptionHandler, logger);
        networkRoundtripTimeout =
            config.getNetworkRoundtripTimeout(MILLISECONDS);
    }

    @Override
    boolean isAsync() {
        return true;
    }

    /**
     * Executes a synchronous request by performing the request asynchronously
     * and waiting for the result.
     */
    @Override
    public Response execute(Request request,
                            RepNodeId targetId,
                            Set<RepNodeId> excludeRNs,
                            LoginManager loginMgr)
        throws FaultException {

        final BlockingExecuteResultHandler handler =
            new BlockingExecuteResultHandler(request);

        /* Grab the timeout value, since it can change during execution */
        final long timeout = request.getTimeout();
        execute(request, targetId, excludeRNs, loginMgr, handler);

        /*
         * Add the network roundtrip time to allow the server time to return an
         * exceptions.
         */
        return handler.await(timeout, getAsyncTimeout(timeout));
    }

    private class BlockingExecuteResultHandler
            extends BlockingResultHandler<Response> {
        private final Request request;
        private AsyncExecuteRequest asyncExecuteRequest = null;
        BlockingExecuteResultHandler(Request request) {
            this.request = request;
        }
        synchronized void setAsyncExecuteRequest(
            AsyncExecuteRequest asyncExecuteRequest) {
            this.asyncExecuteRequest = asyncExecuteRequest;
        }
        @Override
        protected String getDescription() {
            return request.toString();
        }
        @Override
        protected FaultException getTimeoutException(long timeout) {
            RepNodeState target = null;
            int retryCount = 1;
            synchronized (this) {
                if (asyncExecuteRequest != null) {
                    target = asyncExecuteRequest.target;
                    retryCount = asyncExecuteRequest.retryCount;
                }
            }
            return AsyncRequestDispatcherImpl.this.getTimeoutException(
                request, null, (int) timeout, retryCount, target);
        }
    }

    /**
     * Returns the timeout in milliseconds that should be used for the async
     * dialog based on the request timeout.  The amount of time returned is
     * larger than the request timeout so that exceptions detected on the
     * server side can be propagated back to the client over the network.
     */
    long getAsyncTimeout(long requestTimeout) {
        assert networkRoundtripTimeout >= 0;
        long timeout = requestTimeout + networkRoundtripTimeout;

        /* Correct for overflow */
        if ((requestTimeout > 0) && (timeout <= 0)) {
            timeout = Long.MAX_VALUE;
        }
        return timeout;
    }

    /**
     * Dispatches a request asynchronously to a suitable RN.
     *
     * <p> This implementation supports asynchronous operations.
     *
     * @see RequestDispatcher#execute(Request, Set, LoginManager,
     * ResultHandler)
     */
    @Override
    public void execute(Request request,
                        Set<RepNodeId> excludeRNs,
                        LoginManager loginMgr,
                        ResultHandler<Response> handler) {
        execute(request, null, excludeRNs, loginMgr, handler);
    }

    /**
     * Dispatches a request asynchronously, and also provides a parameter for
     * specifying a preferred target node, for testing.
     *
     * <p> This implementation supports asynchronous operations.
     *
     * @see RequestDispatcherImpl#execute(Request, RepNodeId, Set,
     * LoginManager, ResultHandler)
     */
    @Override
    public void execute(Request request,
                        RepNodeId targetId,
                        Set<RepNodeId> excludeRNs,
                        LoginManager loginMgr,
                        ResultHandler<Response> handler) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                       "Executing async request={0} targetId={1}" +
                       " handler={2}\n{3}",
                       new Object[] {
                           request,
                           targetId,
                           handler,
                           CommonLoggerUtils.getStackTrace(new Throwable()) });
        }
        new AsyncExecuteRequest(
            request, targetId, excludeRNs, loginMgr, handler).run();
    }

    /**
     * The execution environment for a single asynchronous request.
     */
    private class AsyncExecuteRequest implements Runnable {

        private final Request request;
        private final RepNodeId targetId;
        private volatile Set<RepNodeId> excludeRNs;
        private final LoginManager loginMgr;
        private final ResultHandler<Response> handler;

        private final RepGroupState rgState;
        private final int initialTimeoutMs;
        private final long limitNs;
        volatile int retryCount;

        private volatile Exception exception;
        volatile RepNodeState target;
        private volatile long retrySleepNs;
        private volatile LoginHandle loginHandle;

        private volatile long startNs;

        /**
         * Whether a remote call to a particular RN is currently considered
         * underway.
         */
        private volatile boolean callStarted;

        AsyncExecuteRequest(Request request,
                            RepNodeId targetId,
                            Set<RepNodeId> excludeRNs,
                            LoginManager loginMgr,
                            ResultHandler<Response> handler) {
            this.request = request;
            this.targetId = targetId;
            this.excludeRNs = excludeRNs;
            this.loginMgr = loginMgr;
            this.handler = handler;

            /*
             * Store this instance in the handler for use in generating timeout
             * exceptions.
             */
            if (handler instanceof BlockingResultHandler) {
                ((BlockingExecuteResultHandler) handler)
                    .setAsyncExecuteRequest(this);
            }

            final RepGroupId repGroupId = startExecuteRequest(request);

            rgState = repGroupStateTable.getGroupState(repGroupId);

            initialTimeoutMs = request.getTimeout();
            limitNs = System.nanoTime() +
                MILLISECONDS.toNanos(initialTimeoutMs);
            retryCount = 0;

            exception = null;
            target = null;
            retrySleepNs = 10000000; /* 10 milliseconds */
            loginHandle = null;
        }

        /**
         * Make requests, finishing if it can obtain a result or failure
         * without blocking, and otherwise scheduling a new call when called
         * back after waiting for an intermediate result.
         */
        @Override
        public void run() {

            /* Retry until timeout or async handoff */
            while ((limitNs - System.nanoTime()) > 0) {
                assert !callStarted;
                try {
                    target = selectTarget(request, targetId, rgState,
                                          excludeRNs);
                } catch (RNUnavailableException e) {
                    handler.onResult(null, e);
                    return;
                } catch (NoSuitableRNException e) {
                    /*
                     * Don't save this exception if the current one is a more
                     * interesting ConsistencyException
                     */
                    if (!(exception instanceof ConsistencyException)) {
                        exception = e;
                    }
                    retrySleepNs = computeWaitBeforeRetry(limitNs,
                                                          retrySleepNs);
                    if (retrySleepNs > 0) {
                        AsyncRegistryUtils
                            .getEndpointGroup()
                            .getSchedExecService()
                            .schedule(this, retrySleepNs, NANOSECONDS);
                        return;
                    }
                    continue;
                }

                /* Have a target RN in hand */
                startNs = 0;
                try {
                    activeRequestCount.incrementAndGet();
                    final int targetRequestCount = target.requestStart();
                    startNs = statsTracker.markStart();
                    callStarted = true;
                    checkStartDispatchRequest(target, targetRequestCount);
                    target.getReqHandlerRef(
                        regUtils, NANOSECONDS.toMillis(limitNs - startNs),
                        new ResultHandler<AsyncRequestHandlerAPI>() {
                            @Override
                            public void onResult(AsyncRequestHandlerAPI api,
                                                 Throwable e) {
                                handleRequestHandler(api, e);
                            }
                        });
                    return;
                } catch (Exception dispatchException) {
                    if (handleResponse(null, dispatchException)) {
                        return;
                    }
                }
            }
            handler.onResult(null,
                             getTimeoutException(request, exception,
                                                 initialTimeoutMs, retryCount,
                                                 target));
        }

        /** Handle the result from attempt to obtain the request handler. */
        void handleRequestHandler(AsyncRequestHandlerAPI requestHandler,
                                  Throwable dispatchException) {
            assert callStarted;
            if (requestHandler == null) {
                /*
                 * Save this exception unless the current one is more
                 * interesting, but don't set dispatchException, because we
                 * want to try again.
                 */
                if (!(exception instanceof ConsistencyException)) {
                    exception = new IllegalStateException(
                        "Could not establish handle to " +
                        target.getRepNodeId());
                }
            } else {
                try {
                    loginHandle = prepareRequest(request, limitNs, retryCount,
                                                 target, loginMgr);
                    requestHandler.execute(
                        request, getAsyncTimeout(request.getTimeout()),
                        new HandleResponse());
                    return;
                } catch (Exception e) {
                    dispatchException = e;
                }
            }
            if (!handleResponse(null, dispatchException)) {
                run();
            }
        }

        private class HandleResponse implements ResultHandler<Response> {
            @Override
            public void onResult(Response response, Throwable e) {
                if (!handleResponse(response, e)) {
                    run();
                }
            }
        }

        /**
         * Performs operations needed when a dispatch attempt is completed,
         * including delivering the result or the exception if appropriate.
         * Returns true if the processing of the request is done, with the
         * result or exception delivered to the result handler, and returns
         * false if the request should be retried.
         */
        boolean handleResponse(Response response, Throwable e) {
            assert callStarted;
            callStarted = false;
            boolean done = false;
            if (e != null) {
                final Throwable throwException = dispatchFailed(e);
                if (throwException != null) {
                    e = throwException;
                    done = true;
                } else {
                    e = exception;
                }
            } else if (response != null) {
                done = true;
            }
            excludeRNs = dispatchCompleted(startNs, request, response, target,
                                           e, excludeRNs);
            if (done) {
                handler.onResult(response, e);
            }
            return done;
        }

        /**
         * Handles an exception encountered during the dispatch of a request,
         * and returns a non-null exception that should be supplied to the
         * result handler if the request should not be retried.
         */
        private Throwable dispatchFailed(Throwable t) {
            if (!(t instanceof Exception)) {
                return t;
            }
            try {
                final Exception dispatchException =
                    (t instanceof DialogException) ?
                    handleDialogException(request, target,
                                          (DialogException) t) :
                    handleDispatchException(request, initialTimeoutMs, target,
                                            (Exception) t, loginHandle);
                if (!(exception instanceof ConsistencyException)) {
                    exception = dispatchException;
                }
                return null;
            } catch (Throwable t2) {
                return t2;
            }
        }
    }

    /**
     * Provide handling for dialog exceptions, checking for side effects on
     * writes, and unwrapping dialog and connection exceptions to obtain the
     * underlying exception.  Returns the exception if the request should be
     * retried, and throws an exception if it should not be retried.
     */
    Exception handleDialogException(Request request,
                                    RepNodeState target,
                                    DialogException dialogException) {
        if (dialogException instanceof DialogUnknownException) {
            throwAsFaultException("Internal error", dialogException);
        }
        Exception underlyingException =
            dialogException.getUnderlyingException();

        /*
         * Fail if there could be side effects and this is a write operation,
         * because the side effects mean it isn't safe to retry automatically.
         */
        if (dialogException.hasSideEffect()) {
            faultIfWrite(request, "Communication problem",
                         underlyingException);
        }

        final ConnectionIOException connectionIOException =
            dialogException.getCause() instanceof ConnectionIOException ?
            (ConnectionIOException) dialogException.getCause() :
            null;
        if (underlyingException instanceof ConnectException) {

            /* Add more information to a ConnectException */
            String addressInfo = "";
            if (connectionIOException != null) {
                final NetworkAddress address =
                    connectionIOException.getRemoteAddress();
                addressInfo = " at host " + address.getHostName() +
                    ", port " + address.getPort();
            }
            final ConnectException connectException = new ConnectException(
                "Unable to connect to the storage agent" + addressInfo +
                ", which may not be running");
            connectException.initCause(underlyingException);
            underlyingException = connectException;
        } else if (connectionIOException instanceof InitialConnectIOException) {

            /* Add security mismatch info to initial connection exception */
            underlyingException = new IOException(
                RegistryUtils.POSSIBLE_SECURITY_MISMATCH_MESSAGE,
                underlyingException);
        }

        target.noteReqHandlerException(underlyingException);
        return underlyingException;
    }

    /**
     * Add handling for async connection timeout exception.
     */
    @Override
    void throwAsFaultException(String faultMessage, Exception exception)
        throws FaultException {

        if (exception instanceof ConnectionTimeoutException) {
            throw new RequestTimeoutException(0, exception.getMessage(),
                                              exception, false);
        }
        super.throwAsFaultException(faultMessage, exception);
    }

    /**
     * Implement synchronous version using an asynchronous request.
     */
    @Override
    public Response executeNOP(RepNodeState rns,
                               int timeoutMs,
                               LoginManager loginMgr)
        throws Exception {

        final BlockingResultHandler<Response> handler =
            new BlockingResultHandler<Response>() {
                @Override
                protected String getDescription() {
                    return "executeNOP";
                }
            };
        executeNOP(rns, timeoutMs, loginMgr, handler);
        return handler.awaitChecked(Exception.class, timeoutMs);
    }

    /**
     * Asynchronous version to dispatch the special NOP request.  Keep this
     * method up-to-date with the sync version in RequestDispatcherImpl.
     *
     * TODO: Update callers to call the asynchronous version directly?
     */
    public void executeNOP(final RepNodeState rns,
                           final int timeoutMs,
                           final LoginManager loginMgr,
                           final ResultHandler<Response> handler) {
        rns.getReqHandlerRef(
            getRegUtils(), timeoutMs,
            new NOPResultHandler(rns, timeoutMs, loginMgr, handler));
    }

    /** Perform a NOP request using the provided request handler. */
    private class NOPResultHandler
            implements ResultHandler<AsyncRequestHandlerAPI> {
        private final RepNodeState rns;
        private final int timeoutMs;
        private final LoginManager loginMgr;
        private final ResultHandler<Response> handler;

        NOPResultHandler(RepNodeState rns,
                         int timeoutMs,
                         LoginManager loginMgr,
                         ResultHandler<Response> handler) {
            this.rns = rns;
            this.timeoutMs = timeoutMs;
            this.loginMgr = loginMgr;
            this.handler = handler;
        }

        @Override
        public void onResult(AsyncRequestHandlerAPI ref, Throwable exception) {
            if (ref == null) {
                /* needs to be resolved. */
                handler.onResult(null, exception);
                return;
            }

            new ResponseHandler(ref).executeNOP();
        }

        /** Perform a request and handle the response. */
        private class ResponseHandler implements ResultHandler<Response> {
            private final AsyncRequestHandlerAPI ref;
            private final long startTimeNs;
            private volatile Request nop;

            ResponseHandler(AsyncRequestHandlerAPI ref) {
                this.ref = ref;

                rns.requestStart();
                activeRequestCount.incrementAndGet();
                startTimeNs = statsTracker.markStart();
            }

            void executeNOP() {
                try {

                    final int topoSeqNumber =
                        getTopologyManager().getTopology().getSequenceNumber();

                    nop = Request.createNOP(
                        topoSeqNumber, getDispatcherId(), timeoutMs);

                    nop.setSerialVersion(
                        rns.getRequestHandlerSerialVersion());

                    if (loginMgr != null) {
                        nop.setAuthContext(
                            new AuthContext(
                                loginMgr.getHandle(
                                    rns.getRepNodeId()).getLoginToken()));
                    }

                    ref.execute(nop, getAsyncTimeout(nop.getTimeout()), this);
                } catch (Throwable e) {
                    onResult(null, e);
                }
            }

            @Override
            public void onResult(Response response, Throwable exception) {
                if (response != null) {
                    try {
                        processResponse(startTimeNs, nop, response);
                    } catch (Throwable e) {
                        exception = e;
                    }
                }

                /*
                 * Note communication exceptions so that the update thread will
                 * correct the problem
                 */
                if (exception instanceof DialogException) {
                    final DialogException de = (DialogException) exception;
                    rns.noteReqHandlerException(de.getUnderlyingException());
                }
                rns.requestEnd();
                activeRequestCount.decrementAndGet();
                statsTracker.markFinish(OpCode.NOP, startTimeNs);
                handler.onResult(response, exception);
            }
        }
    }

    /**
     * This implementation returns the default local dialog layer limit.
     */
    @Override
    protected int getMaxActiveRequests() {
        return EndpointConfigBuilder.getOptionDefault(
            AsyncOption.DLG_LOCAL_MAXDLGS);
    }
}

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

import java.lang.Thread.UncaughtExceptionHandler;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.MarshalException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.ServerError;
import java.rmi.ServerException;
import java.rmi.UnknownHostException;
import java.rmi.UnmarshalException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.ConsistencyException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.RequestLimitConfig;
import oracle.kv.RequestLimitException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ResultHandler;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.TTLFaultException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.WaitableCounter;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.Table;

import com.sleepycat.utilint.Latency;
import com.sleepycat.utilint.LatencyStat;
import com.sleepycat.utilint.StatsTracker;

/**
 * An implementation of RequestDispatcher that does not support asynchronous
 * operations. The implementation does not have any dependences on RepNode or
 * the ReqestHandler, since it's also instantiated in a KV client.
 * <p>
 * The implementation has two primary components that provide the state used
 * to dispatch a request:
 * <ol>
 * <li>The Topology, which is accessed via the TopologyManager. It's updated
 * primarily via the PlannerAdmin, but could also be updated via information
 * obtained in a Response.</li>
 * <li>The  RepGroupStateTable which tracks the dynamic state of RNs within
 * the KVS. It's updated primarily via information contained in a Response,
 * as well as an internal thread that attempts to keep its information
 * current.
 * </li>
 * </ol>
 * The RequestDispatcher is the home for these components and reads the state
 * they represent. There is exactly one instance of each of these shared
 * components in a client or a RN.
 *
 * TODO: Sticky dispatch based upon partitions.
 */
public class RequestDispatcherImpl implements RequestDispatcher {

    /**
     * The unique dispatcher id sent in dispatched requests.
     */
    final ResourceId dispatcherId;

    /**
     * Indicates whether this dispatcher is used on a remote RN for
     * forwarding, or directly on the client.
     */
    final boolean isRemote;

    /**
     * Determines if and how to limit the max requests to a node so that all
     * thread resources aren't consumed by it.
     */
    final RequestLimitConfig requestLimitConfig;

    /**
     * The topology used as the basis for requests.
     */
    final TopologyManager topoManager;

    /**
     *  Tracks the state of rep nodes.
     */
    final RepGroupStateTable repGroupStateTable;

    /**
     * The thread is only started for client nodes where it maintains the
     * RepNodeState associated with every node in the state table.
     */
    private final RepNodeStateUpdateThread stateUpdateThread;

    /**
     * The internal login manager used to authenticate client proxy calls.
     * Only non-null when in a rep-node context.
     */
    private final LoginManager internalLoginMgr;

    /**
     * The login manager that is held by regUtils and use do perform topology
     * access operations, NOP executions, and requestHandler resolves.
     */
    private volatile LoginManager regUtilsLoginMgr = null;

    /**
     * Used to create remote handles. It's maintained by RegUtilsMaintListener
     * and can change as a result of topology changes.
     */
    volatile RegistryUtils regUtils = null;

    /**
     * The number of request actively being processed by this request
     * dispatcher. It represents the sum of active requests at every RN.
     */
    final WaitableCounter activeRequestCount = new WaitableCounter();

    /**
     * The total number of requests that were retried.
     */
    final AtomicLong totalRetryCount = new AtomicLong(0);

    final StatsTracker<InternalOperation.OpCode> statsTracker;

    /**
     * Shutdown can only be executed once. The shutdown field protects against
     * multiple invocations.
     */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * The exception that provoked the shutdown, if any.
     */
    private Throwable shutdownException = null;

    /* The default timeout associated with requests. */
    private int requestQuiesceMs = REQUEST_QUIESCE_MS_DEFAULT;

    final Logger logger;

    /**
     * The names of the zones that can be used for read requests, or null if
     * not restricted.  Set to a non-null value for client dispatchers from the
     * associated KVStoreConfig.
     */
    private final Set<String> readZones;

    /**
     * The IDs of the zones that can be used for read requests, or null if not
     * restricted.  Set based on readZones and the current topology.
     */
    private volatile int[] readZoneIds = null;

    /**
     * The maximum number of RNs to query for a topology. The larger the
     * number the more current the topology, but the longer the KVS startup
     * delay.
     */
    private static final int MAX_LOCATOR_RNS = 10;

    /**
     * The time period between update passes by the state update thread
     */
    private static final int STATE_UPDATE_THREAD_PERIOD_MS = 1000;

    /**
     * The maximum time to sleep between retries of a request. Wait times
     * are increased between successive retries until this limit is reached.
     */
    static final int RETRY_SLEEP_MAX_NS = 128000000;

    /**
     * The maximum number of topo changes to be retained at the client.
     */
    private static final int MAX_TOPO_CHANGES_ON_CLIENT = 1000;

    /**
     * Default Request quiesce ms.
     */
    private static final int REQUEST_QUIESCE_MS_DEFAULT = 10000;

    /* The period used to poll for requests becoming quiescent at shutdown. */
    private static final int REQUEST_QUIESCE_POLL_MS = 1000;

    /**
     * Special test hook -- used when testing request dispatching -- that
     * supports the introduction of a NoSuitableRNException; which is needed
     * to verify that the execute method of this class correctly handles a
     * null excludeRNs parameter. For testing only.
     */
    private TestHook<Request> requestExecuteHook;

    /**
     * Test hook used to provoke checked exceptions immediately before
     * a request dispatch. It's primarily used for injecting RMI exceptions.
     */
    volatile ExceptionTestHook<Request,Exception> preExecuteHook;

    /**
     * Creates requestDispatcher for a KVStore RepNode.
     *
     * @param kvsName the name of the KVStore associated with the RN
     *
     * @param repNodeParams the RN params used to configures this RN
     *
     * @param internalLoginMgr the RN's internal login manager.  This is also
     * used as the Metadata login manager.
     *
     * @param exceptionHandler the handler to be associated with the state
     * update thread
     *
     * @param logger an optional Logger
     */
    public RequestDispatcherImpl(String kvsName,
                                 RepNodeParams repNodeParams,
                                 LoginManager internalLoginMgr,
                                 UncaughtExceptionHandler exceptionHandler,
                                 Logger logger) {
        assert (kvsName != null);
        this.logger = logger;
        this.internalLoginMgr = internalLoginMgr;
        this.regUtilsLoginMgr = internalLoginMgr;
        final RequestLimitConfig defaultRequestLimitConfig =
            ParameterUtils.getRequestLimitConfig(repNodeParams.getMap());
        requestLimitConfig =
            getRepNodeRequestLimitConfig(defaultRequestLimitConfig);
        topoManager = new TopologyManager(kvsName,
                                           repNodeParams.getMaxTopoChanges(),
                                           logger);
        final ResourceId repNodeId = repNodeParams.getRepNodeId();
        repGroupStateTable =
            new RepGroupStateTable(repNodeId, isAsync(), logger);
        initTopoManager();
        dispatcherId = repNodeId;
        isRemote = true;
        stateUpdateThread =
            new RepNodeStateUpdateThread(this, repNodeId,
                                         STATE_UPDATE_THREAD_PERIOD_MS,
                                         exceptionHandler, logger);

        /* The parameters used below effectively turn off thread dumps. */
        final int maxTrackedLatencyMillis =
            ParameterUtils.getMaxTrackedLatencyMillis(repNodeParams.getMap());
        statsTracker =
            new StatsTracker<OpCode>(InternalOperation.OpCode.values(),
                                     logger,
                                     Integer.MAX_VALUE,
                                     Long.MAX_VALUE,
                                     0,
                                     maxTrackedLatencyMillis);
        requestQuiesceMs = repNodeParams.getRequestQuiesceMs();
        readZones = null;
        stateUpdateThread.start();
    }

    /**
     * Creates RequestDispatcher for a KVStore client. As part of the creation
     * of the client side RequestDispatcher, it contacts one or more SNs from
     * the list of SNs identified by their <code>registryHostport</code>.  This
     * method creates an async dispatcher based on the value of {@link
     * KVStoreConfig#getUseAsync}.
     *
     * @param config the KVStore configuration
     *
     * @param clientId the unique clientId associated with the KVS client
     *
     * @param loginMgr a login manager used to authenticate metadata access.
     * @throws IllegalArgumentException if the configuration specifies read
     * zones that are not found in the topology
     *
     * @throws KVStoreException if an RN could not be contacted to obtain the
     * Topology associated with the KVStore
     */
    public static RequestDispatcherImpl createForClient(
        KVStoreConfig config,
        ClientId clientId,
        LoginManager loginMgr,
        UncaughtExceptionHandler exceptionHandler,
        Logger logger) throws KVStoreException {

        return config.getUseAsync() ?
            new AsyncRequestDispatcherImpl(config, clientId, loginMgr,
                                           exceptionHandler, logger) :
            new RequestDispatcherImpl(config, clientId, loginMgr,
                                      exceptionHandler, logger);
    }

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
     * @throws IllegalArgumentException if the configuration specifies read
     * zones that are not found in the topology
     *
     * @throws KVStoreException if an RN could not be contacted to obtain the
     * Topology associated with the KVStore
     */
    protected RequestDispatcherImpl(KVStoreConfig config,
                                    ClientId clientId,
                                    LoginManager loginMgr,
                                    UncaughtExceptionHandler exceptionHandler,
                                    Logger logger)
        throws KVStoreException {

        this(config.getStoreName(), clientId,
             TopologyLocator.get(
                 config.getHelperHosts(), MAX_LOCATOR_RNS, loginMgr,
                 config.getStoreName()),
             loginMgr,
             config.getRequestLimit(),
             exceptionHandler,
             logger,
             config.getReadZones());

        requestQuiesceMs =
            (int) config.getRequestTimeout(TimeUnit.MILLISECONDS);

        stateUpdateThread.start();
    }

    /**
     * Internal constructor used for a KVStore client (from above constructor)
     * and unit testing a RequestDispatcher directly in the absence of a
     * RepNodeService.
     *
     * Note that it does not start the state update thread.
     */
    RequestDispatcherImpl(String kvsName,
                          ClientId clientId,
                          Topology topology,
                          LoginManager regUtilsLoginMgr,
                          RequestLimitConfig requestLimitConfig,
                          UncaughtExceptionHandler exceptionHandler,
                          Logger logger,
                          String[] readZones) {

        assert (kvsName != null);
        if (!topology.getKVStoreName().equals(kvsName)) {
            throw new IllegalArgumentException
                ("Specified store name, " + kvsName +
                 ", does not match store name at specified host/port, " +
                 topology.getKVStoreName());
        }

        this.logger = logger;
        this.internalLoginMgr = null;
        this.regUtilsLoginMgr = regUtilsLoginMgr;
        /* The parameters used below effectively turn off thread dumps. */
        statsTracker = new StatsTracker<OpCode>
            (InternalOperation.OpCode.values(), logger,
             Integer.MAX_VALUE, Long.MAX_VALUE, 0,
             ParameterState.SP_MAX_LATENCY_MS_DEFAULT);
        this.requestLimitConfig = requestLimitConfig;

        if (readZones == null) {
            this.readZones = null;
        } else {
            final Set<String> allZones = new HashSet<String>();
            for (final Datacenter zone :
                     topology.getDatacenterMap().getAll()) {
                allZones.add(zone.getName());
            }
            final Set<String> unknownZones = new HashSet<String>();
            Collections.addAll(unknownZones, readZones);
            unknownZones.removeAll(allZones);
            if (!unknownZones.isEmpty()) {
                throw new IllegalArgumentException(
                    "Read zones not found: " + unknownZones);
            }
            this.readZones = new HashSet<String>();
            Collections.addAll(this.readZones, readZones);
            logger.log(Level.FINE, "Set read zones: {0}", this.readZones);
        }

	topoManager = new TopologyManager(kvsName,
					  MAX_TOPO_CHANGES_ON_CLIENT,
					  logger);
        repGroupStateTable =
            new RepGroupStateTable(clientId, isAsync(), logger);
        initTopoManager();
        dispatcherId = clientId;
        isRemote = false;
        stateUpdateThread =
            new RepNodeStateUpdateThread(this, clientId,
                                         STATE_UPDATE_THREAD_PERIOD_MS,
                                         exceptionHandler, logger);
        topoManager.update(topology);
    }

    /**
     * Returns the request limit config used by the request dispatcher in an
     * RN. The maximum number of active requests is based upon the the max rmi
     * connections configured for the node. The percentages are fixed since
     * there is no reason to vary them. The request dispatcher in an RN should
     * typically forward requests only within its rep group or for a migrated
     * partition.
     *
     * @param defaultConfig
     */
    private RequestLimitConfig
        getRepNodeRequestLimitConfig(RequestLimitConfig defaultConfig) {

        int maxActiveRequests =
            Math.min(defaultConfig.getNodeLimit(), getMaxActiveRequests());
        return new RequestLimitConfig
            (maxActiveRequests, defaultConfig.getRequestThresholdPercent(),
             defaultConfig.getNodeLimitPercent());
    }

    /**
     * Returns the maximum number of active requests permitted when
     * communicating with a single node.  This implementation limits the value
     * based on the RMI server limit.
     */
    int getMaxActiveRequests() {
        final String maxConnectionsProperty =
            System.getProperty("sun.rmi.transport.tcp.maxConnectionThreads");
        if (maxConnectionsProperty != null) {
            try {
                return Integer.parseInt(maxConnectionsProperty);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException
                    ("RMI max connection threads: " + maxConnectionsProperty);
            }
        }
        return Integer.MAX_VALUE;
    }

    @Override
    public void shutdown(Throwable exception) {

        if (!shutdown.compareAndSet(false, true)) {
            /* Already shutdown. */
            return;
        }

        shutdownException = exception;
        if (stateUpdateThread.isAlive()) {
            stateUpdateThread.shutdown();
            /* No need to join the thread, it will shutdown asynchronously */
        }

        /*
         * Wait for any dispatched requests to quiesce within the
         * requestQuiesceMs period.
         */
        final boolean quiesced =
                        activeRequestCount.awaitZero(REQUEST_QUIESCE_POLL_MS,
                                                     requestQuiesceMs);

        if (!quiesced) {
            logger.info(activeRequestCount.get() +
                        " dispatched requests were in progress on close.");
        }

        logger.log((exception != null) ? Level.WARNING : Level.INFO,
                    "Dispatcher shutdown", exception);

        /* Nothing to do since RMI handles are automatically GC'd. */
    }

    /**
     * Determines whether the request dispatcher has been shutdown.
     */
    void checkShutdown() {

        if (shutdown.get()) {
            final String message = "Request dispatcher has been shutdown.";
            throw new IllegalStateException(message, shutdownException);
        }
    }

    /**
     * Returns whether this implementation supports asynchronous operations.
     */
    boolean isAsync() {
        return false;
    }

    /**
     * For testing only.
     */
    public RepNodeStateUpdateThread getStateUpdateThread() {
        return stateUpdateThread;
    }

    private void initTopoManager() {
        topoManager.addPostUpdateListener(repGroupStateTable);
        topoManager.addPostUpdateListener(new RegUtilsMaintListener());
        if (readZones != null) {
            topoManager.addPostUpdateListener(new UpdateReadZoneIds());
        }
    }

    /**
     * Executes the request at a suitable RN. The dispatch is a two part
     * process where a RG is first selected based upon the key and then a
     * particular RN from within the group is selected based upon the
     * characteristics of the request. Read requests that do not require
     * absolute consistency are load balanced across the the RG.
     * <p>
     * This method deals with all exceptions arising from the execution of a
     * remote request. Whenever possible, the request is retried with a
     * different RN until the request times out.
     * <p>
     * Each request will "fail fast" rather than retry with the same RN. This
     * is to ensure that a limited and fixed number network connections at a
     * client do not all get consumed by a single problem, thus stalling
     * operations across the entire KVS.
     *
     * @param request the request to be executed
     *
     * @param targetId the preferred target, or null in the absence of a
     * preference.
     *
     * @param excludeRNs the set of RNs to be excluded from the selection of an
     * RN at which to execute the request. If the empty set or
     * <code>null</code> is input for this parameter, then no RNs are to be
     * excluded.
     *
     * @throws FaultException
     */
    public Response execute(Request request,
                            RepNodeId targetId,
                            Set<RepNodeId> excludeRNs,
                            LoginManager loginMgr)
        throws FaultException {

        final RepGroupId repGroupId = startExecuteRequest(request);

        final RepGroupState rgState =
            repGroupStateTable.getGroupState(repGroupId);

        final int initialTimeoutMs = request.getTimeout();
        final long limitNs = System.nanoTime() +
            MILLISECONDS.toNanos(initialTimeoutMs);
        int retryCount = 0;

        Exception exception = null;
        RepNodeState target = null;
        long retrySleepNs = 10000000; /* 10 milliseconds */
        LoginHandle loginHandle = null;

        do { /* Retry until timeout. */
            try {
                target = selectTarget(request, targetId, rgState, excludeRNs);
            } catch (RNUnavailableException e) {
                throw e;
            } catch (NoSuitableRNException nsre) {
                /*
                 * Don't save this exception if the current one is a more
                 * interesting ConsistencyException
                 */
                if (!(exception instanceof ConsistencyException)) {
                    exception = nsre;
                }
                retrySleepNs = computeWaitBeforeRetry(limitNs, retrySleepNs);
                if (retrySleepNs > 0) {
                    try {
                        Thread.sleep(NANOSECONDS.toMillis(retrySleepNs));
                    } catch (InterruptedException ie) {
                        throw new OperationFaultException(
                            "Unexpected interrupt", ie);
                    }
                }
                continue;
            }

            /* Have a target RN in hand */
            long startNs = 0;
            Response response = null;
            try {
                activeRequestCount.incrementAndGet();
                final int targetRequestCount = target.requestStart();
                startNs = statsTracker.markStart();
                checkStartDispatchRequest(target, targetRequestCount);
                final RequestHandlerAPI requestHandler =
                    target.getReqHandlerRef(
                        regUtils, NANOSECONDS.toMillis(limitNs - startNs));
                if (requestHandler == null) {
                    /*
                     * Don't save this exception if the current one is a more
                     * interesting ConsistencyException
                     */
                    if (!(exception instanceof ConsistencyException)) {
                        exception = new IllegalStateException
                            ("Could not establish handle to " +
                             target.getRepNodeId());
                    }
                    continue;
                }

                loginHandle = prepareRequest(request, limitNs, retryCount++,
                                             target, loginMgr);
                response = requestHandler.execute(request);
                exception = null;
                return response;
            } catch (Exception dispatchException) {
                final Exception newException =
                    handleDispatchException(request,
                                            initialTimeoutMs,
                                            target,
                                            dispatchException,
                                            loginHandle);
                if (!(exception instanceof ConsistencyException)) {
                    exception = newException;
                }
                continue;
            } finally {
                excludeRNs = dispatchCompleted(startNs, request, response,
                                               target, exception, excludeRNs);
            }

            /* Retry with corrective action until timeout. */
        } while ((limitNs - System.nanoTime()) > 0);

        /* Timed out */
        throw getTimeoutException(request, exception, initialTimeoutMs,
                                  retryCount, target);
    }

    /*
     * Set up to start executing a request, returning the the group ID.  Throws
     * RNUnavailableException if the group ID is not found because the RN is
     * not initialized.
     */
    RepGroupId startExecuteRequest(Request request) {
        checkShutdown();

        checkTTL(request);

        /*
         * Select the group associated with the request. If the group ID is not
         * null, use that, otherwise select the group based on the partition.
         */
        final RepGroupId repGroupId = request.getRepGroupId().isNull() ?
            topoManager.getLocalTopology().getRepGroupId(
                request.getPartitionId()) :
            request.getRepGroupId();
        if (repGroupId == null) {
            throw new RNUnavailableException(
                "RepNode not yet initialized");
        }
        request.updateForwardingRNs(dispatcherId, repGroupId.getGroupId());
        return repGroupId;
    }

    /**
     * Attempt to select the target RN, returning state for selected the RN if
     * the target RN was obtained.
     *
     * @throws NoSuitableRNException if no RN was found, but the caller should
     * retry
     * @throws RNUnavailableException if no RN is available and the call should
     * fail
     */
    RepNodeState selectTarget(Request request,
                              RepNodeId targetId,
                              RepGroupState rgState,
                              Set<RepNodeId> excludeRNs)
        throws NoSuitableRNException {

        try {
            return (targetId != null) ?
                repGroupStateTable.getNodeState(targetId) :
                selectDispatchRN(rgState, request, excludeRNs);
        } catch (NoSuitableRNException nsre) {
            if (!request.isInitiatingDispatcher(dispatcherId) ||
                topoManager.inTransit(request.getPartitionId())) {
                throw new RNUnavailableException(nsre.getMessage());
            }
            if (excludeRNs != null) {
                excludeRNs.clear(); /* make a fresh start for retry */
            }
            throw nsre;
        }
    }

    /**
     * Checks the active request limit and that the RN is initialized.
     */
    void checkStartDispatchRequest(RepNodeState target,
                                   int targetRequestCount) {

        /*
         * TODO: It would be nice if we could use the request limit config to
         * specify a value for the DLG_LOCAL_MAXLEN option for the endpoint
         * config.  If we did decide to do that, then note that we would need
         * to handle the case where the request limit is higher than the
         * server-side DLG_LOCAL_MAXLEN value, presumably by lowering the
         * effective limit on this side.
         */

        if ((activeRequestCount.get() >
             requestLimitConfig.getRequestThreshold()) &&
            (targetRequestCount >
             requestLimitConfig.getNodeLimit())) {
            throw RequestLimitException.create(
                requestLimitConfig,
                target.getRepNodeId(),
                activeRequestCount.get(),
                targetRequestCount,
                isRemote);
        }

        if (regUtils == null) {
            throw new RNUnavailableException("RepNode not yet " +
                                             "initialized");
        }
    }

    /**
     * Update the request and other fields in preparation for performing the
     * request now that the request handler is available.  Returns the login
     * handle that should be used to perform authentication if needed, or null
     * in the non-secure case.
     */
    LoginHandle prepareRequest(Request request,
                               long limitNs,
                               int retryCount,
                               RepNodeState target,
                               LoginManager loginMgr)
        throws Exception {

        request.setTimeout(
            (int) NANOSECONDS.toMillis(limitNs - System.nanoTime()));
        if (retryCount > 0) {
            totalRetryCount.incrementAndGet();
        }

        LoginHandle loginHandle = null;
        if (loginMgr != null) {
            loginHandle = loginMgr.getHandle(target.getRepNodeId());
            request.setAuthContext(
                new AuthContext(loginHandle.getLoginToken()));
        } else if (isRemote) {
            if (request.getAuthContext() != null) {
                updateAuthContext(request, target);
            }
        }

        request.setSerialVersion
            (target.getRequestHandlerSerialVersion());
        assert ExceptionTestHookExecute.
            doHookIfSet(preExecuteHook, request);

        return loginHandle;
    }

    /**
     * Record information about a completed dispatch attempt.  Returns the
     * updated set of excluded RNs for use on the next retry, if any.
     */
    Set<RepNodeId> dispatchCompleted(long startNs,
                                     Request request,
                                     Response response,
                                     RepNodeState target,
                                     Throwable exception,
                                     Set<RepNodeId> excludeRNs) {
        if (response != null) {
            processResponse(startNs, request, response);
            if (logger.isLoggable(Level.FINE)) {
                final RepNodeId rnId = response.getRespondingRN();
                final RepNodeState rns =
                    repGroupStateTable.getNodeState(rnId);
                logger.fine("Response from " + rns.printString());
            }
        }

        target.requestEnd();

        final int nRecords = (response != null) ?
            response.getResult().getNumRecords() : 1;
        statsTracker.markFinish(request.getOperation().getOpCode(),
                                startNs, nRecords);
        activeRequestCount.decrementAndGet();

        if (exception != null) {
            /* An anticipated exception during the request. */
            logger.fine(exception.getMessage());
            target.incErrorCount();
        }

        /* Exclude the node from consideration during a retry. */
        if ((response == null) || (exception != null)) {
            /*
             * Eliminate generation of an unused hash table on a successful
             * response where the request will not be retried, that is, in
             * 99.999% of all cases.
             */
            excludeRNs = excludeRN(excludeRNs, target);
        }
        return excludeRNs;
    }

    /**
     * Returns the FaultException to throw for a request that has timed out.
     */
    FaultException getTimeoutException(Request request,
                                       Exception exception,
                                       int initialTimeoutMs,
                                       int retryCount,
                                       RepNodeState target) {
        if (exception instanceof ConsistencyException) {
            /* Throw consistency exception, using the initial consistency */
            final ConsistencyException ce = (ConsistencyException) exception;
            ce.setConsistency(request.getConsistency());
            return ce;
        }
        final String retryText = (retryCount == 1) ? " try." : " retries.";
        return new RequestTimeoutException(
            initialTimeoutMs,
            "Request dispatcher: " + dispatcherId +
            ", dispatch timed out after " + retryCount + retryText +
            " Target: " +
            ((target == null) ? "not available" : target.getRepNodeId()),
            exception, isRemote);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation throws {@link UnsupportedOperationException},
     * since it only supports synchronous operations.
     *
     * @throws UnsupportedOperationException in all cases
     */
    @Override
    public void execute(Request request,
                        Set<RepNodeId> excludeRNs,
                        LoginManager loginMgr,
                        ResultHandler<Response> handler) {

        throw new UnsupportedOperationException(
            "Asynchronous operations are not supported");
    }

    /**
     * Dispatches a request asynchronously, and also provides a parameter for
     * specifying a preferred target node, for testing.
     *
     * <p> This implementation throws {@link UnsupportedOperationException},
     * since it only supports synchronous operations.
     *
     * @param request the request to execute
     * @param targetId the preferred target or null for no preference
     * @param excludeRNs the RNs to exclude from consideration for dispatch, or
     * null for no exclusions
     * @param handler the result handler
     * @throws FaultException
     */
    @SuppressWarnings("unused")
    public void execute(Request request,
                        RepNodeId targetId,
                        Set<RepNodeId> excludeRNs,
                        LoginManager loginMgr,
                        ResultHandler<Response> handler)
        throws FaultException {

        throw new UnsupportedOperationException(
            "Asynchronous operations are not supported");
    }

    /**
     * @hidden
     *
     * Gets the state update interval in ms
     *
     * @return the state update interval in ms
     */
    public int getStateUpdateIntervalMs() {
        return STATE_UPDATE_THREAD_PERIOD_MS;
    }

    /**
     * For a request that is being forwarded by an RN, update the
     * AuthContext on the request to reflect the original client host,
     * if needed.  If there is already a client host then we leave it as is.
     *
     * @param request a Request object that is being forwarded to another RN
     * @param target the state object representing the new target RN
     */
    void updateAuthContext(Request request, RepNodeState target) {

        final AuthContext currCtx = request.getAuthContext();
        if (currCtx != null && currCtx.getClientHost() == null) {

            /*
             * TBD: a rogue client could set the clientHost in a request to
             * a fake address to disguise its actual location.  We could
             * validate that the request is actually forwarded from another
             * RequestHandler.  However, that check is not cheap, so we
             * need to consider the cost/benefit tradeoff.
             */
            request.setAuthContext
                (new AuthContext
                 (currCtx.getLoginToken(),
                  internalLoginMgr.getHandle(
                      target.getRepNodeId()).getLoginToken(),
                  ExecutionContext.getCurrentUserHost()));
        }
    }

    /**
     * Computes the wait before retrying a request. The wait time is
     * doubled with each successive wait.
     *
     * @param limitNs the limiting time for retries
     *
     * @param prevWaitNs the previous wait time
     *
     * @return the amount of time to sleep the next time around in nanoseconds
     */
    long computeWaitBeforeRetry(final long limitNs, long prevWaitNs) {
        long thisWaitNs = Math.min(prevWaitNs << 1, RETRY_SLEEP_MAX_NS);
        final long now = System.nanoTime();
        final long maxWaitNs = limitNs - now;

        if (maxWaitNs <= 0) {
            return 0;
        }

        if (thisWaitNs > maxWaitNs) {
            thisWaitNs = maxWaitNs;
        }

        logger.fine("Retrying after wait: " +
                    NANOSECONDS.toMillis(thisWaitNs) + "ms");

        return thisWaitNs;
    }

    /**
     * Dispatches the special NOP request.  Keep this method and the async
     * version in AsyncRequestDispatcherImpl up-to-date with each other.
     * <p>
     * It only does the minimal exception processing, invalidating the handle
     * if necessary; the real work is done by the invoker. The handle
     * invalidation mirrors the handle invalidation done by
     * handleRemoteException.
     */
    @Override
    public Response executeNOP(RepNodeState rns,
                               int timeoutMs,
                               LoginManager loginMgr)
        throws Exception {

        /* Different clock mechanisms, therefore the two distinct calls. */

        final RequestHandlerAPI ref =
            rns.getReqHandlerRef(getRegUtils(), timeoutMs);

        if (ref == null) {
            /* needs to be resolved. */
            return null;
        }

        rns.requestStart();
        activeRequestCount.incrementAndGet();
        final long startTimeNs = statsTracker.markStart();

        try {

            final int topoSeqNumber =
                getTopologyManager().getTopology().getSequenceNumber();

            final Request nop =
                Request.createNOP(topoSeqNumber, getDispatcherId(), timeoutMs);

            nop.setSerialVersion(rns.getRequestHandlerSerialVersion());

            if (loginMgr != null) {
                nop.setAuthContext(
                    new AuthContext(
                        loginMgr.getHandle(
                            rns.getRepNodeId()).getLoginToken()));
            }

            final Response response = ref.execute(nop);
            processResponse(startTimeNs, nop, response);

            return response;
        } catch (ConnectException ce) {

            /*
             * Make sure it's taken out of circulation so that the state
             * update thread can re-establish it.
             */
            rns.noteReqHandlerException(ce);
            throw ce;
        } catch (ServerError se) {
            rns.noteReqHandlerException(se);

            throw se;
        } catch (NoSuchObjectException noe) {
            rns.noteReqHandlerException(noe);
            throw noe;
        } finally {
            rns.requestEnd();
            activeRequestCount.decrementAndGet();
            statsTracker.markFinish(OpCode.NOP, startTimeNs);
        }
    }

    /**
     * Handles all exceptions encountered during the dispatch of a request.
     * All remote exceptions are re-dispatched to handleRemoteException.
     * <p>
     * The method either throws a runtime exception, as a result of the
     * exception processing, indicating that the request should be terminated
     * immediately, or returns normally indicating that it's ok to continue
     * trying the request with other nodes.
     *
     * @param request the request that resulted in the exception
     * @param initialTimeoutMs the timeout associated with the initial request
     *                          before any retries.
     * @param target identifies the target RN for the request
     * @param dispatchException the exception to be handled
     * @param loginHandle the LoginHandle used to acquire authentication,
     *                    or null if no authentication supplied locally
     */
    Exception handleDispatchException(Request request,
                                      int initialTimeoutMs,
                                      RepNodeState target,
                                      Exception dispatchException,
                                      LoginHandle loginHandle) {
        try {
            throw dispatchException;
        } catch (RemoteException re) {
            handleRemoteException(request, target, re);
        } catch (InterruptedException ie) {
            throw new OperationFaultException("Unexpected interrupt", ie);
        } catch (RNUnavailableException rue) {
            /* Retry at a different RN. */
        } catch (SessionAccessException sae) {
            /* Retry at a different RN. */
        } catch (ConsistencyException ce) {
            if (!request.isInitiatingDispatcher(dispatcherId)) {

                /*
                 * Propagate the exception to the client so it can be
                 * retried.
                 */
                throw ce;
            }
            /* At top level in client, retry until timeout. */
        } catch (WrappedClientException wce) {
            if (!request.isInitiatingDispatcher(dispatcherId)) {
                /* Pass it through. */
                throw wce;
            }
            handleWrappedClientException((RuntimeException) wce.getCause(),
                                         request,
                                         loginHandle);
        } catch (RequestTimeoutException rte) {
            if (request.isInitiatingDispatcher(dispatcherId)) {
              /* Reset it to the initial value. */
              rte.setTimeoutMs(initialTimeoutMs);
            }

            throw rte;
        } catch (FaultException fe) {

            /* If we have a TTL fault we may want to continue to retry */
            if (fe.getFaultClassName().equals
                    (TTLFaultException.class.getName())) {

                /* If back at top level in client, retry until timeout. */
                if (request.isInitiatingDispatcher(dispatcherId)) {
                    return dispatchException;
                }

                /*
                 * If we know that the targeted partition is moving, convert to
                 * a RNUnavailableException and retry until timeout.
                 */
                if (topoManager.inTransit(request.getPartitionId())) {
                    return new RNUnavailableException(fe.getMessage());
                }
            }

            throw fe;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Unexpected exception", e);
        }

        return dispatchException;
    }

    /**
     * Some wrapped exceptions need special handling. Others are re-thrown.
     */
    private void handleWrappedClientException(RuntimeException re,
                                              Request request,
                                              LoginHandle loginHandle) {

        if (re instanceof AuthenticationRequiredException) {
            handleAuthenticationRequiredException(
                request, loginHandle,(AuthenticationRequiredException) re);
            return;
        }

        throw re;
    }

    /**
     * Handles all cases of RemoteException taking appropriate action for each
     * one as described in the catch clauses in the body of the method.
     * <p>
     * The method either throws a runtime exception, as a result of the
     * exception processing, indicating that the request should be terminated
     * immediately, or returns normally indicating that it's ok to continue
     * trying the request with other nodes.
     * <p>
     * Implementation note: The code in the exception-specific catch clauses
     * is redundant wrt the general catch for RemoteException. This is done
     * deliberately to emphasize the treatment of each specific exception and
     * the motivation behind such treatment.
     *
     * @param request the request that resulted in the exception
     * @param rnState the target RN for the request
     * @param exception the exception to be handled
     */
    private void handleRemoteException(Request request,
                                       RepNodeState rnState,
                                       final RemoteException exception) {

        logger.fine(exception.getMessage());

        try {
            /* Rethrow just for the purposes of exception discrimination */
            throw exception;
        } catch (UnknownHostException uhe) {

            /*
             * RMI connection could not be created because the hostname could
             * not be resolved. This is probably due to an incorrect KVStore
             * configuration or due to some DNS glitch. The request can be
             * safely retried with a different RN.
             *
             * Remove this RN from consideration for subsequent dispatches
             * until the state update thread re-established the handle. The
             * same course of action is taken by the next three network
             * related exceptions.
             */
            rnState.noteReqHandlerException(uhe);
            return;
        } catch (ConnectException ce) {

            /*
             * The RN could not be contacted due to a ConnectException,
             * possibly because the RN was not listening on that port. The
             * request can be safely retried with a different RN (subject to
             * the requests constraints) in this case, since the request body
             * itself was not sent to the target RN. <p>
             *
             * The exception could indicate that the RN was down. Or that the
             * RN went down, came back up and was assigned a different RMI
             * server port, but the invoker still has the old port cached in
             * its remote reference and is trying to contact the RN on this
             * old port. In this case, the cached remote connection to the RN
             * will need to be updated, by contacting the registry, and the
             * request retried.
             */

            rnState.noteReqHandlerException(ce);
            return;
        } catch (ConnectIOException cie) {

            /*
             * A problem detected during the initial handshake between the
             * client and the server, for example, because the server thread
             * pool limit has been reached. It may also be due to some network
             * level IO exception. Once again, the request can safely be
             * retried.
             *
             * RMI actually throws this exception whenever there is any
             * network I/O related issue, even for example, when a client
             * cannot connect to a registry, or a service. So remove the
             * RN from consideration for subsequent request dispatches.
             */

            rnState.noteReqHandlerException(cie);
            return;
        } catch (MarshalException me) {

            /*
             * A possible network I/O error during the execution of the
             * request. In this case, the call may or may not have reached and
             * been executed by the server. If the request was for a write
             * operation, the failure is propagated back to the invoker as a
             * KVStore exception.
             */
            faultIfWrite(request, "Problem during marshalling", me);

            return;
        } catch (UnmarshalException ue) {
            faultIfWrite(request, "Problem during unmarshalling", ue);
            return;
        } catch (ServerException se) {

            /*
             * An unhandled runtime exception in the thread processing the
             * request. It's likely that the problem was confined to the
             * specific request. Since the server, in this case, is up and
             * taking requests, it is maintained in its current state in the
             * state table.
             */

            /* A problem in the LocalDispatchHandler */
            faultIfWrite(request, "Exception in server", se);
            return;
        } catch (ServerError se) {

            /*
             * An Error in the thread processing the request. An Error
             * indicates a more serious problem on the Server.
             */
            rnState.noteReqHandlerException(se);

            /* A severe problem in the LocalDispatchHandler */
            faultIfWrite(request, "Error in server", se);
            return;
        } catch (NoSuchObjectException noe) {

            /*
             * The RN object was not found in the target RN's VM. Each remote
             * reference contains an ObjId that uniquely identifies an
             * instance of the object. When a server process goes down, comes
             * back up and rebinds a remote object it may be assigned a
             * different id from the one that is cached in the client's remote
             * reference.
             */
            rnState.noteReqHandlerException(noe);
            return;
        } catch (RemoteException e) {
            /* Likely some transient network related problem. */
            faultIfWrite(request, "unexpected exception", e);
            return;
        }
    }

    /**
     * Handle an AuthenticationRequiredException in the case that we are the
     * initiating dispatcher. We attempt LoginToken renewal, if possible.  This
     * is primarily in support of access by KVSessionManager, which is normally
     * authenticated via an InternalLoginManager.  If renewal succeeds, or if a
     * SessionAccessException occurs during token renewal, this method returns
     *  silently.  Otherwise, the exception is re-thrown.
     */
    private void handleAuthenticationRequiredException(
        Request request,
        LoginHandle loginHandle,
        AuthenticationRequiredException are) {

        /*
         * Attempt token renewal, if possible.  This is relevant for the
         * internal login manager.
         */
        if (request.getAuthContext() == null || loginHandle == null) {
            throw are;
        }

        final LoginToken currToken = request.getAuthContext().getLoginToken();
        try {
            if (loginHandle.renewToken(currToken) == currToken) {
                throw are;
            }
        } catch (SessionAccessException sae) {
            /*
             * Athough the renewal could not be completed as the result
             * of a SessionAccessException, it may be posssible to
             * complete this request against another RN.
             */
            logger.fine(sae.getMessage());
        }
    }

    /**
     * Checks to see if the TTL has expired for the specified request. If so
     * an exception is thrown.
     */
    void checkTTL(Request request) {

        try {
            request.decTTL();
        } catch (TTLFaultException ttlfe) {

            /*
             * If in transit, throw  RNUnavailableException. This wil cause
             * the operation to be retried.
             */
            if (topoManager.inTransit(request.getPartitionId())) {
                throw new RNUnavailableException(ttlfe.getMessage());
            }
            throw ttlfe;
        }
    }

    /**
     * If a RemoteException, or another communication-related exception, was
     * encountered during a write request, then this throws FaultException.
     * With some remote exceptions, it's hard to say for sure whether the
     * requested operation was carried out, or was abandoned and is therefore
     * safe to retry. For example, if the exception took place during the
     * marshalling of the arguments, the transaction was never started. If the
     * exception took place while marshalling the return value or return
     * exception then it may have been carried out.  So in such cases we play
     * it safe and inform the client of the failure.
     *
     * Should we use a specific fault so the app knows that the write may have
     * completed?
     */
    void faultIfWrite(Request request,
                      String faultMessage,
                      Exception exception)
        throws FaultException {

        if (request.isWrite()) {
            throwAsFaultException(faultMessage, exception);
        }

        /* A read operation can be safely retried at some other RN. */
    }

    /**
     * Throws the appropriate FaultException using the specified message and
     * with the exception as the cause.  In particular, throws
     * RequestTimeoutException if it can determine that the failure was caused
     * by a timeout.
     */
    void throwAsFaultException(String faultMessage, Exception exception)
        throws FaultException {

        String message = null;
        Throwable cause = exception.getCause();
        while (cause != null) {
            try {
                throw cause;
            } catch (java.net.SocketTimeoutException STE) {
                message = STE.getMessage();
                break;
            } catch (Throwable T) {
                /* Continue down the cause list. */
                cause = cause.getCause();
            }
        }

        if (message == null) {
            throw new FaultException(faultMessage, exception, isRemote);
        }

        throw new RequestTimeoutException(0, message, exception, isRemote);
    }

    /**
     * Utility method to maintain the exclude set: the set of RNs, that are
     * removed from further consideration during the request dispatch.
     *
     * @return the set augmented to hold the rep node state.
     */
    Set<RepNodeId> excludeRN(Set<RepNodeId> excludeSet, RepNodeState rnState) {
        if (rnState == null) {
            return excludeSet;
        }

        /* Exclude the node from consideration during a retry. */
        if (excludeSet == null) {
            excludeSet = new HashSet<RepNodeId>();
        }
        excludeSet.add(rnState.getRepNodeId());
        return excludeSet;
    }

    /**
     * Directs the request to a specific RN. It may be forwarded by that RN
     * if it's unsuitable.
     *
     * @param request the request to be executed.
     * @param targetId the initial target for execution
     *
     * @return the response resulting from execution of the request
     */
    public Response execute(Request request,
                            RepNodeId targetId,
                            LoginManager loginMgr)
        throws FaultException {

        return execute(request, targetId, null, loginMgr);
    }

    @Override
    public Response execute(Request request,
                            Set<RepNodeId> excludeRNs,
                            LoginManager loginMgr)
        throws FaultException {

        return execute(request, null, excludeRNs, loginMgr);
    }

    @Override
    public Response execute(Request request,
                            LoginManager loginMgr)
        throws FaultException {

        return execute(request, null, null, loginMgr);
    }

    /**
     * Process the response, updating any topology or status updates resulting
     * from the response.
     * <p>
     * Note that if this request dispatcher is hosted in a replica RN, it
     * cannot save them to the LADB. It's possible that the replica RN already
     * has a copy of the updated of the topology via the the replication
     * stream. In future, a trigger mechanism may prove to be useful so that
     * we can track changes and update the in-memory copy of the Topology. For
     * now, it may be worth polling the LADB on some periodic basis to see if
     * the sequence number associated with the Topology has changed and if so
     * update the "in-memory" copy.
     * <p>
     * @param startNs the start time associated with the request
     * @param request the request associated with the response
     * @param response the response
     */
    void processResponse(long startNs, Request request, Response response) {

        final TopologyInfo topoInfo = response.getTopoInfo();

        if (topoInfo != null) {
            if (topoInfo.getChanges() != null) {
                /* Responder has more recent topology */
                topoManager.update(topoInfo);
            } else if (topoInfo.getSourceSeqNum() >
                       topoManager.getTopology().getSequenceNumber()) {
                /*
                 * Responder has more recent topology but could not supply the
                 * changes in incremental form, need to pull over the entire
                 * topology.
                 */
                stateUpdateThread.pullFullTopology(response.getRespondingRN(),
                                                   topoInfo.getSourceSeqNum());
            }
        }

        repGroupStateTable.
            update(request, response,
                   (int) NANOSECONDS.toMillis((System.nanoTime() - startNs)));
    }

    /**
     * Selects a suitable target RN from amongst the members of the
     * replication group.
     *
     * @param rgState the group that identifies the candidate RNs
     * @param request the request to be dispatched
     * @param excludeRNs the RNs that must be excluded either because they
     * have forwarded the request or because they are not active.
     *
     * @return a suitable non-null RN
     *
     * @throws NoSuitableRNException if an RN could not be found.
     */
    RepNodeState selectDispatchRN(RepGroupState rgState,
                                  Request request,
                                  Set<RepNodeId> excludeRNs)
        throws NoSuitableRNException {

        /* When testing, we want this method to throw a NoSuitableRNException.
         * The problem is that NoSuitableRNException is defined to be a
         * checked exception rather than a RuntimeException. Additionally,
         * it is also defined to be a private, non-static inner class.
         * Because of this, the test hook executed below cannot be defined
         * to throw an instance of NoSuitableRNException directly; because
         * NoSuitableRNException cannot be instantiated by classes (such
         * as test classes) outside of this class, and also because
         * TestHook.doHook does not throw Exception. As a result, the
         * test hook is defined to throw an instance of RuntimeException.
         * Thus, when testing, and only when testing, upon receiving a
         * RuntimeException from the test hook, this method will then throw
         * a NoSuitableRNException in response.
         */
        try {
            assert TestHookExecute.doHookIfSet(requestExecuteHook, request);
        } catch (RuntimeException e) {
            throw new NoSuitableRNException("from test");
        }

        final boolean needsMaster = request.needsMaster();
        if (needsMaster) {
            final RepNodeState master = rgState.getMaster();
            if ((master != null) &&
                (excludeRNs == null ||
                 !excludeRNs.contains(master.getRepNodeId())) &&
                request.isPermittedZone(master.getZoneId())) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Dispatching to master: " +
                                master.getRepNodeId());
                }
                return master;
            }

            /*
             * Needs a master. If any of the other nodes in the RG are
             * available use them instead, they may be able to re-direct to
             * the master, since they may have more current information. It's
             * not as efficient, but is better than failing the request.
             */
        } else if (request.needsReplica()) {
            /* Exclude the master node from consideration for the request. */
            excludeRNs = excludeRN(excludeRNs, rgState.getMaster());
        }

        final RepNodeState rnState =
            rgState.getLoadBalancedRN(request, excludeRNs);

        if (rnState != null) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Dispatching target RN: " +
                            rnState.getRepNodeId());
            }
            return rnState;
        }

        /*
         * No active nodes in the RG based upon the state information
         * available at this RG. This dispatcher may not have a current
         * topology and the RG itself has been removed. If that's the case
         * this dispatcher will eventually hear about the change. If all the
         * nodes in the RG are truly not active, then all we can do is retry
         * until the nodes come on line. For now, just dispatch to a random
         * node in the group where it may fail or succeed.
         */
        final RepGroupId groupId = rgState.getResourceId();
        final RepNodeState randomRN = rgState.getRandomRN(request, excludeRNs);
        if (randomRN == null) {
            final String message =
                ((needsMaster) ?
                 ("No active (or reachable) master in rep group: " +  groupId) :
                 ("No suitable node currently available to service" +
                  " the request in rep group: " +  groupId)) +
                ". Unsuitable nodes: " +
                ((excludeRNs == null) ? "none" : excludeRNs) +
                ((readZones != null) ? ". Read zones: " + readZones : "");
            logger.fine(message);

            throw new NoSuitableRNException(message);
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Dispatching to random RN: " +
                        randomRN.getRepNodeId());
        }
        return randomRN;
    }

    /* Primarily for debugging use. */
    public void logRequestStats() {
        for (RepNodeState rnState: repGroupStateTable.getRepNodeStates()) {
            logger.info(rnState.printString());
        }
    }

    @Override
    public Topology getTopology() {
        return topoManager.getTopology();
    }

    @Override
    public TopologyManager getTopologyManager() {
        return topoManager;
    }

    @Override
    public RepGroupStateTable getRepGroupStateTable() {
        return repGroupStateTable;
    }

    @Override
    public ResourceId getDispatcherId() {
        return dispatcherId;
    }

    @Override
    public PartitionId getPartitionId(byte[] keyBytes) {
        return topoManager.getTopology().getPartitionId(keyBytes);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.api.RequestDispatcher#getRegUtils()
     */
    @Override
    public RegistryUtils getRegUtils() {
       return regUtils;
    }

    @Override
    public UncaughtExceptionHandler getExceptionHandler() {
        return stateUpdateThread.getUncaughtExceptionHandler();
    }

    @Override
    public void setRegUtilsLoginManager(LoginManager loginMgr) {
        this.regUtilsLoginMgr = loginMgr;
        updateRegUtils();
    }

    /**
     * Provides a mechanism for updating regUtils.  Because either of the
     * topology or login manager may change, we synchronize here to ensure
     * a consistent update.
     */
    private synchronized void updateRegUtils() {
        final Topology topo = topoManager.getTopology();
        regUtils = new RegistryUtils(topo, regUtilsLoginMgr);
    }

    /**
     * A exception used within the request dispatcher to signal that no
     * suitable RN was found.
     */
    @SuppressWarnings("serial")
    class NoSuitableRNException extends Exception {
        NoSuitableRNException(String message) {
            super(message);
        }

    }

    @Override
    public Table getTable(KVStoreImpl store,
                          String namespace,
                          String tableName)
        throws FaultException {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid name " + tableName);
        }

        /*
         * Only the last exception is kept.
         */
        Exception cause = null;
        TableImpl table = null;
        for (RepNodeId rnid : getRepNodeIds()) {
            try {
                try {
                    cause = null;
                    table = getTableFromRepNode(namespace, tableName, rnid);
                    if (table != null) {
                        return table;
                    }
                } catch (AuthenticationRequiredException are) {
                    /* try to reauthenticate if required */
                    final LoginManager requestLoginMgr =
                        KVStoreImpl.getLoginManager(store);
                    if (!store.tryReauthenticate(requestLoginMgr)) {
                        throw are;
                    }

                    /* re-try the the operation one more time */
                    table = getTableFromRepNode(namespace, tableName, rnid);
                    if (table != null) {
                        return table;
                    }
                }
            } catch (Exception e) {
                cause = e;
            }
        }
        if (cause != null) {
            if ( cause instanceof KVSecurityException) {
                /* do not wrap into a FaultException if it's a KVSecurityExc */
                throw (KVSecurityException)cause;
            }

            String nsName =
                TableMetadata.makeNamespaceName(namespace, tableName);
            String message = "Unable to find table " + nsName +
                ": " + cause.getMessage();
            throw new FaultException(message, cause, true);
        }
        return null;
    }

    @Override
    public Table getTableById(KVStoreImpl store, long tableId) {

        Exception cause = null;
        TableImpl table = null;
        for (RepNodeId rnid : getRepNodeIds()) {
            try {
                try {
                    cause = null;
                    table = getTableFromRepNode(tableId, rnid);
                    if (table != null) {
                        return table;
                    }
                } catch (AuthenticationRequiredException are) {
                    /* try to re-authenticate */
                    final LoginManager requestLoginMgr =
                        KVStoreImpl.getLoginManager(store);
                    if (!store.tryReauthenticate(requestLoginMgr)) {
                        throw are;
                    }

                    /* re-try the the operation one more time */
                    table = getTableFromRepNode(tableId, rnid);
                    if (table != null) {
                        return table;
                    }
                } catch (UnsupportedOperationException uoe) {
                    /* If this is not a supported node, try another */
                    continue;
                }
            } catch (Exception e) {
                cause = e;
            }
        }
        if (cause != null) {
            if ( cause instanceof KVSecurityException) {
                /* do not wrap into a FaultException */
                throw (KVSecurityException)cause;
            }

            String message =
                "Unable to find table with id " +
                TableImpl.createIdString(tableId) + ": " + cause.getMessage();
            throw new FaultException(message, cause, true);
        }
        return null;
    }

    /**
     * Gets the TableMetadata object from a RepNode.
     * It is also used by the public getTables() interface.
     */
    @Override
    public TableMetadata getTableMetadata()
        throws FaultException {
        /*
         * Only the last exception is kept.
         */
        Exception cause = null;
        TableMetadata md = null;

        for (RepNodeId rnid : getRepNodeIds()) {
            try {
                cause = null;
                md = getTableMetadataFromRepNode(rnid);
                if (md != null) {
                    return md;
                }
            } catch (Exception e) {
                cause = e;
            }
        }
        if (cause != null) {
            String message =
                "Unable to get table metadata:" + cause.getMessage();
            throw new FaultException(message, cause, true);
        }
        return null;
    }

    /**
     * Returns list of RepNodeIds to use for methods that need to iterate
     * them to get metadata.
     */
    private Set<RepNodeId> getRepNodeIds() {

        return getTopologyManager().getTopology().getRepNodeIds();
    }

    /**
     * Goes to the specified RN and returns the table associated with the table
     * name.   A failure may be either a null return value or an exception.  The
     * caller needs to handle exceptions.
     */
    private TableImpl getTableFromRepNode(String namespace,
                                          String tableName,
                                          RepNodeId rnid)
        throws Exception {

        if (regUtils == null) {
            return null;
        }
        final RepNodeAdminAPI repNodeAdmin = regUtils.getRepNodeAdmin(rnid);
        if (repNodeAdmin != null) {
            return (TableImpl)repNodeAdmin.getMetadata
                (MetadataType.TABLE,
                 new TableMetadata.TableMetadataKey
                 (namespace, tableName).
                 getMetadataKey(), 0);
        }
        return null;
    }

    private TableImpl getTableFromRepNode(long tableId,
                                          RepNodeId rnid)
        throws Exception {

        if (regUtils == null) {
            return null;
        }
        final RepNodeAdminAPI repNodeAdmin = regUtils.getRepNodeAdmin(rnid);
        if (repNodeAdmin != null) {
            return (TableImpl)repNodeAdmin.getTableById(tableId);
        }
        return null;
    }

    /**
     * Returns the TableMetadata object from the specified RepNode.  A failure
     * may be either a null return value or an exception.  The caller needs to
     * handle exceptions.
     */
    private TableMetadata getTableMetadataFromRepNode(RepNodeId rnid)
        throws Exception {

        if (regUtils == null) {
            return null;
        }
        final RepNodeAdminAPI repNodeAdmin = regUtils.getRepNodeAdmin(rnid);
        if (repNodeAdmin != null) {
            return (TableMetadata) repNodeAdmin.
                getMetadata(MetadataType.TABLE);
        }
        return null;
    }

    /**
     * The listener used to update the local copy of regUtils whenever there
     * is a Topology change.
     */
    private class RegUtilsMaintListener implements
        TopologyManager.PostUpdateListener {

        @Override
        public boolean postUpdate(Topology topology) {
            updateRegUtils();
            return false;
        }
    }

    /**
     * A topology post-update listener used to update the list of read zone
     * IDs.  Only used when readZones is not null.
     */
    private class UpdateReadZoneIds
            implements TopologyManager.PostUpdateListener {

        @Override
        public boolean postUpdate(final Topology topo) {
            assert readZones != null;
            final List<Integer> ids = new ArrayList<Integer>(readZones.size());
            final Set<String> unknownZones = new HashSet<String>(readZones);
            for (final Datacenter zone : topo.getDatacenterMap().getAll()) {
                if (readZones.contains(zone.getName())) {
                    ids.add(zone.getResourceId().getDatacenterId());
                    unknownZones.remove(zone.getName());
                }
            }
            if (!unknownZones.isEmpty() && logger.isLoggable(Level.WARNING)) {
                logger.warning("Some read zones not found: " + unknownZones);
            }
            final int[] array = new int[ids.size()];
            int i = 0;
            for (final int id : ids) {
                array[i++] = id;
            }
            readZoneIds = array;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Updated read zone IDs: {0}", ids);
            }
            return false;
        }
    }

    @Override
    public Map<OpCode, Latency> getLatencyStats(boolean clear) {

        final Map<OpCode, Latency> map = new HashMap<OpCode, Latency>();

        for (Map.Entry<OpCode, LatencyStat> entry :
             statsTracker.getIntervalLatency().entrySet()) {
            final Latency latency = clear ?
                    entry.getValue().calculateAndClear() :
                    entry.getValue().calculate();
            map.put(entry.getKey(), latency);
        }

        return map;
    }

    /* The total number of requests that were retried. */
    @Override
    public long getTotalRetryCount(boolean clear) {
        return clear ? totalRetryCount.getAndSet(0) : totalRetryCount.get();
    }

    /* For testing only. */
    public void setTestHook(TestHook<Request> hook) {
        requestExecuteHook = hook;
    }

    public void setPreExecuteHook
        (ExceptionTestHook<Request, Exception> hook) {
        preExecuteHook = hook;
    }

    @Override
    public int[] getReadZoneIds() {
        return readZoneIds;
    }
}

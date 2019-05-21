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
import static oracle.kv.impl.util.contextlogger.ContextUtils.finestWithCtx;
import static oracle.kv.impl.util.contextlogger.ContextUtils.isLoggableWithCtx;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ResultHandler;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.async.BlockingResultHandler;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.StandardDialogTypeFamily;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.SystemFaultException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.EnvironmentFailureRetryException;
import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.OperationsStatsTracker;
import oracle.kv.impl.rep.RepEnvHandleManager.StateChangeListenerFactory;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RequestTypeUpdater.RequestType;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.rep.table.ThroughputCollector;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConsistencyTranslator;
import oracle.kv.impl.util.DurabilityTranslator;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.WaitableCounter;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.SecondaryReferenceException;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.DatabasePreemptedException;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.LockPreemptedException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.RollbackProhibitedException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.utilint.StatsTracker;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * @see RequestHandler
 */
@SuppressWarnings("deprecation")
public class RequestHandlerImpl
    extends VersionedRemoteImpl
        implements AsyncRequestHandler, ParameterListener, RequestHandler {

    /**
     * Amount of time to wait after a lock conflict. Since we order all
     * multi-key access in the KV Store by key, the only source of lock
     * conflicts should be on the Replica as {@link LockPreemptedException}s.
     */
    private static final int LOCK_CONFLICT_RETRY_NS = 100000000;

    /**
     * Amount of time to wait after an environment failure. The wait allows
     * some time for the environment to restart, partition databases to be
     * re-opened, etc.  This is also the amount of time used to wait when
     * getting the environment, so the effective wait for the environment is
     * twice this value.
     */
    private static final int ENV_RESTART_RETRY_NS = 100000000;

    /**
     * The max number of faults to track for the purposes of log rate limiting.
     */
    private static final int LIMIT_FAULTS = 20;

    /**
     * 1 min in millis
     */
    private static final int ONE_MINUTE_MS = 60 * 1000;

    /**
     * An empty, immutable privilege list, used when an operation is requested
     * that does not require authentication.
     */
    private static final List<KVStorePrivilege> emptyPrivilegeList =
        Collections.emptyList();

    /**
     * A thread local that stores the current thread's dialog context while the
     * execute(Request) method is being called via an asynchronous remote call,
     * else null.  The dialog context is available to provide information known
     * to the dialog layer about the current call.
     */
    private static final ThreadLocal<DialogContext> threadDialogContext =
        new ThreadLocal<DialogContext>();

    /**
     * The parameters used to configure the request handler.
     */
    private RepNodeService.Params params;

    /**
     * The rep node that will be used to satisfy all operations.
     */
    private RepNode repNode;

    /**
     *  Identifies the rep node servicing local requests.
     */
    private RepNodeId repNodeId;

    /**
     * Tracks the number of requests that are currently active, that is,
     * are being handled.
     */
    private final WaitableCounter activeRequests = new WaitableCounter();

    /**
     * Tracks the total requests received in this interval.
     */
    private final WaitableCounter totalRequests = new WaitableCounter();
    
    
    /**
     * Table operation counters. These counters keep track of active table
     * operations associated with a specific version of table metadata.
     * The sequence number of the table metadata at the time the operation
     * starts is used to index into the array. The counter at that location
     * is incremented when the operation starts and decremented at the end.
     * The same counter is decremented even if the metadata changes during
     * the operation.
     * 
     */
    /* Number of counters must be power of 2 */
    private static final int N_COUNTERS = 8;

    /*
     * The mask is used to create an index into the tableOpCounter array
     * from a metadata seq num.
    */
    private static final int INDEX_MASK = N_COUNTERS-1;
    
    /* Counters keeping track of active table operations by metadata version */
    final WaitableCounter[] tableOpCounters = new WaitableCounter[N_COUNTERS];

    /* Keeps track of the aggregate "raw" RW throughput. */
    private final AggregateThroughputTracker aggregateTracker =
                                            new AggregateThroughputTracker();

    /**
     * A Map to track exception requests count at this node in this interval.
     * Key is fully qualified exception name, value is count.
     */
    private final AtomicReference<Map<String, AtomicInteger>> exceptionCounts;

    /**
     * All user operations directed to the requestHandler are implemented by
     * the OperationHandler.
     */
    private OperationHandler operationHandler;

    /**
     * The requestDispatcher used to forward requests that cannot be handled
     * locally.
     */
    private final RequestDispatcher requestDispatcher;

    /**
     * Mediates access to the shared topology.
     */
    private final TopologyManager topoManager;

    /**
     *  The table used to track the rep state of all rep nodes in the KVS
     */
    private final RepGroupStateTable stateTable;

    /**
     *  The last state change event communicated via the Listener.
     */
    private volatile StateChangeEvent stateChangeEvent;

    /**
     * The set of requesters that have been informed of the latest state change
     * event. The map is not currently bounded. This may become an issue if the
     * KVS is used in contexts where clients come and go on a frequent basis.
     * May want to clear out this map on some periodic basis as a defensive
     * measure.
     */
    private final Map<ResourceId, StateChangeEvent> requesterMap;

    /**
     * opTracker encapsulates all repNode stat recording.
     */
    private OperationsStatsTracker opTracker;

    /**
     * The amount of time to wait for the active requests to quiesce, when
     * stopping the request handler.
     */
    private int requestQuiesceMs;

    /**
     * The poll period used when quiescing requests.
     */
    private static final int REQUEST_QUIESCE_POLL_MS = 100;

    private final ProcessFaultHandler faultHandler;

    /**
     * Test hook used to during request handling to introduce request-specific
     * behavior.
     */
    private TestHook<Request> requestExecute;

    /**
     * Test hook used to during NOP handling.
     */
    private TestHook<Request> requestNOPExecute;

    /**
     * The access checking implementation.  This will be null if security is
     * not enabled.
     */
    private final AccessChecker accessChecker;

    /**
     * Test hook to be invoked immediately before initiating a request
     * transaction commit.
     */
    private TestHook<RepImpl> preCommitTestHook;

    private final LogMessageAbbrev logAbbrev;

    private Logger logger = null;

    private ListenHandle asyncServerHandle = null;

    /** Thread pool for executing async requests. */
    private volatile ThreadPoolExecutor asyncThreadPool;

    /*
     * Encapsulates the above logger to limit the rate of log messages
     * associated with a specific fault.
     */
    private RateLimitingLogger<String> rateLimitingLogger;

    /**
     * Flags indicate request type enabled on this node.
     */
    private volatile RequestType enabledRequestsType = RequestType.ALL;

    public RequestHandlerImpl(RequestDispatcher requestDispatcher,
                              ProcessFaultHandler faultHandler,
                              AccessChecker accessChecker) {
        super();

        this.requestDispatcher = requestDispatcher;
        this.faultHandler = faultHandler;
        this.topoManager = requestDispatcher.getTopologyManager();
        this.accessChecker = accessChecker;
        stateTable = requestDispatcher.getRepGroupStateTable();
        requesterMap = new ConcurrentHashMap<>();
        logAbbrev = new LogMessageAbbrev();
        this.exceptionCounts =
           new AtomicReference<>(new ConcurrentHashMap<String,
                                                       AtomicInteger>());
        for (int i = 0; i < N_COUNTERS; i++) {
            tableOpCounters[i] = new WaitableCounter();
        }
    }

    @SuppressWarnings("hiding")
    public void initialize(RepNodeService.Params params,
                           RepNode repNode,
                           OperationsStatsTracker opStatsTracker) {
        this.params = params;

        /* Get the rep node that we'll use for handling our requests. */
        this.repNode = repNode;
        opTracker = opStatsTracker;
        repNodeId = repNode.getRepNodeId();
        operationHandler = new OperationHandler(repNode, params);
        final RepNodeParams rnParams = params.getRepNodeParams();
        requestQuiesceMs = rnParams.getRequestQuiesceMs();
        logger = LoggerUtils.getLogger(this.getClass(), params);
        rateLimitingLogger = new RateLimitingLogger<>
                                        (ONE_MINUTE_MS, LIMIT_FAULTS, logger);
        enableRequestType(rnParams.getEnabledRequestType());

        /*
         * Immediately create threads on demand by using a synchronous queue.
         *
         * TODO: Consider adding support for a queue, including a parameter to
         * control the queue size and the ability to update the queue in place
         * in response to a parameter change.
         */
        asyncThreadPool = new ThreadPoolExecutor(
            0, rnParams.getAsyncExecMaxThreads(),
            rnParams.getAsyncExecThreadKeepAliveMs(), MILLISECONDS,
            new SynchronousQueue<>(),
            new KVThreadFactory("RequestHandlerImpl(Async)", logger));
    }

    /* Implement ParameterListener */
    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        final RepNodeParams oldParams = new RepNodeParams(oldMap);
        final RepNodeParams newParams = new RepNodeParams(newMap);
        if (oldParams.getAsyncExecMaxThreads() !=
            newParams.getAsyncExecMaxThreads()) {
            asyncThreadPool.setMaximumPoolSize(
                newParams.getAsyncExecMaxThreads());
        }
        if (oldParams.getAsyncExecThreadKeepAliveMs() !=
            newParams.getAsyncExecThreadKeepAliveMs()) {
            asyncThreadPool.setKeepAliveTime(
                newParams.getAsyncExecThreadKeepAliveMs(), MILLISECONDS);
        }
    }

    /**
     * Returns the number of requests currently being processed at this node.
     */
    public int getActiveRequests() {
        return activeRequests.get();
    }

    /**
     * Returns the aggregate read/write KB resulting from direct application
     * requests. RWKB from housekeeping tasks is not included.
     */
    public AggregateThroughputTracker getAggrateThroughputTracker() {
        return aggregateTracker;
    }

    /**
     * Returns total requests received at this node and then reset it to 0.
     */
    public int getAndResetTotalRequests() {
        return totalRequests.getAndSet(0);
    }

    /**
     * Returns the exception count map at this node and reset counts to 0.  The
     * returned exception count map will no longer be modified, and can be used
     * by the caller without fear of ConcurrentModificationeExceptions.
     */
    public Map<String, AtomicInteger> getAndResetExceptionCounts() {

        Map<String, AtomicInteger> nextCounts =
            new ConcurrentHashMap<>();
        Map<String, AtomicInteger> oldCounts =
            exceptionCounts.getAndSet(nextCounts);
        return oldCounts;
    }

    /**
     * Returns the RepNode associated with this request handler.
     */
    public RepNode getRepNode() {
       return repNode;
    }

    /**
     * Returns the request dispatcher used to forward requests.
     */
    public RequestDispatcher getRequestDispatcher() {
        return requestDispatcher;
    }

    public StateChangeListenerFactory getListenerFactory() {
        return new StateChangeListenerFactory() {

            @Override
            public StateChangeListener create(ReplicatedEnvironment repEnv) {
                return new Listener(repEnv);
            }
        };
    }

    public void setTestHook(TestHook<Request> hook) {
        requestExecute = hook;
    }

    public void setTestNOPHook(TestHook<Request> hook) {
        requestNOPExecute = hook;
    }

    public void setPreCommitTestHook(TestHook<RepImpl> hook) {
        preCommitTestHook = hook;
    }

    @Override
    public void getSerialVersion(short serialVersion,
                                 long timeoutMillis,
                                 ResultHandler<Short> handler) {
        handler.onResult(SerialVersion.CURRENT, null);
    }

    /**
     * Apply security checks and execute the request
     */
    @Override
    public Response execute(final Request request)
        throws FaultException, RemoteException {

        return faultHandler.execute(new ProcessFaultHandler.
                                    SimpleOperation<Response>() {
            @Override
            public Response execute() {
                final ExecutionContext execCtx = checkSecurity(request);
                if (execCtx == null) {
                    return trackExecuteRequest(request);
                }

                return ExecutionContext.runWithContext(
                    new ExecutionContext.SimpleOperation<Response>() {
                        @Override
                        public Response run() {
                            return trackExecuteRequest(request);
                        }},
                    execCtx);
            }
        });
    }

    @Override
    public void execute(final Request request,
                        long timeoutMillis,
                        ResultHandler<Response> handler) {
        final DialogContext context =
            AsyncRequestHandlerResponder.getDialogContext(handler);
        asyncThreadPool.submit(
            () -> {
                final Response response;
                try {
                    threadDialogContext.set(context);
                    try {
                        response = execute(request);
                    } finally {
                        threadDialogContext.set(null);
                    }
                    handler.onResult(response, null);
                } catch (Throwable e) {
                    try {
                        handler.onResult(null, e);
                    } catch (Throwable t) {
                        logger.log(Level.WARNING, "Unexpected exception: " + t,
                                   t);
                        handler.onResult(null,
                                         new RuntimeException(
                                             "Unexpected exception: " + t));
                    }
                }
            });
    }

    /** Returns the async thread pool -- for testing. */
    public ThreadPoolExecutor getAsyncThreadPool() {
        return asyncThreadPool;
    }

    /**
     * Determine what host the current remote request came from, either over
     * RMI or the async call layer.  If there is no active call in progress,
     * returns null.
     *
     * @return the remote client hostname or null
     */
    @Nullable
    public static String getClientHost() {

        /*
         * The caller doesn't know the source of the call, sync or async, so
         * try the async one first, since a null failure result there is less
         * expensive than the exception thrown by RMI's method if no remote
         * call is in progress.
         */
        final DialogContext context = threadDialogContext.get();
        if (context != null) {
            final NetworkAddress address = context.getRemoteAddress();
            return (address != null) ? address.getHostName() : null;
        }
        try {
            return RemoteServer.getClientHost();
        } catch (ServerNotActiveException snae) {
            /* We're not in the context of an RMI call */
            return null;
        }
    }

    /**
     * Verify that the request is annotated with an appropriate access
     * token if security is enabled.  Only basic access checking is performed
     * at this level.  Table/column level access checks are implemented
     * at a deeper level.
     *
     * @throw SessionAccessException if there is an internal security error
     * @throw KVSecurityException if the a security exception is
     * generated by the requesting client
     * @throw WrappedClientException if there is a
     * AuthenticationRequiredException, wrap the exception and return to client.
     */
    private ExecutionContext checkSecurity(Request request)
        throws SessionAccessException, KVSecurityException,
               WrappedClientException {
        if (accessChecker == null) {
            return null;
        }

        try {
            final RequestContext reqCtx = new RequestContext(request);
            return ExecutionContext.create(
                accessChecker, request.getAuthContext(), reqCtx);
        } catch (AuthenticationRequiredException are) {
            /* Return the exception to the client */
            throw new WrappedClientException(are);
        }
    }
    
    /**
     * Add tracking request count and exception count to execute the request.
     */
    private Response trackExecuteRequest(final Request request) {
      
        activeRequests.incrementAndGet();
        totalRequests.incrementAndGet();

        /*
         * The table operation counters keep track of active table
         * operations associated with a specific version of table metadata.
         * The sequence number of the table metadata at the time the operation
         * starts is used to index into the array. The counter at that location
         * is incremented and then decremented when the operation completes.
         * The same counter is decremented even if the metadata changes during
         * the operation.
         */
        final boolean isTableOp = request.getOperation().isTableOp();
        int index = 0;
        if (isTableOp) {
            /* During unit tests the MD sequence number returned may be 0 */
            index = repNode.getMetadataSeqNum(MetadataType.TABLE) & INDEX_MASK;
            tableOpCounters[index].incrementAndGet();
        }
        try {
            return executeRequest(request);
        } catch (Exception e) {
            final String exception = e.getClass().getName();
            Map<String, AtomicInteger> counts = exceptionCounts.get();
            counts.putIfAbsent(exception, new AtomicInteger(0));
            counts.get(exception).incrementAndGet();
            exceptionCounts.compareAndSet(counts, counts);
            throw e;
        } finally {
            activeRequests.decrementAndGet();
            
            if (isTableOp) {
                tableOpCounters[index].decrementAndGet();
            }
        }
    }

    /**
     * Executes the operation associated with the request.
     * <p>
     * All recoverable JE operations are handled at this level and retried
     * after appropriate corrective action. The implementation body comments
     * describe the correct action.
     * <p>
     */
    private Response executeRequest(final Request request) {

        try {
            final OpCode opCode = request.getOperation().getOpCode();

            if (OpCode.NOP.equals(opCode)) {
                return executeNOPInternal(request);
            } else if (topoManager.getTopology() != null) {
                return executeInternal(request);
            } else {

                /*
                 * The Topology is null. The node is waiting for one
                 * of the members in the rep group to push topology
                 * to it. Have the dispatcher re-direct the request to
                 * another node, or keep retrying until the topology
                 * is available.
                 */
                final String message = "awaiting topology push";
                throw new RNUnavailableException(message);
            }
        }  catch (ThreadInterruptedException  tie) {
            /* This node may be going down. */
            final String message =
                "RN: " + repNodeId + " was interrupted.";
            logger.info(message);
            /* Have the request dispatcher retry at a different node.*/
            throw new RNUnavailableException(message);
        }
    }

    /**
     * Wraps the execution of the request in a transaction and handles
     * any exceptions that might result from the execution of the request.
     */
    private Response executeInternal(Request request) {

        assert TestHookExecute.doHookIfSet(requestExecute, request);
        Response response = forwardIfRequired(request);
        if (response != null) {
            return response;
        }

        LogContext lc = request.getLogContext();
        if (isLoggableWithCtx(logger, Level.FINEST, lc)) {
            finestWithCtx(logger, "executing " + request.toString(), lc);
        }
        /* Can be processed locally. */

        /*
         * Track resource usage. If tc is null the operation is not
         * a table operation or tracking for the table is not enabled.
         */
        final InternalOperation internalOp = request.getOperation();
        final ThroughputCollector tc =
                repNode.getTableManager().
                                getThroughputCollector(internalOp.getTableId());
        if (tc != null) {
            /*
             * If there is tracking first check to see if a limit has been
             * exceeded.
             */
            tc.checkForLimitExceeded(internalOp);
            internalOp.setThroughputTracker(tc, request.getConsistency());
        } else {
            /* Tracking not enabled for table, or non-table (KV) request. */
            internalOp.setThroughputTracker(aggregateTracker,
                                            request.getConsistency());
        }

        OperationFailureException exception = null;
        final long limitNs = System.nanoTime() +
                             MILLISECONDS.toNanos(request.getTimeout());

        do {
            final TransactionConfig txnConfig =
                setupTxnConfig(request, limitNs);
            long sleepNs = 0;
            Transaction txn = null;
            MigrationStreamHandle streamHandle = null;
            ReplicatedEnvironment repEnv = null;
            final StatsTracker<OpCode> tracker = opTracker.getStatsTracker();
            final long startNs = tracker.markStart();

            try {

                checkEnabledRequestType(request);
                final long getEnvTimeoutNs =
                    Math.min(limitNs - startNs, ENV_RESTART_RETRY_NS);
                repEnv = repNode.getEnv(NANOSECONDS.toMillis(getEnvTimeoutNs));
                if (repEnv == null) {

                    if (startNs + getEnvTimeoutNs < limitNs) {

                        /*
                         * It took too long to get the environment, but there
                         * is still time remaining before the request times
                         * out.  Chances are that another RN will be able to
                         * service the request before this environment is
                         * ready, because environment restarts are easily
                         * longer (30 seconds) than typical request timeouts (5
                         * seconds).  Report this RN as unavailable and let the
                         * client try another one.  [#22661]
                         */
                        throw new RNUnavailableException(
                            "Environment for RN: " + repNodeId +
                            " was unavailable after waiting for " +
                            (getEnvTimeoutNs/1000) + "ms");
                    }

                    /*
                     * Request timed out -- sleepBeforeRetry will throw the
                     * right exception.
                     */
                    sleepBeforeRetry(request, null,
                                     ENV_RESTART_RETRY_NS, limitNs);
                    continue;
                }

                txn = repEnv.beginTransaction(null, txnConfig);
                streamHandle = MigrationStreamHandle.initialize
                    (repNode, request.getPartitionId(), txn);
                final Result result = operationHandler.execute(
                    internalOp, txn, request.getPartitionId());
                if (internalOp.isTableOp()) {
                    int seqNum = repNode.getMetadataSeqNum(MetadataType.TABLE);
                    result.setMetadataSeqNum(seqNum);
                }

                final OpCode opCode = internalOp.getOpCode();
                if (txn.isValid()) {
                    streamHandle.prepare();
                    /* If testing SR21210, throw InsufficientAcksException. */
                    assert TestHookExecute.doHookIfSet
                        (preCommitTestHook,
                         RepInternal.getRepImpl(repEnv));
                    txn.commit();
                } else {
                    /*
                     * The transaction could have been invalidated in the
                     * an unsuccessful Execute.execute operation, or
                     * asynchronously due to a master->replica transition.
                     *
                     * Note that single operation (non Execute requests)
                     * never invalidate transactions explicitly, so they are
                     * always forwarded.
                     */
                    if (!opCode.equals(OpCode.EXECUTE) ||
                        result.getSuccess()) {
                        /* Async invalidation independent of the request. */
                      throw new ForwardException();
                    }
                    /*
                     * An execute operation failure, allow a response
                     * generation which contains the reason for the failure.
                     */
                }
                response = createResponse(repEnv, request, result);

                tracker.markFinish(opCode, startNs, result.getNumRecords());
                return response;
            } catch (InsufficientAcksException iae) {
                /* Propagate RequestTimeoutException back to the client */
                throw new RequestTimeoutException
                    (request.getTimeout(),
                     "Timed out due to InsufficientAcksException", iae, true);
            } catch (ReplicaConsistencyException rce) {
                /* Propagate it back to the client */
                throw new ConsistencyException
                    (rce, ConsistencyTranslator.translate(
                      rce.getConsistencyPolicy(),
                      request.getConsistency()));
            } catch (InsufficientReplicasException ire) {
                /* Propagate it back to the client */
                throw new DurabilityException
                    (ire,
                     DurabilityTranslator.translate(ire.getCommitPolicy()),
                     ire.getRequiredNodeCount(), ire.getAvailableReplicas());
            } catch (DiskLimitException dle) {
                /* Propagate it back to the client */
                throw new FaultException(dle, true);
            } catch (ReplicaWriteException rwe) {
                /* Misdirected message, forward to the master. */
                return forward(request, repNodeId.getGroupId());
            } catch (UnknownMasterException rwe) {
                /* Misdirected message, forward to the master. */
                return forward(request, repNodeId.getGroupId());
            } catch (ForwardException rwe) {
                /* Misdirected message, forward to the master. */
                return forward(request, repNodeId.getGroupId());
            } catch (LockConflictException lockConflict) {

                /*
                 * Retry the transaction until the timeout associated with the
                 * request is exceeded. Note that LockConflictException covers
                 * the HA LockPreemptedException.
                 */
                exception = lockConflict;
                sleepNs = LOCK_CONFLICT_RETRY_NS;
            } catch (RollbackException rre) {
                /* Re-establish handles. */
                repNode.asyncEnvRestart(repEnv, rre);
                sleepNs = ENV_RESTART_RETRY_NS;
            } catch (RollbackProhibitedException pe) {
                logAbbrev.log(Level.SEVERE,
                              "Rollback prohibited admin intervention required",
                              repEnv,
                              pe);

                /*
                 * Rollback prohibited, ensure that the process exits and that
                 * the SNA does no try to restart, since it will result in the
                 * same exception until some corrective action is taken.
                 */
                throw new SystemFaultException("rollback prohibited", pe);
            } catch (IncorrectRoutingException ire) {

                /*
                 * An IncorrectRoutingException can occur at the end of a
                 * partition migration, where the local topology has been
                 * updated with the partition's new location (here) but the
                 * parition DB has not yet been opened (see
                 * RepNode.updateLocalTopology).
                 */
                return handleException(request, ire);
            } catch (DatabasePreemptedException dpe) {

                /*
                 * A DatabasePreemptedException can occur when the partition
                 * DB has been removed due to partition migration activity
                 * during the request.
                 */
                return handleException(request, dpe);
            } catch (SecondaryReferenceException sre) {
                /*
                 * TODO - Currently this is fatal and will cause a restart and
                 * will likely happen again. Instead the secondary should be
                 * removed, disabled, or rebuilt so that the store stays up.
                 */
                logger.log(Level.SEVERE, "Failure with secondary DB {0}: {1}",
                           new Object[]{sre.getSecondaryDatabaseName(),
                                        sre.getLocalizedMessage()});
                throw sre;
            } catch (EnvironmentFailureException efe) {
                /*
                 * All subclasses of EFE that needed explicit handling were
                 * handled above. Throw out, so the process fault handler can
                 * restart the RN.
                 */
                  if (!request.isWrite() || notCommitted(txn)) {
                      throw new EnvironmentFailureRetryException(efe);
                  }
                  throw efe;
            } catch (UnauthorizedException ue) {
                /*
                 * Data access violation in operations, log it and throw out.
                 */
                AccessCheckUtils.logSecurityError(
                    ue, "API request: " + internalOp,
                    rateLimitingLogger);
                throw ue;
            } catch (WrappedClientException wce) {
                /*
                 * These are returned to the client.
                 */
                throw wce;
            } catch (KVSecurityException kvse) {
                /*
                 * These are returned to the client.
                 */
                throw new WrappedClientException(kvse);
            } catch (MetadataNotFoundException mnfe) {
                /*
                 * MetadataNotFoundException was added as of SerialVersion.V8,
                 * so do not throw it for earlier clients.
                 */
                if (request.getSerialVersion() < SerialVersion.V8) {
                    throw new FaultException(mnfe.getMessage(), true);
                }
                throw new WrappedClientException(mnfe);
            } catch (RNUnavailableException rnue) {
                final String msg = rnue.getMessage();
                rateLimitingLogger.log(msg, Level.INFO, msg);

                /* Propagate it back to the client. */
                throw rnue;
            } catch (FaultException fe) {

                /*
                 * All FEs coming from the server should be remote. Since FEs
                 * are usually caused by bad client parameters, invalid
                 * requests, or out-of-sync metadata, we limit the logging to
                 * avoid flooding the log files. The exception message is used
                 * as the key because, in general, the FE message does not vary
                 * given the same cause.
                 */
                if (fe.wasLoggedRemotely()) {
                    final String msg = fe.getMessage();
                    rateLimitingLogger.log(msg, Level.INFO, msg);
                } else {
                    logger.log(Level.SEVERE, "unexpected fault", fe);
                }
                /* Propagate it back to the client. */
                throw fe;
            } catch (RuntimeException re) {
                final Response resp  =
                    handleRuntimeException(repEnv, txn, request, re);

                if (resp != null) {
                    return resp;
                }
                sleepNs = ENV_RESTART_RETRY_NS;
            } finally {
                if (response == null) {
                    TxnUtil.abort(txn);
                    /* Clear the failed operation's activity */
                    tracker.markFinish(null, 0L);
                }
                if (streamHandle != null) {
                    streamHandle.done();
                }
            }

            sleepBeforeRetry(request, exception, sleepNs, limitNs);

        } while (true);
    }

    /**
     * The method makes provisions for special handling of RunTimeException.
     * The default action on encountering a RuntimeException is to exit the
     * process and have it restarted by the SNA, since we do not understand the
     * nature of the problem and restarting, while high overhead, is safe.
     * However there are some cases where we would like to avoid RN process
     * restarts and this method makes provisions for such specialized handling.
     *
     * RuntimeExceptions can arise because JE does not make provisions for
     * asynchronous close of the environment. This can result in NPEs or ISEs
     * from JE when trying to perform JE operations, using database handles
     * belonging to the closed environments.
     *
     * TODO: The scope of this handler is rather large and this check could be
     * narrowed down to enclose only the JE operations.
     *
     * @return a Response, if the request could be handled by forwarding it
     * or null for a retry at this same RN.
     *
     * @throws RNUnavailableException if the request can be safely retried
     * by the requestor at some other RN.
     */
    private Response handleRuntimeException(ReplicatedEnvironment repEnv,
                                            Transaction txn,
                                            Request request,
                                            RuntimeException re)
        throws RNUnavailableException {

        if ((repEnv == null) || repEnv.isValid()) {

            /*
             * If the environment is OK (or has not been established) and the
             * exception is an IllegalStateException, it may be a case that
             *
             * - the database has been closed due to partition migration.
             *
             * - the transaction commit threw an ISE because the node
             * transitioned from master to replica, and the transaction was
             * asynchronously aborted by the JE RepNode thread.
             *
             * If so, try forward the request.
             */
            if (re instanceof IllegalStateException) {
                final Response resp = forwardIfRequired(request);
                if (resp != null) {
                    logger.log(Level.INFO,
                               "Request forwarded due to ISE: {0}",
                               re.getMessage());
                    return resp;
                }
                /*
                 * Response could have been processed at this node but wasn't.
                 *
                 * The operation may have used an earlier environment handle,
                 * acquired via the database returned by PartitionManager,
                 * which resulted in the ISE. [#23114]
                 *
                 * Have the caller retry the operation at some other RN if we
                 * can ensure that the environment was not modified by the
                 * transaction.
                 */
                if (notCommitted(txn)) {

                    final String msg = "ISE:" +  re.getMessage() +
                        " Retry at some different RN." ;

                    logger.info(msg);
                    throw new RNUnavailableException(msg);
                }
            }

            /* Unknown failure, propagate the RE */
            logger.log(Level.SEVERE, "unexpected exception", re);
            throw re;
        }

        logAbbrev.log(Level.INFO,
                      "Ignoring exception and retrying at this RN, " +
                      "environment has been closed or invalidated",
                      repEnv,
                      re);

        /* Retry at this RN. */
        return null;
    }

    private boolean notCommitted(Transaction txn) {
        return (txn == null) ||
               ((txn.getState() != Transaction.State.COMMITTED) &&
                (txn.getState() !=
                 Transaction.State.POSSIBLY_COMMITTED));
    }

    /**
     * Implements the execution of lightweight NOPs which is done without
     * the need for a transaction.
     */
    private Response executeNOPInternal(final Request request)
        throws RequestTimeoutException {

        assert TestHookExecute.doHookIfSet(requestNOPExecute, request);

        final ReplicatedEnvironment repEnv =
            repNode.getEnv(request.getTimeout());

        if (repEnv == null) {
            throw new RequestTimeoutException
                (request.getTimeout(),
                 "Timed out trying to obtain environment handle.",
                 null,
                 true /*isRemote*/);
        }
        final Result result =
            operationHandler.execute(request.getOperation(), null, null);
        return createResponse(repEnv, request, result);
    }

    /**
     * Handles a request that has been incorrectly directed at this node or
     * there is some internal inconsistency. If this node's topology has more
     * up-to-date information, forward it, otherwise throws a
     * RNUnavailableException which will cause the client to retry.
     *
     * The specified RuntimeException must an instance of either a
     * DatabasePreemptedException or an IncorrectRoutingException.
     */
    private Response handleException(Request request, RuntimeException re) {
        assert (re instanceof DatabasePreemptedException) ||
               (re instanceof IncorrectRoutingException);

        /* If this is a non-partition request, then rethrow */
        /* TODO - Need to check on how these exceptions are handled for group
         * directed dispatching.
         */
        if (request.getPartitionId().isNull()) {
            throw re;
        }
        final Topology topology = topoManager.getLocalTopology();
        final Partition partition = topology.get(request.getPartitionId());

        /*
         * If the local topology is newer and the partition has moved, forward
         * the request
         */
        if ((topology.getSequenceNumber() > request.getTopoSeqNumber()) &&
            !partition.getRepGroupId().sameGroup(repNode.getRepNodeId())) {
            request.clearForwardingRNs();
            return forward(request, partition.getRepGroupId().getGroupId());
        }
        throw new RNUnavailableException
                             ("Partition database is missing for partition: " +
                              partition.getResourceId());
    }

    /**
     * Forward the request, if the RG does not own the key, if the request
     * needs a master and this node is not currently the master, or if
     * the request can only be serviced by a node that is neither the
     * master nor detached and this node is currently either the master
     * or detached.
     *
     * @param request the request that may need to be forwarded
     *
     * @return the response if the request was processed after forwarding it,
     * or null if the request can be processed at this node.
     *
     * @throws KVStoreException
     */
    private Response forwardIfRequired(Request request) {

        /*
         * If the request has a group ID, use that, otherwise, use the partition
         * iID. If that is null, the request is simply directed to this node,
         * e.g. a NOP request.
         */
        RepGroupId repGroupId = request.getRepGroupId();

        if (repGroupId.isNull()) {
            final PartitionId partitionId = request.getPartitionId();
            repGroupId = partitionId.isNull() ?
                      new RepGroupId(repNodeId.getGroupId()) :
                      topoManager.getLocalTopology().getRepGroupId(partitionId);
        }
        if (repGroupId == null) {
            throw new RNUnavailableException("RepNode not yet initialized");
        }

        if (repGroupId.getGroupId() != repNodeId.getGroupId()) {
            /* Forward the request to the appropriate RG */
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("RN does not contain group: " +
                            repGroupId + ", forwarding request.");
            }
            request.clearForwardingRNs();
            return forward(request, repGroupId.getGroupId());
        }

        final RepGroupState rgState = stateTable.getGroupState(repGroupId);
        final RepNodeState master = rgState.getMaster();

        if (request.needsMaster()) {
            /* Check whether this node is the master. */
            if ((master != null) &&
                 repNodeId.equals(master.getRepNodeId())) {
                /* All's well, it can be processed here. */
                return null;
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("RN is not master, forwarding request. " +
                            "Last known master is: " +
                            ((master != null) ?
                             master.getRepNodeId() :
                             "unknown"));
            }
            return forward(request, repNodeId.getGroupId());

        } else if (request.needsReplica()) {

            ReplicatedEnvironment.State rnState =
                rgState.get(repNodeId).getRepState();

            /* If the RepNode is the MASTER or DETACHED, forward the request;
             * otherwise, service the request.
             */
            if ((master != null) && repNodeId.equals(master.getRepNodeId())) {
                rnState = ReplicatedEnvironment.State.MASTER;
            } else if (rnState.isReplica() || rnState.isUnknown()) {
                return null;
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("With requested consistency policy, RepNode " +
                            "cannot be MASTER or DETACHED, but RepNode [" +
                            repNodeId + "] is " + rnState + ". Forward the " +
                            "request.");
            }
            return forward(request, repNodeId.getGroupId());
        }
        return null;
    }

    /**
     * Returns topology information to be returned as part of the response. If
     * the Topology numbers match up there's nothing to be done.
     *
     * If the handler has a new Topology return the changes needed to bring the
     * requester up to date wrt the handler.
     *
     * If the handler has a Topology that's obsolete vis a vis the requester,
     * return the topology sequence number, so that the requester can push the
     * changes to it at some future time.
     *
     * @param reqTopoSeqNum the topology associated with the request
     *
     * @return null if the topologies match, or the information needed to
     * update either end.
     */
    private TopologyInfo getTopologyInfo(int reqTopoSeqNum) {

        final Topology topology = topoManager.getTopology();

        if (topology == null) {

            /*
             * Indicate that this node does not have topology so that the
             * request invoker can push Topology to it.
             */
            return TopologyInfo.EMPTY_TOPO_INFO;
        }

        final int topoSeqNum = topology.getSequenceNumber();

        if (topoSeqNum == reqTopoSeqNum) {
            /* Short circuit. */
            return null;
        }

        /* Topology mismatch, send changes, if this node has them. */
        final TopologyInfo topoInfo = topology.getChangeInfo(reqTopoSeqNum + 1);

        /*
         * For secure store, there is a window between topology and its
         * signature updates. Don't send changes if signature is not updated.
         */
        if (ExecutionContext.getCurrent() != null &&
            (topoInfo.getTopoSignature() == null ||
             topoInfo.getTopoSignature().length == 0)) {
            return null;
        }
        return topoInfo;
    }

    /**
     * Packages up the result of the operation into a Response, including all
     * additional status information about topology changes, etc.
     *
     * @param result the result of the operation
     *
     * @return the response
     */
    private Response createResponse(ReplicatedEnvironment repEnv,
                                    Request request,
                                    Result result) {

        final StatusChanges statusChanges =
            getStatusChanges(request.getInitialDispatcherId());

        VLSN currentVLSN = VLSN.NULL_VLSN;
        if (repEnv.isValid()) {
            final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
            if (repImpl != null) {
                currentVLSN = repImpl.getVLSNIndex().getRange().getLast();
            }
        }

        return new Response(repNodeId,
                            currentVLSN,
                            result,
                            getTopologyInfo(request.getTopoSeqNumber()),
                            statusChanges,
                            request.getSerialVersion());
    }

    /**
     * Sleep before retrying the operation locally.
     *
     * @param request the request to be retried
     * @param exception the exception from the last preceding retry
     * @param sleepNs the amount of time to sleep before the retry
     * @param limitNs the limiting time beyond which the operation is timed out
     * @throws RequestTimeoutException if the operation is timed out
     */
    private void sleepBeforeRetry(Request request,
                                  OperationFailureException exception,
                                  long sleepNs,
                                  final long limitNs)
        throws RequestTimeoutException {

        if ((System.nanoTime() + sleepNs) >  limitNs) {

            final String message =  "Request handler: " + repNodeId +
             " Request: " + request.getOperation() + " current timeout: " +
              request.getTimeout() + " ms exceeded." +
              ((exception != null) ?
               (" Last retried exception: " + exception.getClass().getName() +
                " Message: " + exception.getMessage()) :
               "");

            throw new RequestTimeoutException
                (request.getTimeout(), message, exception, true /*isRemote*/);
        }

        if (sleepNs == 0) {
            return;
        }

        try {
            Thread.sleep(NANOSECONDS.toMillis(sleepNs));
        } catch (InterruptedException ie) {
            throw new IllegalStateException("unexpected interrupt", ie);
        }
    }

    /**
     * Forwards the request, modifying request and response attributes.
     * <p>
     * The request is modified to denote the topology sequence number at this
     * node. Note that the dispatcher id is retained and continues to be that
     * of the node originating the request.
     *
     * @param request the request to be forwarded
     *
     * @param repGroupId the group to which the request is being forwarded
     *
     * @return the modified response
     *
     * @throws KVStoreException
     */
    private Response forward(Request request, int repGroupId) {

        Set<RepNodeId> excludeRN = null;
        if (repNodeId.getGroupId() == repGroupId) {
            /* forwarding within the group */
            excludeRN = request.getForwardingRNs(repGroupId);
            excludeRN.add(repNodeId);
        }
        final short requestSerialVersion = request.getSerialVersion();
        final int topoSeqNumber = request.getTopoSeqNumber();
        request.setTopoSeqNumber
            (topoManager.getTopology().getSequenceNumber());

        final Response response =
            requestDispatcher.execute(request, excludeRN,
                                      (LoginManager) null);
        return updateForwardedResponse(requestSerialVersion, topoSeqNumber,
                                       response);

    }

    /**
     * Updates the response from a forwarded request with the changes needed
     * by the initiator of the request.
     *
     * @param requestSerialVersion the serial version of the original request.
     * @param reqTopoSeqNum of the topo seq number associated with the request
     * @param response the response to be updated
     *
     * @return the updated response
     */
    private Response updateForwardedResponse(short requestSerialVersion,
                                             int reqTopoSeqNum,
                                             Response response) {

        /*
         * Before returning the response to the client we must set its serial
         * version to match the version of the client's request, so that the
         * response is serialized with the version requested by the client.
         * This version may be different than the version used for forwarding.
         */
        response.setSerialVersion(requestSerialVersion);

        /*
         * Potential future optimization, use the request handler id to avoid
         * returning the changes multiple times.
         */
        response.setTopoInfo(getTopologyInfo(reqTopoSeqNum));
        return response;
    }

    /**
     * Create a transaction configuration. If the transaction is read only, it
     * uses Durability.READ_ONLY_TXN for the Transaction.
     *
     * The transaction may be for a DML update statement, which is both a read
     * and a write operation. For the read part, a consistency must be set.
     * However, since the transaction will take place at the master,
     * NO_CONSISTENCY can be used as the consistency value.
     *
     * The limitNs parameter specifies the end time for the transaction and
     * consistency timeouts.
     */
    private TransactionConfig setupTxnConfig(Request request,
                                             long limitNs) {
        final TransactionConfig txnConfig = new TransactionConfig();

        final long timeoutMs = Math.max(
            0, NANOSECONDS.toMillis(limitNs - System.nanoTime()));
        txnConfig.setTxnTimeout(timeoutMs, MILLISECONDS);

        if (request.isWrite()) {
            final com.sleepycat.je.Durability haDurability =
                DurabilityTranslator.translate(request.getDurability());
            txnConfig.setDurability(haDurability);

            final ReplicaConsistencyPolicy haConsistency =
                NoConsistencyRequiredPolicy.NO_CONSISTENCY;

            txnConfig.setConsistencyPolicy(haConsistency);
            return txnConfig;
        }

        /* A read transaction. */
        txnConfig.setDurability(com.sleepycat.je.Durability.READ_ONLY_TXN);

        final Consistency reqConsistency = request.getConsistency();

        /*
         * If the consistency requirement was absolute assume it was directed
         * at this node the master since it would have been forwarded otherwise
         * by the forwardIfRequired method. Substitute the innocuous HA
         * NO_CONSISTENCY in this case, since there is no HA equivalent for
         * ABSOLUTE.
         */
        final ReplicaConsistencyPolicy haConsistency =
            Consistency.ABSOLUTE.equals(reqConsistency) ?
            NoConsistencyRequiredPolicy.NO_CONSISTENCY :
            ConsistencyTranslator.translate(reqConsistency, timeoutMs);
        txnConfig.setConsistencyPolicy(haConsistency);
        return txnConfig;
    }

    /**
     * Bind the request handler in the registry so that it can start servicing
     * requests.
     */
    public void startup()
        throws RemoteException {

        final StorageNodeParams snParams = params.getStorageNodeParams();
        final GlobalParams globalParams = params.getGlobalParams();
        final RepNodeParams repNodeParams = params.getRepNodeParams();
        final String kvStoreName = globalParams.getKVStoreName();
        final String csfName = ClientSocketFactory.
                factoryName(kvStoreName,
                            RepNodeId.getPrefix(),
                            InterfaceType.MAIN.interfaceName());
        RMISocketPolicy rmiPolicy =
            params.getSecurityParams().getRMISocketPolicy();
        SocketFactoryPair sfp =
            repNodeParams.getRHSFP(rmiPolicy,
                                   snParams.getServicePortRange(),
                                   csfName,
                                   kvStoreName);
        String serviceName = RegistryUtils.bindingName(
            kvStoreName, repNode.getRepNodeId().getFullName(),
            RegistryUtils.InterfaceType.MAIN);
        RegistryUtils.rebind(snParams.getHostname(),
                             snParams.getRegistryPort(),
                             serviceName,
                             this,
                             sfp.getClientFactory(),
                             sfp.getServerFactory());
        if (AsyncRegistryUtils.serverUseAsync) {
            class RebindResultHandler
                extends BlockingResultHandler<ListenHandle> {
                @Override
                protected String getDescription() {
                    return "RegistryUtils.rebind " +
                        repNode.getRepNodeId().getFullName();
                }
            }
            final RebindResultHandler asyncRebindHandler =
                new RebindResultHandler();
            long timeout = AsyncRegistryUtils.rebind(
                snParams.getHostname(),
                snParams.getRegistryPort(),
                kvStoreName,
                serviceName,
                StandardDialogTypeFamily.ASYNC_REQUEST_HANDLER,
                new RequestHandlerDialogHandlerFactory(),
                sfp.getClientFactory(),
                sfp.getServerFactory(),
                logger,
                asyncRebindHandler);
            asyncServerHandle = asyncRebindHandler.await(timeout);
        }
    }

    private class RequestHandlerDialogHandlerFactory
            implements DialogHandlerFactory {
        @Override
        public DialogHandler create() {
            return new AsyncRequestHandlerResponder(RequestHandlerImpl.this,
                                                    logger);
        }
        @Override
        public void onChannelError(ListenerConfig config,
                                   Throwable e,
                                   boolean channelClosed) {
        }
    }

    /**
     * Unbind registry entry so that no new requests are accepted. The method
     * waits for requests to quiesce so as to minimize any exceptions on the
     * client side.
     *
     * If any exceptions are encountered, during the unbind, they are merely
     * logged and otherwise ignored, so that other components can continue
     * to be shut down.
     */
    public void stop() {

        final StorageNodeParams snParams = params.getStorageNodeParams();
        final GlobalParams globalParams = params.getGlobalParams();
        final String serviceName = RegistryUtils.bindingName(
            globalParams.getKVStoreName(),
            repNode.getRepNodeId().getFullName(),
            RegistryUtils.InterfaceType.MAIN);

        /* Stop accepting new requests. */
        try {
            RegistryUtils.unbind(snParams.getHostname(),
                                 snParams.getRegistryPort(),
                                 serviceName,
                                 this);
        } catch (RemoteException e) {
            logger.log(Level.INFO,
                       "Ignoring exception while stopping request handler",
                       e);
        }

        if (AsyncRegistryUtils.serverUseAsync) {
            class UnbindResultHandler extends BlockingResultHandler<Void> {
                @Override
                protected String getDescription() {
                    return "RegistryUtils.unbind " +
                        repNode.getRepNodeId().getFullName();
                }
            }
            final UnbindResultHandler asyncUnbindHandler =
                new UnbindResultHandler();
            final long timeout = AsyncRegistryUtils.unbind(
                snParams.getHostname(),
                snParams.getRegistryPort(),
                globalParams.getKVStoreName(),
                serviceName,
                logger,
                asyncUnbindHandler);
            try {
                asyncUnbindHandler.await(timeout);
            } catch (Exception e) {
                logger.log(Level.INFO,
                           "Ignoring exception while stopping request handler",
                           e);
            }
        }

        if (asyncServerHandle != null) {
            try {
                asyncServerHandle.shutdown(true);
            } catch (IOException e) {
                logger.log(Level.INFO,
                       "Ignoring exception while stopping request handler",
                       e);
            }
        }

        /*
         * Wait for the requests to quiesce within the requestQuiesceMs
         * period.
         */
        activeRequests.awaitZero(REQUEST_QUIESCE_POLL_MS, requestQuiesceMs);

        /* Log requests that haven't quiesced. */
        final int activeRequestCount = activeRequests.get();
        if (activeRequestCount > 0) {
            logger.info("Requested quiesce period: " + requestQuiesceMs +
                        "ms was insufficient to quiesce all active " +
                        "requests for soft shutdown. " +
                        "Pending active requests: " + activeRequestCount);
        }

        requestDispatcher.shutdown(null);

        if (asyncThreadPool != null) {
            asyncThreadPool.shutdown();
        }
    }

    /**
     * Waits for table operations to complete. Operations which are using
     * table metadata with the sequence number equal to seqNum are excluded.
     * This method will timeout if operations do not complete.
     */
    public int awaitTableOps(int seqNum) {
        int totalOutstanding = 0;
        final int skip = seqNum & INDEX_MASK;
        for (int i = 0; i < N_COUNTERS; i++) {
            if (i != skip) {
                final int count = tableOpCounters[i].get();
                if (count > 0) {
                    totalOutstanding += count;
                    tableOpCounters[i].awaitZero(100, 500);
                }
            }
        }
        return totalOutstanding;
    }
    
    /**
     * Returns the status changes that the requester identified by
     * <code>remoteRequestHandlerId</code> is not aware of.
     *
     * @param resourceId the id of the remote requester
     *
     * @return the StatusChanges if there are any to be sent back
     */
    public StatusChanges getStatusChanges(ResourceId resourceId) {

        if (stateChangeEvent == null) {

            /*
             * Nothing of interest to communicate back. This is unexpected, we
             * should not be processing request before the node is initialized.
             */
            return null;
        }
        if (requesterMap.get(resourceId) == stateChangeEvent) {

            /*
             * The current SCE is already know to the requester ignore it.
             */
            return null;
        }

        try {
            final State state = stateChangeEvent.getState();

            if (state.isMaster() || state.isReplica()) {
                final String masterName = stateChangeEvent.getMasterNodeName();
                return new StatusChanges(state,
                                         RepNodeId.parse(masterName),
                                         stateChangeEvent.getEventTime());
            }

            /* Not a master or a replica. */
            return new StatusChanges(state, null, 0);
        } finally {
            requesterMap.put(resourceId, stateChangeEvent);
        }
    }

    /**
     * Enable given request type served on this node. The allowed types are
     * all, none, readonly.
     *
     * @param requestType request type is being enabled
     */

    public void enableRequestType(RequestType requestType) {
        enabledRequestsType = requestType;
        logger.info("Request type " + enabledRequestsType + " is enabled");
    }

    /**
     * Check if type of client request is enabled.
     *
     * @throws RNUnavailableException if type of given request is not enabled
     */
    private void checkEnabledRequestType(Request request) {
        if (enabledRequestsType == RequestType.ALL) {
            return;
        }

        if (!request.getInitialDispatcherId().getType().isClient()) {
            return;
        }

        if (enabledRequestsType == RequestType.READONLY) {
            /*
             * If permits read request, only throw exception for write request.
             */
            if (request.isWrite()) {
                throw new RNUnavailableException(
                    "RN: " + repNodeId + " was unavailable because " +
                    "write requests are disabled");
            }
            return;
        }

        throw new RNUnavailableException(
            "RN: " + repNodeId + " was unavailable because " +
            "all requests have been disabled");
    }

    /**
     * For testing only.
     */
    LogMessageAbbrev getMessageAbbrev() {
        return logAbbrev;
    }

    /**
     * Listener for local state changes at this node.
     */
    private class Listener implements StateChangeListener {

        final ReplicatedEnvironment repEnv;

        public Listener(ReplicatedEnvironment repEnv) {
            this.repEnv = repEnv;
        }

        /**
         * Takes appropriate action based upon the state change. The actions
         * must be simple and fast since they are performed in JE's thread of
         * control.
         */
        @Override
        public void stateChange(StateChangeEvent sce)
            throws RuntimeException {

            stateChangeEvent = sce;
            final State state = sce.getState();
            final NodeType nodeType = (params != null) ?
                params.getRepNodeParams().getNodeType() : null;
            logger.info("State change event: " + new Date(sce.getEventTime()) +
                        ", State: " + state +
                        ", Type: " + nodeType +
                        ", Master: " +
                        ((state.isMaster() || state.isReplica()) ?
                         sce.getMasterNodeName() :
                         "none"));
            /* Ensure that state changes are sent out again to requesters */
            requesterMap.clear();
            stateTable.update(sce);
            if (repNode != null) {
                repNode.noteStateChange(repEnv, sce);
            }
        }
    }

    /**
     * Utility class to avoid a class of unnecessary stack traces. These stack
     * traces can cause important information to scroll out of the log window.
     *
     * The stack traces in this case are rooted in the same environment close
     * or invalidation. In such circumstances JE can throw an ISE, etc. in all
     * request threads as a request makes an attempt to access a JE resource
     * like a cursor or a database after the environment invalidation.
     */
    class LogMessageAbbrev {

        /**
         * Tracks the last exception to help determine whether it should be
         * printed along with a stack trace by the logAbbrev() method below.
         */
        private final AtomicReference<Environment> lastEnv =
            new AtomicReference<>();

        /* stats for test verification. */
        volatile int abbrevCount = 0;
        volatile int fullCount = 0;

        /**
         * Log the message in an abbreviated form, without the stack trace, if
         * it's associated with the same invalid environment. Log it with the
         * full stack trace, if it's the first message associated with the
         * environment since its close or invalidation.
         *
         * It's the caller's responsibility to determine which messages can
         * benefit from such abbreviation and to ensure that the method is only
         * invoked when it detects the exception in the context of an
         * invalidated environment.
         *
         * @param logMessage the message to be logged
         * @param env the environment associated with the exception
         * @param exception the exception to be logged
         */
        private void log(Level level,
                         String logMessage,
                         Environment env,
                         RuntimeException exception) {
            if ((env != null) && (lastEnv.getAndSet(env) != env)) {
                /* New env, log it in full with a stack trace. */
                logger.log(level, logMessage, exception);
                fullCount++;
            } else {
                /*
                 * Runtime exception against the same environment, log just the
                 * exception message.
                 */
                final StackTraceElement[] traces = exception.getStackTrace();
                logger.log(level, logMessage +
                           " Exception:" + exception.getClass().getName() +
                           ", Message:" + exception.getMessage() +
                           ", Method:" +
                           (((traces == null) || (traces.length == 0)) ?
                            "unknown" : exception.getStackTrace()[0]));
                abbrevCount++;
            }
        }
    }

    /**
     * A utility exception used to indicate that the request must be forwarded
     * to some other node because the transaction used to implement the
     * request has been invalidated, typically because of a master-&gt;replica
     * transition.
     */
    @SuppressWarnings("serial")
    private class ForwardException extends Exception {}

    /**
     * Provides an implementation of OperationContext for access checking.
     */
    public class RequestContext implements OperationContext {
        private final Request request;

        private RequestContext(Request request) {
            this.request = request;
        }

        public Request getRequest() {
            return request;
        }

        @Override
        public String describe() {
            return "API request: " + request.getOperation().toString();
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            final OpCode opCode = request.getOperation().getOpCode();

            /* NOP does not require authentication */
            if (OpCode.NOP.equals(opCode)) {
                return emptyPrivilegeList;
            }

            return operationHandler.getRequiredPrivileges(
                request.getOperation());
        }
    }
}

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

package oracle.kv.impl.api.rgstate;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.Consistency;
import oracle.kv.ResultHandler;
import oracle.kv.impl.api.AsyncRequestHandlerAPI;
import oracle.kv.impl.api.RequestHandlerAPI;
import oracle.kv.impl.api.Response;
import oracle.kv.impl.api.ops.NOP;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.utilint.VLSN;

/**
 * The operational state associated with the RN or KV client. This state is
 * dynamic and is used primarily to dispatch a request to a specific RN within
 * a replication group.
 * <p>
 * Note that the methods used to modify state are specified as
 * <code>update</code> rather that <code>set</code> methods. Due to the
 * distributed and concurrent nature of the processing it's entirely possible
 * that state update requests can be received out of order, so each update
 * method must be prepared to ignore obsolete update requests. The return value
 * from the <code>update</code> methods indicates whether an update was
 * actually made, or whether the request was obsolete and therefore ignored.
 */
public class RepNodeState {

    private final RepNodeId rnId;

    private final ResourceId trackerId;

    private final Logger logger;

    /** The ID of the zone containing the RN, or null if not known. */
    private volatile DatacenterId znId = null;

    /**
     * The remote request handler for the RN, supporting either the sync or
     * async API.
     */
    private final ReqHandlerRef reqHandlerRef;

    /**
     * The last known haState associated with the RN. We simply want the latest
     * state so access to it is unsynchronized.
     */
    private volatile State repState = null;

    /**
     * The VLSN state associated with this RN
     */
    private final VLSNState vlsnState;

    private int topoSeqNum = Metadata.UNKNOWN_SEQ_NUM;

    /**
     * The size of the sample used for the trailing average. The sample needs
     * to be large enough so that it represents the different types of
     * operations, but not too large so that it can't react rapidly enough to
     * changes in the response times.
     */
    public static final int SAMPLE_SIZE = 8;

    /**
     * Accumulates the response time from read requests, so they can be load
     * balanced.
     */
    private final ResponseTimeAccumulator readAccumulator;

    /**
     * The accumulated response times across both read and write operations.
     */
    private final AtomicLong accumRespTimeMs = new AtomicLong(0);

    /**
     * The number of request actively being processed by this node.
     */
    private final AtomicInteger activeRequestCount = new AtomicInteger(0);

    /* The max active request count. Just a rough metric. */
    private volatile int maxActiveRequestCount = 0;

    /**
     * The total requests dispatched to this node. We don't use an atomic long
     * here, since it's just intended to be a rough metric and losing an
     * occasional increment is acceptable.
     */
    private volatile long totalRequestCount;

    /**
     * The total number of requests that resulted in exceptions.
     */
    private volatile int errorCount;

    /**
     * The approx interval over which the VLSN rate is updated. We may want
     * to have a different rate for a client hosted dispatcher versus a RN
     * hosted dispatcher to minimize the number of open tcp connections. RNS
     * will use these connections only when forwarding requests, which should
     * be a relatively uncommon event.
     */
    public static int RATE_INTERVAL_MS = 10000;

    RepNodeState(RepNodeId rnId,
                 ResourceId trackerId,
                 boolean async,
                 Logger logger) {
        this(rnId, trackerId, async, logger, RATE_INTERVAL_MS);
    }

    RepNodeState(RepNodeId rnId,
                 ResourceId trackerId,
                 boolean async,
                 Logger logger,
                 int rateIntervalMs) {
        this.rnId = rnId;
        this.trackerId = trackerId;
        this.logger = checkNull("logger", logger);
        readAccumulator = new ResponseTimeAccumulator();
        reqHandlerRef =
            async ? new AsyncReqHandlerRef() : new SyncReqHandlerRef();
        vlsnState = new VLSNState(rateIntervalMs);

        /*
         * Start it out as being in the replica state, so an attempt is made to
         * contact it by sending it a request. At this point the state can be
         * adjusted appropriately.
         */
        repState = State.REPLICA;
    }

    /**
     * Returns the unique resourceId associated with the RepNode
     */
    public RepNodeId getRepNodeId() {
        return rnId;
    }

    /**
     * Returns the ID of the zone containing the RepNode, or {@code null} if
     * not known.
     *
     * @return the zone ID or {@code null}
     */
    public DatacenterId getZoneId() {
        return znId;
    }

    /**
     * Sets the ID of the zone containing the RepNode.
     *
     * @param znId the zone ID
     */
    public void setZoneId(final DatacenterId znId) {
        this.znId = znId;
    }

    /**
     * See ReqHandlerRef.get
     */
    public RequestHandlerAPI getReqHandlerRef(RegistryUtils registryUtils,
                                              long timeoutMs) {
        return reqHandlerRef.getSync(registryUtils, timeoutMs);
    }

    /** Async version. */
    public void getReqHandlerRef(RegistryUtils registryUtils,
                                 long timeoutMs,
                                 ResultHandler<AsyncRequestHandlerAPI> hand) {
        reqHandlerRef.getAsync(registryUtils, timeoutMs, hand);
    }

    /**
     * See ReqHandlerRef.needsResolution()
     */
    public boolean reqHandlerNeedsResolution() {
        return reqHandlerRef.needsResolution();
    }

    /**
     * See ReqHandlerRef.needsRepair()
     */
    public boolean reqHandlerNeedsRepair() {
        return reqHandlerRef.needsRepair();
    }

    /**
     * This method returns its results via a result handler even when the
     * implementation is synchronous.  Callers should not assume that results
     * will be returned immediately, either due to blocking or a delay until
     * the callback handler is called.
     *
     * See ReqHandlerRef.resolve
     */
    public void resolveReqHandlerRef(RegistryUtils registryUtils,
                                     long timeoutMs,
                                     ResultHandler<Boolean> handler) {
        reqHandlerRef.resolve(registryUtils, timeoutMs, handler);
    }

    /**
     * See ReqHandlerRef.reset
     */
    public void resetReqHandlerRef() {
        reqHandlerRef.reset();
    }

    /**
     * See ReqHandlerRef.noteException(Exception)
     */
    public void noteReqHandlerException(Exception e) {
        reqHandlerRef.noteException(e);
    }

    /**
     * Returns the SerialVersion that should be used with the request handler.
     */
    public short getRequestHandlerSerialVersion() {
        return reqHandlerRef.getSerialVersion();
    }

    /**
     * Returns the current known replication state associated with the
     * node.
     */
    public ReplicatedEnvironment.State getRepState() {
        return repState;
    }

    /**
     * Updates the rep state.
     *
     * @param state the new rep state
     */
    void updateRepState(State state) {
        repState = state;
    }

    /**
     * Returns the current VLSN associated with the RN.
     * <p>
     * The VLSN can be used to determine how current a replica is with respect
     * to the Master. It may return a null VLSN if the RN has never been
     * contacted.
     */
    VLSN getVLSN() {
        return vlsnState.getVLSN();
    }

    boolean isObsoleteVLSNState() {
        return vlsnState.isObsolete();
    }

    /**
     * Updates the VLSN associated with the node.
     *
     * @param newVLSN the new VLSN
     */
    void updateVLSN(VLSN newVLSN) {
       vlsnState.updateVLSN(newVLSN);
    }

    /**
     * Returns the current topo seq number associated with the node. A negative
     * return value means that the sequence number is unknown.
     */
    int getTopoSeqNum() {
        return topoSeqNum;
    }

    /**
     * Updates the topo sequence number, moving it forward. If it's less than
     * the current sequence number it's ignored.
     *
     * @param newTopoSeqNum the new sequence number
     */
    synchronized void updateTopoSeqNum(int newTopoSeqNum) {
        if (topoSeqNum < newTopoSeqNum) {
            topoSeqNum = newTopoSeqNum;
        }
    }

    /**
     * Resets the topo sequence number as specified.
     */
    synchronized void resetTopoSeqNum(int endSeqNum) {
        topoSeqNum = endSeqNum;
    }

    /**
     * Returns the error count.
     */
    public int getErrorCount() {
        return errorCount;
    }

    /**
     * Increments the error count associated with the RN.
     *
     * @return the incremented error count
     */
    public int incErrorCount() {
        return ++errorCount;
    }

    /**
     * Returns the average trailing read response time associated with the RN
     * in milliseconds.
     * <p>
     * It's used primarily for load balancing purposes.
     */
    int getAvReadRespTimeMs() {
        return readAccumulator.getAverage();
    }

    /**
     * Accumulates the average response time by folding in this contribution
     * from a successful request to the accumulated response times. This method
     * is only invoked upon successful completion of a read request.
     * <p>
     * <code>lastAccessTime</code> is also updated each time this method is
     * invoked.
     *
     * @param forWrite determines if the time is being accumulated for a write
     * operation
     * @param responseTimeMs the response time associated with this successful
     * request.
     */
    public void accumRespTime(boolean forWrite, int responseTimeMs) {
        if (!forWrite) {
            readAccumulator.update(responseTimeMs);
        }
        accumRespTimeMs.getAndAdd(responseTimeMs);
    }

    /**
     * Returns the number of outstanding remote requests to the RN.
     * <p>
     * It's used to ensure that all the outgoing connections are not used
     * up because the node is slow in responding to requests.
     */
    int getActiveRequestCount() {
        return activeRequestCount.get();
    }

    /**
     * Returns the max active requests at this RN, since the count was last
     * reset.
     */
    public int getMaxActiveRequestCount() {
        return maxActiveRequestCount;
    }

    /**
     * Returns the total number of requests dispatched to this node.
     */
    public long getTotalRequestCount() {
        return totalRequestCount;
    }

    public long getAccumRespTimeMs() {
        return accumRespTimeMs.get();
    }

    /**
     * Resets the total request count.
     */
    public void resetStatsCounts() {
        totalRequestCount = 0;
        maxActiveRequestCount = 0;
        accumRespTimeMs.set(0);
        errorCount = 0;
    }

    /**
     * Invoked before each remote request is issued to maintain the outstanding
     * request count. It must be paired with a requestEnd in a finally block.
     *
     * @return the number of requests active at this node
     */
    public int requestStart() {
        totalRequestCount++;
        final int count = activeRequestCount.incrementAndGet();
        maxActiveRequestCount = Math.max(maxActiveRequestCount, count);
        return count;
    }

    /**
     * Invoked after each remote request completes. It, along with the
     * requestStart method, is used to maintain the outstanding request count.
     */
    public void requestEnd() {
        activeRequestCount.decrementAndGet();
    }

    /**
     * Returns a descriptive string for the rep node state
     */
    public String printString() {
        return String.format("node: %s " +
                             "state: %s " +
                             "errors: %,d" +
                             "av resp time %,d ms " +
                             "total requests: %,d",
                             getRepNodeId().toString(),
                             getRepState().toString(),
                             getErrorCount(),
                             getAvReadRespTimeMs(),
                             getTotalRequestCount());
    }

    @Override
    public String toString() {
        return "RepNodeState[" + getRepNodeId() + ", " + getRepState() + "]";
    }

    /**
     * AttributeValue encapsulates a RN state value along with a sequence
     * number used to compare attribute values for recency.
     *
     * @param <V> the type associate with the attribute value.
     */
    @SuppressWarnings("unused")
    private static class AttributeValue<V> {
        final V value;
        final long sequence;

        private AttributeValue(V value, long sequence) {
            super();
            this.value = value;
            this.sequence = sequence;
        }

        /** Returns the value associated with the attribute. */
        private V getValue() {
            return value;
        }

        /**
         * Returns a sequence that can be used to order two attribute
         * values in time. Given two values v1 and v2, if v2.getSequence()
         * > v1.getSequence() implies that v2 is the more recent value.
         */
        private long getSequence() {
            return sequence;
        }
    }

    /**
     * Encapsulates the computation of average response times.
     */
    private static class ResponseTimeAccumulator {

        /*
         * The samples in ms.
         */
        final short[] samples;
        int sumMs = 0;
        int index = 0;

        private ResponseTimeAccumulator() {
            samples = new short[SAMPLE_SIZE];
            sumMs = 0;
        }

        private synchronized void update(int sampleMs) {
            if (sampleMs > Short.MAX_VALUE) {
                sampleMs = Short.MAX_VALUE;
            }

            index = (++index >= SAMPLE_SIZE) ? 0 : index;
            sumMs += (sampleMs - samples[index]);
            samples[index] = (short)sampleMs;
        }

        /*
         * Unsynchronized to minimize contention, a slightly stale value is
         * good enough.
         */
        private int getAverage() {
            return (sumMs / SAMPLE_SIZE);
        }
    }

    /**
     * Wraps a remote RequestHandlerAPI reference along with its accompanying
     * state. It also encapsulates the operations used to maintain the remote
     * reference.
     * <p>
     * The RemoteReference is typically resolved via the first call to
     * getSync() or getAsync(). If an exception is encountered at any time,
     * either during the initial resolve call, or subsequently when an attempt
     * is made to invoke a method through it, the exception is noted in
     * exceptionSummary and the remote reference is considered to be in
     * error. Such erroneous remote references are not considered as candidates
     * for request dispatch. The RequestHandlerUpdate thread will attempt to
     * resolve all such references and restore them on a periodic basis. This
     * resolution is done out of the request's thread of control to minimize
     * the timeout latencies associated with erroneous references.
     */
    private abstract class ReqHandlerRef {

        volatile short serialVersion;

        ReqHandlerRef() {
            /* TODO: Set to SerialVersion.UNKNOWN to catch errors. */
            serialVersion = SerialVersion.CURRENT;
        }

        /**
         * Resets the state of the reference back to its initial state.
         */
        abstract void reset();

        /**
         * Returns the serial version number used to serialize requests to this
         * request handler.
         */
        short getSerialVersion() {
            return serialVersion;
        }

        /**
         * Returns true if the ref is not currently resolved. This may be
         * because the reference has an exception associated with it or simply
         * because it was never referenced before.
         */
        abstract boolean needsResolution();

        /**
         * Returns true if the ref has a pending exception associated with it
         * and the reference needs to be resolved before before it can be used.
         */
        abstract boolean needsRepair();

        /**
         * Attempts to resolve the request handler, returning a boolean to the
         * result handler to say whether the resolution succeeded, and an
         * exception if there was a specific cause for a failure.  Note that
         * the implementation may be asynchronous.
         */
        abstract void resolve(RegistryUtils registryUtils,
                              long timeoutMs,
                              ResultHandler<Boolean> handler);

        /**
         * Returns a remote reference to the RN that can be used to make remote
         * method calls. It will try resolve the reference if it has never been
         * resolved before.  This method is synchronous.
         *
         * @param registryUtils is used to resolve "never been resolved before"
         * references
         * @param timeoutMs the amount of time to be spent trying to resolve
         * the handler
         *
         * @return a remote reference to the RN's request handler, or null.
         */
        RequestHandlerAPI getSync(RegistryUtils registryUtils,
                                  long timeoutMs) {
            throw new UnsupportedOperationException(
                "The sync operation is not supported");
        }

        /**
         * Like the previous get overloading, but asynchronous.
         */
        @SuppressWarnings("unused")
        void getAsync(RegistryUtils registryUtils,
                      long timeoutMs,
                      ResultHandler<AsyncRequestHandlerAPI> handler) {
            throw new UnsupportedOperationException(
                "The async operation is not supported");
        }

        /**
         * Makes note of the exception encountered during the execution of a
         * request. This RN is removed from consideration until the background
         * thread resolves it once again.
         */
        abstract void noteException(Exception e);
    }

    /**
     * Synchronous implementation based on RMI.
     */
    private class SyncReqHandlerRef extends ReqHandlerRef {

        /* used to coordinate updates to the handler. */
        private final Semaphore semaphore = new Semaphore(1,true);

        /**
         * The remote request handler (an RMI reference) for the RN. It's
         * volatile, since any request thread can invalidate it upon
         * encountering an exception and is read asynchronously by needs
         * resolution.
         */
        private volatile RequestHandlerAPI requestHandler;

        /**
         * It's non-null if there is an error that resulted in the
         * requestHandler iv above being nulled. It's volatile because it's
         * read without acquiring the semaphore by needsRepair() as part of a
         * request dispatch.
         */
        private volatile ExceptionSummary exceptionSummary;

        private SyncReqHandlerRef() {
            requestHandler = null;
            exceptionSummary = null;
        }

        @Override
        void reset() {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new OperationFaultException("Unexpected interrupt", e);
            }
            try {
                requestHandler = null;
                /* TODO: Set to SerialVersion.UNKNOWN to catch errors. */
                serialVersion = SerialVersion.CURRENT;
                exceptionSummary = null;
            } finally {
                semaphore.release();
            }
        }

        @Override
        boolean needsResolution() {
            return (requestHandler == null);
        }

        /**
         * Note that, in this implementation, the exception summary is read
         * outside the semaphore and that's ok, we just need an approximate
         * answer.
         */
        @Override
        boolean needsRepair() {
            return (exceptionSummary != null);
        }

        @Override
        void resolve(RegistryUtils registryUtils,
                     long timeoutMs,
                     ResultHandler<Boolean> handler) {
            try {
                handler.onResult(resolve(registryUtils, timeoutMs) != null,
                                 null);
            } catch (Exception e) {
                handler.onResult(null, e);
            }
        }

        /**
         * Resolves the remote reference based upon its Topology in
         * RegistryUtils.
         *
         * This method tries to "fail fast" so that it does not run down the
         * request timeout limit, when it's invoked in a request context.
         *
         * It implements this "fail fast" strategy by noting whether it
         * acquired the lock with contention. If it acquired the lock with
         * contention and the state is still unresolved, it means that the
         * immediately preceding attempt to repair it failed and there is no
         * reason to retry resolving it immediately, particularly since
         * resolving it may involved getting stuck in a connect open timeout
         * which would eat into the request timeout limit and prevent retries
         * at other nodes which may be available. The first thread that
         * acquires the lock uncontended will be the one that attempts to
         * resolve the state. If it fails, a retry at a higher level, either by
         * the RepNodeStateUpdateThread, or RequestDispatcherImpl, depending on
         * which thread gets there first, will resolve it.
         *
         * @param registryUtils used to resolve the remote reference
         *
         * @param timeoutMs the amount of time to be spent trying to resolve
         * the handler
         *
         * @return the handle or null if one could not be established due to a
         * timeout, a remote exception, a recently failed attempt by a
         * different thread, etc.
         */
        private RequestHandlerAPI resolve(RegistryUtils registryUtils,
                                          long timeoutMs) {

            {   /* Try getting it contention-free first. */
                RequestHandlerAPI refNonVolatile = requestHandler;
                if (refNonVolatile != null) {
                    return refNonVolatile;
                }
            }

            /**
             * First try to acquire an uncontended lock
             */
            boolean waited = false;
            try {
                if (!semaphore.tryAcquire()) {
                    /*
                     * If not, try waiting. The wait could be as long as the SO
                     * connect timeout for a node that was down, if some other
                     * thread was trying to resolve the reference and had the
                     * lock.
                     */
                    if (!semaphore.tryAcquire(timeoutMs,
                                              TimeUnit.MILLISECONDS)) {
                        /* Could not acquire after waiting. */
                        return null;
                    }
                    waited = true;
                }
            } catch (InterruptedException e) {
                throw new OperationFaultException("Unexpected interrupt", e);
            }

            if (waited) {
                /* Acquired semaphore after waiting. */
                boolean needsRepair = true;
                try {
                    needsRepair = needsRepair();
                    if (needsRepair) {
                        /*
                         * Don't bother retrying immediately, since another
                         * thread just failed. Retrying in this case may have
                         * an adverse impact in that it could run a request out
                         * of time, which could otherwise be used to retry a
                         * request at some other node.
                         */
                        return null;
                    }
                } finally {
                    if (needsRepair) {
                        /* Exiting method via return or exception. */
                        semaphore.release();
                    }
                }

                /* Semaphore acquired after waiting. */
            }

            /* Semaphore acquired by this thread. */
           try {
                if (requestHandler != null) {
                    return requestHandler;
                }

                try {
                    /*
                     * registryUtils can be null if the RN has not finished
                     * initialization.
                     */
                    if (registryUtils == null) {
                        return null;
                    }
                    requestHandler = registryUtils.getRequestHandler(rnId);

                    /*
                     * There is a possibility that the requested RN is not yet
                     * in the topology. In this case requestHandler will be
                     * null. This can happen during some elasticity operations.
                     */
                    if (requestHandler == null) {
                        noteExceptionInternal(
                              new IllegalArgumentException(rnId +
                                                           " not in topology"));
                        return null;
                    }
                    serialVersion = requestHandler.getSerialVersion();
                    exceptionSummary = null;
                    return requestHandler;
                } catch (RemoteException e) {
                    noteExceptionInternal(e);
                } catch (NotBoundException e) {
                    /*
                     * The service has not yet appeared in the registry, it may
                     * be down, or is in the process of coming up.
                     */
                    noteExceptionInternal(e);
                } catch (AuthenticationFailureException e) {
                    noteExceptionInternal(e);
                }

                return null;
            } finally {
                semaphore.release();
            }
        }

        @Override
        RequestHandlerAPI getSync(RegistryUtils registryUtils,
                                  long timeoutMs) {
            final RequestHandlerAPI refNonVolatile = requestHandler;

            if (refNonVolatile != null) {
                /* The fast path. NO synchronization. */
                return refNonVolatile;
            }

            /*
             * Never been resolved, resolve it.
             */
            if (exceptionSummary == null) {
                return resolve(registryUtils, timeoutMs);
            }

            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new OperationFaultException("Unexpected interrupt", e);
            }
            try {
                if (exceptionSummary == null) {
                    return null;
                }

                /*
                 * If last resolve failed because of
                 * AuthenticationFailureException, throw the exception here
                 * since it cannot be fixed automatically by the next
                 * resolution.
                 */
                final Exception lastException = exceptionSummary.getException();
                if (lastException instanceof AuthenticationFailureException) {
                    exceptionSummary = null;
                    throw (AuthenticationFailureException) lastException;
                }
            } finally {
                semaphore.release();
            }
            return null;
        }

        @Override
        void noteException(Exception e) {
            try {
                semaphore.acquire();
            } catch (InterruptedException ie) {
                throw new OperationFaultException("Unexpected interrupt", ie);
            }
            try {
                noteExceptionInternal(e);
            } finally {
                semaphore.release();
            }
        }

        /**
         * Assumes that the caller has already set the semaphore
         */
        private void noteExceptionInternal(Exception e) {
            assert semaphore.availablePermits() == 0;

            requestHandler = null;
            if (exceptionSummary == null) {
                exceptionSummary = new ExceptionSummary();
            }
            exceptionSummary.noteException(e);
        }
    }

    /**
     * Asynchronous implementation based on the dialog layer using
     * AsyncRequestHandlerAPI.
     */
    private class AsyncReqHandlerRef extends ReqHandlerRef {

        /*
         * Access to all fields must be protected by synchronization on this
         * instance.
         */

        /** The remote request handler for the RN. */
        private AsyncRequestHandlerAPI requestHandler;

        /**
         * Non-null if there is an error that resulted in the requestHandler
         * field being nulled.
         */
        private ExceptionSummary exceptionSummary;

        /** Whether a resolve operation is underway. */
        private boolean resolving;

        /** Handlers for results of resolve calls. */
        private final List<ResolveHandler> handlers =
            new ArrayList<ResolveHandler>();

        /**
         * Handle results of resolve calls, including delivering a null result
         * on timeout.
         */
        private class ResolveHandler implements Runnable {

            /*
             * Synchronize on the containing AsyncRequestHandlerAPI instance
             * when accessing these fields
             */

            /** The handler to receive the request handler, or null if done. */
            private ResultHandler<AsyncRequestHandlerAPI> handler;

            /** The future that is waiting for the timeout, or null if done. */
            private Future<?> future;

            /** Whether the result delivery is done. */
            private boolean done;

            ResolveHandler(ResultHandler<AsyncRequestHandlerAPI> handler,
                           ScheduledExecutorService executor,
                           long timeoutMs) {
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(
                        "AsyncReqHandlerRef.ResolveHandler create" +
                        " handler=" + handler + " timeoutMs=" + timeoutMs);
                }
                assert handler != null;
                final Future<?> f =
                    executor.schedule(this, timeoutMs, MILLISECONDS);
                boolean cancel = false;
                synchronized (AsyncReqHandlerRef.this) {

                    /*
                     * Check for done in case onResult was called (via the
                     * executor timeout processing) before we got to this
                     * point.
                     */
                    if (done) {
                        cancel = true;
                    } else {
                        this.handler = handler;
                        future = f;
                        handlers.add(this);
                    }
                }
                if (cancel) {

                    /* Should already be canceled, but just to be sure */
                    f.cancel(false);
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.finest(
                            "AsyncReqHandlerRef.ResolveHandler cancel" +
                            " handler=" + handler);
                    }
                }
            }

            /** Deliver null result on timeout. */
            @Override
            public void run() {
                onResult(null, null);
            }

            /** Deliver the request handler or exception to the handler. */
            void onResult(AsyncRequestHandlerAPI rh, Throwable e) {
                final ResultHandler<AsyncRequestHandlerAPI> h;
                final Future<?> f;
                synchronized (AsyncReqHandlerRef.this) {
                    if (done) {
                        return;
                    }
                    done = true;
                    h = handler;
                    f = future;
                    handler = null;
                    future = null;
                    handlers.remove(this);
                }
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(
                        "AsyncReqHandlerRef.ResolveHandler onResult" +
                        " handler=" + handler +
                        " rh=" + rh +
                        " e=" + e);
                }

                /*
                 * Check for nulls on the off chance that onResult was called
                 * (via the executor timeout processing) before the handler and
                 * future fields were set.
                 */
                if (h != null) {
                    h.onResult(rh, e);
                }
                if (f != null) {
                    f.cancel(false);
                }
            }
        }

        @Override
        synchronized void reset() {
            requestHandler = null;
            /* TODO: Set to SerialVersion.UNKNOWN to catch errors. */
            serialVersion = SerialVersion.CURRENT;
            exceptionSummary = null;
        }

        @Override
        synchronized boolean needsResolution() {
            return (requestHandler == null);
        }

        @Override
        synchronized boolean needsRepair() {
            return (exceptionSummary != null);
        }

        @Override
        void resolve(RegistryUtils registryUtils,
                     long timeoutMs,
                     final ResultHandler<Boolean> handler) {
            class ResolveResultHandler
                implements ResultHandler<AsyncRequestHandlerAPI> {
                @Override
                public void onResult(AsyncRequestHandlerAPI ref, Throwable e) {
                    handler.onResult(ref != null, e);
                }
            }
            resolveInternal(registryUtils, timeoutMs,
                            new ResolveResultHandler(), true);
        }

        @Override
        void getAsync(RegistryUtils registryUtils,
                      long timeoutMs,
                      ResultHandler<AsyncRequestHandlerAPI> handler) {
            resolveInternal(registryUtils, timeoutMs, handler, false);
        }

        /**
         * Implement resolution.  If tryResolve is false, then only attempt to
         * resolve the reference if we don't already have information about a
         * previous failure.
         */
        private void resolveInternal(
            RegistryUtils registryUtils,
            long timeoutMs,
            ResultHandler<AsyncRequestHandlerAPI> handler,
            boolean tryResolve) {

            final AsyncRequestHandlerAPI rh;
            Exception exception = null;
            synchronized (this) {
                rh = requestHandler;

                /* Check if we need to get a new request handler */
                if ((rh == null) &&
                    (registryUtils != null) &&
                    (tryResolve || !needsRepair())) {

                    /*
                     * If there is no request handler, registryUtils are
                     * available (they might not be if the RN has not finished
                     * initialization), and either we are trying to resolve or
                     * there is no exception recorded for a failed resolve,
                     * then get an executor and use it to create a handler to
                     * receive the result when it becomes available.
                     */
                    final ScheduledExecutorService execService =
                        AsyncRegistryUtils.getEndpointGroup()
                        .getSchedExecService();

                    /*
                     * Suppress unused warnings -- the handler is stored as a
                     * side effect
                     */
                    @SuppressWarnings("unused")
                    final ResolveHandler h =
                        new ResolveHandler(handler, execService, timeoutMs);

                    /* If a resolution attempt is not underway, start one */
                    if (!resolving) {
                        resolving = true;
                        getRequestHandler(registryUtils, timeoutMs);
                    }

                    /* Done -- the handler will get called back */
                    return;
                }

                /*
                 * If last resolve failed because of
                 * AuthenticationFailureException, throw that exception rather
                 * than just returning null since the problem won't be fixed
                 * automatically by the next resolution.
                 */
                if (exceptionSummary != null) {
                    final Exception lastEx = exceptionSummary.getException();
                    if (lastEx instanceof AuthenticationFailureException) {
                        exception = lastEx;
                        exceptionSummary = null;
                    }
                }
            }

            /* Return the current handler or exception */
            handler.onResult(rh, exception);
        }

        /** Make a remote request for the request handler. */
        private void getRequestHandler(final RegistryUtils registryUtils,
                                       final long timeoutMs) {
            AsyncRegistryUtils.getRequestHandler(
                registryUtils.getTopology(), rnId, trackerId, timeoutMs,
                logger,
                new ResultHandler<AsyncRequestHandlerAPI>() {
                    @Override
                    public void onResult(AsyncRequestHandlerAPI r,
                                         Throwable e) {
                        deliverResult(r, e);
                    }
                });
        }

        /**
         * Deliver the results of a remote call to obtain the request handler.
         * If a request handler was obtained successfully, supply it to all
         * waiting handlers.  If the request handler was not available, note
         * the exception and deliver the failure results immediately.  Retrying
         * in that case might have an adverse impact because it could run a
         * request out of time which could otherwise be used to retry the
         * request on another node.
         */
        private void deliverResult(AsyncRequestHandlerAPI rh, Throwable e) {
            synchronized (this) {
                assert resolving;
                requestHandler = rh;
                if (rh != null) {
                    serialVersion = rh.getSerialVersion();
                    exceptionSummary = null;
                } else if (e == null) {

                    /*
                     * There is a possibility that the requested RN is not yet
                     * in the topology. In this case requestHandler will be
                     * null. This can happen during some elasticity operations.
                     * Note the exception but return null so that the caller
                     * can retry.
                     */
                    noteException(new IllegalArgumentException(
                                      rnId + " not in topology"));
                } else if (e instanceof Exception) {
                    noteException((Exception) e);
                } else if (e instanceof Error) {
                    throw (Error) e;
                } else {
                    throw new AssertionError(e);
                }
            }

            /* Deliver the results */
            while (true) {
                final ResolveHandler handler;
                synchronized (this) {
                    if (handlers.isEmpty()) {
                        resolving = false;
                        break;
                    }
                    handler = handlers.remove(0);
                }
                handler.onResult(rh, e);
            }
        }

        @Override
        synchronized void noteException(Exception e) {
            requestHandler = null;
            if (exceptionSummary == null) {
                exceptionSummary = new ExceptionSummary();
            }
            exceptionSummary.noteException(e);
        }
    }

    /**
     * Tracks errors encountered when trying to create the reference.
     */
    private static class ExceptionSummary {

        /**
         * Track errors resulting from attempts to create a remote reference.
         * If the value is > 0 then exception must be non null and denotes the
         * last exception that resulted in the reference being unresolved.
         */
        private int errorCount = 0;
        private Exception exception = null;
        private long exceptionTimeMs = 0;

        private void noteException(Exception e) {
            exception = e;
            errorCount++;
            exceptionTimeMs = System.currentTimeMillis();
        }

        private Exception getException() {
            return exception;
        }

        @SuppressWarnings("unused")
        private long getExceptionTimeMs() {
            return exceptionTimeMs;
        }

        @SuppressWarnings("unused")
        private int getErrorCount() {
            return errorCount;
        }
    }

    /**
     * Returns true if the version based consistency requirements can be
     * satisfied at time <code>timeMs</code> based upon the historical rate of
     * progress associated with the node.
     *
     * @param timeMs the time at which the consistency requirement is to be
     * satisfied
     *
     * @param consistency the version consistency requirement
     */
    boolean inConsistencyRange(long timeMs, Consistency.Version consistency) {
        assert consistency != null;

        final VLSN consistencyVLSN =
            new VLSN(consistency.getVersion().getVLSN());

        return vlsnState.vlsnAt(timeMs).compareTo(consistencyVLSN) >= 0;
    }

    /**
     * Returns true if the time based consistency requirements can be satisfied
     * at time <code>timeMs</code> based upon the historical rate of progress
     * associated with the node.
     *
     * @param timeMs the time at which the consistency requirement is to be
     * satisfied
     *
     * @param consistency the time consistency requirement
     */
    boolean inConsistencyRange(long timeMs,
                               Consistency.Time consistency,
                               RepNodeState master) {
        assert consistency != null;

        if (master == null) {
            /*
             * No master information, assume it is. The information will get
             * updated as a result of the request directed to it.
             */
            return true;
        }

        final long lagMs =
                consistency.getPermissibleLag(TimeUnit.MILLISECONDS);
        final VLSN consistencyVLSN = master.vlsnState.vlsnAt(timeMs - lagMs);
        return vlsnState.vlsnAt(timeMs).compareTo(consistencyVLSN) >= 0;
    }

    /**
     * VLSNState encapsulates the handling of all VLSN state associated with
     * this node.
     * <p>
     * The VLSN state is typically updated based upon status
     * information returned in a {@link Response}. It's read during a request
     * dispatch for requests that specify a consistency requirement to help
     * direct the request to a node that's in a good position to satisfy the
     * constraint.
     * <p>
     * The state is also updated by {@link RepNodeStateUpdateThread} on a
     * periodic basis through the use of {@link NOP} requests.
     */
    private static class VLSNState {

        /**
         *  The last known vlsn
         */
        private volatile VLSN vlsn;

        /**
         * The time at which the vlsn above was last updated.
         */
        private long lastUpdateMs;

        /**
         * The starting vlsn used to compute the next iteration of the
         * vlsnsPerSec.
         *
         * Invariant: intervalStart <= vlsn
         */
        private VLSN intervalStart;

        /**
         *  The time at which the intervalStart was updated.
         *
         *  Invariant: intervalStartMs <= lastUpdateMs
         */
        private long intervalStartMs;

        /* The vlsn rate: vlsns/sec */
        private long vlsnsPerSec;

        private final int rateIntervalMs;

        private VLSNState(int rateIntervalMs) {
            vlsn = VLSN.NULL_VLSN;
            intervalStart = VLSN.NULL_VLSN;
            intervalStartMs = 0l;
            vlsnsPerSec = 0;
            this.rateIntervalMs = rateIntervalMs;
        }

        /**
         * Returns the current VLSN associated with this RN
         */
        private VLSN getVLSN() {
           return vlsn;
        }

        /**
         * Returns true if the state information has not been updated over an
         * interval exceeding MAX_RATE_INTERVAL_MS
         */
        private synchronized boolean isObsolete() {
            return (lastUpdateMs + rateIntervalMs)  <
                    System.currentTimeMillis();
        }

        /**
         * Updates the VLSN and maintains the vlsn rate associated with it.
         *
         * @param newVLSN the new VLSN
         */
        private synchronized void updateVLSN(VLSN newVLSN) {

            if ((newVLSN == null) || newVLSN.isNull()) {
                return;
            }

            lastUpdateMs = System.currentTimeMillis();
            if  (newVLSN.compareTo(vlsn) > 0) {
                vlsn = newVLSN;
            }

            final long intervalMs = (lastUpdateMs - intervalStartMs);
            if (intervalMs <= rateIntervalMs) {
                return;
            }

            if (intervalStartMs == 0) {
                resetRate(lastUpdateMs);
                return;
            }

            final long vlsnDelta = (vlsn.getSequence() -
                                    intervalStart.getSequence());
            if (vlsnDelta < 0) {
                /* Possible hard recovery */
                resetRate(lastUpdateMs);
            } else {
                intervalStart = vlsn;
                intervalStartMs = lastUpdateMs;
                vlsnsPerSec = (vlsnDelta * 1000) / intervalMs;
            }
        }

        /**
         * Returns the VLSN that the RN will have progressed to at
         * <code>timeMs</code> based upon its current known state and its rate
         * of progress.
         *
         * @param timeMs the time in ms
         *
         * @return the predicted VLSN at timeMs or NULL_VLSN if the state has
         * not yet been initialized.
         */
        private synchronized VLSN vlsnAt(long timeMs) {
            if (vlsn.isNull()) {
                return VLSN.NULL_VLSN;
            }

            final long deltaMs = (timeMs - lastUpdateMs);
            /* deltaMs can be negative. */

            final long vlsnAt = vlsn.getSequence() +
                            ((deltaMs * vlsnsPerSec) / 1000);

            /*
             * Return the zero VLSN if the extrapolation yields a negative
             * value since vlsns cannot be negative.
             */
            return vlsnAt < 0 ? new VLSN(0) : new VLSN(vlsnAt);
        }

        private void resetRate(long now) {
            intervalStart = vlsn;
            intervalStartMs = now;
            vlsnsPerSec = 0;
        }
    }
}

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

package oracle.kv.impl.pubsub;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.CheckpointFailureException;
import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.stats.SubscriptionMetrics;

import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.VLSN;

/**
 * Object represents an implementation of NoSQLSubscription interface. After
 * a subscription is created by publisher, the publisher will return a handle
 * to the instance of this object, which can be subsequently retrieved by user
 * to manipulate the subscription, e.g., start and cancel streaming, do
 * checkpoint, etc.
 */
public class NoSQLSubscriptionImpl implements NoSQLSubscription {

    /* time limit to dump trace if no msg can be dequeued from publisher */
    private final static long TRACE_IDLE_TIME_SECS = 30 * 60;

    /* interval to update current stream position */
    private final static long UPDATE_STREAM_POSITION_INTERVAL_MS = 100;

    /* subscription worker thread prefix */
    private final static String workerThreadPrefix = "SubscriptionWorkerThread";

    /* parent publishing unit creating this subscription */
    private final PublishingUnit parentPU;
    /* subscriber that uses this subscription */
    private final NoSQLSubscriber subscriber;
    /* private logger */
    private final Logger logger;
    /* a FIFO queue from which to request stream operations */
    private final BlockingQueue<? extends StreamOperation> queue;
    /* init stream position */
    private final StreamPosition initPos;
    /* true if this subscription is shut down */
    /* true if subscription cancelled, access needs be synchronized */
    private boolean canceled;

    /* to sync worker thread */
    private final Object workerLock = new Object();
    /*
     * Worker thread to dequeue the stream operations.  Synchronized on
     * workerLock when accessing this field.
     */
    private SubscriptionWorkerThread subscriptionWorkerThread;
    /*
     * # of ops to stream. Per Rule Subscription.17, a demand equal to
     * java.lang.Long.MAX_VALUE is considered as "effectively unbounded".
     * Synchronized on workerLock when accessing this field.
     */
    private long numToStreamInThread;

    /* current stream position */
    private volatile StreamPosition currStreamPos;
    /* worker thread to checkpoint */
    private volatile StreamCkptWorkerThread streamCkptWorkerThread;

    /*
     * # of ops consumed by subscriber in the life time of this subscription,
     * operations may be streamed by multiple, sequentially running worker
     * threads.
     */
    private volatile long numStreamedOps;

    /*
     * true if enforce exact current stream position, in which the
     * current stream position will be updated for each incoming
     * operation; false if the current stream position is updated
     * periodically for efficiency. Its default is true;
     */
    private boolean exactCurrStrPos;

    /*-- For test and internal use only. ---*/
    /* true if enable checkpoint in subscription, default is true */
    private final boolean enableCheckpoint;

    /* Creates and initiates a subscription instance */
    private NoSQLSubscriptionImpl(PublishingUnit parentPU,
                                  NoSQLSubscriber subscriber,
                                  StreamPosition initPos,
                                  boolean enableCheckpoint,
                                  Logger logger)
        throws SubscriptionFailureException {

        /* cannot be null, null position shall have been already converted */
        if (initPos == null) {
            throw new SubscriptionFailureException(subscriber
                                                       .getSubscriptionConfig()
                                                       .getSubscriberId(),
                                                   "Cannot create " +
                                                   "subscription instance " +
                                                   "with null initial " +
                                                   "position");
        }

        this.parentPU = parentPU;
        this.subscriber = subscriber;
        this.initPos = initPos;
        this.enableCheckpoint = enableCheckpoint;
        this.logger = logger;

        queue = parentPU.getOutputQueue();
        canceled = false;
        currStreamPos = initPos;
        subscriptionWorkerThread = null;
        streamCkptWorkerThread = null;
        exactCurrStrPos = true;
        numStreamedOps = 0;
    }

    /**
     * Returns a handle to the NoSQLSubscription
     *
     * @param parentPU    parent publishing unit
     * @param subscriber  NoSQL subscriber that uses the subscription
     * @param startPos    start stream position
     * @param logger      private logger
     *
     * @return an instance of the NoSQL subscription
     *
     * @throws SubscriptionFailureException if fail to create a subscription
     */
    static NoSQLSubscriptionImpl get(PublishingUnit parentPU,
                                     NoSQLSubscriber subscriber,
                                     StreamPosition startPos,
                                     boolean enableCheckpoint,
                                     Logger logger)
        throws SubscriptionFailureException {

        return new NoSQLSubscriptionImpl(parentPU, subscriber, startPos,
                                         enableCheckpoint, logger);
    }

    /**
     * Returns the index associated with the subscriber
     *
     * @return the index associated with the subscriber
     */
    @Override
    public NoSQLSubscriberId getSubscriberId() {
        return subscriber.getSubscriptionConfig().getSubscriberId();
    }

    /**
     * Returns the instantaneous position in the stream. All elements up to
     * and including this position have been delivered to the Subscriber via
     * Subscriber.onNext().
     *
     * This position can be used by a subsequent subscriber to resume the
     * stream from this point forwards, effectively resuming an earlier
     * subscription.
     *
     * To make it efficient, current stream position will be updated at a fixed
     * time interval, instead of for each incoming operation.
     */
    @Override
    public StreamPosition getCurrentPosition() {
        return new StreamPosition(currStreamPos);
    }

    /**
     * Does subscription checkpoint. The checkpoint will be made to the source
     * NoSQL DB. Each subscription has its own checkpoints stored in a
     * particular table. The table is created and populated the first time the
     * checkpoint is made, and updated when subscription makes each subsequent
     * checkpoints.
     *
     * @param streamPosition the stream position to checkpoint
     */
    @Override
    public synchronized void doCheckpoint(StreamPosition streamPosition) {

        if (!enableCheckpoint) {
            /* unit test only */
            logger.warning(lm("Checkpoint disabled"));
            return;
        }

        if (isCanceled()) {
            logger.warning(lm("Subscription already canceled, no more " +
                              "checkpoint."));
            return;
        }

        /* no concurrent checkpoint threads for a subscription at any time */
        if(streamCkptWorkerThread != null &&
           streamCkptWorkerThread.isAlive()) {
            final NoSQLSubscriberId sid = subscriber.getSubscriptionConfig()
                                                    .getSubscriberId();
            final String err = "Cannot do checkpoint because there " +
                               "is a concurrently running checkpoint " +
                               "for subscriber " + sid;
            final Throwable cause =
                new CheckpointFailureException(sid, null, err, null);

            logger.warning(lm(err));
            subscriber.onCheckpointComplete(streamPosition, cause);
        }

        streamCkptWorkerThread = new StreamCkptWorkerThread(streamPosition);
        streamCkptWorkerThread.start();
    }

    /**
     * Starts stream a given number of operations from publisher. It spawns a
     * thread running to dequeue the operations from publisher, and apply the
     * subscriber-defined callbacks on each operation.
     *
     * Per Rule Subscription.16, Subscription.request MUST return normally.
     * The only legal way to signal failure to a Subscriber is via the
     * onError method.
     *
     * @param n  number of operations to stream.
     */
    @Override
    public synchronized void request(long n) {

        if (isCanceled()) {
            /*
             * Per Rule Subscription.6, after the Subscription is canceled,
             * additional Subscription.request(long n) MUST be NOPs.
             */
            logger.info(lm("Subscription has already been canceled, NOP."));
            return;
        }

        if (n <= 0) {
            /*
             * Per Rule Subscription.9, while the Subscription is not
             * canceled, Subscription.request(long n) MUST signal onError
             * with a java.lang.IllegalArgumentException if the argument is <=
             * 0. The cause message MUST include a reference to this rule
             * and/or quote the full rule.
             */
            final IllegalArgumentException err =
                new IllegalArgumentException("Per Rule Subscription.9 in " +
                                             "reactive stream spec, the " +
                                             "argument of Subscription" +
                                             ".request cannot be less than or" +
                                             " equal to 0.");
            subscriberOnError(err);
            return;
        }

        /*
         * About recursive request call
         *
         * Per Rule Subscription.3 in reactive streams spec, Subscription
         * .request MUST place an upper bound on possible synchronous
         * recursion between Publisher and Subscriber. But recursion can only
         * happen if a call to request can produce a synchronous call to
         * onNext, which doesn't apply in our implementation because an async
         * subscriptionWorkerThread is created in which all onNext will be
         * called from there. Therefore this rule does not apply to us and we
         * don't need the recursion check at all.
         */

        /*
         * Synchronized with existing worker thread clean up if any
         */
        synchronized (workerLock) {
            /* no running worker, safe to create a new one */
            if (subscriptionWorkerThread == null) {
                numToStreamInThread = n;
                subscriptionWorkerThread = new SubscriptionWorkerThread();
                subscriptionWorkerThread.start();
                logger.info(lm("A new worker thread " +
                               subscriptionWorkerThread.getName() +
                               " has been created to stream " +
                               (n == Long.MAX_VALUE ? "infinite" : n) +
                               " ops," +
                               " # ops already streamed in subscription " +
                               numStreamedOps));
            } else {
                /* there is a running worker */
                numToStreamInThread += n;
                if (numToStreamInThread < 0) {
                    /* handle overflow */
                    numToStreamInThread = Long.MAX_VALUE;
                }
                logger.info(lm("Worker adds " +
                               (n == Long.MAX_VALUE ? "infinite" : n) +
                               " ops to stream, new " +
                               "total is " + numToStreamInThread +
                               ", # ops already streamed in subscription " +
                               numStreamedOps));
            }
        }
    }

    /**
     * User requires to cancel an ongoing subscription. The subscription is
     * canceled and publisher will clean up and free resources. Terminates
     * shard streams and checkpoint threads, clear queues, and close handle
     * to source kvstore etc. The publisher will also signal onComplete to
     * subscriber after the subscription is canceled.
     *
     * Reactive stream spec requires Subscription.cancel return in a timely
     * manner, and be idempotent and be thread-safe. Also, per Rule
     * Subscription.15, Subscription.cancel MUST return normally. The only
     * legal way to signal failure to a Subscriber is via the onError method.
     */
    @Override
    public void cancel() {

       /* user requires to cancel the subscription */
       cancel(null);
    }

    /**
     * Returns true if the subscription has been shut down.
     *
     * @return  true if the subscription has been shut down, false otherwise.
     */
    @Override
    public synchronized boolean isCanceled() {
        return canceled;
    }

    /**
     * Gets the last checkpoint stored in kv store for the given subscription
     *
     * @return the last checkpoint associated with that subscription, or null
     * if this subscription does not have any persisted checkpoint in kv store.
     */
    @Override
    public StreamPosition getLastCheckpoint() {
        return parentPU.getLastCheckpoint();
    }

    /**
     * Returns the subscription metrics
     *
     * @return the subscription metrics
     */
    @Override
    public SubscriptionMetrics getSubscriptionMetrics() {
        return parentPU.getStatistics();
    }

    /**
     * For test use only
     *
     * @return id of shards this subscription covers
     */
    public Set<RepGroupId> getCoveredShards() {
        return parentPU.getCoveredShards();
    }

    /**
     * Gets init stream position from where streaming begins.
     *
     * @return init stream position
     */
    StreamPosition getInitPos() {
        return initPos;
    }

    /**
     * For test use only
     *
     * @return subscriber
     */
    NoSQLSubscriber getSubscriber() {
        return subscriber;
    }

    /**
     * Clean up and free resources. Terminates shard streams and checkpoint
     * threads, clear queues, and close handle to source kvstore etc.
     *
     * @param cause     cause of subscription is canceled, null if shutdown
     *                  normally without error.
     */
    synchronized void cancel(Throwable cause) {

        /*
         * Cancel subscription needs be sync with request and checkpoint since
         * they both check if subscription has been cancelled
         */

         /* avoid multiple simultaneous cancels */
        if (canceled) {
            /*
             * Per Rule Subscription.7, after the Subscription is canceled,
             * additional Subscription.cancel() MUST be NOPs.
             */
            return;
        }
        canceled = true;

        /*
         * first try soft shut down and wait till worker thread exit, if fail
         * to soft shutdown, interruption will be signaled to make thread
         * exit. In either case, subscriptionWorkerThread wont be modified at
         * the same time by caller and thread itself, hence no need to put in
         * lock.
         */
        shutDownWorkerThread(subscriptionWorkerThread);
        subscriptionWorkerThread = null;

        shutDownWorkerThread(streamCkptWorkerThread);
        streamCkptWorkerThread = null;

        /*
         * Close parent PU because per Rule Subscription.12, Subscription
         * .cancel() MUST request Publisher to eventually stop signaling its
         * Subscriber.
         */
        parentPU.close(cause);

        logger.info(lm("Subscription canceled at stream position " +
                       currStreamPos));
    }

    long getNumStreamedOps() {
        return numStreamedOps;
    }

    /**
     * Gets current time stamp in ms
     *
     * @return current time stamp in ms
     */
    static long getCurrTimeMs() {
         // TODO: consider using nanoTime if it becomes a performance issue
        return System.currentTimeMillis();
    }

    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private String lm(String msg) {
        return "[SI-" +
               (parentPU == null || parentPU.getCkptTableManager() == null ?
                   "<na>" : parentPU.getCkptTableManager().getCkptTableName()) +
               "-" + subscriber.getSubscriptionConfig().getSubscriberId()
               + "] " + msg;
    }

    /* shut down worker thread */
    private void shutDownWorkerThread(StoppableThread thread) {
        if (thread == null || !thread.isAlive() || thread.isShutdown()) {
            return;
        }
        thread.shutdownThread(logger);
        logger.info(lm("Thread " + thread.getName() + "(id " + thread.getId() +
                       ") shut down."));
    }

    /* update the current stream position */
    private synchronized long updateStreamPos(StreamOperation op) {
        final int shardId = op.getRepGroupId();
        final StreamSequenceId seq = (StreamSequenceId)op.getSequenceId();
        final VLSN vlsn = new VLSN(seq.getSequence());
        final StreamPosition.ShardPosition currPos =
            currStreamPos.getShardPosition(shardId);

        currPos.setVlsn(vlsn);
        return getCurrTimeMs();
    }

    /* rebalance multiple subscribers if necessary */
    private void rebalance() {
        /*
         * TODO: check ckpt rows of other shards to see any elastic
         * operations happened in kvstore, rebalance if necessary
         */
    }

    /* private worker thread to dequeue and process messages */
    private class SubscriptionWorkerThread extends StoppableThread {

        /* stats report interval in terms of # of ops */
        private final static int REPORT_INTV = 1024;
        /* # ops streamed in this worker thread */
        private volatile long numStreamedInThread;

        SubscriptionWorkerThread() {
            super(workerThreadPrefix + "-" +
                  subscriber.getSubscriptionConfig().getSubscriberId() +
                  "-" + parentPU.getCkptTableManager().getCkptTableName());
            numStreamedInThread = 0;
        }

        @Override
        public void run() {
            logger.info(lm("Subscription worker thread " + getName() +
                           " starts dequeue " +
                           (numToStreamInThread == Long.MAX_VALUE ?
                               "infinite" : numToStreamInThread) +
                           " ops."));
            /* last time to dequeue a msg */
            long lastMsgTimeMs= 0;
            /* last time to update current position */
            long lastUpdPosTimeMs = 0;
            /* # ops last reported */
            long lastReported = 0;
            try {
                while (true) {
                    /*
                     * Loop exits normally. Protected in lock to prevent user
                     * from adding more entries to stream after worker leaves
                     * the loop.
                     */
                    synchronized (workerLock) {
                        if (isShutdown() ||
                            (numStreamedInThread >= numToStreamInThread)) {
                            logger.fine(lm("Start exit worker thread,  " +
                                           "forced shutdown? " + isShutdown() +
                                           ", # ops streamed " +
                                           numStreamedInThread +
                                           ", # ops to stream " +
                                           (numToStreamInThread ==
                                            Long.MAX_VALUE ? " infinite " :
                                               numToStreamInThread)));

                            exitThread();
                            subscriptionWorkerThread = null;
                            break;
                        }
                    }

                    final Object obj =
                        queue.poll(PublishingUnit.OUTPUT_QUEUE_TIMEOUT_MS,
                                   TimeUnit.MILLISECONDS);

                    /* timeout, sleep and retry */
                    if (obj == null) {

                        /*
                         * If there is no write in kvstore, subscription may
                         * be idle and unable to dequeue anything from the
                         * publisher for a long time, dump trace to show it
                         * is still alive.
                         */
                        final long idle =
                            (getCurrTimeMs() - lastMsgTimeMs) / 1000;
                        /* dump idle msg */
                        if (lastMsgTimeMs > 0 && idle > TRACE_IDLE_TIME_SECS) {
                            logger.fine(lm("Idle for " + idle + " seconds."));
                        }
                        continue;
                    }

                    /* get an obj from queue, reset */
                    lastMsgTimeMs = getCurrTimeMs();

                    /* get an error from Publisher */
                    if (obj instanceof Throwable) {
                        subscriberOnError((Throwable)obj);
                        break;
                    }


                    /*
                     * User may cancel the subscription in onNext, thus we
                     * need to update the stats before signal subscriber.
                     */

                    /* must be a regular stream operation, update stats */
                    final StreamOperation operation = (StreamOperation) obj;
                    if (exactCurrStrPos || lastUpdPosTimeMs == 0 ||
                        (getCurrTimeMs() - lastUpdPosTimeMs) >
                        UPDATE_STREAM_POSITION_INTERVAL_MS) {
                        lastUpdPosTimeMs = updateStreamPos(operation);
                    }

                    /*
                     * update local and global counter
                     */
                    numStreamedInThread++;
                    numStreamedOps++;

                    /* ask pu to collect stats periodically */
                    if (parentPU != null &&
                        (numStreamedInThread - lastReported) >= REPORT_INTV) {
                        parentPU.doStatCollection();
                        lastReported = numStreamedInThread;
                    }

                    /* finally signal subscriber */
                    subscriberOnNext(subscriber, operation);
                }
            } catch (InterruptedException ie) {
                final String err ="Unable to dequeue due to interruption";
                logger.warning(err);
                final SubscriptionFailureException sfe =
                    new SubscriptionFailureException(
                        subscriber.getSubscriptionConfig().getSubscriberId(),
                        err);

                /* notify subscriber */
                subscriberOnError(sfe);
            }
        }

        private void exitThread() {

            assert Thread.holdsLock(workerLock);

            /* ask pu to collect stats before exits */
            if (parentPU != null) {
                parentPU.doStatCollection();
            }

            if (numStreamedInThread < numToStreamInThread) {

                /*
                 * unable to stream all required entries because it is
                 * cancelled in the middle of subscription
                 */
                logger.warning("Worker thread exits due to cancellation, ops " +
                               "streamed by worker: " + numStreamedInThread +
                               " while requested: " +
                               (numToStreamInThread == Long.MAX_VALUE ?
                                   "infinite" : numToStreamInThread));

                if (queue != null && !queue.isEmpty()) {
                    logger.fine(lm("# unconsumed messages in queue: " +
                                   queue.size() +
                                   ", remaining messages: \n" +
                                   Arrays.toString(queue.toArray())));
                }
            } else {
                logger.info(lm("Worker thread exits after streaming all " +
                               "requested " +
                               (numToStreamInThread == Long.MAX_VALUE ?
                                   "infinite" : numToStreamInThread) +
                               " ops, "));
            }

            logger.info(lm("Worker thread has done streaming with final " +
                           "position " + currStreamPos +
                           ", total # streamed ops in the subscription:" +
                           numStreamedOps));
        }

        @Override
        protected int initiateSoftShutdown() {
            /*
             * when shutdown by StoppableThread.shutdownThread(), wait a bit to
             * let thread itself detect shutdown flag and exit neatly in soft
             * shutdown.
             */
            final boolean alreadySet = shutdownDone(logger);
            logger.fine(lm("Signal worker thread to shutdown, " +
                           "shutdown already signalled? " + alreadySet +
                           ", wait for " +
                           PublishingUnit.OUTPUT_QUEUE_TIMEOUT_MS +
                           " ms to let it exit"));
            return PublishingUnit.OUTPUT_QUEUE_TIMEOUT_MS;
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    /* private worker thread to checkpoint */
    private class StreamCkptWorkerThread extends StoppableThread {

        private final StreamPosition pos;

        StreamCkptWorkerThread(StreamPosition pos) {
            super("StreamCkptWorkerThread-" + 
                  subscriber.getSubscriptionConfig().getSubscriberId());

            this.pos = pos;
        }

        @Override
        public void run() {

            parentPU.getCkptTableManager().updateCkptTableInTxn(pos);

            /* rebalance subscribers if necessary */
            rebalance();

            /* finally let subscriber know the result */
            subscriber.onCheckpointComplete(pos, null);
        }

        @Override
        protected int initiateSoftShutdown() {
            final boolean alreadySet = shutdownDone(logger);
            logger.fine(lm("Signal checkpoint worker to shutdown, " +
                           "shutdown already signalled? " + alreadySet +
                           ", wait for " +
                           CheckpointTableManager.CKPT_TIMEOUT_MS +
                           " ms to let it exit"));
            return CheckpointTableManager.CKPT_TIMEOUT_MS;
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    /* utility function call subscriber onError */
    private void subscriberOnError(Throwable err) {

        /*
         * Per Rule Subscription.6, If a Publisher signals either onError
         * or onComplete on a Subscriber, that Subscriber's Subscription
         * MUST be considered canceled.
         */
        /* final stat collection */
        if (parentPU != null) {
            parentPU.doStatCollection();
        }

        /* pu will signal onError to subscriber */
        cancel(err);
    }

    /* utility function call subscriber onNext */
    private void subscriberOnNext(NoSQLSubscriber s, StreamOperation op) {
        try {
            s.onNext(op);
        } catch (Exception exp) {

            final String opStr = (op instanceof StreamOperation.PutEvent) ?
                op.asPut().getRow().toJsonString(true) :
                op.asDelete().getPrimaryKey().toJsonString(true);

            /*
             * Looks there is no rule in reactive stream spec that when
             * non-normal returns from onNext, we shall cancel the
             * subscription or not. For safety, we notify subscriber and
             * cancel the subscription, and user need to fix her onNext in
             * order to subscribe streams.
             */
            final String err = "Exception in onNext for " + opStr;
            final SubscriptionFailureException sfe =
                new SubscriptionFailureException(
                    subscriber.getSubscriptionConfig().getSubscriberId(),
                    err, exp);
            subscriberOnError(sfe);

            logger.warning(lm("Cancel subscription stream because of " +
                              "exception in executing subscriber onNext for " +
                              "stream operation " + opStr +
                              exp.getMessage() + "\n" +
                              LoggerUtils.getStackTrace(exp)));
        }
    }
}

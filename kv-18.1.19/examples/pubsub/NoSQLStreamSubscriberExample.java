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

package pubsub;

import java.util.logging.Logger;

import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;

import org.reactivestreams.Subscription;

/**
 * Example subscriber used in NoSQLStreamExample
 */
class NoSQLStreamSubscriberExample implements NoSQLSubscriber {

    /* checkpoint timeout in example */
    private final static long CHECKPOINT_TIMEOUT_MS = 60 * 1000;

    /* subscription configuration */
    private final NoSQLSubscriptionConfig config;

    /* number of operations to stream */
    private final int numOps;

    /* checkpoint interval in number of ops */
    private final long ckptInv;

    /* true if checkpoint successful */
    private volatile boolean ckptSucc;

    /* number of streamed ops */
    private long streamOps;

    /* number of interesting ops passing filtering */
    private long processOps;

    private NoSQLSubscription subscription;

    private boolean isSubscribeSucc;

    private Throwable causeOfFailure;

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    NoSQLStreamSubscriberExample(NoSQLSubscriptionConfig config,
                                 int numOps, long ckptIntv) {
        this.config = config;
        this.numOps = numOps;
        this.ckptInv = ckptIntv;

        causeOfFailure = null;
        isSubscribeSucc = false;
        streamOps = 0;
        subscription = null;
    }

    @Override
    public NoSQLSubscriptionConfig getSubscriptionConfig() {
        return config;
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscription = (NoSQLSubscription) s;
        subscription.request(numOps);
        isSubscribeSucc = true;
    }

    @Override
    public void onError(Throwable t) {
        causeOfFailure = t;
        logger.severe("Error: " + t.getMessage());
    }

    @Override
    public void onComplete() {
        /* shall be no-op */
    }

    @Override
    public void onWarn(Throwable t) {
        logger.warning("Warning: " + t.getMessage());
    }

    /* called when publisher finishes a checkpoint */
    @Override
    public void onCheckpointComplete(StreamPosition pos, Throwable cause) {
        if (cause == null) {
            ckptSucc = true;
            logger.info("Finish checkpoint at position " + pos);
        } else {
            ckptSucc = false;
            logger.warning("Fail to checkpoint at position " + pos +
                           ", cause: " + cause.getMessage());
        }
    }

    @Override
    public void onNext(StreamOperation t) {

        /*
         * Perform one of a few possible functions on each stream operation,
         * other than the default, user can select value of choice from [0,
         * 1, 2] to try one of these functions.
         */
        final int choice = -1;

        switch (choice) {
            case 0:
                onNext_count(t);
                break;
            case 1:
                onNext_filter1(t);
                break;
            case 2:
                onNext_filter2(t);
                break;

            default:
                onNext_default(t);
        }
    }

    /*
     * Just print every received stream operation on screen, conduct
     * checkpoint if necessary.
     */
    private void onNext_default(StreamOperation t) {
        switch (t.getType()) {
            case PUT:
            case DELETE:
                streamOps++;
                processOps++;

                System.out.println(t);
                doCheckpoint();

                if (streamOps == numOps) {
                    getSubscription().cancel();
                    logger.fine("Subscription cancelled after receiving " +
                                numOps + " ops.");
                }
                break;

            default:
                throw new IllegalStateException("Receive unsupported stream " +
                                                "operation from shard " +
                                                t.getRepGroupId() +
                                                ", seq: " + t.getSequenceId());
        }
    }

    /*
     * Just count without any real processing of each received operation.
     * Cancel the subscription after receiving all expected operations. It can
     * be used to measure throughput performance.
     */
    private void onNext_count(StreamOperation t) {
        switch (t.getType()) {
            case PUT:
            case DELETE:
                streamOps++;
                processOps++;
                if (streamOps <= numOps && streamOps % 100000 == 0) {
                    System.out.println((streamOps / 1000) +
                                       "k ops have been streamed...");
                }

                doCheckpoint();
                if (streamOps == numOps) {
                    getSubscription().cancel();
                    logger.fine("Subscription cancelled after receiving " +
                                numOps + " ops.");
                }

                break;

            default:
                throw new IllegalStateException("Receive unsupported stream " +
                                                "operation from shard " +
                                                t.getRepGroupId() +
                                                ", seq: " + t.getSequenceId());
        }
    }

    private void doCheckpoint() {

        if (ckptInv == 0) {
            return;
        }

        if((streamOps > 0) && (streamOps % ckptInv == 0)) {
            final long start = System.currentTimeMillis();
            final StreamPosition ckptPos = subscription.getCurrentPosition();
            subscription.doCheckpoint(ckptPos);
            /* wait until checkpoint done */
            while (!ckptSucc && !isCkptTimeout(start)) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Interrupted during " +
                                                    "checkpoint " + ckptPos);
                }
            }

            if (ckptSucc) {
                System.out.println("\nCheckpoint succeeded after " + streamOps +
                                   " operations at position " + ckptPos +
                                   ", elapsed time in ms " +
                                   (System.currentTimeMillis() - start));
                /* reset for next checkpoint */
                ckptSucc = false;
            } else {
                System.out.println("\nCheckpoint timeout " +
                                   "at position " + ckptPos +
                                   ", elapsed time in ms " +
                                   (System.currentTimeMillis() - start));
            }
        }
    }

    private boolean isCkptTimeout(long start) {
        return (System.currentTimeMillis() - start) > CHECKPOINT_TIMEOUT_MS;
    }

    /**
     * A simple filter that selects users from particular states
     */
    private void onNext_filter1(StreamOperation t) {
        switch (t.getType()) {
            case PUT:
                streamOps++;

                final StreamOperation.PutEvent putEvent = t.asPut();
                final String state = putEvent.getRow()
                                             .get("state")
                                             .asString()
                                             .get();
                if (streamOps <= numOps && (state.equals("NY") ||
                                            state.equals("MA"))) {
                    System.out.println(putEvent.getRow().toJsonString(true));
                    processOps++;
                }

                if (streamOps == numOps) {
                    getSubscription().cancel();
                    logger.fine("subscription canceled after receiving all " +
                                numOps + " ops");
                }

                break;
            case DELETE:
                streamOps++;
                break;

            default:
                throw new IllegalStateException("Receive unsupported stream " +
                                                "operation from shard " +
                                                t.getRepGroupId() +
                                                ", seq: " + t.getSequenceId());
        }
    }

    /**
     * Another simple filter that selects users such that 1) its userID is
     * within a range and 2) its userID ends with 0.
     */
    private void onNext_filter2(StreamOperation t) {
        switch (t.getType()) {
            case PUT:
                final int id = t.asPut().getRow().get("userID").asInteger()
                                .get();
                if(streamOps <= numOps &&
                   id >= 10 && id <= 100 && id % 10 == 0) {
                    System.out.println(t.asPut().getRow().toJsonString(true));
                    processOps++;
                }

                if (streamOps == numOps) {
                    getSubscription().cancel();
                    logger.fine("subscription canceled after receiving all " +
                                numOps + "ops");
                }

                streamOps++;
                break;
            case DELETE:
                streamOps++;
                break;

            default:
                throw new IllegalStateException("Receive unsupported stream " +
                                                "operation from shard " +
                                                t.getRepGroupId() +
                                                ", seq: " + t.getSequenceId());
        }
    }

    String getCauseOfFailure() {
        if (causeOfFailure == null) {
            return "success";
        }
        return causeOfFailure.getMessage();
    }

    boolean isSubscriptionSucc() {
        return isSubscribeSucc;
    }

    long getStreamOps() {
        return streamOps;
    }

    long getProcessOps() {
        return processOps;
    }

    NoSQLSubscription getSubscription() {
        return subscription;
    }
}

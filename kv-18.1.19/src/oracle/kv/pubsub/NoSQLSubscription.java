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

package oracle.kv.pubsub;

import oracle.kv.stats.SubscriptionMetrics;

import org.reactivestreams.Subscription;

/**
 * A NoSQL subscription to the source kvstore. It is created by an instance of
 * NoSQLPublisher with given subscription configuration.
 * <p>
 * A NoSQL subscription has real resources (threads, network connections,
 * iterator state etc.) associated with it. These resources are released when
 * the subscription is canceled via {@link #cancel}.
 */
public interface NoSQLSubscription extends Subscription {

    /* override original reactive stream subscription interface */

    /**
     * Streams a fixed number operations from source kvstore interested to
     * the subscribed tables.
     */
    @Override
    void request(long n);

    /**
     * Clean up and free resources; terminates shard streams in particular.
     */
    @Override
    void cancel();

    /* additional interface functions introduced by NoSQLSubscription */

    /**
     * Returns ID of the subscriber that created the subscription.
     *
     * @return ID of the subscriber that created the subscription
     */
    NoSQLSubscriberId getSubscriberId();

    /**
     * Returns a current position in the stream. All elements up to and
     * including this position have been delivered to the Subscriber via
     * {@link NoSQLSubscriber#onNext}.
     *
     * <p>This position can be used by a subsequent subscriber to resume the
     * stream from this point forwards, effectively resuming an earlier
     * subscription.
     *
     * @return current stream position
     */
    StreamPosition getCurrentPosition();

    /**
     * Gets the last checkpoint stored in kv store for the given subscription
     *
     * @return the last checkpoint associated with that subscription, or null
     * if this subscription does not have any persisted checkpoint in kvstore.
     */
    StreamPosition getLastCheckpoint();

    /**
     * Returns true if the subscription has been canceled.
     *
     * @return  true if the subscription has been canceled, false otherwise.
     */
    boolean isCanceled();

    /**
     * Do subscription checkpoint. The checkpoint will be made to the source
     * kvstore. The checkpoint from a subscription is stored in a table
     * dedicated to this particular subscription. Each row in the table
     * represents a checkpoint for a shard. Each row is inserted when the
     * checkpoint is made for the first time, and updated when subsequent
     * checkpoint is made.
     * <p>
     * Note the checkpoint is an asynchronous call. When called, it creates a
     * separate thread to do the check to kvstore, and it itself instantly
     * returns to caller. The result of checkpoint will be signaled to
     * subscriber via {@link NoSQLSubscriber#onCheckpointComplete}.
     * <p>
     * It is illegal to call this method concurrently for a subscription. The
     * method should be called only after
     * {@link NoSQLSubscriber#onCheckpointComplete} is called in the previous
     * call of this method, which indicates the previous checkpoint is done.
     * Otherwise {@link SubscriptionFailureException} will be raised.
     *
     * @param streamPosition the stream position to checkpoint
     */
    void doCheckpoint(StreamPosition streamPosition);

    /**
     * Returns the subscription metrics {@link SubscriptionMetrics}
     *
     * @return the subscription metrics
     */
    SubscriptionMetrics getSubscriptionMetrics();
}

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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * The subscriber interface is to be implemented by the application. The
 * NoSQLSubscriber interface defines additional methods that are described
 * below along with details of NoSQL-specific considerations for the existing
 * methods.
 * <p>
 * Implementation of NOSQLSubscriber should follow the Subscriber rules in the
 * <a href="https://github.com/reactive-streams/reactive-streams-jvm/tree/v1.0.0#specification">Reactive
 * Streams Specification</a>.
 */
public interface NoSQLSubscriber extends Subscriber<StreamOperation> {

    /* override the reactive stream subscription interface */

    /**
     * Invoked after the {@link NoSQLPublisher} has successfully established
     * contact with the store using helper hosts. When this method is called by
     * NoSQLPublisher, the argument is an implementation of {@link
     * NoSQLSubscription}.
     *
     * @see NoSQLSubscription#request
     */
    @Override
    void onSubscribe(Subscription s);

    /**
     * Signals the next NoSQL Operation.
     * <p>
     * The sequence of onNext calls represents the stream of changes, both
     * updates and deletions, made to the store.  The order of calls to onNext
     * for a given key represents the exact order of the operations on that key
     * performed on the store. There are no guarantees about the order of
     * operations across different keys in the stream.
     * <p>
     * If a shard is down, events associated with rows stored on that shard
     * could be arbitrarily delayed until the shard comes back up again and the
     * NoSQL Publisher can establish a shard stream to it.
     */
    @Override
    void onNext(StreamOperation t);

    /**
     * Signals an unrecoverable error in subscription.
     * <p>
     * There are many potential sources of error that the publisher may signal
     * via this method. One of them is worth special mention: the publisher may
     * invoke this method if it finds that it cannot resume the stream from
     * {@link NoSQLSubscriptionConfig#getInitialPosition} because the relevant
     * logs are no longer available at one of the shards.
     */
    @Override
    void onError(Throwable t);

    /**
     * Signals the completion of a subscription
     * <p>
     * Note streaming from kvstore table is unbounded by nature since unbounded
     * updates can be applied to a table. Thus onComplete() will never be
     * called in Stream API. User of Stream API shall implement this method
     * as no-op and any no-op implementation will be ignored.
     */
    @Override
    void onComplete();

    /* additional interface introduced by NoSQLSubscriber */

    /**
     * Invoked by the NoSQL publisher when creating a Subscription.  The
     * implementation of this method should return a configuration that
     * identifies the desired position for starting streaming, the tables whose
     * data should be streamed, and other configuration information.
     * <p>
     * An ill configuration or null return will cancel the subscription and
     * the publisher will release all resources allocated for that subscription.
     *
     * @return the configuration for creating the subscription
     */
    NoSQLSubscriptionConfig getSubscriptionConfig();

    /**
     * Signals a warning during subscription.
     * <p>
     * A call to this method warns the user of a potential issue that does not
     * yet represent a disruption in service, for example, a warning that a
     * shard was not available for an extended period of time. Note that
     * onWarn, unlike onError, does not terminate the flow of signals. It's
     * used to warn the subscriber that the publisher's functioning is
     * impaired, but not as yet fatally and some exception-specific action
     * could be taken to restore the health of the Publisher.
     */
    void onWarn(Throwable t);

    /**
     * Signals when a previously requested checkpoint is completed.
     * If checkpoint fails for any reason, the subscription will skip this
     * checkpoint for certain shards and continue streaming. The subscription
     * will try the next checkpoint when it comes.
     *
     * @param streamPosition  the stream position in checkpoint
     * @param failureCause    null if checkpoint succeeds, otherwise the cause
     *                        of checkpoint failure.
     */
    void onCheckpointComplete(StreamPosition streamPosition,
                              Throwable failureCause);
}

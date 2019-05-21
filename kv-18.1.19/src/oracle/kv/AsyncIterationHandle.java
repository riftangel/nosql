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

package oracle.kv;

import java.util.List;

import oracle.kv.stats.DetailedMetrics;

/**
 * An interface for controlling an asynchronous iteration.
 *
 * <p>Applications should call the {@link #iterate iterate} method to obtain
 * the elements of the iteration and to be informed when the iteration
 * completes.
 *
 * @param <E> the type of the iteration element
 * @hidden For internal use only - part of async API
 */
public interface AsyncIterationHandle<E> {

    /**
     * Supplies {@link ResultHandler} and {@link CompletionHandler} instances
     * to obtain the elements of the iteration and to be informed when the
     * iteration completes.  The operation is asynchronous, so calls may be
     * made to the handlers before the method returns.
     *
     * <p>The {@code next} parameter specifies a result handler whose {@link
     * ResultHandler#onResult onResult} method will be called with each element
     * of the iteration, supplying the {@code result} on success, and the
     * {@code exception} on failure.  If {@code onResult} throws an exception,
     * then the iteration will be canceled, and the exception will be supplied
     * in a call to {@link CompletionHandler#onComplete onComplete} on {@code
     * completed}.  If {@code onResult} returns normally after being called
     * with an exception, then the iteration is permitted, but is not required,
     * to continue despite one or more elements possibly being skipped due to
     * the failure.
     *
     * <p>The {@code completed} parameter specifies a {@code CompletionHandler}
     * whose {@code onComplete} method will be called when the iteration
     * completes, either successfully or due to an error.  Once this method is
     * called, no further elements will be returned.
     *
     * <p>Elements will only be returned to the result handler after the {@link
     * #request request} method is invoked, and applications must continue to
     * call the {@code request} method to obtain more elements.  Calling {@link
     * #cancel cancel} will cancel the iteration and cause {@code completed} to
     * be called with an instance of {@link IterationCanceledException}, unless
     * the iteration has already been canceled.
     *
     * <p>Calls on {@code next} are serialized: each call must return before
     * the next call will be made.
     *
     * <p>This method will throw an {@link IllegalStateException} if the method
     * has already been called on this handle or if the iteration has been
     * canceled.
     *
     * <p>The {@link ResultHandler} implementation should complete in a timely
     * manner to avoid preventing the invoking thread from dispatching to other
     * handlers.  Any exceptions thrown by the handler will cancel the
     * iteration and be supplied to the {@link CompletionHandler} unless the
     * iteration has already been canceled.
     *
     * @param next the handler for elements of the iteration
     * @param completed the handler to notify on completion of the iteration
     * @throws IllegalStateException if {@code iterate} has already been called
     * or if the iteration has been canceled
     */
    void iterate(ResultHandler<E> next, CompletionHandler completed);

    /**
     * Requests that the iteration provide the specified number of additional
     * elements.  This method has no effect if the iteration has completed or
     * been canceled.
     *
     * @param n the requested number of additional elements
     * @throws IllegalArgumentException if n is less than 1
     */
    void request(long n);

    /**
     * Cancels the iteration.  Elements may still be supplied to handlers to
     * supply previously requested elements after calling this method since
     * result delivery is asynchronous.  If the iteration has not already been
     * canceled, causes the iteration to fail with an {@link
     * IterationCanceledException}.  This method has no effect if the {@link
     * #iterate} method has not been called.
     */
    void cancel();

    /**
     * Returns per-partition metrics for the iteration. This method may be
     * called at any time during an iteration in order to obtain metrics to
     * that point or it may be called at the end to obtain metrics for the
     * entire scan. If there are no metrics available yet for a particular
     * partition, then that partition will not have an entry in the list.
     *
     * @return the per-partition metrics for iteration
     */
    List<DetailedMetrics> getPartitionMetrics();

    /**
     * Returns per-shard metrics for the iteration. This method may be called
     * at any time during an iteration in order to obtain metrics to that point
     * or it may be called at the end to obtain metrics for the entire scan.
     * If there are no metrics available yet for a particular shard, then that
     * shard will not have an entry in the list.
     *
     * @return the per-shard metrics for the iteration
     */
    List<DetailedMetrics> getShardMetrics();
}

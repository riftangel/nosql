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

/**
 * A handler for an asynchronous operation that returns a result.
 *
 * <p>The {@link #onResult onResult} method is called when the operation
 * produces a result, supplying the result, which may be {@code null}, if the
 * operation is successful, and an exception if the operation fails.  The
 * {@code exception} will be {@code null} if the operation succeeds, and
 * non-{@code null} if it fails.  Typically, the {@code result} will be {@code
 * null} on failure, but in some cases may be an object related to the failure.
 * Implementations of this method should return in a timely manner to allow the
 * invoking thread to attend to other tasks.
 *
 * <p>For all instances, the {@code onResult} method can be called with an
 * exception of type {@link FaultException}, which represents a variety of
 * general error conditions.  Methods with {@code ResultHandler} parameters
 * should document all expected exception types.
 *
 * <p>Implementations of the {@code onResult} method should complete in a
 * timely manner to avoid preventing the invoking thread from dispatching to
 * other handlers.
 *
 * @param <R> the type of the operation result
 * @hidden For internal use only - part of async API
 */
public interface ResultHandler<R> {

    /**
     * Called when an operation returns a result.
     *
     * @param result the result of a successful operation, which may be {@code
     * null}, and {@code null} or possibly a non-{@code null} value on failure
     * @param exception an exception that represents a failure, or {@code null}
     * on success
     */
    void onResult(R result, Throwable exception);
}

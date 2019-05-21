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
 * A handler for the completion of an asynchronous operation that does not
 * return a result.
 *
 * <p>The {@link #onComplete onComplete} method is called when the operation
 * completes, passing an exception if the operation fails, and {@code null} if
 * it succeeds.  Implementations of this method should return in a timely
 * manner to allow the invoking thread to attend to other tasks.
 *
 * <p>For all instances, the {@code onComplete} method can be called with an
 * exception of type {@link FaultException}, which represents a variety of
 * general error conditions.  Methods with {@code CompletionHandler} parameters
 * should document all expected exception types.
 *
 * @hidden For internal use only - part of async API
 */
public interface CompletionHandler {

    /**
     * Called when an operation completes.
     *
     * @param exception an exception that represents a failure, or {@code null}
     * on success
     */
    void onComplete(Throwable exception);
}

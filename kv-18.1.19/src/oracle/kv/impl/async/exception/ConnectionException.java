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

package oracle.kv.impl.async.exception;

/**
 * This base exception representing the cause of a connection abort.
 *
 */
public abstract class ConnectionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final boolean fromRemote;

    /**
     * Constructs the exception.
     *
     * @param fromRemote {@code true} if is aborted by the remote
     * @param message the message of the exception
     * @param cause the cause of the exception
     */
    public ConnectionException(boolean fromRemote,
                               String message,
                               Throwable cause) {
        super(message, cause);
        this.fromRemote = fromRemote;
    }

    /**
     * Returns {@code true} if the connection is aborted by the remote
     * endpoint.
     */
    public boolean fromRemote() {
        return fromRemote;
    }

    /**
     * Returns a {@link DialogException} based on this exception.
     *
     * @param hasSideEffect {@code true} if has side effect on the remote
     * @return the dialog exception
     */
    public DialogException getDialogException(boolean hasSideEffect) {
        checkSideEffect(hasSideEffect);
        if (shouldBackoff()) {
            return new DialogBackoffException(
                    hasSideEffect, fromRemote(), getMessage(), this);
        }
        return new DialogNoBackoffException(
                hasSideEffect, fromRemote(), getMessage(), this);
    }

    /**
     * Checks whether the hasSideEffect value is permitted for this exception.
     * For example, some connection exceptions are guaranteed to have no side
     * effects, so the caller should always specify a false value for the
     * parameter.
     *
     * @param hasSideEffect whether the exception has side effects
     * @throws IllegalArgumentException if the value of the hasSideEffect
     * argument is not permitted
     */
    public void checkSideEffect(boolean hasSideEffect) {
        /* Default behavior is do nothing */
    }

    /**
     * Returns {@code true} if the problem is likely to be persistent and the
     * upper layer should backoff before retry the connection.
     */
    public abstract boolean shouldBackoff();

    /**
     * Returns the non-dialog layer exception that underlies this exception, if
     * any, or else this exception.
     */
    public Exception getUnderlyingException() {
        final Throwable cause = getCause();
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return this;
    }
}

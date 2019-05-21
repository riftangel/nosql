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

import oracle.kv.impl.async.DialogHandler;

/**
 * Instances of this exception class are presented to {@link
 * DialogHandler#onAbort}.  Note that classes for all instantiated instances of
 * this class should inherit from either {@link DialogBackoffException} or
 * {@link DialogNoBackoffException}.
 */
public abstract class DialogException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final boolean hasSideEffect;
    private final boolean fromRemote;

    /**
     * Constructs the exception.
     *
     * @param hasSideEffect {@code true} if the dialog incurs any side effect
     * on the remote
     * @param fromRemote {@code true} if the exception is reported from the
     * remote
     * @param message the message of the exception
     * @param cause the cause of the exception
     */
    protected DialogException(boolean hasSideEffect,
                              boolean fromRemote,
                              String message,
                              Throwable cause) {
        super(message, cause);
        this.hasSideEffect = hasSideEffect;
        this.fromRemote = fromRemote;
    }

    /**
     * Returns whether the caller of the operation that produced this exception
     * should wait for some period of time before attempting to retry the
     * operation, because the problem is likely a persistent one.
     */
    public abstract boolean shouldBackoff();

    /**
     * Returns whether the dialog incurs any side effect on the remote.
     *
     * @return {@code true} if there is side effect
     */
    public boolean hasSideEffect() {
        return hasSideEffect;
    }

    /**
     * Returns whether the exception is reported by the remote.
     *
     * @return {@code true} if from remote
     */
    public boolean fromRemote() {
        return fromRemote;
    }

    /**
     * Returns the non-dialog layer exception that underlies this exception, if
     * any, or else this exception.
     */
    public Exception getUnderlyingException() {
        final Throwable cause = getCause();
        if (cause instanceof ConnectionException) {
            return ((ConnectionException) cause).getUnderlyingException();
        }
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return this;
    }
}


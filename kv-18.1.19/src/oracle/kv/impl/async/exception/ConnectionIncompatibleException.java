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
 * The exception is caused by the two endpoints having some incompatiblity
 * issue.
 */
public class ConnectionIncompatibleException extends ConnectionException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param fromRemote {@code true} if is aborted by the remote
     * @param message the message of the exception
     */
    public ConnectionIncompatibleException(boolean fromRemote,
                                           String message) {
        super(fromRemote, message, null);
    }

    /**
     * Returns {@code true} since there is some incompatiblity between
     * endpoints; no use to retry immediately.
     */
    @Override
    public boolean shouldBackoff() {
        return true;
    }

    /**
     * Checks for side effect.
     */
    @Override
    public void checkSideEffect(boolean hasSideEffect) {
        if (hasSideEffect) {
            throw new IllegalArgumentException(
                "This exception should have no side effects");
        }
    }
}


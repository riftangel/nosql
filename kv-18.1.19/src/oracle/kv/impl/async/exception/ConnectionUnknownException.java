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
 * The exception is caused by some unknwon fault.
 */
public class ConnectionUnknownException extends ConnectionException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception caused by local fault.
     *
     * @param cause the cause of the exception
     */
    public ConnectionUnknownException(Throwable cause) {
        super(false /* not remote */,
              cause.getMessage(),
              cause);
    }

    /**
     * Constructs the exception reported by remote.
     *
     * @param message the message of the exception
     */
    public ConnectionUnknownException(String message) {
        super(true /* remote */,
              message,
              null /* no cause */);
    }

    /**
     * Returns {@code true} since we do not know what happens.
     */
    @Override
    public boolean shouldBackoff() {
        return true;
    }
}


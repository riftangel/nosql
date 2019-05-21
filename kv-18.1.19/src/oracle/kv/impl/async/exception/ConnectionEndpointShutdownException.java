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

import oracle.kv.impl.async.EndpointGroup;

/**
 * The exception is caused by endpoints deliberately shutting down the
 * connection, e.g., {@link EndpointGroup#shutdown} was called.
 */
public class ConnectionEndpointShutdownException
    extends ConnectionException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param fromRemote {@code true} if is aborted by the remote
     * @param message the message of the exception
     */
    public ConnectionEndpointShutdownException(boolean fromRemote,
                                               String message) {
        super(fromRemote, message, null);
    }

    /**
     * Returns {@code true} since the endpoint is shutdown, no use to retry
     * immediately.
     */
    @Override
    public boolean shouldBackoff() {
        return true;
    }
}


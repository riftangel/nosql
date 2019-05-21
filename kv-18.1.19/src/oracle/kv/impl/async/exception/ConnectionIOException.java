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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import oracle.kv.impl.async.NetworkAddress;

/**
 * The exception is caused by IOException.
 */
public class ConnectionIOException extends ConnectionException {

    private static final long serialVersionUID = 1L;

    /** The remote address for the connection. */
    private final NetworkAddress remoteAddress;

    /** Whether to backoff on connection retries. */
    private boolean shouldBackoff;

    /**
     * Creates the exception based on the cause.
     *
     * @param cause the cause of the exception
     * @param remoteAddress the remote address of the connection
     */
    public ConnectionIOException(IOException cause,
                                 NetworkAddress remoteAddress) {
        super(false, cause.getMessage(), cause);
        this.remoteAddress = checkNull("remoteAddress", remoteAddress);
        shouldBackoff =
            (cause instanceof ConnectException) ? false :
            (cause instanceof UnknownHostException) ? true :
            false;
    }

    /**
     * Returns the remote address of the connection
     *
     * @return the remote address of the connection
     */
    public NetworkAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public boolean shouldBackoff() {
        return shouldBackoff;
    }
}

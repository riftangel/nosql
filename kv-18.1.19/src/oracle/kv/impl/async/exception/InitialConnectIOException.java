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

import java.io.IOException;

import oracle.kv.impl.async.NetworkAddress;

/**
 * An exception that is caused by an IOException that happens during the
 * initial socket connection.  We use this information in some cases to point
 * out to users that the problem may be caused by a mix of SSL and non-SSL
 * components on the network.
 */
public class InitialConnectIOException extends ConnectionIOException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs an instance of this class.
     *
     * @param cause the cause of the exception
     * @param remoteAddress the remote address of the connection
     */
    public InitialConnectIOException(IOException cause,
                                     NetworkAddress remoteAddress) {
        super(cause, remoteAddress);
    }
}


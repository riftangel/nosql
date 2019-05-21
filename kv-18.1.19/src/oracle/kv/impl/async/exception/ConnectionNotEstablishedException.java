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

import oracle.kv.impl.async.ResponderEndpoint;

/**
 * This exception is thrown when {@link ResponderEndpoint#startDialog} is
 * called and there is no underlying connection to start the dialog.
 *
 * The {@link ResponderEndpoint} cannot actively initiates a connection,
 * therefore, starting a dialog when the conneciton is not established on the
 * responder endpoint will result in it being aborted.
 */
public class ConnectionNotEstablishedException extends ConnectionException {

    private static final String MESSAGE =
        "No existing connection for responder endpoint";
    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     */
    public ConnectionNotEstablishedException() {
        super(false /* not from remote */,
              MESSAGE,
              null /* no cause */);
    }

    /**
     * Returns {@code true} since the connection will not likely to be
     * established very soon.
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


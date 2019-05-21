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
 * This exception is thrown because the remote endpoint cannot create a
 * suitable handler for this dialog type.
 *
 * Most likely, this exception occurs when the client is requesting a dialog
 * that is of a correct type, but the server has not managed to register the
 * handler yet.
 */
public class DialogNoSuchTypeException extends DialogBackoffException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     */
    public DialogNoSuchTypeException(String message) {
        super(false /* no side effect */,
              true /* from remote */,
              message,
              null /* no cause */);
    }
}


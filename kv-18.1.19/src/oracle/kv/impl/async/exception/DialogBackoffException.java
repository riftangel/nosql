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
 * A dialog exception that can be handled by retrying, but most likely only
 * after waiting for some period of time because it represents a more
 * persistent failure.
 *
 * Upon seeing the exception, the layer managing the dialog should wait and
 * query the status of the responder before starting the dialog again.
 * Alternatively, it could consider choosing another responder to serve the
 * dialog.
 */
public class DialogBackoffException extends DialogException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param hasSideEffect {@code true} if the dialog incurs any side effect
     * on the remote.
     * @param fromRemote {@code true} if the exception is reported from the
     * remote
     * @param message the message of the exception
     * @param cause the cause of the exception
     */
    public DialogBackoffException(boolean hasSideEffect,
                                  boolean fromRemote,
                                  String message,
                                  Throwable cause) {
        super(hasSideEffect, fromRemote, message, cause);
    }

    /**
     * Callers should backoff by waiting before retrying an operation that
     * resulted in this exception.
     */
    @Override
    public boolean shouldBackoff() {
        return true;
    }
}


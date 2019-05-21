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

package oracle.kv.impl.async.dialog;

import oracle.kv.impl.async.exception.ConnectionException;

/**
 * The exception is present due to a protocol violation is detected by either
 * endpoint.
 */
public class ProtocolViolationException extends ConnectionException {

    public static final String ERROR_INVALID_DIALOG_STATE =
        "Invalid dialog state:";
    public static final String ERROR_INVALID_FIELD =
        "Invalid field value:";
    public static final String ERROR_INVALID_HANDLER_STATE =
        "Invalid endpoint handler state:";
    public static final String ERROR_INVALID_MAGIC_NUMBER =
        "Invalid magic number:";
    public static final String ERROR_MAX_DIALOGS =
        "Max number of dialogs exceeded:";
    public static final String ERROR_MAX_LENGTH_EXCEEDED =
        "Max length exceeded:";
    public static final String ERROR_MAX_TOTLEN_EXCEEDED =
        "Max totlen exceeded:";
    public static final String ERROR_UNKNOWN_IDENTIFIER =
        "Unknown identifier:";

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param fromRemote {@code true} if is aborted by the remote
     * @param message the message of the exception
     */
    public ProtocolViolationException(boolean fromRemote, String message) {
        super(fromRemote, message, null);
    }

    /**
     * Returns {@code true} since protocol violation means at least one
     * endpoint is buggy or incompatible with the other.
     */
    @Override
    public boolean shouldBackoff() {
        return true;
    }
}


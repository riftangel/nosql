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

package oracle.kv.impl.admin;

/**
 * Exception to indicate that an assertion-style condition has failed, but it
 * should not cause the Admin to exit and restart.  Such exceptions are logged
 * at Level.SEVERE and abort the current operation.
 */
public class NonfatalAssertionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NonfatalAssertionException(String message) {
        super(message);
    }

    public NonfatalAssertionException(String message, Throwable t) {
        super(message, t);
    }
}

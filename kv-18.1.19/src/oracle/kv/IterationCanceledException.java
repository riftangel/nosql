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

package oracle.kv;

/**
 * Used as the cause of the failure of an iteration that was canceled by a call
 * to {@link AsyncIterationHandle#cancel AsyncIterationHandle.cancel}.
 *
 * @hidden For internal use only - part of async API
 */
public class IterationCanceledException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * For internal use only.
     * @hidden
     */
    public IterationCanceledException(String msg) {
        super(msg);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public IterationCanceledException(Throwable cause) {
        super(cause.getMessage(), cause);
    }
}

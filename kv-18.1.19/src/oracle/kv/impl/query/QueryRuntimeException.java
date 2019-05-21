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

package oracle.kv.impl.query;

/**
 * The QueryRuntimeException is internal used at server side only.
 *
 * A wrapper exception used to wrap RuntimeException thrown from executing
 * query plan at server side. The RuntimeException is wrapped as its cause,
 * the RequestHandler at the sever unwraps the cause and rethrows the
 * corresponding exception of the cause.
 */
public class QueryRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueryRuntimeException(RuntimeException wrappedException) {
        super(wrappedException);
    }
}

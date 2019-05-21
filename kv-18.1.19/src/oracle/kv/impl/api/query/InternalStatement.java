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

package oracle.kv.impl.api.query;

import oracle.kv.AsyncExecutionHandle;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.query.ExecuteOptions;

public interface InternalStatement {

    StatementResult executeSync(KVStoreImpl store,
                                ExecuteOptions options);

    /**
     * Execute the statement, returning results asynchronously through the
     * execution handle.
     *
     * @param store the store
     * @param options options that override the defaults
     * @return a handle for controlling the statement execution
     */
    AsyncExecutionHandle executeAsync(KVStoreImpl store,
                                      ExecuteOptions options);
}

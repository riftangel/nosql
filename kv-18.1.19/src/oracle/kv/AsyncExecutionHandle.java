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

import oracle.kv.StatementResult.Kind;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.Statement;
import oracle.kv.table.RecordValue;

/**
 * An interface for controlling and obtaining information about an asynchronous
 * statement execution.
 *
 * @see KVStore#executeAsync(String, ExecuteOptions)
 * @hidden For internal use only - part of async API
 */
public interface AsyncExecutionHandle
        extends AsyncIterationHandle<RecordValue> {

    /**
     * {@inheritDoc}
     *
     * <p>When this method is called for a data definition or administrative
     * operation, the {@code next} handler will not be called, and the {@link
     * #getInfo} or {@link #getInfoAsJson} methods should be used to obtain the
     * results of the operation.
     *
     * <p>If an error occurs during an update, then the {@code onResult} method
     * of the {@code next} parameter will be called with a non-{@code null}
     * {@code result} argument that represents the record value being updated,
     * in addition to the exception. In all other cases where the {@code
     * exception} argument is non-{@code null}, the {@code result} argument
     * will be {@code null}.
     */
    @Override
    void iterate(ResultHandler<RecordValue> next, CompletionHandler completed);

    /**
     * Returns the kind of statement result produced by the associated
     * statement execution.
     *
     * @return the kind of statement result
     */
    Kind getKind();

    /**
     * Returns the statement object for the associated statement execution.
     *
     * @return the statement
     */
    Statement getStatement();

    /**
     * Returns the administrative plan id for the associated statement
     * execution if the statement was a DDL statement: create or remove table,
     * a create or remove index, or an alter index. When using the Admin CLI
     * (runadmin) utility, administrative operations are identified by plan
     * id. The plan id can be used to correlate data definition and
     * administrative statements issued programmatically using the API against
     * operations viewed via the interactive Admin CLI or other monitoring
     * tool.
     *
     * <p>Returns 0 if the statement was not an administrative operation, or
     * did not require execution.
     *
     * @return the plan ID or {@code 0}
     */
    int getPlanId();

    /**
     * Returns information about the completed execution of the associated
     * statement, in human readable form. If the statement was a data
     * definition command, the information will show the start and end time of
     * the operation and details about server side processing.  Returns {@code
     * null} if the statement was not an administrative operation, or if
     * statement execution is not completed.
     *
     * @return info or {@code null}
     */
    String getInfo();

    /**
     * Returns the same information as {@link #getInfo}, in JSON format.
     * Returns {@code null} if the statement was not an administrative
     * operation, or if statement execution is not completed.
     *
     * @return info or {@code null}
     */
    String getInfoAsJson();
}

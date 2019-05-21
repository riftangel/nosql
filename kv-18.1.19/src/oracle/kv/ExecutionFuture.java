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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.kv.FaultException;

/**
 * An ExecutionFuture is a {@link Future} that provides a handle to an
 * administrative statement that has been issued and is being processed by the
 * kvstore. ExecutionFuture provides a way to check on the interim status of
 * the administrative operation, wait for the operation to complete, or cancel
 * the operation.
 * <p>
 *
 * For example:
 * <pre>
 * // Create a table and get a handle on the operation.
 * ExecutionFuture future = null;
 * try {
 *     future = kvstore.execute
 *          ("CREATE TABLE users (" +
 *           "id INTEGER, " +
 *           "firstName STRING, " +
 *           "lastName STRING, " +
 *           "age INTEGER, " +
 *           "PRIMARY KEY (id))");
 * } catch (IllegalArgumentException e) {
 *     System.out.println("The statement is invalid: " + e);
 * } catch (FaultException e) {
 *     System.out.println("There is a transient problem, retry the " +
 *                          "operation: " + e);
 * }
 * // Get current status on the asynchronously executing DDL statement
 * StatementResult result = future.updateStatus();
 * </pre>
 *
 * Note that this class supersedes oracle.kv.table.ExecutionFuture.
 * @since 3.3
 */
public interface ExecutionFuture extends Future<StatementResult> {

    /**
     * Attempts to cancel execution of this statement. Returns false if the
     * statement couldn't be cancelled, possibly because it has already
     * finished. If the statement hasn't succeeded already, and is stopped,
     * the operation will deem to have failed.
     *
     * @param mayInterruptIfRunning Since command execution begins immediately,
     * if mayInterreuptIfRunning is false, cancel returns false.
     * @throws FaultException if the Admin service connection fails.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) throws FaultException;

    /**
     * Block until the command represented by this future completes. Returns
     * information about the execution of the statement. This call will result
     * in communication with the kvstore server.
     * <p>
     * Note that ExecutionException is thrown if the statement execution
     * threw an exception. Once get() throws ExecutionException, all further
     * calls to get() will continue to throw ExecutionException.
     *
     * @throws ExecutionException if the command failed.
     * @throws CancellationException if the command was cancelled.
     * @throws InterruptedException if the current thread was interrupted
     * @throws FaultException if tbw
     */
    @Override
    public StatementResult get()
        throws CancellationException,
               ExecutionException,
               InterruptedException;

    /**
     * Block until the administrative operation has finished or the timeout
     * period is exceeded. This call will result in communication with the
     * kvstore server.
     * <p>
     * Note that ExecutionException is thrown if the statement execution
     * threw an exception. Once get() throws ExecutionException, all further
     * calls to get() will continue to throw ExecutionException.
     * @return information about the execution of the statement
     * @throws TimeoutException if the timeout is exceeded.
     * @throws InterruptedException
     */
    @Override
    public StatementResult get(long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException, ExecutionException;

    /**
     * Returns true if the operation was cancelled. The cancellation may
     * still be in progress, and the operation may still be running.
     */
    @Override
    public boolean isCancelled();

    /**
     * Returns true if the operation has been terminated. If the operation is
     * still executing, or if status has never been requested before, this call
     * will result in communication with the kvstore server to obtain up to
     * date status.
     * <p>
     * When the statement has terminated, results and status can be obtained
     * via {@link StatementResult}
     */
    @Override
    public boolean isDone();

    /**
     * Returns information about the execution of the statement. If the
     * statement is still executing, this call will result in communication
     * with the kvstore server to obtain up to date status, and the status
     * returned will reflect interim information.
     * @throws FaultException if there was any problem with accessing the
     * server. In general, such issues are transient, and the request can
     * be retried.
     */
    public StatementResult updateStatus()
        throws FaultException;

    /**
     * Returns information about the execution of the statement. The information
     * returned is that obtained by the last communication with the kvstore
     * server, and will not cause any additional communication. To request a
     * current check, use {@link #updateStatus}
     */
    public StatementResult getLastStatus();

    /**
     * Return the statement which has been executed.
     */
    public String getStatement();

    /**
     * Return a serialized version of the ExecutionFuture, which can be
     * saved for later use, via {@link KVStore#getFuture}. For example:
     * <pre>
     * byte[] futureBytes = future.toByteArray();
     *
     * // futureBytes can be saved and used later to recreate an 
     * // ExecutionFuture instance
     * ExecutionFuture laterFuture = store.getFuture(futureBytes);
     * StatementResult laterResult = laterFuture.get();
     * </pre>
     * <p>
     * Values returned by calls to this method can be used with current and
     * newer releases, but are not guaranteed to be compatible with earlier
     * releases.
     *
     * @since 3.3
     */
    public byte[] toByteArray();
}

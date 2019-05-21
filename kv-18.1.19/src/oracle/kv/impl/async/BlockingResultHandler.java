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

package oracle.kv.impl.async;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import oracle.kv.FaultException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ResultHandler;
import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * Implements a result handler that can be used to wait for a single result.
 *
 * @param <R> the type of the result
 */
public class BlockingResultHandler<R> implements ResultHandler<R> {
    private R value;
    private Throwable exception;
    private boolean done;

    /**
     * Creates an instance of this class.
     */
    public BlockingResultHandler() { }

    /**
     * Waits for the result for the specified amount of time.
     *
     * @param timeoutMillis the number of milliseconds to wait for the result,
     * or 0 to wait forever
     * @return the result
     * @throws RuntimeException an unchecked exception thrown while attempting
     * to perform the operation will be rethrown
     * @throws FaultException if a checked exception is thrown while attempting
     * to perform the operation
     * @throws IllegalArgumentException if timeoutMillis is less than 0
     * @throws RequestTimeoutException if the timeout is reached before the
     * operation completes
     */
    public R await(long timeoutMillis) {
        return await(timeoutMillis, timeoutMillis);
    }

    /**
     * Waits for the result for the specified amount of time, specifying the
     * original timeout for use in any timeout exception even if a shorter
     * timeout is being used for this call because of retries.
     *
     * @param originalTimeoutMillis the original number of milliseconds
     * specified for waiting
     * @param currentTimeoutMillis the number of milliseconds to wait for the
     * result, or 0 to wait forever
     * @return the result
     * @throws RuntimeException an unchecked exception thrown while attempting
     * to perform the operation will be rethrown
     * @throws FaultException if a checked exception is thrown while attempting
     * to perform the operation
     * @throws IllegalArgumentException if timeoutMillis is less than 0
     * @throws RequestTimeoutException if the timeout is reached before the
     * operation completes
     */
    public R await(long originalTimeoutMillis, long currentTimeoutMillis) {
        awaitInternal(currentTimeoutMillis);
        FaultException timeoutException = null;
        while (true) {
            synchronized (this) {
                if (done) {
                    if (exception != null) {
                        rethrowException(exception, RuntimeException.class);
                    }
                    return value;
                }
                if (timeoutException != null) {
                    exception = timeoutException;
                    done = true;
                    throw timeoutException;
                }
            }
            timeoutException = getTimeoutException(originalTimeoutMillis);
            assert timeoutException != null;
        }
    }

    /**
     * Return the fault exception that should be thrown when a timeout occurs.
     *
     * @param originalTimeoutMillis the original number of milliseconds
     * specified for waiting
     * @return the timeout exception
     */
    protected FaultException getTimeoutException(long originalTimeoutMillis) {
        return new RequestTimeoutException(
            (int) originalTimeoutMillis,
            "Request timed out" + getDescriptionString(), null, false);
    }

    /**
     * Waits for the result for the specified amount of time, potentially
     * throwing a checked exception of type E.
     *
     * @param timeoutMillis the number of milliseconds to wait for the result,
     * or 0 to wait forever
     * @return the result
     * @throws RuntimeException if an unchecked exception is thrown while
     * attempting to perform the operation, it will be rethrown
     * @throws FaultException if a checked exception is thrown while attempting
     * to perform the operation
     * @throws IllegalArgumentException if timeoutMillis is less than 0
     * @throws RequestTimeoutException if the timeout is reached before the
     * result becomes available
     */
    public <E extends Exception> R awaitChecked(Class<E> exceptionType,
                                                long timeoutMillis)
        throws E {

        checkNull("exceptionType", exceptionType);
        awaitInternal(timeoutMillis);
        synchronized (this) {
            if (!done) {
                throw new RequestTimeoutException(
                    (int) timeoutMillis,
                    "Request timed out" + getDescriptionString(), null, false);
            }
            if (exception != null) {
                rethrowException(exception, exceptionType);
            }
            return value;
        }
    }

    /**
     * Returns a description of the operation being performed to
     * include in exception messages, or null for no description.
     */
    protected String getDescription() {
        return null;
    }

    private String getDescriptionString() {
        final String desc = getDescription();
        return (desc == null) ? "" : ": " + desc;
    }

    /** Wait for the operation to complete or timeout. */
    private void awaitInternal(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("Timeout must be 0 or greater");
        }
        final long stop = System.nanoTime() +
            NANOSECONDS.convert(timeoutMillis, MILLISECONDS);
        while (!done) {
            final long wait =
                MILLISECONDS.convert(stop - System.nanoTime(), NANOSECONDS);
            if (wait <= 0) {
                break;
            }
            try {
                synchronized (this) {
                    wait(wait);
                }
            } catch (InterruptedException e) {
                throw new FaultException(
                    "Operation was interrupted while waiting for result" +
                    getDescriptionString(),
                    e, false);
            }
        }
    }

    /** Rethrows the exception. */
    private <E extends Throwable> void rethrowException(
        Throwable e, Class<E> allowableThrowType)
        throws E {

        if (allowableThrowType.isInstance(e)) {
            CommonLoggerUtils.appendCurrentStack(e);
            throw allowableThrowType.cast(e);
        }
        if (e instanceof Error) {
            CommonLoggerUtils.appendCurrentStack(e);
            throw (Error) e;
        }
        throw new FaultException("Unexpected exception: " + e, e, false);
    }

    /**
     * {@inheritDoc}
     *
     * <p>In this implementation, if the operation is not already complete,
     * arranges for calls to {@link #await} or {@link #awaitChecked} to return
     * the result or throw the appropriate exception.
     */
    @Override
    public synchronized void onResult(R result, Throwable e) {
        if (!done) {
            value = result;
            exception = e;
            done = true;
            notifyAll();
        }
    }
}

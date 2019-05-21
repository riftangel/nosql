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

package oracle.kv.query;

import static oracle.kv.impl.api.table.TableImpl.validateNamespace;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.concurrent.TimeUnit;
import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.table.TableIteratorOptions;

/**
 * Class contains several options for the KVStore.execute() methods.
 * It contains the following execution options: consistency, durability and
 * timeout.
 *
 * @see KVStore#execute(String, ExecuteOptions)
 * @see KVStore#executeSync(String, ExecuteOptions)
 * @see KVStore#executeSync(Statement, ExecuteOptions)
 *
 * @since 4.0
 */
public class ExecuteOptions {

    private Consistency consistency;

    private Durability durability;

    private long timeout;

    private TimeUnit timeoutUnit;

    private int maxConcurrentRequests;

    private int resultsBatchSize;

    private byte traceLevel;

    private MathContext mathContext = MathContext.DECIMAL32;

    /* added in 4.4 */
    private String namespace;

    private PrepareCallback prepareCallback;

    /* added in 18.1 */
    private int maxReadKB;

    private byte[] continuationKey;
    /*
     * The flag indicates if use resultsBatchSize as the limit to the number of
     * results returned, the resultsBatchSize can be configured with
     * {@link #setResultsBatchSize}
     */
    private boolean useBatchSizeAsLimit;

    private LogContext logContext = null;

    private boolean doPrefetching = true;

    public ExecuteOptions() {}

    /**
     * @hidden
     */
    public ExecuteOptions(TableIteratorOptions options) {

        if (options != null) {
            maxConcurrentRequests = options.getMaxConcurrentRequests();
            resultsBatchSize = options.getResultsBatchSize();
            maxReadKB = options.getMaxReadKB();
        }
    }


    /**
     * Sets the execution consistency.
     */
    public ExecuteOptions setConsistency(Consistency consistency) {
        this.consistency = consistency;
        return this;
    }

    /**
     * Gets the last set execution consistency.
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Sets the execution durability.
     */
    public ExecuteOptions setDurability(Durability durability) {
        this.durability = durability;
        return this;
    }

    /**
     * Gets the last set execution durability.
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * The {@code timeout} parameter is an upper bound on the time interval for
     * processing the operation.  A best effort is made not to exceed the
     * specified limit. If zero, the {@link KVStoreConfig#getRequestTimeout
     * default request timeout} is used.
     * <p>
     * If {@code timeout} is not 0, the {@code timeoutUnit} parameter must not
     * be {@code null}.
     *
     * @param timeout the timeout value to use
     * @param timeoutUnit the {@link TimeUnit} used by the
     * <code>timeout</code> parameter or null
     */
    public ExecuteOptions setTimeout(long timeout,
                                     TimeUnit timeoutUnit) {

        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }
        if ((timeout != 0) && (timeoutUnit == null)) {
            throw new IllegalArgumentException("A non-zero timeout requires " +
                "a non-null timeout unit");
        }

        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        return this;
    }

    /**
     * Gets the timeout, which is an upper bound on the time interval for
     * processing the read or write operations.  A best effort is made not to
     * exceed the specified limit. If zero, the
     * {@link KVStoreConfig#getRequestTimeout default request timeout} is used.
     *
     * @return the timeout
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Gets the unit of the timeout parameter, and may
     * be {@code null} only if {@link #getTimeout} returns zero.
     *
     * @return the timeout unit or null
     */
    public TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    /**
     * Returns the maximum number of concurrent requests.
     */
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    /**
     * Sets the maximum number of concurrent requests.
     */
    public ExecuteOptions setMaxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    /**
     * Returns the number of results per request.
     */
    public int getResultsBatchSize() {
        return (resultsBatchSize > 0 ?
                resultsBatchSize :
                getMaxReadKB() == 0 ? KVStoreImpl.DEFAULT_ITERATOR_BATCH_SIZE : 0);
    }

    /**
     * Sets the number of results per request.
     */
    public ExecuteOptions setResultsBatchSize(int resultsBatchSize) {

        if (resultsBatchSize < 0) {
            throw new IllegalArgumentException(
                "The batch size can not be a negative value: " + maxReadKB);
        }
        this.resultsBatchSize = resultsBatchSize;
        return this;
    }

    /**
     * Returns the {@link MathContext} used for {@link BigDecimal} and
     * {@link BigInteger} operations. {@link MathContext#DECIMAL32} is used by
     * default.
     */
    public MathContext getMathContext() {
        return mathContext;
    }

    /**
     * Sets the {@link MathContext} used for {@link BigDecimal} and
     * {@link BigInteger} operations. {@link MathContext#DECIMAL32} is used by
     * default.
     */
    public ExecuteOptions setMathContext(MathContext mathContext) {
        this.mathContext = mathContext;
        return this;
    }

    /**
     * Returns the trace level for a query
     * @hidden
     */
    public byte getTraceLevel() {
        return traceLevel;
    }

    /**
     * Sets the trace level for a query
     * @hidden
     */
    public ExecuteOptions setTraceLevel(byte level) {
        this.traceLevel = level;
        return this;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Sets the namespace to use for the query.
     *
     * @since 4.4
     */
    public ExecuteOptions setNamespace(String namespace) {
        if (namespace != null) {
            validateNamespace(namespace);
        }
        this.namespace = namespace;
        return this;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns the namespace to use for the query, null if not set.
     *
     * @since 4.4
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @hidden
     * Sets the PrepareCallback to use for the query.
     *
     * @param prepareCallback an instance of PrepareCallback
     *
     * @since 18.1
     */
    public ExecuteOptions setPrepareCallback(PrepareCallback prepareCallback) {
        this.prepareCallback = prepareCallback;
        return this;
    }

    /**
     * @hidden
     * Returns the PrepareCallback set, or null if not set.
     *
     * @since 18.1
     */
    public PrepareCallback getPrepareCallback() {
        return prepareCallback;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Sets the max number of KBytes read during a query.
     *
     * @since 18.1
     */
    public ExecuteOptions setMaxReadKB(int maxReadKB) {
        if (maxReadKB < 0) {
            throw new IllegalArgumentException("The max read KB can not " +
                "be a negative value: " + maxReadKB);
        }
        this.maxReadKB = maxReadKB;
        return this;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Returns the max number of KBytes read during a query.
     *
     * @since 18.1
     */
    public int getMaxReadKB() {
        return maxReadKB;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Sets if use resultsBatchSize as the limit to the number of results
     * returned, the resultsBatchSize can be configured with
     * {@link #setResultsBatchSize}
     *
     * @since 18.1
     */
    public ExecuteOptions setUseBatchSizeAsLimit(boolean value) {
        useBatchSizeAsLimit = value;
        return this;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Returns true if use resultsBatchSize as the limit to the number of
     * results returned.
     *
     * @since 18.1
     */
    public boolean getUseBatchSizeAsLimit() {
        return useBatchSizeAsLimit;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Sets the continuation Key from which the query resumed.
     *
     * @since 18.1
     */
    public ExecuteOptions setContinuationKey(byte[] continuationKey) {
        this.continuationKey = continuationKey;
        return this;
    }

    /**
     * Used for http proxy only.
     * @hidden
     *
     * Returns the continuation Key specified.
     *
     * @since 18.1
     */
    public byte[] getContinuationKey() {
        return continuationKey;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Sets the LogContext to use for the query.
     *
     * @since 18.1
     */
    public ExecuteOptions setLogContext(LogContext lc) {
        this.logContext = lc;
        return this;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns the LogContext to use for the query, null if not set.
     *
     * @since 18.1
     */
    public LogContext getLogContext() {
        return logContext;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public void setDoPrefetching(boolean v) {
        doPrefetching = v;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public boolean getDoPrefetching() {
        return doPrefetching;
    }
}

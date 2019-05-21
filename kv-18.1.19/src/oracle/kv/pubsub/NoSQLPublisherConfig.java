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

package oracle.kv.pubsub;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import oracle.kv.KVStoreConfig;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.impl.security.ssl.SSLConfig;

/**
 * Configuration used to create an instance of NoSQLPublisher. User need to
 * specify
 * <ul>
 * <li> an instance of KVStoreConfig
 * <li> a path to a writable directory to create the publisher root directory.
 * </ul>
 *
 * In addition, user can also specify
 * <ul>
 * <li> shard timeout in milliseconds, which represents a warning threshold of
 * time that NoSQLPublisher does not hear from a shard. If a subscription does
 * not hear from a shard for more than this threshold,  a ShardTimeoutException
 * will be signaled via NoSQLSubscriber.onWarn;
 * <li> maximum concurrent subscriptions, which represents a upper bound of the
 * maximum number of concurrent subscriptions the publisher can support.
 * </ul>
 *
 * For secure store, user need to specify
 * <ul>
 * <li> login credentials which will be used to authenticate with NoSQL DB
 * <li> a reauthentication handler which will be used to re-authenticate with
 * the NoSQL DB.
 * </ul>
 */
public class NoSQLPublisherConfig {

    /* configuration of source kvstore */
    private final KVStoreConfig kvStoreConfig;
    /* signal if shard is unreachable after this timeout period */
    private final long shardTimeoutMs;
    /* max number of concurrent subscriptions */
    private final int maxConcurrentSubs;
    /* path to root publisher directory */
    private final String rootPath;
    /* re-authentication handler */
    private final ReauthenticateHandler reauthHandler;
    /* HA security property */
    private final Properties haSecurityProp;

    /* Creates an instance from builder */
    private NoSQLPublisherConfig(Builder builder) {

        kvStoreConfig = builder.kvStoreConfig;
        rootPath = builder.rootPath;
        shardTimeoutMs = builder.shardTimeoutMs;
        maxConcurrentSubs = builder.maxConcurrentSubs;
        reauthHandler = builder.reauthHandler;
        haSecurityProp = builder.haSecurityProp;
    }

    /**
     * Gets the source kvstore name
     *
     * @return the source kvstore name
     */
    public String getStoreName() {
        return kvStoreConfig.getStoreName();
    }

    /**
     * Gets the list of source kvstore helper hosts
     *
     * @return the list of source kvstore helper hosts
     */
    public String[] getHelperHosts() {
        return kvStoreConfig.getHelperHosts().clone();
    }

    /**
     * Gets the configured shard timeout in ms
     *
     * @return the configured shard timeout in ms
     */
    public long getShardTimeoutMs() {
        return shardTimeoutMs;
    }

    /**
     * Gets the maximum number of concurrent subscriptions supported by this
     * publisher.
     *
     * @return the maximum number of concurrent subscriptions
     */
    public int getMaxConcurrentSubs() {
        return maxConcurrentSubs;
    }

    /**
     * Gets the root directory of the publisher
     *
     * @return the root directory
     */
    public String getRootPath() {
        return rootPath;
    }

    /**
     * Gets the subscriber provided reauthentication handler
     *
     * @return reauthentication handler
     */
    ReauthenticateHandler getReauthHandler() {
        return reauthHandler;
    }

    /**
     * @return HA security properties
     * @hidden Gets HA security properties
     */
    public Properties getHaSecurityProperties() {
        return haSecurityProp;
    }

    /**
     * Gets source kvstore configuration
     *
     * @return source kvstore configuration
     */
    KVStoreConfig getKvStoreConfig() {
        return kvStoreConfig;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null) {
            return false;
        }

        if (!(obj instanceof NoSQLPublisherConfig)) {
            return false;
        }

        final NoSQLPublisherConfig otherConf = (NoSQLPublisherConfig) obj;
        return getStoreName().equals(otherConf.getStoreName())  &&
               shardTimeoutMs == otherConf.getShardTimeoutMs()  &&
               maxConcurrentSubs == otherConf.getMaxConcurrentSubs();

    }

    @Override
    public int hashCode() {
        return getStoreName().hashCode() +
               (int) (shardTimeoutMs ^ (shardTimeoutMs >>> 32)) +
               maxConcurrentSubs;
    }

    /**
     * Builder to help construct a NoSQLPublisherConfig instance
     */
    public static class Builder {

        /* configuration of source kvstore */
        private final KVStoreConfig kvStoreConfig;
        private final String rootPath;

        private long shardTimeoutMs;
        private int maxConcurrentSubs;
        private ReauthenticateHandler reauthHandler;
        private Properties haSecurityProp;

        /**
         * Creates a publisher configuration with following default parameter
         * values:
         * <ul>
         * <li>Shard timeout in milliseconds: {@value
         *     oracle.kv.pubsub.NoSQLPublisher#DEFAULT_SHARD_TIMEOUT_MS}
         * <li>Maximum number of subscriptions: {@value
         *     oracle.kv.pubsub.NoSQLPublisher#DEFAULT_MAX_CONCURRENT_SUBS}
         * <li>Null login credentials
         * <li>Null reauthenticate handler
         * </ul>
         *
         * @param kvStoreConfig kvstore configuration
         * @param rootPath      path to the publisher root directory
         * @throws IllegalArgumentException if missing or invalid parameters
         */
        public Builder(KVStoreConfig kvStoreConfig, String rootPath)
            throws IllegalArgumentException {

            final String storeName = kvStoreConfig.getStoreName();
            if (storeName == null || storeName.isEmpty()) {
                throw new IllegalArgumentException(
                    "Missing source kvstore name.");
            }

            if (!Files.exists(Paths.get(rootPath))) {
                throw new IllegalArgumentException(
                    "Publisher root path does not exist " + rootPath);
            }


            this.kvStoreConfig = kvStoreConfig;
            this.rootPath = rootPath;

            shardTimeoutMs = NoSQLPublisher.DEFAULT_SHARD_TIMEOUT_MS;
            maxConcurrentSubs = NoSQLPublisher.DEFAULT_MAX_CONCURRENT_SUBS;
            reauthHandler = null;
            haSecurityProp = SSLConfig.getJESecurityProp(
                kvStoreConfig.getSecurityProperties());
        }

        /**
         * Builds a NoSQLSubscriptionConfig instance from builder
         *
         * @return a NoSQLSubscriptionConfig instance
         */
        public NoSQLPublisherConfig build() {
            return new NoSQLPublisherConfig(this);
        }

        /* Setters with parameter check */

        /**
         * Sets reauthentication handler. If null, the default login handler
         * will be used.
         *
         * @param reauthHandler reauthentication handler
         *
         * @return this instance
         */
        public Builder setReauthHandler(ReauthenticateHandler reauthHandler) {
            this.reauthHandler = reauthHandler;
            return this;
        }

        /**
         * Sets maximum concurrently running subscriptions in publisher
         *
         * @param maxConcurrentSubs maximum concurrently running subscriptions
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if maximum concurrently running
         * subscriptions is smaller than 1.
         */
        public Builder setMaxConcurrentSubs(int maxConcurrentSubs)
            throws IllegalArgumentException {

            if (maxConcurrentSubs < 1) {
                throw new IllegalArgumentException("Max number of concurrent " +
                                                   "subscriptions must be " +
                                                   "at least 1.");
            }
            this.maxConcurrentSubs = maxConcurrentSubs;
            return this;
        }

        /**
         * Sets shard timeout in milliseconds. No shard timeout if set to 0.
         *
         * @param shardTimeoutMs shard timeout in milliseconds
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if shard timeout is smaller than 0.
         */
        public Builder setShardTimeoutMs(long shardTimeoutMs)
            throws IllegalArgumentException {

            if (shardTimeoutMs < 0) {
                throw new IllegalArgumentException("Shard timeout must be " +
                                                   "positive, or zero for no " +
                                                   "timeout");
            }
            this.shardTimeoutMs = (shardTimeoutMs == 0) ?
                Long.MAX_VALUE : shardTimeoutMs;

            return this;
        }
    }
}

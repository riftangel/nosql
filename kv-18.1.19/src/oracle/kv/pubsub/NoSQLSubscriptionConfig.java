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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;

/**
 * Configuration used by the NoSQL Publisher to create a subscription.
 */
public class NoSQLSubscriptionConfig {

    /* default stream mode */
    private final static NoSQLStreamMode DEFAULT_STREAM_MODE =
        NoSQLStreamMode.FROM_CHECKPOINT;

    /* subscriber subscriberId */
    private final NoSQLSubscriberId subscriberId;

    /* the position at which to resume the stream */
    private final StreamPosition initialPosition;

    /* The tables associated with this subscription. */
    private final Set<String> tables;

    /*
     * checkpoint table name; The user must specify the checkpoint table to
     * use in the subscription, and she must have permission to read and write
     * the table, otherwise the subscription will fail at the beginning and
     * a SubscriptionFailureException will be signalled to subscriber.
     */
    private final String ckptTableName;

    /*
     * True to create a new checkpoint table. Possible cases:
     *
     * When set true, that means user want to create a new checkpoint table
     * if specified checkpoint table does not exist
     * - If the table does not exist, subscription will create a new one if
     * user is eligible to create it; If the user does not have the create
     * table privilege, subscription will fail.
     * - If the table already exists, subscription will succeed since no new
     * table need be created.
     *
     * When set false, it means user want to use an existing checkpoint table:
     * - If the table does not exist, subscription will fail;
     * - If the table exists and the user is eligible to read/write it, the
     * subscription will succeed, otherwise, it will fail.
     *
     * The default is true to allow publisher create checkpoint table for user
     */
    private final boolean newCkptTable;

    /* subscription stream mode */
    private final NoSQLStreamMode streamMode;

    /* unit test only */
    private TestHook<?> testHook = null;

    /* unit test only, always true unless test disable it */
    private boolean useFeederFilter = true;

    /* unit test only, always true unless test disable it */
    private boolean enableCkptTable = true;

    /* unit test only, only set internally */
    private LoginToken token = null;

    /**
     * unit test only
     *
     * Reauthentication interval in ms. If it is not set, we compute
     * re-authentication time from toke lifetime. If set, we would compute
     * from this reAuthIntervalMs
     */
    private final long reAuthIntervalMs;

    private NoSQLSubscriptionConfig(Builder builder) {

        ckptTableName = builder.ckptTableName;
        subscriberId = builder.subscriberId;
        tables = builder.subscribedTables;
        reAuthIntervalMs = builder.reAuthIntervalMs;
        newCkptTable = builder.newCkptTable;
        streamMode = builder.streamMode;

        /*
         * If FROM_(EXACT_)STREAM_POSITION, user must specify a stream
         * position in builder
         */
        if (builder.initialPosition == null &&
            (streamMode.equals(NoSQLStreamMode
                                   .FROM_STREAM_POSITION) ||
             streamMode.equals(NoSQLStreamMode
                                   .FROM_EXACT_STREAM_POSITION))){

            throw new IllegalArgumentException(
                "Unspecified start stream position with stream mode " +
                streamMode);
        }


        if (streamMode.equals(NoSQLStreamMode.FROM_NOW) ||
            streamMode.equals(NoSQLStreamMode.FROM_CHECKPOINT) ||
            streamMode.equals(NoSQLStreamMode.FROM_EXACT_CHECKPOINT)) {
            initialPosition = null;
        } else {
            initialPosition = builder.initialPosition;
        }
    }

    /* getters */

    /**
     * Gets the name of checkpoint table associated with subscription
     *
     * @return  the name of checkpoint table
     */
    public String getCkptTableName() {
        return ckptTableName;
    }

    /**
     * Returns the initial stream position to be used when creating a
     * Subscription. The first call to <code>onNext()</code> by the Publisher
     * will <code>signal</code> the element following this position in the
     * stream.
     * <p>
     * If stream mode is
     * {@link NoSQLStreamMode#FROM_NOW},
     * {@link NoSQLStreamMode#FROM_CHECKPOINT}, or
     * {@link NoSQLStreamMode#FROM_EXACT_CHECKPOINT}, it always
     * returns null since these modes do not use the initial position
     * specified in config.
     */
    public StreamPosition getInitialPosition() {
        return initialPosition;
    }

    /**
     * Gets the subscriber ID of the configuration.
     *
     * @return subscriber ID
     */
    public NoSQLSubscriberId getSubscriberId() {
        return subscriberId;
    }

    /**
     * Returns the tables to be associated with a subscription. If null or an
     * empty set, it means all tables be streamed.
     */
    public Set<String> getTables() {
        return tables;
    }

    /**
     * Returns true if the subscription should attempt to create the checkpoint
     * table if it doesn't already exist.
     *
     * @return true if new checkpoint table should be created if needed
     */
    public boolean useNewCheckpointTable() {
        return newCkptTable;
    }


    @Override
    public String toString() {
        final String tableNames = (tables == null || tables.isEmpty()) ?
            "all tables in NoSQL store" : Arrays.toString(tables.toArray());

        if (streamMode.equals(NoSQLStreamMode.FROM_CHECKPOINT) ||
            streamMode.equals(NoSQLStreamMode.FROM_EXACT_CHECKPOINT)) {
            return "Subscription " + subscriberId.toString() +
                   " configured to stream from checkpoint with stream mode:" +
                   streamMode + ", subscribed tables: " + tableNames;
        }

        if (streamMode.equals(NoSQLStreamMode.FROM_STREAM_POSITION) ||
            streamMode.equals(NoSQLStreamMode.FROM_EXACT_STREAM_POSITION)) {
            return "Subscription " + subscriberId.toString() +
                   " configured to stream from position " + initialPosition +
                   " with stream mode: " + streamMode +  ", subscribed " +
                   "tables: " + tableNames;
        }

        return "Subscription " + subscriberId.toString() +
               " configured to stream with stream mode: " + streamMode +
               ", subscribed tables: " + tableNames;
    }

    /**
     * @hidden
     *
     * Returns true if enable subscription feeder filter, false to disable
     * the filter.
     *
     * @return true if enable filter, false if not
     */
    public boolean isFeederFilterEnabled() {
        return useFeederFilter;
    }

    /**
     * @hidden
     *
     * Returns true if enable subscription feeder filter, false to disable
     * the filter.
     *
     * @return true if enable filter, false if not
     */
    public boolean isCkptEnabled() {
        return enableCkptTable;
    }

    /**
     * @hidden
     *
     * Unit test only
     *
     * Disables feeder filter, can be a knob for performance tuning or
     * currently used in test only.
     */
    public void disableFeederFilter() {
        useFeederFilter = false;
    }

    /**
     * @hidden
     *
     * Unit test only
     *
     * Disables checkpoint table
     */
    public void disableCheckpoint() {
        enableCkptTable = false;
    }

    /**
     * @hidden
     *
     * Used in unit test only
     *
     * Gets the reauthentication interval in ms
     *
     * @return reauthentication interval in ms
     */
    public long getReAuthIntervalMs() {
        return reAuthIntervalMs;
    }

    /**
     * @hidden
     *
     * Computes the mapping from subscriber id to group of shards that the
     * subscriber streams from.
     *
     * @param si       subscriber id
     * @param topology topology of source kv store
     *
     * @return group of rep groups that the subscriber streams from
     *
     * @throws SubscriptionFailureException if number of subscribers is more
     * than number of shards in store
     */
    public static Set<RepGroupId> computeShards(NoSQLSubscriberId si,
                                                Topology topology)
        throws SubscriptionFailureException {

        final Set<RepGroupId> all = topology.getRepGroupIds();

        /* short cut for single member group */
        if (si.getTotal() == 1) {
            return all;
        }

        if (si.getTotal() > all.size()) {
            throw new SubscriptionFailureException(
                si, "number of subscribers in group (" + si.getTotal() +
                    ") exceeds number of shards in store (" + all.size() + ")");
        }

        /*
         * mapping algorithm by example:
         *
         * suppose 10 shards, 3 subscribers
         * rg1, rg4, rg7, rg10 -> subscriber 1
         * rg2, rg5, rg8 -> subscriber 2
         * rg3, rg6, rg9 -> subscriber 3
         */
        final Set<RepGroupId> ret = new HashSet<>();
        for (RepGroupId repGroupId : all) {
            final int gid = repGroupId.getGroupId();
            final int idx = (gid - 1) % si.getTotal() ;

            if (idx == si.getIndex()) {
                ret.add(repGroupId);
            }
        }

        return ret;
    }

    /**
     * @hidden
     *
     * Set test hook
     */
    public void setILETestHook(TestHook<?> hook) {
        testHook = hook;
    }

    /**
     * @hidden
     *
     * @return ILE test hooker
     */
    public TestHook<?> getILETestHook() {
        return testHook;
    }

    /**
     * @hidden
     *
     * Constructs a singleton subscriber id with single member in group.
     *
     * @return a subscriber id with machine generated uuid as the group id.
     */
    private static NoSQLSubscriberId getSingletonSubscriberId() {
        return new NoSQLSubscriberId( 1, 0);
    }

    /**
     * @hidden
     * Unit test only
     */
    public LoginToken getTestToken() {
        return token;
    }

    /**
     * @hidden
     * Unite test only
     *
     * @param t  token to use
     */
    public void setTestToken(LoginToken t) {
        token = t;
    }

    /**
     * Returns the start stream mode.
     *
     * @return the start stream mode
     */
    public NoSQLStreamMode getStreamMode() {
        return streamMode;
    }

    /* Returns default mode */
    static NoSQLStreamMode getDefaultStreamMode() {
        return DEFAULT_STREAM_MODE;
    }

    /**
     * Builder to construct a NoSQLSubscriptionConfig instance
     */
    public static class Builder {

        /* required parameter */
        private final String ckptTableName;

        /*
         * Optional parameters
         *
         * Default: a singleton group subscription with default stream mode
         * and feeder filter enabled, create new ckpt table if not exist,
         * subscribe all user tables.
         */
        private NoSQLSubscriberId subscriberId = getSingletonSubscriberId();
        private StreamPosition initialPosition = null;
        private Set<String> subscribedTables = null;
        private long reAuthIntervalMs = 0;
        private boolean newCkptTable = true;
        private NoSQLStreamMode streamMode = DEFAULT_STREAM_MODE;

        /**
         * Makes a builder for NoSQLSubscriptionConfig with required parameter
         *
         * @param ckptTableName name of checkpoint table
         */
        public Builder(String ckptTableName) {

            if (ckptTableName == null || ckptTableName.isEmpty()) {
                throw new IllegalArgumentException(
                    "Invalid checkpoint table name");
            }

            this.ckptTableName = ckptTableName;
        }

        /**
         * Builds a NoSQLSubscriptionConfig instance from builder
         *
         * @return a NoSQLSubscriptionConfig instance
         */
        public NoSQLSubscriptionConfig build() {
            return new NoSQLSubscriptionConfig(this);
        }

        /**
         * Sets the subscribed tables. If not set or set to null or empty set,
         * the stream will subscribe all user tables.
         *
         * @param tables set of table names to subscribe
         * @return this instance
         */
        public Builder setSubscribedTables(Set<String> tables) {
            subscribedTables = tables;
            return this;
        }

        /**
         * Sets the subscribed tables. If not set, the stream will subscribe
         * all user tables.
         *
         * @param tables set of table names to subscribe
         * @return this instance
         */
        public Builder setSubscribedTables(String... tables) {
            subscribedTables = new HashSet<>(Arrays.asList(tables));
            return this;
        }

        /**
         * Sets the subscriber id which owns the subscription. If it is not set,
         * the subscription is considered a singleton subscription with single
         * subscriber streaming from all shards.
         *
         * @param id subscriber id
         * @return this instance
         */
        public Builder setSubscriberId(NoSQLSubscriberId id) {
            subscriberId = id;
            return this;
        }

        /**
         * Sets the start stream position. Depending on stream mode, the
         * start stream position has different semantics as follows.
         * <p> If stream mode is set to
         * {@link NoSQLStreamMode#FROM_CHECKPOINT}, which is the default if
         * stream mode is not set, the stream will start from the checkpoint
         * persisted in the specified checkpoint table, and any specified
         * stream position will be ignored. If the checkpoint table does not
         * exist, the stream will create a new checkpoint table for user if
         * allowed. If the checkpoint table is empty, or if the position
         * specified by the checkpoint table is not available, subscription
         * will stream from the next available position.
         *
         * <p> If stream mode is set to
         * {@link NoSQLStreamMode#FROM_EXACT_CHECKPOINT}, the stream will
         * start from the checkpoint persisted in the specified checkpoint
         * table, and any specified stream position will be ignored. If the
         * table does not exist, the stream will create a new checkpoint
         * table for user if allowed. If the checkpoint table is empty,
         * subscription will to stream from the first available entry. If
         * a checkpoint has been saved but that stream position is not
         * available, the subscription will fail and the publisher will signal
         * {@link SubscriptionInsufficientLogException} to the subscriber via
         * {@link NoSQLSubscriber#onError}.
         *
         * <p> If stream mode is set to
         * {@link NoSQLStreamMode#FROM_STREAM_POSITION} the stream will
         * start from specified stream position if set. If the start stream
         * position is not available, the subscription will stream from the
         * next available position. If the start stream position is not
         * set or is set to null, {@link IllegalArgumentException} will be
         * raised when the configuration is created.
         *
         * <p> If stream mode is set to
         * {@link NoSQLStreamMode#FROM_EXACT_STREAM_POSITION}, the stream will
         * start from specified stream position if set. If the specified
         * position is not available, the subscription will fail and the
         * publisher will signal {@link SubscriptionInsufficientLogException}
         * to the subscriber via {@link NoSQLSubscriber#onError}. If the
         * start stream position is not set or set to null,
         * {@link IllegalArgumentException} will be raised when the
         * configuration is created.
         *
         * <p> If stream mode is set to
         * {@link NoSQLStreamMode#FROM_NOW}, the stream will start from
         * the latest available stream position and any specified start stream
         * position will be ignored.
         *
         * @param position               stream position to start stream
         *
         * @return this instance
         */
        public Builder setStartStreamPosition(StreamPosition position) {
            initialPosition = position;
            return this;
        }

        /**
         * Sets if publisher is allowed to create a new checkpoint table if
         * it does not exist. If not set, the default is that publisher would
         * create a new checkpoint table if it does not exist.
         *
         * @param allow   true if publisher is allowed to create a new
         *                checkpoint table for user if it does not exist;
         *                false otherwise, and subscription will fail if
         *                checkpoint table does not exist.
         *
         * @return this instance
         */
        public Builder setCreateNewCheckpointTable(boolean allow) {
            newCkptTable = allow;
            return this;
        }

        /**
         * Sets the stream mode.  If not set, the stream mode defaults to
         * {@link NoSQLStreamMode#FROM_CHECKPOINT}.
         *
         * @param mode the stream mode
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if specified stream mode is null.
         */
        public Builder setStreamMode(NoSQLStreamMode mode)
            throws IllegalArgumentException {

            if (mode == null) {
                throw new IllegalArgumentException(
                    "Stream mode cannot be null");
            }
            streamMode = mode;
            return this;
        }

        /**
         * @hidden
         *
         * Used in test only
         *
         * Set the reauthentication interval in ms. During subscription,
         * subscriber will reauthenticate itself periodically by renew the
         * token from KVStore. Subscriber can specify a time to
         * reauthenticate via this parameter. If the parameter is not set or
         * set to 0, subscriber will compute the time to reauthenticate from
         * the token expiration time. The default is 0.
         *
         * @param intervalMs reauthentication interval in ms
         *
         * @return this instance
         *
         * @throws IllegalArgumentException if interval is not positive.
         */
        public Builder setReAuthIntervalMs(long intervalMs)
            throws IllegalArgumentException {
            if (intervalMs <= 0) {
                throw new IllegalArgumentException(
                    "Reauthentication interval must be positive");
            }

            reAuthIntervalMs = intervalMs;
            return this;
        }
    }
}

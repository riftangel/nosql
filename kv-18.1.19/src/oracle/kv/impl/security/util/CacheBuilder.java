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
package oracle.kv.impl.security.util;

/**
 * CacheBuilder is used to construct cache instances.
 *<p>
 * The builder currently can build two cache instances based on LRU and LFU
 * eviction policy, which are thread safe themselves. To support entry lifetime
 * expiration, the value entry cached in these instances should extend the
 * CacheEntry class that save the entry create time. If cache instance enable
 * the cleanup process, a background TimeBasedCleanup task will be launched,
 * which periodically invalid the expired entry from cache.
 */
public class CacheBuilder {

    /**
     * Eviction policy.
     */
    public static enum EvictionPolicy {
        LRU, LFU;
    }

    /*
     * Prevent from instantiation
     */
    private CacheBuilder() {
    }

    /**
     * Build cache instance.
     *
     * Currently, only cache instance based on LRU and LFU eviction policy
     * are supported. To use LFU cache, configure the eviction policy is
     * required.
     *
     * @return cache instance
     */
    public static <K, V extends CacheEntry> Cache<K, V>
        build(CacheConfig config) {
        switch (config.getEvictionPolicy()) {
        case LRU:
            return new LruCacheImpl<K, V>(config);
        case LFU:
            /* TODO: Add LFU cache impl in future */
            throw new UnsupportedOperationException(
                "LFU cache is not yet supported in this version");
        default:
            throw new AssertionError();
        }
    }

    /**
     * A class for configuring the cache.
     *
     * Maintained two required cache configuration parameters.
     */
    public static class CacheConfig {

        private int capacity = -1;
        private long entryLifetime = -1;
        private boolean enableEviction = true;
        private EvictionPolicy evictionPolicy = EvictionPolicy.LRU;
        private float thresholdRatio = 1;
        private int evictionBatchSize = 1;

        /**
         * Set the maximum number of entries that are allowed to be maintained
         * in the cache. If the value is less than or equal to 0, the capacity
         * is unlimited. Default to be -1.
         *
         * @param newCapacity capacity of the cache, unlimited if less or equal
         * to 0
         * @return this
         */
        public CacheConfig capacity(final int newCapacity) {
            this.capacity = newCapacity;
            return this;
        }

        /**
         * The maximum duration that an entry may reside in the cache,
         * specified in units of milliseconds. If the value is less than or
         * equal to 0 then there is no fixed lifetime, and entry eviction from
         * the cache is based solely on eviction policy specified. Default to
         * -1.
         *
         * @param newLifetime entry lifetime in millisecond, unlimited if less
         * or equal to 0
         * @return this
         */
        public CacheConfig entryLifetime(final long newLifetime) {
            this.entryLifetime = newLifetime;
            return this;
        }

        /**
         * Defines the eviction policy, see {@link CacheBuilder.EvictionPolicy}
         *
         * @param policy the eviction policy, default to be LRU.
         * @return this
         */
        public CacheConfig evictionPolicy(final EvictionPolicy policy) {
            this.evictionPolicy = policy;
            return this;
        }

        /**
         * Enables the eviction of cache. See {@link
         * CacheBuilder.CacheConfig#evictionPolicy(CacheBuilder.EvictionPolicy)}
         * for the setting of eviction policy. Default to be true.
         * @return this
         */
        public CacheConfig enableEviction() {
            this.enableEviction = true;
            return this;
        }

        /**
         * Defines the threshold of the cache capacity on which the eviction
         * will take action. Used for LFU only. Default to 1.
         *
         * @param thdRatio threshold ratio
         * @return this
         */
        public CacheConfig thresholdRatio(final float thdRatio) {
            if (Float.compare(0, thdRatio) >= 0 ||
                Float.compare(thdRatio, 1) > 0 ) {
                throw new IllegalArgumentException(
                    "Threshold ratio must fall in range of (0, 1]");
            }
            this.thresholdRatio = thdRatio;
            return this;
        }

        /**
         * The batch size of entries that are evicted from cache each time.
         * Used for LFU cache only. Default to 1.
         * @param batchSize batch size
         * @return this
         */
        public CacheConfig evictionBatchSize(final int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException(
                    "Batch size must be positive");
            }
            this.evictionBatchSize = batchSize;
            return this;
        }

        public int getCapacity() {
            return capacity;
        }

        public long getEntryLifetime() {
            return entryLifetime;
        }

        public boolean isEvictionEnabled() {
            return enableEviction;
        }

        public float getThresholdRatio() {
            return thresholdRatio;
        }

        public int getEvictionBatchSize() {
            return evictionBatchSize;
        }

        public EvictionPolicy getEvictionPolicy() {
            return evictionPolicy;
        }
    }

    /**
     * The cache entry object.
     *
     * The base class for value entry of cache instances CacheBuilder
     * construct. It maintains the create time of entry to support entry
     * lifetime expiration mechanism.
     */
    public static class CacheEntry {

        /* The time at which the entry was created */
        private final long createTime;

        protected CacheEntry() {
            this.createTime = System.currentTimeMillis();
        }

        public long getCreateTime() {
            return createTime;
        }
    }

    /**
     * Background time-based cleanup.
     *
     * Once cache instance enable the background cleanup, this task will be
     * launched periodically to look up expired value entry and removed from
     * cache.
     *
     * Notes that the background cleanup task is intensive, since it is aim to
     * look up and remove all expired value entries.
     */
    public static abstract class TimeBasedCleanupTask implements Runnable {

        private volatile boolean terminated = false;

        private final long cleanupPeriodMs;

        private final Thread cleanUpThread;

        TimeBasedCleanupTask(final long cleanupPeriodMs) {
            this.cleanupPeriodMs = cleanupPeriodMs;
            cleanUpThread = new Thread(this, "CacheCleanUpThread");
            cleanUpThread.setDaemon(true);
            cleanUpThread.start();
        }

        /**
         * Attempt to stop the background activity for the cleanup.
         * @param wait if true, the the method attempts to wait for the
         * background thread to finish background activity.
         */
        void stop(boolean wait) {
            /* Set the flag to notify the run loop that it should exit */
            terminated = true;
            cleanUpThread.interrupt();

            if (wait) {
                try {
                    cleanUpThread.join();
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
        }

        @Override
        public void run() {
            while (true) {
                if (terminated) {
                    return;
                }

                try {
                    Thread.sleep(cleanupPeriodMs);
                } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
                if (terminated) {
                    return;
                }
                cleanup();
            }
        }

        /**
         * Cleanup the expired entry from cache.
         */
        abstract void cleanup();
    }
}

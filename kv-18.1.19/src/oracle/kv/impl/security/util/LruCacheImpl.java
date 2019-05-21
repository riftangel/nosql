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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import oracle.kv.impl.security.util.CacheBuilder.CacheConfig;
import oracle.kv.impl.security.util.CacheBuilder.CacheEntry;
import oracle.kv.impl.security.util.CacheBuilder.TimeBasedCleanupTask;

/**
 * Cache implementation based on LRU eviction policy.
 * <p>
 * In addition to the general least-used entry eviction, the implementation
 * also supports expired entry cleanup when a lookup is performed. If the
 * lifetime of the entry exceed the configured maximum entry lifetime, the
 * entry will be removed from the cache.
 * <p>
 * Once enable the background cleanup, the TimeBasedCleanup will be launched,
 * periodically lookup and remove the expired entries.
 *
 * @param <K> type of key in the cache
 * @param <V> type of value in the cache, which is required to be the subclass
 *            of CacheEntry
 */
public class LruCacheImpl<K, V extends CacheEntry>
    implements Cache<K, V> {

    /* Load factor for the session map */
    private static final float LOAD_FACTOR = 0.6f;

    /* The factor of calculation the eviction period */
    private static final int EVICT_PERIOD_FACTOR = 10;

    /* The maximum capacity for the cache */
    private final int capacity;

    /* Maximum lifetime for a value entry in ms */
    private volatile long entryLifetime;

    /* Map of key and value */
    private final LinkedHashMap<K, V> cacheMap;

    /* Background expired entry cleanup task */
    private TimeBasedCleanupTask cleanupTask;

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Construct the LRUCache instance.
     */
    LruCacheImpl(final CacheConfig config) {
        this.capacity = config.getCapacity();
        this.entryLifetime = config.getEntryLifetime();
        if (capacity > 0) {
            cacheMap = new LinkedHashMap<K, V>(capacity, LOAD_FACTOR,
                                               true /* access ordered */) {
                private static final long serialVersionUID = 1L;
                @Override
                protected boolean removeEldestEntry(
                    Map.Entry<K, V> entry) {
                    return size() > LruCacheImpl.this.capacity;
                }
            };
        } else {
            cacheMap = new LinkedHashMap<K, V>();
        }

        if (config.isEvictionEnabled() && entryLifetime > 0) {
            final long evictionPeriodMs = entryLifetime * EVICT_PERIOD_FACTOR;
            cleanupTask = new TimeBasedCleanupTask(evictionPeriodMs) {

                @Override
                void cleanup() {
                    if (!lock.tryLock()) {
                        return;
                    }
                    try {
                        final Iterator<Map.Entry<K, V>> iter =
                            cacheMap.entrySet().iterator();
                        while (iter.hasNext()) {
                            final Map.Entry<K, V> entry = iter.next();
                            if (isEntryExpire(entry.getValue())) {
                                    iter.remove();
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            };
        }
    }

    private boolean isEntryExpire(V value) {
        final long now = System.currentTimeMillis();

        if (entryLifetime > 0) {
            if (now > (value.getCreateTime() + entryLifetime)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(K key) {
        V value = null;
        lock.lock();
        try {
            value = cacheMap.get(key);
            if (value == null) {
                return null;
            }
            if (isEntryExpire(value)) {
                cacheMap.remove(key);
                return null;
            }
            return value;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(K key, V value) {
        lock.lock();
        try {
            cacheMap.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V invalidate(K key) {
        lock.lock();
        try {
            return cacheMap.remove(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getCapacity() {
        return this.capacity;
    }

    @Override
    public Set<V> getAllValues() {
        lock.lock();
        try {
            final Set<V> copy = new HashSet<V>();
            copy.addAll(cacheMap.values());
            return copy;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop(boolean wait) {
        if (cleanupTask != null) {
            cleanupTask.stop(wait);
        }
    }

    @Override
    public void setEntryLifetime(long lifeTimeInMillis) {
        this.entryLifetime = lifeTimeInMillis;
    }
}

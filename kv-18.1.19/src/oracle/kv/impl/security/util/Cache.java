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

import java.util.Set;

/**
 * Defines the interface to provide basic cache functionality.
 *
 * @param <K> type of key object
 * @param <V> type of value object
 */
public interface Cache<K, V> {

    /**
     * Returns the value associated with given key in the cache, or null if
     * no value cached for key. 
     * 
     * @param key
     * @return value
     */
    V get(K key);

    /**
     * Store the key value pair in the cache.
     *
     * If the cache contains the value associated with key, replace the old
     * value by given value 
     * 
     * @param key
     * @param value
     */
    void put(K key, V value);

    /**
     * Remove cached value for given key.
     * 
     * @param key
     * @return the previously cached value or null
     */
    V invalidate(K key);

    /**
     * Return cache maximum capacity.
     *
     * @return cache maximum capacity
     */
    int getCapacity();

    /**
     * Return a copy of all available values in the cache.
     *
     * @return all values.
     */
    Set<V> getAllValues();

    /**
     * Update entry lifetime with new value in milliseconds.
     * 
     * @param lifeTimeInMillis new lifetime in milliseconds
     */
    void setEntryLifetime(long lifeTimeInMillis);

    /**
     * Stop all background tasks of cache.
     * 
     * @param wait whether wait for the background task finish
     */
    void stop(boolean wait);
}

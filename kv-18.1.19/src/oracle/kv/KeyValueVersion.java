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

/**
 * Represents a key/value pair along with its version.
 *
 * <p>A KeyValueVersion instance is returned by methods such as {@link
 * KVStore#storeIterator(Direction, int)} and {@link
 * KVStore#multiGetIterator(Direction, int, Key, KeyRange, Depth)}.  The key,
 * version and value properties will always be non-null.</p>
 */
public class KeyValueVersion {

    private final Key key;
    private final Value value;
    private final Version version;

    /**
     * Internal use only
     * @hidden
     * Creates a KeyValueVersion with non-null properties.
     */
    public KeyValueVersion(final Key key,
                           final Value value,
                           final Version version) {
        assert key != null;
        assert value != null;
        assert version != null;
        this.key = key;
        this.value = value;
        this.version = version;
    }

    /**
     * Internal use only
     * @hidden
     * Creates a KeyValueVersion with non-null values for key and value
     */
    public KeyValueVersion(final Key key,
                           final Value value) {
        assert key != null;
        assert value != null;
        this.key = key;
        this.value = value;
        this.version = null;
    }

    /**
     * Returns the Key part of the KV pair.
     */
    public Key getKey() {
        return key;
    }

    /**
     * Returns the Value part of the KV pair.
     */
    public Value getValue() {
        return value;
    }

    /**
     * Returns the Version of the KV pair.
     */
    public Version getVersion() {
        return version;
    }

    /**
     * Internal use only
     * @hidden
     * Returns the expiration time of the record.This default method always
     * returns 0 (no expiration). A subclass may return a non-zero value.
     * See impl/api/KeyValueVersionInternal.
     * @since 4.0
     */
    public long getExpirationTime() {
        return 0L;
    }

    @Override
    public String toString() {
        return key.toString() + ' ' + value + ' ' + version;
    }
}

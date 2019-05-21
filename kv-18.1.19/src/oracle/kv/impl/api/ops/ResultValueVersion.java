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

package oracle.kv.impl.api.ops;

import oracle.kv.Version;

/**
 * Holds Value bytes, Version, and expiration time during result processing.
 */
public class ResultValueVersion {

    private byte[] valueBytes;
    private final Version version;
    private final long expirationTime;

    /**
     * Used internally to create an object with a value and version.
     */
    public ResultValueVersion(byte[] valueBytes,
                              Version version,
                              long expirationTime) {
        this.valueBytes = valueBytes;
        this.version = version;
        this.expirationTime = expirationTime;
    }

    void setValueBytes(byte[] valueBytes) {
        this.valueBytes = valueBytes;
    }

    public byte[] getValueBytes() {
        return valueBytes;
    }

    public Version getVersion() {
        return version;
    }

    public long getExpirationTime() {
        return expirationTime;
    }
}

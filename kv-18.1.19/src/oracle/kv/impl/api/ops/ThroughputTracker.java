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

/**
 * A read/write throughput tracker.
 */
public interface ThroughputTracker {

    /**
     * Adds the specified read bytes to this tracker instance. The value added
     * may be modified by rounding or other functions. The actual value
     * recorded (in KB) is returned.
     *
     * @param bytes the number of read bytes to record
     * @param isAbsolute true if the read operation used absolute consistency
     * @return the actual value (in KB) recorded
     */
    public int addReadBytes(int bytes, boolean isAbsolute);

    /**
     * Adds the specified write bytes to this tracker instance. The value added
     * may be modified by rounding or other functions. The actual value
     * recorded (in KB) is returned.
     *
     * @param bytes the number of write bytes to record
     * @param nIndexWrites the number of indexes (secondary DBs) which were
     * updated associated with the operation
     *
     * @return the actual value (in KB) recorded
     */
    public int addWriteBytes(int bytes, int nIndexWrites);
}

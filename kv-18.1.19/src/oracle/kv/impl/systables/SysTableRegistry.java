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

package oracle.kv.impl.systables;

/**
 * The registry for system tables. Every system table should have a
 * descriptor in the descriptors array.
 */
public class SysTableRegistry {

    /**
     * The system tables. (keep in alphabetical order for readability)
     */
    public static final SysTableDescriptor[] descriptors = {
                                            new IndexStatsLeaseDesc(),
                                            new PartitionStatsLeaseDesc(),
                                            new TableStatsIndexDesc(),
                                            new TableStatsPartitionDesc()
                                            };

    private SysTableRegistry() { }

    /**
     * Gets the descriptor instance for the specified class.
     *
     * For unit tests.
     */
    public static SysTableDescriptor getDescriptor(Class<?> c) {
        for (SysTableDescriptor desc : descriptors) {
            if (c.isInstance(desc)) {
                return desc;
            }
        }
        throw new IllegalStateException(
                                "Requesting unknown system table descriptor: " +
                                c.getSimpleName());
    }
}

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

import oracle.kv.impl.api.table.TableBuilder;

/**
 * Descriptor for the partition (table) stats lease system table.
 */
public class PartitionStatsLeaseDesc extends StatsLeaseDesc {

    public static final String TABLE_NAME =
            makeSystemTableName("PartitionStatsLease");

    /* The partition-specific columns in the lease table.  */
    public static final String COL_NAME_PARTITION_ID = "partitionId";

    /** Schema version of the table */
    private static final int TABLE_VERSION = 1;

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    protected int getCurrentSchemaVersion() {
        return TABLE_VERSION;
    }

    @Override
    protected void buildTable(TableBuilder builder) {
        builder.addInteger(COL_NAME_PARTITION_ID);
        super.buildTable(builder);
        builder.primaryKey(COL_NAME_PARTITION_ID);
        builder.shardKey(COL_NAME_PARTITION_ID);
    }
}

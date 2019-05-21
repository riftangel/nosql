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
import oracle.kv.impl.api.table.TableEvolver;

/**
 * Descriptor for the table stats system table.
 */
public class TableStatsPartitionDesc extends SysTableDescriptor {

    public static final String TABLE_NAME =
            makeSystemTableName("TableStatsPartition");

    /* All fields within table TableStatsPartition */
    public static final String COL_NAME_TABLE_NAME = "tableName";
    public static final String COL_NAME_PARTITION_ID = "partitionId";
    public static final String COL_NAME_SHARD_ID = "shardId";
    public static final String COL_NAME_COUNT = "count";
    public static final String COL_NAME_AVG_KEY_SIZE = "avgKeySize";

    /* Field for table size added in version 2 */
    public static final String COL_NAME_TABLE_SIZE = "tableSize";
    private static final int TABLE_SIZE_VERSION = 2;

    /** Schema version of the table */
    private static final int TABLE_VERSION = 2;

    TableStatsPartitionDesc() { }

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
        builder.addString(COL_NAME_TABLE_NAME);
        builder.addInteger(COL_NAME_PARTITION_ID);
        builder.addInteger(COL_NAME_SHARD_ID);
        builder.addLong(COL_NAME_COUNT);
        builder.addInteger(COL_NAME_AVG_KEY_SIZE);
        /* Table size added in v2 */
        builder.addLong(COL_NAME_TABLE_SIZE);
        builder.primaryKey(COL_NAME_TABLE_NAME, COL_NAME_PARTITION_ID);
        builder.shardKey(COL_NAME_TABLE_NAME, COL_NAME_PARTITION_ID);
    }

    @Override
    protected int evolveTable(TableEvolver ev, int schemaVersion) {
        if (schemaVersion < TABLE_SIZE_VERSION) {
            ev.addLong(COL_NAME_TABLE_SIZE);
            schemaVersion = TABLE_SIZE_VERSION;
        }
        return schemaVersion;
    }
}

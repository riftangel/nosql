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

package table;

import java.util.List;

import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

/**
 * Example 3: Shard keys
 *
 * This example uses a table that has a composite primary key and
 * a defined shard key.  This demonstrates the ability to ensure that
 * rows with the same shard key are stored in the same shard and are
 * therefore accessible in an atomic manner.  Such rows can also be
 * accessed using the various "multi*" API operations.
 */
public class ShardKeysExample extends BaseExample {
    @Override
    public String getShortDescription() {
        return "Shard key based read";
    }

    @Override
    public void setup() {
        executeDDL(
             "CREATE TABLE IF NOT EXISTS shardUsers  " +
             "(firstName STRING, " +
             " lastName STRING, " +
             " email STRING, " +
             " PRIMARY KEY (shard(lastName), firstName))");

    }

    @Override
    public void teardown() {
        executeDDL("DROP TABLE IF EXISTS shardUsers");
    }

    @Override
    public Void call() {

        Table table = getTable("shardUsers");

        /*
         * Insert rows into table
         * The primary key is (lastName, firstName) and the shard key
         * is lastName.
         */
        Row row = table.createRow();
        row.put("firstName", "Alex");
        row.put("lastName", "Robertson");
        row.put("email", "alero@email.com");
        getTableAPI().put(row, null, null);

        /*
         * Insert a second row with lastName Robertson.
         * Since the previous row is inserted with the same shard key, this row
         * and the previous are guaranteed to be stored on the same shard.
         */
        row = table.createRow();
        row.put("firstName", "Beatrix");
        row.put("lastName", "Robertson");
        row.put("email", "bero@email.com");
        getTableAPI().put(row, null, null);

        /*
         * Insert row with lastName Swanson.
         * Since this row has a different shard key the row may be stored in a
         * different shard.
         */
        row = table.createRow();
        row.put("firstName", "Bob");
        row.put("lastName", "Swanson");
        row.put("email", "bob.swanson@email.com");
        getTableAPI().put(row, null, null);

        /* Use a complete shard key to allow use of multiGet() */
        PrimaryKey key = table.createPrimaryKey();

        /* shard key is "lastName" */
        key.put("lastName", "Robertson");

        /*
         * Use the multiget function, to retrieve all the rows with the same
         * shard key.  The tableIterator() API call will also work but may not
         * be atomic.
         */
        List<Row> rows = getTableAPI().multiGet(key, null, null);

        /* Print the rows as JSON */
        System.out.println("\nRows with lastName Robertson via multiGet()");
        for (Row r: rows) {
            System.out.println(r.toJsonString(true));
        }

        return null;
    }

}

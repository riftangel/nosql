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

import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

/**
 * Example 4: Parent and Child tables
 *
 * This example demonstrates use of parent and child tables.  The parent
 * table is the shardUsers table used above and the child table is a
 * table of addresses for a given user, allowing definition of multiple
 * addresses for a user.
 */
public class ChildTableExample extends BaseExample {

    @Override
    public String getShortDescription() {
        return "Use of child tables";
    }

    @Override
    public void setup() {
        executeDDL(
                "CREATE TABLE IF NOT EXISTS shardUsers  " +
                "(firstName STRING, " +
                " lastName STRING, " +
                " email STRING, " +
                " PRIMARY KEY (shard(lastName), firstName))");

           executeDDL(
                "CREATE TABLE IF NOT EXISTS shardUsers.address  " +
                "(street STRING, " +
                " state STRING, " +
                " zip INTEGER, " +
                " addressName STRING, " +
                " addressID INTEGER, " +
                " PRIMARY KEY (addressID))");

           executeDDL(
                "CREATE INDEX IF NOT EXISTS addressIndex ON " +
                "shardUsers.address(addressName)");
    }

    @Override
    public void teardown() {
        executeDDL("DROP INDEX IF EXISTS addressIndex ON shardUsers.address");
        executeDDL ("DROP TABLE IF EXISTS shardUsers.address");
        executeDDL ("DROP TABLE IF EXISTS shardUsers");

    }

    @Override
    public Void call() {
        Table parentTable = getTable("shardUsers");
        Table childTable = parentTable.getChildTable("address");

        /*
         * Insert rows into the child table.  Create a parent table
         * record first.
         */
        /* Create a parent (user) row. */
        Row row = parentTable.createRow();
        row.put("firstName", "Robert");
        row.put("lastName", "Johnson");
        row.put("email", "bobbyswan@email.com");
        getTableAPI().put(row, null, null);

        /*
         * Create multiple child rows for the same parent.  To do this
         * create a row using the child table but be sure to set the
         * inherited parent table primary key fields (firstName and
         * lastName).
         */
        row = childTable.createRow();

        /* Parent key fields */
        row.put("firstName", "Robert");
        row.put("lastName", "Johnson");
        /* Child key */
        row.put("addressID", 1);
        /* Child data fields */
        row.put("Street", "Street Rd 132");
        row.put("State", "California");
        row.put("ZIP", 90011);
        row.put("addressName", "home");
        getTableAPI().putIfAbsent(row, null, null);

        /*
         * Reuse the Row to avoid repeating all fields.  This is safe.
         * This requires a new child key and data.
         */
        row.put("addressID", 2);
        row.put("Street", "Someplace Ave. 162");
        row.put("State", "California");
        row.put("ZIP", 90014);
        row.put("addressName", "work");
        getTableAPI().putIfAbsent(row, null, null);

        /*
         * Retrieve rows from the child table.
         * The child table primary key is a concatenation of its parent table's
         * primary key and it's own defined fields.
         */
        PrimaryKey key = childTable.createPrimaryKey();
        key.put("firstName", "Robert");
        key.put("lastName", "Johnson");
        key.put("addressID", 1);
        row = getTableAPI().get(key, null);

        System.out.println(row.toJsonString(true));

        /*
         * There is an index on the "addressName" field of the child table.
         * Use that to retrieve all "work" addresses.
         */
        Index index = childTable.getIndex("addressIndex");
        IndexKey indexKey = index.createIndexKey();
        indexKey.put("addressName", "work");

        TableIterator<Row> iter = null;
        try {
            iter =  getTableAPI().tableIterator(indexKey, null, null);
            System.out.println("\nAll \"work\" addresses");

            while(iter.hasNext()) {
                row = iter.next();
                System.out.println(row.toJsonString(true));
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
        return null;
    }

}

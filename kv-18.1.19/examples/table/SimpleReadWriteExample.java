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

import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

/**
 * Example: Simple read and write
 *
 * This example shows a simple use of tables, it uses a simple table
 * holding an id field as a primary key and 2 fields -- name and surname --
 * as Strings.  It does basic put and get.
 */
public class SimpleReadWriteExample extends BaseExample {

    @Override
    public String getShortDescription() {
        return "Basic read/write operations";
    }

    @Override
    public void setup() {
        executeDDL("CREATE TABLE IF NOT EXISTS simpleUsers  " +
             "(firstName STRING, " +
             " lastName STRING, " +
             " userID INTEGER, " +
             " PRIMARY KEY (userID))");
    }

    @Override
    public void teardown() {
        executeDDL("DROP TABLE IF EXISTS simpleUsers");
    }

    @Override
    public Void call() {
        Table table = getTable("simpleUsers");

        /* Insert row */
        Row row = table.createRow();
        row.put("userID", 1);
        row.put("firstName", "Alex");
        row.put("lastName", "Robertson");
        getTableAPI().put(row, null, null);

        /* Insert row, if it is not already in the table */
        row = table.createRow();
        row.put("userID", 2);
        row.put("firstName", "John");
        row.put("lastName", "Johnson");
        getTableAPI().putIfAbsent(row, null, null);

        /* Insert row only if it is already in the table */
        row = table.createRow();
        row.put("userID", 2);
        row.put("firstName", "John");
        row.put("lastName", "Jameson");
        getTableAPI().putIfPresent(row, null, null);

        /*
         * Read a row from table using the primary key
         */

        /* Create a primary key and assign the field value */
        PrimaryKey key = table.createPrimaryKey();
        key.put("userID", 1);

        /* Get the matching row */
        row = getTableAPI().get(key, new ReadOptions(null, 0, null));

        /* Print the full row as JSON */
        System.out.println(row.toJsonString(true));

        /* Access a specific field */
        System.out.println("firstName field as JSON: " +
                           row.get("firstName").toJsonString(false));

        return null;
    }
}

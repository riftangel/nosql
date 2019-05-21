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
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

/**
 * Example: Index read and write
 *
 * This example demonstrates definition and use of indexes by creating
 * a simple single-field index and a 2-field composite index. It creates
 * some records and accesses them via index operations.
 */
public class IndexReadAndWrite extends BaseExample {

    @Override
    public String getShortDescription() {
        return "Index based read and write";
    }

    @Override
    public void setup() {
        executeDDL(
                "CREATE TABLE IF NOT EXISTS simpleUsers  " +
                 "(firstName STRING, " +
                 " lastName STRING, " +
                 " userID INTEGER, " +
                 " PRIMARY KEY (userID))");

        System.out.println("Creating index simpleIndex");
        executeDDL(
            "CREATE INDEX IF NOT EXISTS simpleIndex ON " +
            "simpleUsers(firstName)");

        System.out.println("Creating index compoundIndex");
        executeDDL(
            "CREATE INDEX IF NOT EXISTS compoundIndex ON " +
            "simpleUsers(lastName, firstName)");
    }

    @Override
    public void teardown() {

        /*
         * NOTE: dropping a table will implicitly drop its indexes.
         * The DROP INDEX statements are here for demonstrative purposes.
         */
        System.out.println("Dropping index simpleIndex");
        executeDDL("DROP INDEX IF EXISTS simpleIndex ON simpleUsers");

        System.out.println("Dropping index compoundIndex");
        executeDDL("DROP INDEX IF EXISTS compoundIndex ON simpleUsers");

        System.out.println("Dropping table simpleUsers");
        executeDDL("DROP TABLE IF EXISTS simpleUsers");
    }

    /**
     * This example uses 2 indexes on the table "simpleUsers."  One is a
     * simple, single-field index on "firstName" and the other is a
     * composite index on "lastName, firstName".
     */
    @Override
    public Void call() {
        Table table = getTable("simpleUsers");

        /* Insert new rows */
        Row row = table.createRow();
        row.put("userID", 3);
        row.put("firstName", "Joel");
        row.put("lastName", "Robertson");
        getTableAPI().putIfAbsent(row, null, null);

        row = table.createRow();
        row.put("userID", 4);
        row.put("firstName", "Bob");
        row.put("lastName", "Jameson");
        getTableAPI().putIfAbsent(row, null, null);

        row = table.createRow();
        row.put("userID", 5);
        row.put("firstName", "Jane");
        row.put("lastName", "Jameson");
        getTableAPI().putIfAbsent(row, null, null);

        row = table.createRow();
        row.put("userID", 6);
        row.put("firstName", "Joel");
        row.put("lastName", "Jones");
        getTableAPI().putIfAbsent(row, null, null);

        TableIterator<Row> iter = null;
        try {

            /*
             * Use the simple index on firstName to retrieve all users with
             * the firstName of "Joel"
             */
            Index simple_index = table.getIndex("simpleIndex");

            /*
             * Create an IndexKey and assign the firstName value.
             * The IndexKey works similarly to a PrimaryKey and only allows
             * assignment of fields that are part of the index.
             */
            IndexKey simpleIndexKey = simple_index.createIndexKey();
            simpleIndexKey.put("firstName", "Joel");

            /* Get the matching rows */
            iter = getTableAPI().tableIterator(simpleIndexKey, null, null);

            /* Print rows as JSON */
            System.out.println("\nUsers with firstName Joel");
            while(iter.hasNext()) {
                System.out.println(iter.next().toJsonString(true));
            }

            /*
             * TableIterator instances must be closed to release resources
             */
            iter.close();
            iter = null;

            /*
             * Use the composite index to match both last and firstname.
             */
            Index compound_index = table.getIndex("compoundIndex");

            /*
             * Create and initialize the IndexKey
             */
            IndexKey compositeIndexKey = compound_index.createIndexKey();
            compositeIndexKey.put("firstName", "Bob");
            compositeIndexKey.put("lastName", "Jameson");

            /* Get the matching rows */
            iter = getTableAPI().tableIterator(compositeIndexKey, null, null);

            /* Print rows as JSON */
            System.out.println("\nUsers with full name Bob Jameson");
            while(iter.hasNext()) {
                System.out.println(iter.next().toJsonString(true));
            }
            iter.close();
            iter = null;

            /*
             * Use the composite index to match all rows with
             * lastName "Jameson".
             */
            compositeIndexKey = compound_index.createIndexKey();
            compositeIndexKey.put("lastName", "Jameson");

            /* Get the matching rows */
            iter = getTableAPI().tableIterator(compositeIndexKey, null, null);

            /* Print rows as JSON */
            System.out.println("\nAll users with last name Jameson");
            while(iter.hasNext()) {
                System.out.println(iter.next().toJsonString(true));
            }
        } finally {
            /*
             * Make sure that the TableIterator is closed.
             */
            if (iter != null) {
                iter.close();
            }
        }
        return null;
    }

}

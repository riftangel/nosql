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

import oracle.kv.table.ArrayValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

/**
 * Example: Complex fields
 *
 * This example demonstrates definition and use of complex data types --
 * RECORD, MAP and ARRAY. It creates a table with an example of each and
 * does put and get operations on records and individual complex fields.
 */
public class ComplexFieldExample extends BaseExample {
    @Override
    public String getShortDescription() {
        return "Use of complex fields";
    }

    @Override
    public void setup() {
        executeDDL
            ("CREATE TABLE IF NOT EXISTS complexUsers " +
             "(name RECORD (firstName STRING, lastName STRING), " +
             " likes ARRAY(STRING), " +
             " optionalInformation MAP(STRING), " +
             " userID INTEGER, " +
             " PRIMARY KEY (userID))");
    }

    @Override
    public void teardown() {
       executeDDL ("DROP TABLE IF EXISTS complexUsers ");
    }

    /**
     * Example 5: Complex fields
     *
     * This example demonstrates how to create, populate and read complex
     * fields (Array, Map, Record) in a table.
     */
    @Override
    public Void call() {
        Table table = getTable("complexUsers");

        /*
         * Insert data into complex fields.
         */
        Row row = table.createRow();

        /*
         * The putRecord function creates a RecordValue instance that
         * is then populated with its own fields.
         * The "name" field is a record with 2 fields --
         * firstName and lastName.
         */
        RecordValue recordValue = row.putRecord("name");
        recordValue.put("firstName", "Bob");
        recordValue.put("lastName", "Johnson");

        /*
         * The putArray function returns an ArrayValue instance which
         * is then populated.  In this case the array is an array of
         * String values.
         */
        ArrayValue arrayValue = row.putArray("likes");

        /* use the add() overload that takes an array as input */
        arrayValue.add(new String[]{"sports", "movies"});

        /*
         * The putMap function returns a MapValue instance which is then
         * populated.  In this table the map is a map of String values.
         */
        MapValue mapValue = row.putMap("optionalInformation");
        mapValue.put("email", "bob.johnson@email.com");
        mapValue.put("group", "work");

        /* Insert id */
        row.put("userID", 1);
        getTableAPI().putIfAbsent(row, null, null);

        /*
         * Retrieve information from the table, examining the complex
         * fields.
         */
        /* Get row matching the requested primary key. */
        PrimaryKey key = table.createPrimaryKey();
        key.put("userID", 1);
        row = getTableAPI().get(key, null);

        /*
         * Read the "name" record field.  Use asRecord() to cast.  If
         * this is done on a field that is not a record an exception is
         * thrown.
         */
        RecordValue record = row.get("name").asRecord();

        /* The RecordValue can be output as JSON */
        System.out.println("\nName record: " + record.toJsonString(false));

        /*
         * Read the "likes" array field.
         * Use the get function to return the field and cast it to
         * ArrayValue using the asArray function.
         */
        ArrayValue array = row.get("likes").asArray();

        /* The ArrayValue can be output as JSON */
        System.out.println("\nlikes array: " + array.toJsonString(false));

        /*
         * Read the map field.
         */
        MapValue map = row.get("optionalInformation").asMap();

        /* The MapValue can be output as JSON */
        System.out.println("\noptionalInformation map: " +
                           map.toJsonString(false));

        /* Print the entire row as JSON */
        System.out.println("\n The full row:\n" + row.toJsonString(true));

        return null;
    }
}

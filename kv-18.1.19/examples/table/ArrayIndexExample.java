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
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

    /**
     * Example 6: Secondary index on array
     *
     * This example demonstrates use of a secondary index on an array.
     * An important thing to note about indexes on arrays is that a
     * independent index entry is generated for each value in the array.
     * This can lead to an explosion of entries as well as potential for
     * duplication in results (e.g. if the same array value repeats in
     * the same row).
     *
     * This example uses the same table as the complex field example.
     * There is an index on the "likes" array.
     */
public class ArrayIndexExample extends BaseExample {

    @Override
    public String getShortDescription() {
        return "Secondary index on array";
    }

    @Override
    public void setup() {
        executeDDL(
         "CREATE TABLE IF NOT EXISTS complexUsers " +
         "(name RECORD (firstName STRING, lastName STRING), " +
         " likes ARRAY(STRING), " +
         " optionalInformation MAP(STRING), " +
         " userID INTEGER, " +
         " PRIMARY KEY (userID))");

        executeDDL(
             "CREATE INDEX IF NOT EXISTS arrayIndex ON complexUsers(likes[])");
    }

    @Override
    public void teardown() {
       executeDDL("DROP INDEX IF EXISTS arrayIndex ON complexUsers");
       executeDDL ("DROP TABLE IF EXISTS complexUsers ");
    }

    @Override
    public Void call() {
        final String tableName = "complexUsers";

        Table table = getTable(tableName);

        /*
         * Insert rows into table.
         */
        Row row = table.createRow();
        RecordValue recordValue = row.putRecord("name");
        recordValue.put("firstName", "Joseph");
        recordValue.put("lastName", "Johnson");
        ArrayValue arrayValue = row.putArray("likes");
        arrayValue.add(new String[]{"sports"});
        MapValue mapValue = row.putMap("optionalInformation");
        mapValue.put("email", "jjson@email.com");
        row.put("userID", 2);
        getTableAPI().putIfAbsent(row, null, null);

        row = table.createRow();
        recordValue = row.putRecord("name");
        recordValue.put("firstName", "Burt");
        recordValue.put("lastName", "Nova");
        arrayValue = row.putArray("likes");
        arrayValue.add(new String[]{"sports", "movies", "technology"});
        mapValue = row.putMap("optionalInformation");
        row.put("userID", 3);
        getTableAPI().putIfAbsent(row, null, null);

        /*
         * Retrieve information using the secondary index
         * Retrieve all the rows with the value "movies" in the "likes" array
         * field.
         */
        Index index = table.getIndex("arrayIndex");

        /*
         * Create an IndexKey to request the information.
         */
        IndexKey indexKey = index.createIndexKey();
        indexKey.put("likes[]", "movies"); /* match "movies" */

        TableIterator<Row> iter = getTableAPI().tableIterator(indexKey, null, null);
        System.out.println("\nUsers who \"like\" movies");
        try {
            while (iter.hasNext()) {
                row = iter.next();
                System.out.println(row.toJsonString(true));
            }
            iter.close();
            iter = null;

            /*
             * Do it again with "sports"
             */
            indexKey.put("likes[]", "sports");
            iter = getTableAPI().tableIterator(indexKey, null, null);
            System.out.println("\nUsers who \"like\" sports");
            while (iter.hasNext()) {
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

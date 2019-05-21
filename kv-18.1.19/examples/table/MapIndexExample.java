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

import java.util.Random;

import oracle.kv.table.FieldRange;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MapValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

/**
 * This is an example of using map indexes.  It creates a simple table with a
 * map of properties field.  An index is created on that map.  The example
 * demonstrates how to create and use such an index.  The index is on the
 * map key plus the map values.  This 2-component index allows the following
 * index scan operations:
 * <ol>
 * <li>Specific key plus specific value
 * <li>Specific key plus range of values
 * <li>Range of keys
 * </ol>
 * It is not possible to search a range of values without a specific key
 * because of the way multi-component indexes are stored.
 *<p>
 * The map information used is artificial and comprises a small set of
 * keys that are known.  Using a map allows addition or removal of a new
 * key string without schema evolution, which is an advantage over using a
 * fixed record instead.
 *<p>
 */
public class MapIndexExample extends BaseExample {
    /*
     * DDL statement to create the table named Properties
     */
    private static final String CREATE_DDL = "CREATE TABLE IF NOT EXISTS " +
        "Properties(id INTEGER, PRIMARY KEY(id), properties MAP(DOUBLE))";

    /*
     * DDL statement to clean data
     */
    private static final String CLEAN_DDL = "DROP TABLE IF EXISTS Properties";

    /*
     * DDL statement to create the index PropertyIndex on table, Properties
     */
    private static final String INDEX_DDL = "CREATE INDEX IF NOT EXISTS " +
        "PropertyIndex ON Properties(properties.keys(), properties.values())";

    /*
     * The number of rows to use in the example.  A small number is used to
     * limit the output.
     */
    private static final int NUM_ROWS = 50;

    /*
     * A static array of key strings for the properties.  If a set of
     * strings is truly fixed it is better to use a record than a map, as
     * it is more type-safe and storage is more efficient.
     * The values for each key are generated randomly.
     */
    private static final String keys[] = {
        "height", "width", "length", "volume", "area",
        "circumference", "radius", "diameter", "weight"
    };

	@Override
	public String getShortDescription() {
		return "Use of map indexes";
	}

	@Override
	protected void setup() {
    	executeDDL(CREATE_DDL);
    	executeDDL(INDEX_DDL);
    }

    @Override
	protected void teardown() {
        executeDDL(CLEAN_DDL);
    }

    @Override
	public Void call() {
        Table table = getTable("Properties");
        populate(table);
        query(table);
        return null;
    }

   /*
     * Do some index operations.
     */
    private void query(Table table) {

        Index index = table.getIndex("PropertyIndex");

        /*
         * Find a specify key + value in the index.  To do this,
         * 1. create an IndexKey
         * 2. put values into the IndexKey to use for the search.
         */

        IndexKey ikey = index.createIndexKey();

        /*
         * The index is on both key and value, put the key to match, then value
         */
        ikey.put("properties.keys()", "height");
        ikey.put("properties.values()", 1.0);

        /*
         * Iterate using default values.  A key iterator is used as these
         * are more efficient.  They do not have to read the Row value.  If
         * fields from the row that are not indexed or part of the primary
         * key are desired then a row iterator should be used.  In this example
         * there are no such fields.
         */
        TableIterator<KeyPair> iter =
            getTableAPI().tableKeysIterator(ikey, null, null);
        displayResults(iter, "height",
                       "Results for key \"height\" and value 1.0");
        iter.close();

        /*
         * Now, match all rows that have some key "weight".  This will match
         * all rows because they all have this key string in the map.
         */
        ikey.clear();
        ikey.put("properties.keys()", "weight");
        iter = getTableAPI().tableKeysIterator(ikey, null, null);
        displayResults(iter, "weight", "Results for key \"weight\"");
        iter.close();

        /*
         * This time, create a range of values for the key "width".  Depending
         * on the random generation multiple (or no) rows may match.
         */
        FieldRange range = index.createFieldRange("properties.values()");
        range.setStart(10.0, true); /* inclusive */
        range.setEnd(100.0, true); /* inclusive */

        /*
         * Refresh the IndexKey to add just the key field.
         */
        ikey.clear();
        ikey.put("properties.keys()", "width");

        iter = getTableAPI().tableKeysIterator(ikey,
        		range.createMultiRowOptions(),
                                      null);
        displayResults(iter, "width",
                       "Results for key \"width\", value 10 - 100");
        iter.close();
    }

    /*
     * Displays a truncated version of each row in the result set.  Display
     * includes:
     * 1.  the row's id (primary key)
     * 2.  the value of the map entry from the index.
     *
     * Note that the values will be sorted based on the value, because index
     * scans are sorted by default.
     */
    private void displayResults(TableIterator<KeyPair> iter,
                                String key, String desc) {
        System.out.println(desc);
        if (!iter.hasNext()) {
            System.out.println("\tNo matching entries (this is expected)");
            return;
        }
        while (iter.hasNext()) {
            KeyPair pair = iter.next();
            String id = pair.getPrimaryKey().get("id").toString();
            String val = pair.getIndexKey().get("properties.values()").toString();
            System.out.println("\tid: " + id + ", " + key + " : " + val);
        }
    }

    /*
     * Populates the table with some data.  The use of putIfAbsent() will
     * cause this code to not overwrite existing rows with the same primary
     * key.
     */
    private void populate(Table table) {
        Random random = new Random();
        final double start = 0.0;
        final double end = 1000.0;
        for (int i = 0; i < NUM_ROWS; i++) {
            Row row = table.createRow();
            row.put("id", i); /* use the index as the primary key */
            MapValue map = row.putMap("properties");

            /* generate random double values for values */
            for (String key : keys) {
                double value = start + (random.nextDouble() * (end - start));
                map.put(key, value);
            }

            /*
             * Use default options, putIfAbsent.
             */
            getTableAPI().putIfAbsent(row, null, null);
        }
    }

}

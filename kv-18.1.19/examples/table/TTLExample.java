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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;


/**
 * Example: TTL
 *
 * This example demonstrates basic use of the TTL (time-to-live)
 * feature. It demonstrates how to set and update TTL on a per-record
 * basis, as well as returning a record's expiration time for records
 * returned from a get operation.
 */
public class TTLExample extends BaseExample {
    private static String TABLE_NAME = "movement";

    @Override
    public String getShortDescription() {
        return "Time-To-Live for records";
    }

    @Override
    public void setup() {
        executeDDL
            ("CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
             " (x FLOAT, y FLOAT, ts LONG, PRIMARY KEY (ts))");

     }

    @Override
    public void teardown() {
       executeDDL ("DROP TABLE IF EXISTS " + TABLE_NAME);

    }

    /**
     * Demonstrates TTL usage during insert or update of records.
     */
    @Override
    public Void call() {
        Table table = getTable(TABLE_NAME);
        TableAPI api = getTableAPI();

         // Insert row with non-zero TTL
        Row row = table.createRow();
        row.put("ts", System.currentTimeMillis());
        row.setTTL(TimeToLive.ofDays(20));
        api.put(row, null, null);
        printTTL("Row (with TTL)", row, TimeUnit.DAYS);

        // Inert row with no TTL
        row = table.createRow();
        row.put("ts", System.currentTimeMillis());
        row.setTTL(TimeToLive.ofDays(0));
        api.put(row, null, null);
        printTTL("Row (with no TTL)", row, TimeUnit.DAYS);

        // Update row with TTL modified
        row = table.createRow();
        row.put("ts", System.currentTimeMillis());
        row.setTTL(TimeToLive.ofDays(20));
        getTableAPI().put(row, null, null);
        printTTL("Row (on insert)", row, TimeUnit.DAYS);
        row.setTTL(TimeToLive.ofDays(10));
        api.putIfPresent(row, null, new WriteOptions()
                         /* must be explicitly set to true to update TTL */
                         .setUpdateTTL(true));
        printTTL("Row (TTL modified on update)", row, TimeUnit.DAYS);

        // Update row with TTL unmodified
        row = table.createRow();
        row.put("ts", System.currentTimeMillis());
        row.setTTL(TimeToLive.ofDays(20));
        getTableAPI().put(row, null, null);
        printTTL("Row (on insert)", row, TimeUnit.DAYS);
        row.setTTL(TimeToLive.ofDays(10));
        api.putIfPresent(row, null, new WriteOptions()
                         /* will not change TTL. This is default */
                         .setUpdateTTL(false));
        printTTL("Row (TTL unmodified on update)", row, TimeUnit.DAYS);

        return null;
    }

    /**
     * Prints the absolute expiration time for a given record as well as
     * duration from current clock time before tyhe record is cleaned.
     * @param rowQualifier a qualifier for the record
     * @param row a record
     * @param unit time unit for duration
     */
    void printTTL(String rowQualifier, Row row, TimeUnit unit) {
        long expirationTime = row.getExpirationTime();
        if (expirationTime == 0) {
            System.err.println(rowQualifier + " does not expire");
            return;
        }
        TimeToLive ttl = TimeToLive.fromExpirationTime(
            expirationTime, System.currentTimeMillis());
        System.err.println(rowQualifier + " expires in " + ttl +
                           " at " + new Date(expirationTime));
    }
}

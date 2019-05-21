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

import oracle.kv.KVStore;
import oracle.kv.StatementResult;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.RecordValue;

/**
 * Example: Execute Aggregate Functions supported by Oracle NoSQL
 * 
 * Before running the example ensure to have run 
 * SQLParentChildJoinsExamples.cli
 * 
 */
public class AggregateFunction extends BaseExample {

    @Override
    public String getShortDescription() {
        return "Execute Aggregate functions on table columns";
    }

    @Override
    public void setup() {
        
    }

    @Override
    public void teardown() {
        executeDDL("DROP TABLE IF EXISTS JSONPersons.quotes");
    }

    @Override
    public Void call() {
        count();
        average();
        sum();
        max();

        return null;
    }

    private void count() {
        KVStore store = getKVStore();

        System.out.println
        ("\n  A count of all records in tables:");

        /* Preparation and execution in one step. */
        StatementResult result =
            store.executeSync("SELECT count(*) as Count from "
            		+ "JSONPersons.quotes jq");

        /* Iterate the results of the query */
        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        /* Close the result */
        result.close();
    }

    private void average() {
        KVStore store = getKVStore();

        System.out.println("\n  Average of all insurnace premiums"
        		+ "for a given Person:");

        PreparedStatement preparedStatement =
            store.prepare("SELECT avg(jq.quotes.charges) as Average FROM "
            		+ "JSONPersons.quotes jq "
            		+ "where jq.id = 1");

        /* Preparation and execution in one step. */
        StatementResult result = store.executeSync(preparedStatement);

        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        result.close();
    }

    private void sum() {
    	KVStore store = getKVStore();

        System.out.println("\n  Sum of all the charges for"
        		+ "insurance premiums to be paid by a Person:");

        PreparedStatement preparedStatement =
        		store.prepare("SELECT sum(jq.quotes.charges) as Sum FROM "
        				+ "JSONPersons.quotes jq "
        				+ "where jq.id = 1");


        /* Preparation and execution in one step. */
        StatementResult result = store.executeSync(preparedStatement);

        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        result.close();
    }
    
    private void max() {
    	KVStore store = getKVStore();

        System.out.println("\n  Maximum Insurance Premium "
        		+ "paid by a Person:");

        PreparedStatement preparedStatement =
        		store.prepare("SELECT max(jq.quotes.charges) as Max FROM "
        				+ "JSONPersons.quotes jq "
        				+ "where jq.id = 1");


        /* Preparation and execution in one step. */
        StatementResult result = store.executeSync(preparedStatement);

        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        result.close();
    }
}

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
 * Example: Execute join queries across Parent-Child tables
 *
 * This example showcases the API for select queries: how to execute 
 * join query and have access to the results.
 * 
 * Before running the example ensure to have run 
 * SQLParentChildJoinsExamples.cli
 * 
 */
public class ParentChildJoin extends BaseExample {

    @Override
    public String getShortDescription() {
        return "Select Join queries across Parent-Child Tables";
    }

    @Override
    public void setup() {
        
    }

    @Override
    public void teardown() {
        executeDDL("DROP TABLE IF EXISTS JSONPersons.quotes");
        executeDDL("DROP TABLE IF EXISTS JSONPersons");
    }

    @Override
    public Void call() {
        simpleJoin();
        joinOnClause();
        joinWithPredicate();

        return null;
    }

    private void simpleJoin() {
        KVStore store = getKVStore();

        System.out.println
        ("\n  A simple select from parent and chile tables:");

        /* Preparation and execution in one step. */
        StatementResult result =
            store.executeSync("SELECT j.id, j.person, jq.quotes FROM NESTED TABLES "
            		+ "(JSONPersons j descendants (JSONPersons.quotes jq))");

        /* Iterate the results of the query */
        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        /* Close the result */
        result.close();
    }

    private void joinOnClause() {
        KVStore store = getKVStore();

        System.out.println("\n  Parent Child Join with an On Clause:");

        PreparedStatement preparedStatement =
            store.prepare("SELECT j.id, j.person, jq.quotes FROM NESTED TABLES "
            		+ "(JSONPersons j descendants (JSONPersons.quotes jq ON "
            		+ "jq.quotes.charges > 16000))");

        /* Preparation and execution in one step. */
        StatementResult result = store.executeSync(preparedStatement);

        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        result.close();
    }

    private void joinWithPredicate() {
    	KVStore store = getKVStore();

        System.out.println("\n  Parent Child Join with predicate:");

        PreparedStatement preparedStatement =
            store.prepare("SELECT j.id, j.person, jq.quotes FROM NESTED TABLES "
            		+ "(JSONPersons j descendants (JSONPersons.quotes jq))  "
            		+ "where j.person.age < 35");

        /* Preparation and execution in one step. */
        StatementResult result = store.executeSync(preparedStatement);

        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        result.close();
    }
}

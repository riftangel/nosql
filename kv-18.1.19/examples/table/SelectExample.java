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
import oracle.kv.query.BoundStatement;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

/**
 * Example: Execute select queries and read the results.
 *
 * This example showcases the API for select queries: how to execute a query
 * and have access to the results, how to reuse compiled queries, how to
 * use variables in parametrized queries and how to access the execution plans.
 */
public class SelectExample extends BaseExample {

    @Override
    public String getShortDescription() {
        return "Select queries";
    }

    @Override
    public void setup() {
        executeDDL("CREATE TABLE IF NOT EXISTS simpleUsers  " +
             "(firstName STRING, " +
             " lastName STRING, " +
             " userID INTEGER, " +
             " PRIMARY KEY (userID))");

        insertData();
    }

    @Override
    public void teardown() {
        executeDDL("DROP TABLE IF EXISTS simpleUsers");
    }

    @Override
    public Void call() {
        simple();
        multipleExecutions();
        bindVariables();
        queryExecutionPlan();

        return null;
    }

    private void simple() {
        KVStore store = getKVStore();

        System.out.println("\n  A simple select all:");

        /* Preparation and execution in one step. */
        StatementResult result =
            store.executeSync("SELECT * FROM simpleUsers");

        /* Iterate the results of the query */
        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        /* Close the result */
        result.close();
    }

    private void multipleExecutions() {
        KVStore store = getKVStore();

        System.out.println("\n  Single prepare with multiple executions:");

        PreparedStatement preparedStatement =
            store.prepare("SELECT lastName FROM simpleUsers");

        /* Execute the first time */
        StatementResult result1 = store.executeSync(preparedStatement);

        for (RecordValue record : result1) {
            FieldValue value = record.get(0);
            System.out.println("      - Last name: " + value.asString().get());
        }

        /* Add extra data */
        System.out.println("    Add extra data");
        Table table = getTable("simpleUsers");
        Row row = table.createRow();
        row.put("userID", 100);
        row.put("firstName", "Jim");
        row.put("lastName", "Anderson");
        getTableAPI().put(row, null, null);

        /* Execute the same statement on the current data. */
        StatementResult result2 = store.executeSync(preparedStatement);

        /* Iterate over the records in the result */
        for (RecordValue record : result2) {
            FieldValue value = record.get(0);
            System.out.println("      - Last name: " + value.asString().get());
        }

        result1.close();
        result2.close();
    }

    private void bindVariables() {
        KVStore store = getKVStore();

        System.out.println("\n  Single prepare with parameterized multiple " +
            "executions:");

        PreparedStatement preparedStatement =
            store.prepare("DECLARE $id INTEGER;" +
                "SELECT * FROM simpleUsers WHERE userID = $id");

        /* Create a statement that accepts values for query variables */
        BoundStatement boundStatement =
            preparedStatement.createBoundStatement();

        /* Bind the $id variable */
        boundStatement.setVariable("$id", 1);

        /* Execute with $id = 1 */
        StatementResult result = store.executeSync(boundStatement);

        for (RecordValue record : result) {
            FieldValue value = record.get("firstName");
            if (value.isString()) {
                System.out.println(value.asString().get());
            }
        }

        result.close();

        /* Execute with $id = 2 */
        boundStatement.setVariable("$id", 2);
        result = store.executeSync(boundStatement);

        for (RecordValue record : result) {
            FieldValue value = record.get("firstName");
            if (value.isString()) {
                System.out.println(value.asString().get());
            }
        }

        result.close();
    }

    private void queryExecutionPlan() {
        KVStore store = getKVStore();

        System.out.println("\n  Print query execution plans:");

        String query = "DECLARE $firstName STRING; $lastName STRING;" +
            "SELECT * FROM simpleUsers WHERE lastName = $lastName AND " +
            "firstName = $firstName";

        PreparedStatement preparedStatement =
            store.prepare(query);

        /* Print the query plan which uses the primary index:
         * simpleUsers via primary index */
        System.out.println(preparedStatement);

        /* Create index on firstName and lastName */
        System.out.println("    Create index on first and last name columns.");
        store.executeSync("CREATE INDEX indx_firstName_lastName ON " +
            "simpleUsers (firstName, lastName)");

        /* Re-prepare to make use of the new index */
        preparedStatement =
            store.prepare(query);

        /* Print the query plan which uses the newly available index:
         * simpleUsers via covering index indx_firstName_lastName */
        System.out.println(preparedStatement);
    }

    private void insertData() {
        Table table = getTable("simpleUsers");

        /* Insert rows.

           Note: At this time INSERT, UPDATE, DELETE are not supported using
            the query API. These operations are still done using the
            TableAPI operations.
         */
        Row row = table.createRow();

        for (int i = 0; i < 5; i++) {
            row.put("userID", i);
            row.put("firstName", "first-name-" + i);
            row.put("lastName", "last-name-" + i);
            getTableAPI().put(row, null, null);
        }
    }
}

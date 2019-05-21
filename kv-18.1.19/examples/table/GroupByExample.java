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
import oracle.kv.table.RecordValue;

/**
 * Example: Group By clause supported by Oracle NoSQL
 * 
 * Before running the example ensure to have run 
 * SQLParentChildJoinsExamples.cli
 * 
 */
public class GroupByExample extends BaseExample {

    @Override
    public String getShortDescription() {
        return "Execute Aggregate functions on table columns";
    }

    @Override
    public void setup() {
    	executeDDL("CREATE INDEX idx on JSONPersons(person.age as INTEGER)");
        
    }

    @Override
    public void teardown() {
        executeDDL("DROP INDEX idx on JSONPersons");
    }

    @Override
    public Void call() {
        groupBy();

        return null;
    }

    private void groupBy() {
        KVStore store = getKVStore();

        System.out.println
        ("\n  A count of all records in tables Grouped By :");

        /* Preparation and execution in one step. */
        StatementResult result =
            store.executeSync("SELECT count(*) as Count from "
            		+ "JSONPersons jq GROUP BY jq.person.age");

        /* Iterate the results of the query */
        for (RecordValue record : result) {
            /* Print the full record as JSON */
            System.out.println(record.toJsonString(true));
        }

        /* Close the result */
        result.close();
    }
}

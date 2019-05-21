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
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/**
 * Derivable base implementation for table examples.
 *
 *
 */
public abstract class BaseExample implements Example {

    /* Handles onto the store */
    private KVStore store;

    @Override
    public void init(KVStore s) {
        store = s;
        setup();
    }

    /**
     * Returns a table of given name.
     */
    protected Table getTable(String table) {
        return getTableAPI().getTable(table);
    }

    /**
     * Returns the store object.
     */
    protected KVStore getKVStore() {
        return store;
    }

    /**
     * Returns api for table operations.
     */
    protected TableAPI getTableAPI() {
        return store.getTableAPI();
    }

    /**
     * Executes given DDL statement synchronously against a store.
     * @param ddl a DDL statement
     */
    protected void executeDDL(String ddl) {
        System.out.println(ddl);
        try {
            store.executeSync(ddl);
        } catch (Exception ex) {
            //ex.printStackTrace();
        }
    }

    @Override
    public void end() {
        teardown();
    }

    /**
     * Gets a short description fit for printing on console.
     * Returns simple name of this class.
     */
    @Override
    public String getShortDescription() {
        return this.getClass().getSimpleName();
    }

    /**
     * Create requisite tables and indices.
     */
    protected abstract void setup();

    /**
     * Drop tables and indices {@link #setup() created}
     * earlier.
     */
    protected abstract void teardown();
}

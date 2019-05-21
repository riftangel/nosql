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

package oracle.kv.impl.api.table;

/**
 * TableMetadataHelper
 *
 * Interface used by code that needs to acquire TableImpl instances. It will
 * be extended to also allow callbacks to manipulate table and namespace names.
 */

public interface TableMetadataHelper {

    /**
     * Returns the table object, if it exists, null if not.
     * @param tableName the table name, which may be a child table name of the
     * format parent.child
     * @return the table object, or null if it does not exist
     */
    public TableImpl getTable(String namespace, String tableName);

    /**
     * Returns the table object, if it exists, null if not.
     * @param tablePath the table name in component form where each component
     * is a parent or child table name.
     * @return the table object, or null if it does not exist
     */
    public TableImpl getTable(String namespace, String[] tablePath);
}

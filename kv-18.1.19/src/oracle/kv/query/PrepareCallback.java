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

package oracle.kv.query;

import oracle.kv.table.Table;
import oracle.kv.impl.api.table.TableMetadataHelper;

/**
 * @hidden
 * Internal use only
 *
 * This is an interface that is used by query compilation to call back to users
 * when it encounters specific syntax in a query. At this time the callbacks
 * occur for:
 * 1. table name
 * 2. "if not exists" on table creation
 * 3. "if exists" on table drop
 *
 * This interface is only used by query preparation.
 *
 * Others can be added as required, possibly including setter methods.
 *
 * @since 4.4
 */
public interface PrepareCallback {
    public enum QueryOperation {
        CREATE_TABLE,
        ALTER_TABLE,
        DROP_TABLE,
        CREATE_INDEX,
        DROP_INDEX,
        SELECT,
        UPDATE,
        CREATE_USER,
        ALTER_USER,
        DROP_USER,
        CREATE_ROLE,
        DROP_ROLE,
        GRANT,
        REVOKE,
        DESCRIBE,
        SHOW
    }

    /**
     * Called when the table name is encountered during parsing.
     *
     * @param tableName is a fully-qualified name of the format
     * tableName[.childName]*
     */
    public void tableName(String tableName);

    /**
     * Called when the index name is encountered during parsing.
     *
     * @param indexName is a simple string
     */
    public void indexName(String indexName);

    /**
     * Called when the query operation type is encountered during parsing.
     *
     * @param queryOperation the operation type
     */
    public void queryOperation(QueryOperation queryOperation);

    /**
     * Called when "if not exists" if encountered during parsing. The call will
     * not happen if that clause is not found, which means that the users must
     * assume a default state
     */
    public void ifNotExistsFound();

    /**
     * Called when "if exists" if encountered during parsing. The call will
     * not happen if that clause is not found, which means that the users must
     * assume a default state
     */
    public void ifExistsFound();

    /**
     * Returns true if the caller wants the prepare to complete if possible.
     * If the caller is only interested in the callback information and
     * not the result, return false. The latter will work in the absence
     * of table metadata required for complete preparation.
     */
    public boolean prepareNeeded();

    /**
     * Called when create FULLTEXT index. The call will not happen if statement
     * is not to create FULLTEXT index, which means that the users must assume
     * a default state
     */
    public void isTextIndex();

    /**
     * Called when a new table has been constructed during query
     * compilation. This happens only if the operation is
     * CREATE_TABLE or ALTER_TABLE. It allows the caller to do additional
     * validation of the table if desired.
     */
    public void newTable(Table table);

    /**
     * Returns an instance of TableMetadataHelper if available to the
     * implementing class. This allows preparation of DDL statements with
     * child tables or schema evolution to succeed in the prepare-only case.
     */
    public TableMetadataHelper getMetadataHelper();
}

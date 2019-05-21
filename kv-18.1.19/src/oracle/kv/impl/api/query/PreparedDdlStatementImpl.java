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

package oracle.kv.impl.api.query;

import java.util.Map;

import oracle.kv.impl.api.KVStoreImpl;

import oracle.kv.AsyncExecutionHandle;
import oracle.kv.StatementResult;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.FieldDef;
import oracle.kv.table.RecordDef;

/**
 * Encapsulates a "prepared" DDL query, which is just the original query string.
 * That is, DDL queries are never prepared locally (at the client or query
 * coordinator); they are always routed to an admin.
 */
public class PreparedDdlStatementImpl
    implements PreparedStatement,
               InternalStatement {

    private final char[] query;
    private final String namespace;


    public PreparedDdlStatementImpl(char[] query, String namespace) {
        this.query = query;
        this.namespace = namespace;
    }

    public String getNamespace() {
        return namespace;
    }

    /*
     * DDL operations do not (at this time) have metadata.  This may change for
     * some of them (e.g. show, describe, etc) - those which have logical
     * tabular views.
     */
    @Override
    public RecordDef getResultDef() {
        return null;
    }

    @Override
    public Map<String, FieldDef> getVariableTypes() {
        throw new IllegalArgumentException("Cannot bind a DDL query");
    }

    @Override
    public FieldDef getVariableType(String variableName) {
        throw new IllegalArgumentException("Cannot bind a DDL query");
    }

    @Override
    public BoundStatement createBoundStatement() {
        throw new IllegalArgumentException("Cannot bind a DDL query");
    }

    @Override
    public String toString() {
        return new String(query);
    }

    public char[] getQuery() {
        return query;
    }

    @Override
    public StatementResult executeSync(
        KVStoreImpl store,
        ExecuteOptions options) {

        /* DDL queries are routed to an admin */
        return store.executeSync(query, options);
    }

    @Override
    public AsyncExecutionHandle executeAsync(KVStoreImpl store,
                                             ExecuteOptions options) {
        throw new UnsupportedOperationException(
            "Asynchronous execution of DDL statements is not supported");
    }
}

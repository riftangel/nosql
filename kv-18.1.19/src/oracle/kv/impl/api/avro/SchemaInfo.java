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

package oracle.kv.impl.api.avro;

import org.apache.avro.Schema;

/**
 * A simple immutable property container for a Schema and its ID.  Also refers
 * to the previous version of the schema.
 *
 * @see SchemaCache
 */
@SuppressWarnings("javadoc")
class SchemaInfo {
    private final Schema schema;
    private final int id;
    private final SchemaInfo prevVersion;
    /* Mutable property for schema pointer in C API.  May be zero. */
    private volatile long cSchema;

    SchemaInfo(Schema schema, int id, SchemaInfo prevVersion) {
        this.schema = schema;
        this.id = id;
        this.prevVersion = prevVersion;
    }

    Schema getSchema() {
        return schema;
    }

    int getId() {
        return id;
    }

    SchemaInfo getPreviousVersion() {
        return prevVersion;
    }

    long getCSchema() {
        return cSchema;
    }

    void setCSchema(long cSchema) {
        this.cSchema = cSchema;
    }
}

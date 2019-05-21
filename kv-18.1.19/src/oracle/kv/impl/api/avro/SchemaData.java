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
 * Holds the stored data for a schema, and includes schema plus metadata.
 */
public class SchemaData {

    private final AvroSchemaMetadata metadata;
    private final Schema schema;

    public SchemaData(AvroSchemaMetadata metadata, Schema schema) {
        this.metadata = metadata;
        this.schema = schema;
    }

    public AvroSchemaMetadata getMetadata() {
        return metadata;
    }

    public Schema getSchema() {
        return schema;
    }
}

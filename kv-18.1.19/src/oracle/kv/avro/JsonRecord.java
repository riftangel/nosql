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

package oracle.kv.avro;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

/**
 * A JsonRecord a represents an Avro object as a {@link Schema} along with a
 * {@link JsonNode}. It is used with a {@link JsonAvroBinding}.
 *
 * @see JsonAvroBinding
 * @see AvroCatalog#getJsonBinding getJsonBinding
 * @see AvroCatalog#getJsonMultiBinding getJsonMultiBinding
 *
 * @since 2.0
 *
 * @deprecated as of 4.0, use the table API instead.
 */
@Deprecated
public class JsonRecord {
    private final JsonNode jsonNode;
    private final Schema schema;

    /**
     * Creates a JsonRecord from a {@link Schema} and a {@link JsonNode}.
     */
    public JsonRecord(JsonNode jsonNode, Schema schema) {
        this.jsonNode = jsonNode;
        this.schema = schema;
    }

    /**
     * Returns the {@link JsonNode} for this JsonRecord.
     */
    public JsonNode getJsonNode() {
        return jsonNode;
    }

    /**
     * Returns the Avro {@link Schema} for this JsonRecord.
     */
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof JsonRecord)) {
            return false;
        }
        final JsonRecord o = (JsonRecord) other;
        return jsonNode.equals(o.jsonNode) && schema.equals(o.schema);
    }

    @Override
    public int hashCode() {
        return jsonNode.hashCode();
    }

    @Override
    public String toString() {
        return jsonNode.toString() + "\nSchema: " + schema.toString();
    }
}

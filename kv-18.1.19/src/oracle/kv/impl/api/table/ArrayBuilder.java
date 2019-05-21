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

import java.io.IOException;

import oracle.kv.table.ArrayDef;
import oracle.kv.impl.util.JsonUtils;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;

/**
 * ArrayBuilder
 */
public class ArrayBuilder extends CollectionBuilder {

    ArrayBuilder(String description) {
        super(description);
    }

    ArrayBuilder() {
    }

    @Override
    public String getBuilderType() {
        return "Array";
    }

    @Override
    public ArrayDef build() {
        if (field == null) {
            throw new IllegalArgumentException
                ("Array has no field and cannot be built");
        }
        return FieldDefFactory.createArrayDef((FieldDefImpl)field, description);
    }

    @Override
    TableBuilderBase generateAvroSchemaFields(Schema schema,
                                              String name1,
                                              JsonNode defaultValue,
                                              String desc) {

        Schema elementSchema = schema.getElementType();
        super.generateAvroSchemaFields(elementSchema,
                                       elementSchema.getName(),
                                       null, /* no default */
                                       elementSchema.getDoc());
        return this;
    }

    /*
     * Create a JSON representation of the array field
     **/
    public String toJsonString(boolean pretty) {
        ObjectWriter writer = JsonUtils.createWriter(pretty);
        ObjectNode o = JsonUtils.createObjectNode();
        ArrayDefImpl tmp = FieldDefFactory.createArrayDef((FieldDefImpl)field,
                                                          description);
        tmp.toJson(o);
        try {
            return writer.writeValueAsString(o);
        } catch (IOException ioe) {
            return ioe.toString();
        }
    }
}

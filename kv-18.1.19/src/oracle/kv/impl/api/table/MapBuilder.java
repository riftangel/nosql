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

import oracle.kv.impl.util.JsonUtils;
import oracle.kv.table.MapDef;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;

/**
 * MapBuilder
 */
public class MapBuilder extends CollectionBuilder {

    MapBuilder(String description) {
        super(description);
    }

    MapBuilder() {
    }

    @Override
    public String getBuilderType() {
        return "Map";
    }

    @Override
    public MapDef build() {
        if (field == null) {
            throw new IllegalArgumentException
                ("Map has no field and cannot be built");
        }
        return FieldDefFactory.createMapDef((FieldDefImpl)field, description);
    }

    @Override
    TableBuilderBase generateAvroSchemaFields(Schema schema,
                                              String name1,
                                              JsonNode defaultValue,
                                              String desc) {

        Schema elementSchema = schema.getValueType();
        super.generateAvroSchemaFields(elementSchema,
                                       elementSchema.getName(),
                                       null, /* no default */
                                       elementSchema.getDoc());
        return this;
    }

    /*
     * Create a JSON representation of the map field
     **/
    public String toJsonString(boolean pretty) {
        ObjectWriter writer = JsonUtils.createWriter(pretty);
        ObjectNode o = JsonUtils.createObjectNode();
        MapDefImpl tmp = FieldDefFactory.createMapDef((FieldDefImpl)field,
                                                      description);
        tmp.toJson(o);
        try {
            return writer.writeValueAsString(o);
        } catch (IOException ioe) {
            return ioe.toString();
        }
    }
}

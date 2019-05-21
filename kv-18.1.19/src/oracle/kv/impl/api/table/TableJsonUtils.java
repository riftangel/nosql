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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * This class provides utilities for interaction with Jackson JSON processing
 * libraries, as well as helpful JSON operations, for use in implementing
 * Tables.
 */
public class TableJsonUtils extends JsonUtils {

    /*
     * There are a number of string constants used for JSON input/output
     * as well as for generating proper Avro schema names.  These are all
     * defined here for simplicity.
     */

    /*
     * Tables JSON input/output format labels
     */
    final static String PARENT = "parent";
    final static String OWNER = "owner";
    final static String SHARDKEY = "shardKey";
    final static String PRIMARYKEY = "primaryKey";
    final static String CHILDREN = "children";
    final static String FIELDS = "fields";
    final static String JSON_VERSION = "json_version";
    final static String R2COMPAT = "r2compat";
    final static String CHILDTABLES = "childTables";
    final static String SYSTABLE = "sysTable";
    final static String DESC = "comment";
    final static String NULLABLE = "nullable";
    final static String MIN = "min";
    final static String MAX = "max";
    final static String MIN_INCL = "min_inclusive";
    final static String MAX_INCL = "max_inclusive";
    final static String COLLECTION = "collection";
    final static String ENUM_NAME = "enum_name";
    final static String TTL = "ttl";
    final static String PKEY_SIZES = "primaryKeySizes";
    /* related to indexes */
    final static String INDEXES = "indexes";
    final static String ANNOTATIONS = "annotations";
    final static String PROPERTIES = "properties";
    final static String TABLE = "table";

    /*
     * These are used for construction of JSON nodes representing FieldDef
     * instances.  Some are used for both tables and Avro schemas.
     *
     * Avro and Tables
     */
    final static String NAME = "name";
    final static String NAMESPACE = "namespace";
    final static String TYPE = "type";
    final static String TYPES = "types";
    final static String DEFAULT = "default";
    final static String ENUM_VALS = "symbols";
    final static String NULL = "null";

    /*
     * Avro type strings
     */
    final static String RECORD = "record";
    final static String ENUM = "enum";
    final static String ARRAY = "array";
    final static String MAP = "map";
    final static String INT = "int";
    final static String LONG = "long";
    final static String STRING = "string";
    final static String BOOLEAN = "boolean";
    final static String DOUBLE = "double";
    final static String FLOAT = "float";
    final static String BYTES = "bytes";
    final static String FIXED = "fixed";
    final static String FIXED_SIZE = "size";
    final static String TIMESTAMP = "timestamp";
    final static String TIMESTAMP_PRECISION = "precision";

    private static final DecoderFactory decoderFactory =
        DecoderFactory.get();
    private static final EncoderFactory encoderFactory =
        EncoderFactory.get();

    public static ObjectMapper getObjectMapper() {
        return mapper;
    }

    static JsonFactory getJsonFactory() {
        return mapper.getJsonFactory();
    }

    static DecoderFactory getDecoderFactory() {
        return decoderFactory;
    }

    static EncoderFactory getEncoderFactory() {
        return encoderFactory;
    }

    /**
     * Translate the specified Base64 string into a byte array.
     */
    public static String encodeBase64(byte[] buf) {
        return mapper.convertValue(buf, String.class);
    }

    /**
     * Decode the specified Base64 string into a byte array.
     */
    public static byte[] decodeBase64(String str) {
        return mapper.convertValue(str, byte[].class);
    }

    /*
     * From here down are utility methods used to help construct tables
     * and fields from JSON.
     */

    /**
     * This is a generic method used to construct FieldDef objects from
     * the JSON representation of a table.  This code could be spread out
     * across the various classes in per-class fromJson() methods but there
     * is no particular advantage in doing so.  Code is better shared in one
     */
    static FieldDefImpl fromJson(ObjectNode node) {
        String nameString = getStringFromNode(node, NAME, false);
        String descString = getStringFromNode(node, DESC, false);
        String minString = getStringFromNode(node, MIN, false);
        String maxString = getStringFromNode(node, MAX, false);
        String sizeString = getStringFromNode(node, FIXED_SIZE, false);
        String typeString = getStringFromNode(node, TYPE, true);
        String precisionString = getStringFromNode(node, TIMESTAMP_PRECISION,
                                                   false);

        FieldDef.Type type = FieldDef.Type.valueOf(typeString.toUpperCase());
        switch (type) {
        case INTEGER:
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.integerDef;
            }
            return new IntegerDefImpl
                (descString,
                minString != null ? Integer.valueOf(minString) : null,
                maxString != null ? Integer.valueOf(maxString) : null);
        case LONG:
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.longDef;
            }
            return new LongDefImpl
                (descString,
                 minString != null ? Long.valueOf(minString) : null,
                 maxString != null ? Long.valueOf(maxString) : null);
        case DOUBLE:
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.doubleDef;
            }
            return new DoubleDefImpl
                (descString,
                 minString != null ? Double.valueOf(minString) : null,
                 maxString != null ? Double.valueOf(maxString) : null);
        case FLOAT:
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.floatDef;
            }
            return new FloatDefImpl
                (descString,
                 minString != null ? Float.valueOf(minString) : null,
                 maxString != null ? Float.valueOf(maxString) : null);
        case STRING:
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.stringDef;
            }
            Boolean minInclusive = getBoolean(node, MIN_INCL);
            Boolean maxInclusive = getBoolean(node, MAX_INCL);
            return new StringDefImpl
                (descString, minString, maxString, minInclusive, maxInclusive);
        case NUMBER:
            if (descString == null) {
                return FieldDefImpl.numberDef;
            }
            return new NumberDefImpl(descString);
        case BINARY:
            if (descString == null) {
                return FieldDefImpl.binaryDef;
            }
            return new BinaryDefImpl(descString);
        case FIXED_BINARY:
            int size = (sizeString == null ? 0 : Integer.valueOf(sizeString));
            return new FixedBinaryDefImpl(nameString, size, descString);
        case BOOLEAN:
            if (descString == null) {
                return FieldDefImpl.booleanDef;
            }
            return new BooleanDefImpl(descString);
        case TIMESTAMP:
            int precision = (precisionString == null ?
                             TimestampDefImpl.DEF_PRECISION :
                             Integer.valueOf(precisionString));
            return new TimestampDefImpl(precision, descString);
        case ARRAY:
        case MAP:
            JsonNode jnode = node.get(COLLECTION);
            if (jnode == null) {
                throw new IllegalArgumentException
                    ("Map and Array require a collection object");
            }
            FieldDefImpl elementDef = fromJson((ObjectNode) jnode);
            if (type == FieldDef.Type.ARRAY) {
                return FieldDefFactory.createArrayDef(elementDef, descString);
            }
            return  FieldDefFactory.createMapDef(elementDef, descString);
        case RECORD:
            JsonNode fieldsNode = node.get(FIELDS);
            if (fieldsNode == null) {
                throw new IllegalArgumentException
                    ("Record is missing fields object");
            }
            final RecordBuilder builder =
                TableBuilder.createRecordBuilder(nameString, descString);
            ArrayNode arrayNode = (ArrayNode) fieldsNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                ObjectNode o = (ObjectNode) arrayNode.get(i);
                String fieldName = getStringFromNode(o, NAME, true);
                builder.fromJson(fieldName, o);
            }
            try {
                return (FieldDefImpl) builder.build();
            } catch (Exception e) {
                throw new IllegalArgumentException
                ("Failed to build record from JSON, field name: " + nameString );
            }
        case ENUM: {
            JsonNode valuesNode = node.get(ENUM_VALS);
            if (valuesNode == null) {
                throw new IllegalArgumentException
                    ("Enumeration is missing values");
            }
            arrayNode = (ArrayNode) valuesNode;
            String enumName = getStringFromNode(node, ENUM_NAME, true);
            String values[] = new String[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                values[i] = arrayNode.get(i).asText();
            }
            return new EnumDefImpl(enumName, values, descString);
        }
        case JSON:
            if (descString == null) {
                return FieldDefImpl.jsonDef;
            }
            return new JsonDefImpl(descString);
        case ANY:
            return FieldDefImpl.anyDef;
        case ANY_ATOMIC:
            return FieldDefImpl.anyAtomicDef;
        case ANY_RECORD:
            return FieldDefImpl.anyRecordDef;
        case ANY_JSON_ATOMIC:
            return FieldDefImpl.anyJsonAtomicDef;
        case EMPTY:
        default:
            throw new IllegalArgumentException
                ("Cannot construct FieldDef type from JSON: " + type);
        }
    }

    /**
     * Adds an index definition from the ObjectNode (JSON) to the table.
     */
    static void indexFromJsonNode(ObjectNode node, TableImpl table) {
        ArrayNode fields = (ArrayNode) node.get(FIELDS);
        ArrayList<String> fieldStrings = new ArrayList<String>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            fieldStrings.add(fields.get(i).asText());
        }
        ArrayList<FieldDef.Type> typeStrings = null;
        ArrayNode types = (ArrayNode) node.get(TYPES);
        if (types != null) {
            typeStrings = new ArrayList<FieldDef.Type>(types.size());
            for (int i = 0; i < fields.size(); i++) {
                JsonNode typeNode = types.get(i);
                if (typeNode.isNull()) {
                    typeStrings.add(null);
                } else {
                    typeStrings.add(FieldDef.Type.valueOf(typeNode.asText()));
                }
            }
        }

        String name = getStringFromNode(node, NAME, true);
        String desc = getStringFromNode(node, DESC, false);
        Map<String,String> annotations = getMapFromNode(node, ANNOTATIONS);
        Map<String,String> properties = getMapFromNode(node, PROPERTIES);
        table.addIndex(new IndexImpl(name, table, fieldStrings, typeStrings,
                                     annotations, properties, desc));
    }

    /**
     * Build Table from JSON string
     *
     * NOTE: this format was test-only in R3, but export/import has made use
     * of it for R4. This means that changes must be made carefully and if
     * state is added to a table or index it needs to be reflected in the JSON
     * format.
     */
    public static TableImpl fromJsonString(String jsonString,
                                           TableImpl parent) {
        JsonNode rootNode = null;
        try {
            rootNode = getObjectMapper().readTree(jsonString);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("IOException parsing Json: " +
                                               ioe);
        }
        return fromJson(rootNode, parent);
    }

    private static TableImpl fromJson(JsonNode rootNode,
                                      TableImpl parent) {
        /*
         * Create a TableBuilder for the table.
         */
        final String namespace = (rootNode.get(NAMESPACE) != null ?
                                  rootNode.get(NAMESPACE).asText() : null);

        TableBuilder tb = null;
        if (rootNode.get(SYSTABLE) != null) {
            tb = TableBuilder.createSystemTableBuilder(rootNode.get(NAME).
                                                       asText());
        } else {
            tb = TableBuilder.createTableBuilder
                (namespace, rootNode.get(NAME).asText(),
                 null, /* handle description below */
                 parent, true);
        }

        /*
         * Create the primary key and shard key lists
         */
        tb.primaryKey(makeListFromArray(rootNode, PRIMARYKEY));
        if (parent == null) {
            tb.shardKey(makeListFromArray(rootNode, SHARDKEY));
        }

        if (rootNode.get(DESC) != null) {
            tb.setDescription(rootNode.get(DESC).asText());
        }

        if (rootNode.get(R2COMPAT) != null) {
            tb.setR2compat(true);
        }

        /*
         * The current version doesn't put this field if there is no owner, but
         * older versions may have put null. Handle that.
         */
        if (rootNode.get(OWNER) != null &&
            !rootNode.get(OWNER).isNull()) {
            tb.setOwner(ResourceOwner.fromString(rootNode.get(OWNER).asText()));
        }

        if (rootNode.get(TTL) != null) {
            String ttlString = rootNode.get(TTL).asText();
            String[] ttlArray = ttlString.split(" ");
            if (ttlArray.length != 2) {
                throw new IllegalArgumentException(
                    "Invalid value for ttl string: " + ttlString);
            }
            tb.setDefaultTTL(TimeToLive.createTimeToLive(
                          Long.parseLong(ttlArray[0]),
                          TimeUnit.valueOf(ttlArray[1])));
        }

        if (rootNode.get(PKEY_SIZES) != null) {
            ArrayNode pks = (ArrayNode) rootNode.get(PKEY_SIZES);
            List<String> pkey = tb.getPrimaryKey();
            assert pks.size() == pkey.size();
            for (int i = 0; i < pks.size(); i++) {
                int size = pks.get(i).asInt();
                if (size > 0) {
                    tb.primaryKeySize(pkey.get(i), size);
                }
            }
        }

        /*
         * Add fields.
         */
        ArrayNode arrayNode = (ArrayNode) rootNode.get(FIELDS);
        for (int i = 0; i < arrayNode.size(); i++) {
            ObjectNode node = (ObjectNode) arrayNode.get(i);
            String fieldName =
                getStringFromNode(node, NAME, true);
            if (parent == null ||
                !(parent).isKeyComponent(fieldName)) {
                tb.fromJson(fieldName, node);
            }
        }

        TableImpl newTable = tb.buildTable();

        /*
         * Add indexes if present
         */
        if (rootNode.get(INDEXES) != null) {
            arrayNode = (ArrayNode) rootNode.get(INDEXES);
            for (int i = 0; i < arrayNode.size(); i++) {
                ObjectNode node = (ObjectNode) arrayNode.get(i);
                indexFromJsonNode(node, newTable);
            }
        }

        /*
         * If child tables are fully specified here, add them. Note that
         * the JSON output format may include a simple list of child table
         * names. If that is the case there is nothing that can be done on
         * input because the child tables cannot be constructed from that
         * list, so they are ignored.
         */
        if (rootNode.get(CHILDTABLES) != null) {
            arrayNode = (ArrayNode) rootNode.get(CHILDTABLES);
            for (int i = 0; i < arrayNode.size(); i++) {
                ObjectNode node = (ObjectNode) arrayNode.get(i);
                TableImpl child = fromJson(node, newTable);
                newTable.getMutableChildTables().put(child.getName(), child);
            }
        }

        return newTable;
    }

    /**
     * Entry point for turning a TableImpl into a String
     */
    static String toJsonString(TableImpl table,
                               boolean pretty,
                               boolean includeChildren) {
        ObjectWriter writer = JsonUtils.createWriter(pretty);
        ObjectNode o = JsonUtils.createObjectNode();
        toJsonString(table, o, includeChildren);

        /*
         * Format the JSON into a string
         */
        try {
            return writer.writeValueAsString(o);
        } catch (IOException ioe) {
            return ioe.toString();
        }
    }

    /**
     * Create a JSON representation of the table and format
     * @param includeChildren if true, embed the JSON representation of the
     * child tables in the same JSON document.
     *
     * The general pattern is for optional fields to be left out entirely.
     * A format version is written so that the deserialization code can handle
     * changes in format over time.
     */
    public static void toJsonString(TableImpl table,
                                    ObjectNode o,
                                    boolean includeChildren) {
        /*
         * This is the serialization version of the table. It is new as of
         * release 4.4 and should increment whenever the format changes.
         */
        o.put(JSON_VERSION, 1);
        o.put(TYPE, "table");
        o.put(NAME, table.getName());

        if (table.getNamespace() != null) {
            o.put(NAMESPACE, table.getNamespace());
        }

        if (table.getDefaultTTL() != null) {
            o.put(TTL, table.getDefaultTTL().toString());
        }

        if (table.getOwner() != null) {
            o.put(OWNER, table.getOwner().toString());
        }

        if (table.isSystemTable()) {
            o.put(SYSTABLE, true);
        }

        if (table.isR2compatible()) {
            o.put(R2COMPAT, true);
        }

        if (table.getDescription() != null) {
            o.put(DESC, table.getDescription());
        }

        if (table.getParent() != null) {
            o.put(PARENT, table.getParent().getName());
        }
        ArrayNode key = o.putArray(SHARDKEY);
        for (String fieldName : table.getShardKeyInternal()) {
            key.add(fieldName);
        }
        key = o.putArray(PRIMARYKEY);
        for (String fieldName : table.getPrimaryKeyInternal()) {
            key.add(fieldName);
        }
        if (table.getPrimaryKeySizes() != null) {
            key = o.putArray(PKEY_SIZES);
            for (int size : table.getPrimaryKeySizes()) {
                key.add(size);
            }
        }

        /* Display limits if this is a top level table */
        if (table.isTop()) {
            final TableLimits limits = table.getTableLimits();
            if (limits != null) {
                limits.putLimits(o);
            }
        }

        /*
         * Add child tables as an array of table names.
         */
        final Map<String, Table> childTables = table.getChildTables();
        if (!childTables.isEmpty()) {
            ArrayNode childArray = o.putArray(CHILDREN);
            for (Map.Entry<String, Table> childEntry : childTables.entrySet()) {
                childArray.add(childEntry.getKey());
            }
        }

        /*
         * Add the fields.
         */
        table.getFieldMap().putFields(o);

        /*
         * Add indexes
         */
        final Map<String, Index> indexes = table.getIndexes();
        if (indexes.size() != 0) {
            ArrayNode indexArray = o.putArray(INDEXES);
            for (Map.Entry<String, Index> indexEntry : indexes.entrySet()) {
                IndexImpl impl = (IndexImpl) indexEntry.getValue();
                impl.toJsonNode(indexArray.addObject());
            }
        }

        if (!childTables.isEmpty() && includeChildren) {
            ArrayNode childArray = o.putArray(CHILDTABLES);
            for (Map.Entry<String, Table> childEntry : childTables.entrySet()) {
                ObjectNode cnode = childArray.addObject();
                toJsonString((TableImpl)childEntry.getValue(), cnode,
                             includeChildren);
            }
        }
    }

    public static BigDecimal jsonParserGetDecimalValue(JsonParser parser)
        throws IOException {

        assert(parser != null);

        try {
            return parser.getDecimalValue();
        } catch (NumberFormatException nfe) {
            throw new JsonParseException("Malformed numeric value: '" +
                parser.getText(), parser.getCurrentLocation(), nfe);
        }
    }

    private static List<String> makeListFromArray(JsonNode node,
                                                  String fieldName) {
        ArrayNode arrayNode = (ArrayNode) node.get(fieldName);
        if (arrayNode == null) {
            return null;
        }
        ArrayList<String> keyList = new ArrayList<String>(arrayNode.size());
        for (int i = 0; i < arrayNode.size(); i++) {
            keyList.add(i, arrayNode.get(i).asText());
        }
        return keyList;
    }

    /**
     * Returns the string value of the named field in the ObjectNode
     * if it exists, otherwise null.
     * @param node the containing node
     * @param name the name of the field in the node
     * @param required true if the field must exist
     * @return the string value of the field, or null
     * @throws IllegalArgumentException if the named field does not
     * exist in the node and required is true
     */
    private static String getStringFromNode(ObjectNode node,
                                            String name,
                                            boolean required) {
        JsonNode jnode = node.get(name);
        if (jnode != null) {
            return jnode.asText();
        } else if (required) {
            throw new IllegalArgumentException
                ("Missing required node in JSON table representation: " + name);
        }
        return null;
    }

    /**
     * Returns a Map<String, String> of the named field in the ObjectNode
     * if it exists, otherwise null.
     * @param node the containing node
     * @param name the name of the field in the node
     * @return a map of the name/value pairs in the object, or null
     * @throws IllegalArgumentException if the node exists and it's not an
     * ObjectNode
     */
    private static Map<String,String> getMapFromNode(ObjectNode node,
                                                     String name) {
        JsonNode jnode = node.get(name);
        if (jnode == null) {
            return null;
        }
        if (!(jnode instanceof ObjectNode)) {
            throw new IllegalArgumentException("Node is not an ObjectNode: " +
                                               name);
        }
        Map<String, String> map = new HashMap<String, String>();
        Iterator<Map.Entry<String, JsonNode>> iter = jnode.getFields();
        while (iter.hasNext()) {
            Map.Entry<String, JsonNode> entry = iter.next();
            map.put(entry.getKey(), entry.getValue().asText());
        }
        return map;
    }
}

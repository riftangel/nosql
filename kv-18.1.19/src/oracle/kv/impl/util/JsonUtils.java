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

package oracle.kv.impl.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Provides utilities to aid in interaction with Jackson JSON processing
 * libraries as well as helpful JSON operations.
 */
public class JsonUtils {

    protected static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Returns a JSON parser that parses input from a reader.
     *
     * @param in the reader
     * @return the parser
     * @throws IOException if there is a problem parsing the input
     */
    public static JsonParser createJsonParser(Reader in)
        throws IOException {

        return enableFeatures(mapper.getJsonFactory().createJsonParser(in));
    }

    /**
     * Returns a JSON parser that parses input from an input stream.
     *
     * @param in the input stream
     * @return the parser
     * @throws IOException if there is a problem parsing the input
     */
    public static JsonParser createJsonParser(InputStream in)
        throws IOException {

        return enableFeatures(mapper.getJsonFactory().createJsonParser(in));
    }

    /**
     * Enables some non-default features:
     * - allow leading 0 in numerics
     * - allow non-numeric values such as INF, -INF, NaN for float and double
     */
    private static JsonParser enableFeatures(JsonParser parser) {
        parser.enable(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS);
        return parser.enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS);
    }

    /**
     * Returns an object node.
     *
     * @return the object node
     */
    public static ObjectNode createObjectNode() {
        return mapper.createObjectNode();
    }

    /**
     * Returns an array node.
     *
     * @return the array node
     */
    public static ArrayNode createArrayNode() {
        return mapper.createArrayNode();
    }

    /**
     * Returns the default object writer.
     *
     * @param pretty whether the writer should do prettyprinting
     * @return the object writer
     */
    @SuppressWarnings("deprecation")
    public static ObjectWriter createWriter(boolean pretty) {

        /*
         * NOTE: currently using jackson 1.9, which deprecates the method
         * defaultPrettyPrintingWriter() and replaces it with the new method
         * writerWithDefaultPrettyPrinter(). Unfortunately, the latest
         * versions of CDH are distributed with version 1.8 of jackson;
         * which specifies only defaultPrettyPrintingWriter(). Thus,
         * although it is preferable to use writerWithDefaultPrettyPrinter(),
         * any Table API based MapReduce jobs that are run will fail because
         * the Hadoop infrastructure has no knowledge of that method.
         *
         * As a result, until CDH upgrades to jackson 1.9 or greater,
         * this method must continue to employ defaultPrettyPrintingWriter()
         * below; after which it may be changed to use
         * writerWithDefaultPrettyPrinter() instead.
         */
        return (pretty ? mapper.defaultPrettyPrintingWriter() :
                mapper.writer());
    }

    /**
     * Returns the value of a field in text format if the node is an object
     * node with the specified value field, otherwise null.
     *
     * @param node the node
     * @param field the field name
     * @return the field value as text or null
     */
    public static String getAsText(JsonNode node, String field) {
        return getAsText(node, field, null);
    }

    /**
     * Returns the value of a field in text format if the node is an object
     * node with the specified value field, otherwise returns the default
     * value.
     *
     * @param node the node
     * @param field the field name
     * @param defaultValue the default value
     * @return the field value as text or the default
     */
    public static String getAsText(JsonNode node, String field,
                                   String defaultValue) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isValueNode()) {
            return defaultValue;
        }
        return fieldNode.asText();
    }

    /**
     * Returns the Long value of a field if the node is an object node with the
     * specified long field, otherwise null.
     *
     * @param node the node
     * @param field the field name
     * @return the field value or null
     */
    public static Long getLong(JsonNode node, String field) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isLong()) {
            return null;
        }
        return fieldNode.getLongValue();
    }

    /**
     * Returns the Integer value of a field if the node is an object node with
     * the specified int field, otherwise null.
     *
     * @param node the node
     * @param field the field name
     * @return the field value or null
     */
    public static Integer getInt(JsonNode node, String field) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isInt()) {
            return null;
        }
        return fieldNode.getIntValue();
    }

    /**
     * Returns the Double value of a field if the node is an object node with
     * the specified double field, otherwise null.
     *
     * @param node the node
     * @param field the field name
     * @return the field value or null
     */
    public static Double getDouble(JsonNode node, String field) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isDouble()) {
            return null;
        }
        return fieldNode.getDoubleValue();
    }

    /**
     * Returns the Boolean value of a field if the node is an object node with
     * the specified boolean field, otherwise null.
     *
     * @param node the node
     * @param field the field name
     * @return the field value or null
     */
    public static Boolean getBoolean(JsonNode node, String field) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isBoolean()) {
            return null;
        }
        return fieldNode.getBooleanValue();
    }

    /**
     * Returns the boolean value of a field if the node is an object node with
     * the specified boolean field, otherwise returns the default value.
     *
     * @param node the node
     * @param field the field name
     * @param defaultValue the default
     * @return the field value or the default
     */
    public static boolean getBoolean(JsonNode node, String field,
                                     boolean defaultValue) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isBoolean()) {
            return defaultValue;
        }
        return fieldNode.getBooleanValue();
    }

    /**
     * Returns the object node for a field if the node is an object node with
     * the specified object field, otherwise null.
     *
     * @param node the node
     * @param field the field name
     * @return the field value or null
     */
    public static ObjectNode getObject(JsonNode node, String field) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isObject()) {
            return null;
        }
        return (ObjectNode) fieldNode;
    }

    /**
     * Returns an iterable object over the elements of an array for a field if
     * the node is an object node with the specified array field, otherwise
     * an empty iterable.
     *
     * @param node the node
     * @param field the field name
     * @return an iterable over the array elements or an empty iterable
     */
    public static Iterable<JsonNode> getArray(JsonNode node, String field) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isArray()) {
            return Collections.emptyList();
        }
        return fieldNode;
    }

    /**
     * Compare the contents of two JSON strings without regard
     * to the order in which fields appear in the string.
     *
     * @param a , b Two JSON strings to compare
     * @return true if the two strings represent equivalent JSON documents
     */
    public static boolean jsonStringsEqual(String a, String b) {
        Map<String, Object> mapa = getMapFromJsonStr(a);
        Map<String, Object> mapb = getMapFromJsonStr(b);
        if (mapa == null) {
            return false;
        }
        return mapa.equals(mapb);
    }

    /**
     * Turn a JSON string into a Map.
     *
     * @param s the JSON string
     * @return A Map representing the JSON document.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getMapFromJsonStr(String s) {
        try {
            return mapper.readValue(s, HashMap.class);
        } catch (IOException e) {
            return null;
        }
    }
}

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

import static oracle.kv.impl.api.table.TableJsonUtils.jsonParserGetDecimalValue;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.BooleanValue;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.FloatValue;
import oracle.kv.table.IntegerValue;
import oracle.kv.table.JsonDef;
import oracle.kv.table.LongValue;
import oracle.kv.table.NumberValue;
import oracle.kv.table.StringValue;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * JsonDefImpl implements the JsonDef interface.
 */
public class JsonDefImpl extends FieldDefImpl implements JsonDef {

    private static final long serialVersionUID = 1L;

    JsonDefImpl(String description) {

        super(FieldDef.Type.JSON, description);
    }

    JsonDefImpl() {
        this((String)null);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public JsonDefImpl clone() {
        return FieldDefImpl.jsonDef;
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    @Override
    public JsonDef asJson() {
        return this;
    }

    @Override
    public boolean equals(Object other) {
        return (other instanceof JsonDefImpl);
    }

    @Override
    public boolean isValidIndexField() {
        return true;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return false;
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        return (superType.isJson() || superType.isAny());
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.QUERY_VERSION_2;
    }

    @Override
    public ArrayValueImpl createArray() {
        return new ArrayValueImpl(FieldDefImpl.arrayJsonDef);
    }

    @Override
    public BooleanValue createBoolean(boolean value) {
        return booleanDef.createBoolean(value);
    }

    @Override
    public DoubleValue createDouble(double value) {
        return doubleDef.createDouble(value);
    }

    @Override
    public FloatValue createFloat(float value) {
        return floatDef.createFloat(value);
    }

    @Override
    public IntegerValue createInteger(int value) {
        return integerDef.createInteger(value);
    }

    @Override
    public LongValue createLong(long value) {
        return longDef.createLong(value);
    }

    @Override
    public NumberValue createNumber(int value) {
        return numberDef.createNumber(value);
    }

    @Override
    public NumberValue createNumber(long value) {
        return numberDef.createNumber(value);
    }

    @Override
    public NumberValue createNumber(float value) {
        return numberDef.createNumber(value);
    }

    @Override
    public NumberValue createNumber(double value) {
        return numberDef.createNumber(value);
    }

    @Override
    public NumberValue createNumber(BigDecimal decimal) {
        return numberDef.createNumber(decimal);
    }

    @Override
    public MapValueImpl createMap() {
        return new MapValueImpl(FieldDefImpl.mapJsonDef);
    }

    @Override
    public StringValue createString(String value) {
        return stringDef.createString(value);
    }

    @Override
    public FieldValue createJsonNull() {
        return NullJsonValueImpl.getInstance();
    }

    /**
     * A common method to validate JSON-compatible values.  This includes all
     * atomic types except (FIXED_)BINARY and ENUM. It excludes RECORD, but
     * includes ARRAY and MAP IFF they are instances of JsonArrayValueImpl or
     * JsonMapValueImpl.
     */
    static void validateJsonType(FieldValue value) {
        FieldDef.Type type = value.getType();

        if (type == FieldDef.Type.BINARY ||
            type == FieldDef.Type.FIXED_BINARY ||
            type == FieldDef.Type.ENUM ||
            type == FieldDef.Type.RECORD) {
            throw new IllegalArgumentException
                ("Type is not supported in JSON: " + type);
        }

        if (type == FieldDef.Type.MAP || type ==  FieldDef.Type.ARRAY) {

            FieldDefImpl elemDef = (FieldDefImpl)value.getDefinition();

            if (!elemDef.isSubtype(FieldDefImpl.jsonDef)) {
                throw new IllegalArgumentException(
                    "Type is not supported in JSON: " + type);
            }
        }
    }

    /**
     * Construct a FieldValue based on arbitrary JSON from the incoming JSON
     * The top-level object may be any valid JSON:
     * 1. an object
     * 2. an array
     * 3. a scalar, including the JSON null value
     *
     * This code creates FieldValue types based on the type inferred from the
     * parser.
     */
    public static FieldValue createFromJson(JsonParser jp, boolean getNext) {

        try {
            JsonToken token = (getNext ? jp.nextToken() : jp.getCurrentToken());
            if (token == null) {
                throw new IllegalStateException(
                    "createFromJson called with null token");
            }

            switch (token) {
            case VALUE_STRING:

                return FieldDefImpl.stringDef.createString(jp.getText());

            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:

                /*
                 * Handle all numeric types here. 4 types are supported (3
                 * until 4.4):
                 *  INTEGER
                 *  LONG (long and integer)
                 *  DOUBLE (double and float)
                 *  NUMBER (anything that won't fit into the two above)
                 */
                JsonParser.NumberType numberType = jp.getNumberType();

                switch (numberType) {
                case BIG_INTEGER:
                case BIG_DECIMAL:
                    return FieldDefImpl.numberDef.createNumber(
                        jsonParserGetDecimalValue(jp));
                case INT:
                    return FieldDefImpl.integerDef.createInteger(jp.getIntValue());
                case LONG:
                    return FieldDefImpl.longDef.createLong(jp.getLongValue());

                /* Never store FLOAT, use DOUBLE */
                case FLOAT:
                case DOUBLE:
                    double dbl = jp.getDoubleValue();
                    /*
                     * Jackson parse a floating-point numbers to a double:
                     *  - if the Math.abs(value) > Double.MAX_VALUE, return
                     *    Infinity.
                     *  - if the abs(value) is smaller than Double.MIN_VALUE,
                     *    then return Zero.
                     *
                     * So check if the double value is Infinity or Zero, try to
                     * read it as BigDecimal value. The real 0.0 is a special
                     * value, it is treated as valid double value.
                     */
                    if (Double.isInfinite(dbl) || dbl == 0.0) {
                        BigDecimal bd = jsonParserGetDecimalValue(jp);
                        if (bd.compareTo(BigDecimal.ZERO) != 0) {
                            return FieldDefImpl.numberDef.createNumber(bd);
                        }
                    }
                    return FieldDefImpl.doubleDef.createDouble(dbl);
                }
                throw new IllegalStateException("Unexpected numeric type: " +
                                                numberType);
            case VALUE_TRUE:

                return FieldDefImpl.booleanDef.createBoolean(true);

            case VALUE_FALSE:

                return FieldDefImpl.booleanDef.createBoolean(false);

            case VALUE_NULL:

                return NullJsonValueImpl.getInstance();

            case START_OBJECT:

                return parseObject(jp);

            case START_ARRAY:

                return parseArray(jp);

            case FIELD_NAME:
            case END_OBJECT:
            case END_ARRAY:
            default:
                throw new IllegalStateException(
                    "Unexpected token while parsing JSON: " + token);
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Failed to parse JSON input: " + ioe.getMessage());
        }
    }

    /**
     * Creates a FieldValue from a Reader. This helper method is shared
     * among the complex types that handle JSON and factors out IOException
     * handling.
     */
    static FieldValue createFromReader(Reader jsonReader) {
        try {
            return FieldValueFactory.createValueFromJson(jsonReader);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to parse JSON " +
                                               "input: " + ioe.getMessage(),
                                               ioe);
        }
    }

    /**
     * Creates a JSON map from the parsed JSON object.
     */
    private static FieldValueImpl parseObject(JsonParser jp)
        throws IOException {

        MapValueImpl map = FieldDefImpl.jsonDef.createMap();

        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            String fieldName = jp.getCurrentName();
            if (token == null || fieldName == null) {
                throw new IllegalArgumentException(
                    "null token or field name parsing JSON object");
            }

            /* true tells the method to fetch the next token */
            FieldValue field = createFromJson(jp, true);
            if (field.isJsonNull()) {
                map.put(fieldName, NullJsonValueImpl.getInstance());
            } else {
                map.put(fieldName, field);
            }
        }
        return map;
    }

    /**
     * Creates a JSON array from the parsed JSON array by adding
     */
    private static FieldValueImpl parseArray(JsonParser jp)
        throws IOException {

        ArrayValueImpl array = FieldDefImpl.jsonDef.createArray();

        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
            if (token == null) {
                throw new IllegalStateException(
                    "null token while parsing JSON array");
            }

            /* false means don't get the next token, it's been fetched */
            array.add(createFromJson(jp, false));
        }
        return array;
    }
}

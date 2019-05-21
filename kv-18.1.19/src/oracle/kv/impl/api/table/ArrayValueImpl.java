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
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import oracle.kv.impl.api.table.ValueSerializer.ArrayValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;

import org.codehaus.jackson.JsonLocation;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonParser.NumberType;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import com.sleepycat.persist.model.Persistent;

/**
 * ArrayValueImpl implements the ArrayValue interface to hold an object of
 * type ArrayDef.
 */
@Persistent(version=1)
public class ArrayValueImpl extends ComplexValueImpl
    implements ArrayValue, ArrayValueSerializer {

    private static final long serialVersionUID = 1L;

    private final ArrayList<FieldValue> array;

    /*
     * Support for homogenous arrays of valid scalars in wildcard arrays,
     * which are arrays of ANY, JSON, ANY_ATOMIC, or ANY_JSON_ATOMIC.
     *
     * This internal feature exists so that storage and serialization can
     * be optimized for homogenous arrays of scalars. Specifically, we don't
     * want to store an extra byte per element to specify the type of the
     * element. We don't track homogeneity for non scalara because the space
     * savings are smaller, and the cpu cost of tracking is higher.
     *
     * Rules:
     *  1. If homogeneousType is non-null, this is an array that may contain
     *     mixed elements, but which currently contains scalars only, all
     *     having homogeneousType as their type. Otherwise, this is either a
     *     "typed" array (it's elementDef is not a wildcard) or a non-
     *     homogeneous wildcard array.
     *  2. Public APIs will always see this as an array of wildcard (e.g. JSON)
     *  3. There are internal APIs to access the homogenous type. These are
     *     used in this class and in other classes that need to know (for now,
     *     FieldValueSerialization only).
     *
     * Empty arrays of wildcards always start with homogeneousType == null.
     * The following are the valid transitions for arrays of wildcard:
     *
     * 1. empty array -> homogeneous array
     *    Empty arrays of wildcards always start with homogeneousType == null.
     *    On first insertion, if the type of inserted element is scalar, it is
     *    stored in this.homogeneousType.
     *
     * 2. homogeneous array -> non-homogeneous array.
     * If an element whose type is not the same as this.homogeneousType is
     * inserted, this.homogeneousType is set to null, making the array a
     * non-homogeneous one.
     *
     * These transitions are handled in trackHomogenousType().
     */

    private FieldDefImpl homogeneousType;

    ArrayValueImpl(ArrayDef def) {
        super(def);
        array = new ArrayList<FieldValue>();
    }

    /* DPL */
    private ArrayValueImpl() {
        super(null);
        array = null;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public ArrayValueImpl clone() {
        ArrayValueImpl newArray = new ArrayValueImpl(getDefinition());
        for (FieldValue val : array) {
            newArray.add(val.clone());
        }
        newArray.homogeneousType = homogeneousType;
        return newArray;
    }

    @Override
    public int hashCode() {
        int code = size();
        for (FieldValue val : array) {
            code += val.hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof ArrayValueImpl) {
            ArrayValueImpl otherValue = (ArrayValueImpl) other;
            /* maybe avoid some work */
            if (this == otherValue) {
                return true;
            }

            /*
             * detailed comparison
             */
            if (size() == otherValue.size() &&
                getDefinition().equals(otherValue.getDefinition()) &&
                (homogeneousType == null ||
                 homogeneousType.equals(otherValue.getHomogeneousType()))) {

                for (int i = 0; i < size(); i++) {
                    if (!get(i).equals(otherValue.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * FieldDef must match.
     *
     * Compare field values in array order.  Return as soon as there is a
     * difference. If this object has a field the other does not, return &gt;
     * 0.  If this object is missing a field the other has, return &lt; 0.
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof ArrayValueImpl) {
            ArrayValueImpl otherImpl = (ArrayValueImpl) other;
            if (!getDefinition().equals(otherImpl.getDefinition())) {
                throw new IllegalArgumentException
                    ("Cannot compare ArrayValues with different definitions");
            }

            for (int i = 0; i < size(); i++) {
                FieldValueImpl val = get(i);
                if (otherImpl.size() < i + 1) {
                    return 1;
                }
                int ret = val.compareTo(otherImpl.get(i));
                if (ret != 0) {
                    return ret;
                }
            }
            /* they must be equal */
            return 0;
        }
        throw new ClassCastException("Object is not an ArrayValue");
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.ARRAY;
    }

    @Override
    public boolean isArray() {
        return true;
    }

    @Override
    public ArrayValue asArray() {
        return this;
    }

    /*
     * Public api methods from ArrayValue
     */

    @Override
    public ArrayDefImpl getDefinition() {
        return (ArrayDefImpl)fieldDef;
    }

    @Override
    public FieldValueImpl get(int index) {
        return (FieldValueImpl)array.get(index);
    }

    @Override
    public int size() {
        return array.size();
    }

    @Override
    public List<FieldValue> toList() {
        return Collections.unmodifiableList(array);
    }

    @Override
    public ArrayValue add(FieldValue value) {
        value = validate(value, getElementDef());
        array.add(value);
        trackHomogeneousType(value);
        return this;
    }

    @Override
    public ArrayValue add(int index, FieldValue value) {
        value = validate(value, getElementDef());
        array.add(index, value);
        trackHomogeneousType(value);
        return this;
    }

    @Override
    public ArrayValue set(int index, FieldValue value) {
        value = validate(value, getElementDef());
        array.set(index, value);
        trackHomogeneousType(value);
        return this;
    }

    /**
     * Integer
     */
    @Override
    public ArrayValue add(int value) {
        addScalar(getElementDef().createInteger(value));
        return this;
    }

    @Override
    public ArrayValue add(int values[]) {
        FieldDefImpl edef = getElementDef();
        for (int i : values) {
            addScalar(edef.createInteger(i));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, int value) {
        addScalar(index, getElementDef().createInteger(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, int value) {
        setScalar(index, getElementDef().createInteger(value));
        return this;
    }

    /**
     * Long
     */
    @Override
    public ArrayValue add(long value) {
        addScalar(getElementDef().createLong(value));
        return this;
    }

    @Override
    public ArrayValue add(long values[]) {
        FieldDef edef = getElementDef();
        for (long l : values) {
            addScalar(edef.createLong(l));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, long value) {
        addScalar(index, getElementDef().createLong(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, long value) {
        setScalar(index, getElementDef().createLong(value));
        return this;
    }

    /**
     * String
     */
    @Override
    public ArrayValue add(String value) {
        addScalar(getElementDef().createString(value));
        return this;
    }

    @Override
    public ArrayValue add(String values[]) {
        FieldDef edef = getElementDef();
        for (String s : values) {
            addScalar(edef.createString(s));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, String value) {
        addScalar(index, getElementDef().createString(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, String value) {
        setScalar(index, getElementDef().createString(value));
        return this;
    }

    /**
     * Double
     */
    @Override
    public ArrayValue add(double value) {
        addScalar(getElementDef().createDouble(value));
        return this;
    }

    @Override
    public ArrayValue add(double values[]) {
        FieldDef edef = getElementDef();
        for (double d : values) {
            addScalar(edef.createDouble(d));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, double value) {
        addScalar(index, getElementDef().createDouble(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, double value) {
        setScalar(index, getElementDef().createDouble(value));
        return this;
    }

    /**
     * Float
     */
    @Override
    public ArrayValue add(float value) {
        addScalar(getElementDef().createFloat(value));
        return this;
    }

    @Override
    public ArrayValue add(float values[]) {
        FieldDefImpl edef = getElementDef();
        for (float d : values) {
            addScalar(edef.createFloat(d));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, float value) {
        addScalar(index, getElementDef().createFloat(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, float value) {
        setScalar(index, getElementDef().createFloat(value));
        return this;
    }

    /*
     * BigDecimal
     */
    @Override
    public ArrayValue addNumber(int value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(int values[]) {
        FieldDef def = getElementDef();
        for (int val : values) {
            addScalar(def.createNumber(val));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, int value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, int value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(long value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(long values[]) {
        FieldDef def = getElementDef();
        for (long val : values) {
            addScalar(def.createNumber(val));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, long value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, long value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(float value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(float values[]) {
        FieldDef def = getElementDef();
        for (float val : values) {
            addScalar(def.createNumber(val));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, float value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, float value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(double value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(double values[]) {
        FieldDef def = getElementDef();
        for (double val : values) {
            addScalar(def.createNumber(val));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, double value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, double value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(BigDecimal value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(BigDecimal values[]) {
        FieldDef def = getElementDef();
        for (BigDecimal bd : values) {
            addScalar(def.createNumber(bd));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, BigDecimal value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, BigDecimal value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    /**
     * Boolean
     */
    @Override
    public ArrayValue add(boolean value) {
        addScalar(getElementDef().createBoolean(value));
        return this;
    }

    @Override
    public ArrayValue add(boolean values[]) {
        FieldDef edef = getElementDef();
        for (boolean b : values) {
            addScalar(edef.createBoolean(b));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, boolean value) {
        addScalar(index, getElementDef().createBoolean(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, boolean value) {
        setScalar(index, getElementDef().createBoolean(value));
        return this;
    }

    /**
     * Binary
     */
    @Override
    public ArrayValue add(byte[] value) {
        addScalar(getElementDef().createBinary(value));
        return this;
    }

    @Override
    public ArrayValue add(byte[] values[]) {
        FieldDef edef = getElementDef();
        for (byte[] b : values) {
            addScalar(edef.createBinary(b));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, byte[] value) {
        addScalar(index, getElementDef().createBinary(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, byte[] value) {
        setScalar(index, getElementDef().createBinary(value));
        return this;
    }

    /**
     * FixedBinary
     */
    @Override
    public ArrayValue addFixed(byte[] value) {
        addScalar(getElementDef().createFixedBinary(value));
        return this;
    }

    @Override
    public ArrayValue addFixed(byte[] values[]) {
        FieldDef edef = getElementDef();
        for (byte[] b : values) {
            addScalar(edef.createFixedBinary(b));
        }
        return this;
    }

    @Override
    public ArrayValue addFixed(int index, byte[] value) {
        addScalar(index, getElementDef().createFixedBinary(value));
        return this;
    }

    @Override
    public ArrayValue setFixed(int index, byte[] value) {
        setScalar(index, getElementDef().createFixedBinary(value));
        return this;
    }

    /**
     * Enum
     */
    @Override
    public ArrayValue addEnum(String value) {
        addScalar(getElementDef().createEnum(value));
        return this;
    }

    @Override
    public ArrayValue addEnum(String values[]) {
        FieldDef edef = getElementDef();
        for (String s : values) {
            addScalar(edef.createEnum(s));
        }
        return this;
    }

    @Override
    public ArrayValue addEnum(int index, String value) {
        addScalar(index, getElementDef().createEnum(value));
        return this;
    }

    @Override
    public ArrayValue setEnum(int index, String value) {
        setScalar(index, getElementDef().createEnum(value));
        return this;
    }

    /**
     * Timestamp
     */
    @Override
    public ArrayValue add(Timestamp value) {
        addScalar(getElementDef().createTimestamp(value));
        return this;
    }

    @Override
    public ArrayValue add(Timestamp values[]) {
        FieldDef def = getElementDef();
        for (Timestamp v : values) {
            addScalar(def.createTimestamp(v));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, Timestamp value) {
        addScalar(index, getElementDef().createTimestamp(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, Timestamp value) {
        addScalar(index, getElementDef().createTimestamp(value));
        return this;
    }

    /**
     * JSON Null
     */
    @Override
    public ArrayValue addJsonNull() {
        addScalar(getElementDef().createJsonNull());
        return this;
    }

    @Override
    public ArrayValue addJsonNull(int index) {
        addScalar(index, getElementDef().createJsonNull());
        return this;
    }

    @Override
    public ArrayValue setJsonNull(int index) {
        setScalar(index, getElementDef().createJsonNull());
        return this;
    }

    /*
     * Record
     */
    @Override
    public RecordValue setRecord(int index) {
        RecordValue val = getElementDef().createRecord();
        array.set(index, val);
        clearHomogeneousType();
        return val;
    }

    @Override
    public RecordValueImpl addRecord() {
        RecordValue val = getElementDef().createRecord();
        array.add(val);
        clearHomogeneousType();
        return (RecordValueImpl) val;
    }

    @Override
    public RecordValue addRecord(int index) {
        RecordValue val = getElementDef().createRecord();
        array.add(index, val);
        clearHomogeneousType();
        return val;
    }

    /*
     * Map
     */
    @Override
    public MapValue setMap(int index) {
        MapValue val = getElementDef().createMap();
        array.set(index, val);
        clearHomogeneousType();
        return val;
    }

    @Override
    public MapValueImpl addMap() {
        MapValue val = getElementDef().createMap();
        array.add(val);
        clearHomogeneousType();
        return (MapValueImpl) val;
    }

    @Override
    public MapValue addMap(int index) {
        MapValue val = getElementDef().createMap();
        array.add(index, val);
        clearHomogeneousType();
        return val;
    }

    /*
     * Array
     */
    @Override
    public ArrayValue setArray(int index) {
        ArrayValue val = getElementDef().createArray();
        array.set(index, val);
        clearHomogeneousType();
        return val;
    }

    @Override
    public ArrayValueImpl addArray() {
        ArrayValue val = getElementDef().createArray();
        array.add(val);
        clearHomogeneousType();
        return (ArrayValueImpl) val;
    }


    @Override
    public ArrayValue addArray(int index) {
        ArrayValue val = getElementDef().createArray();
        array.add(index, val);
        clearHomogeneousType();
        return val;
    }

    /*
     * JSON
     */
    @Override
    public ArrayValueImpl addJson(String jsonInput) {
        Reader reader = new StringReader(jsonInput);
        try {
            return addJson(reader);
        } finally {
            try { reader.close(); } catch (IOException ioe) {}
        }
    }

    @Override
    public ArrayValueImpl addJson(Reader jsonReader) {
        add(JsonDefImpl.createFromReader(jsonReader));
        return this;
    }

    @Override
    public ArrayValueImpl addJson(int index, String jsonInput) {
        Reader reader = new StringReader(jsonInput);
        try {
            return addJson(index, reader);
        } finally {
            try { reader.close(); } catch (IOException ioe) {}
        }
    }

    @Override
    public ArrayValueImpl addJson(int index, Reader jsonReader) {
        add(index, JsonDefImpl.createFromReader(jsonReader));
        return this;
    }

    @Override
    public ArrayValueImpl setJson(int index, String jsonInput) {
        Reader reader = new StringReader(jsonInput);
        try {
            return setJson(index, reader);
        } finally {
            try { reader.close(); } catch (IOException ioe) {}
        }
    }

    @Override
    public ArrayValueImpl setJson(int index, Reader jsonReader) {
        set(index, JsonDefImpl.createFromReader(jsonReader));
        return this;
    }

    /*
     * Methods from ComplexValueImpl
     */

    /**
     * Parse a JSON array and put the extracted values into "this" array.
     */
    @Override
    public void addJsonFields(
        JsonParser jp,
        String fieldName,
        boolean exact,
        boolean addMissingFields) {

        try {
            FieldDef element = getElementDef();

            JsonToken t = jp.getCurrentToken();

            JsonLocation location = jp.getCurrentLocation();

            if (t != JsonToken.START_ARRAY) {
                jsonParseException(("Expected [ token to start array, instead "
                                    + "found " + t), location);
            }

            while ((t = jp.nextToken()) != JsonToken.END_ARRAY) {

                if (t == null || t == JsonToken.END_OBJECT) {
                    jsonParseException("Did not find end of array", location);
                }

                /*
                 * Handle null.
                 */
                if (jp.getCurrentToken() == JsonToken.VALUE_NULL &&
                    !element.isJson()) {
                    throw new IllegalArgumentException
                        ("Invalid null value in JSON input for array");
                }

                switch (element.getType()) {
                case INTEGER:
                    checkNumberType(null, NumberType.INT, jp);
                    add(jp.getIntValue());
                    break;
                case LONG:
                    checkNumberType(null, NumberType.LONG, jp);
                    add(jp.getLongValue());
                    break;
                case DOUBLE:
                    checkNumberType(null, NumberType.DOUBLE, jp);
                    add(jp.getDoubleValue());
                    break;
                case FLOAT:
                    checkNumberType(null, NumberType.FLOAT, jp);
                    add(jp.getFloatValue());
                    break;
                case NUMBER:
                    checkNumberType(null, NumberType.BIG_DECIMAL, jp);
                    addNumber(jsonParserGetDecimalValue(jp));
                    break;
                case STRING:
                    add(jp.getText());
                    break;
                case BINARY:
                    add(jp.getBinaryValue());
                    break;
                case FIXED_BINARY:
                    addFixed(jp.getBinaryValue());
                    break;
                case BOOLEAN:
                    add(jp.getBooleanValue());
                    break;
                case TIMESTAMP:
                    add(element.asTimestamp().fromString(jp.getText()));
                    break;
                case ARRAY:
                    ArrayValueImpl array1 = addArray();
                    array1.addJsonFields(jp, null, exact,
                                         addMissingFields);
                    break;
                case MAP:
                    MapValueImpl map = addMap();
                    map.addJsonFields(jp, null, exact,
                                      addMissingFields);
                    break;
                case RECORD:
                    RecordValueImpl record = addRecord();
                    record.addJsonFields(jp, null, exact,
                                         addMissingFields);
                    break;
                case ENUM:
                    addEnum(jp.getText());
                    break;
                case JSON:
                case ANY_JSON_ATOMIC:
                    array.add(JsonDefImpl.createFromJson(jp, false));
                    break;
                case ANY:
                    throw new IllegalArgumentException(
                        "ARRAY(ANY) not suported yet");
                case ANY_ATOMIC:
                    throw new IllegalArgumentException(
                        "ARRAY(ANY_ATOMIC) not suported yet");
                case ANY_RECORD:
                    throw new IllegalStateException(
                        "An array type cannot have ANY_RECORD as its " +
                        "element type");
                case EMPTY:
                    throw new IllegalStateException(
                        "An array type cannot have EMPTY as its element type");
                }
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                (("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        } catch (RuntimeException re) {
            if (re instanceof IllegalArgumentException) {
                throw re;
            }
            throw new IllegalArgumentException
                (("Failed to parse JSON input: " + re.toString()), re);
        }
    }

    /*
     * FieldValueImpl internal api methods
     */

    /**
     * Increment the value of the array element, not the array.  There
     * can only be one element in this array.
     */
    @Override
    public FieldValueImpl getNextValue() {
        if (size() != 1) {
            throw new IllegalArgumentException
                ("Array values used in ranges must contain only one element");
        }
        ArrayValueImpl newArray = new ArrayValueImpl(getDefinition());
        FieldValueImpl fvi = get(0).getNextValue();
        newArray.add(fvi);
        return newArray;
    }

    @Override
    public FieldValueImpl getMinimumValue() {
        if (size() != 1) {
            throw new IllegalArgumentException
                ("Array values used in ranges must contain only one element");
        }
        ArrayValueImpl newArray = new ArrayValueImpl(getDefinition());
        FieldValueImpl fvi = get(0).getMinimumValue();
        newArray.add(fvi);
        return newArray;
    }


    @Override
    public JsonNode toJsonNode() {
        ArrayNode node = JsonNodeFactory.instance.arrayNode();
        for (FieldValue value : array) {
            node.add(((FieldValueImpl)value).toJsonNode());
        }
        return node;
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append('[');
        for (int i = 0; i < array.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            FieldValueImpl value = (FieldValueImpl)array.get(i);
            value.toStringBuilder(sb);
        }
        sb.append(']');
    }


    @SuppressWarnings("unchecked")
    static ArrayValueImpl fromJavaObjectValue(FieldDef def, Object o) {

        Iterable<Object> coll = null;

        if (o instanceof Iterable<?>) {
            coll = (Iterable<Object>) o;
        } else {
            coll = Arrays.asList((Object[]) o);
        }

        ArrayValueImpl newArray = (ArrayValueImpl)def.createArray();

        for (Object value : coll) {
            newArray.add(FieldValueImpl.fromJavaObjectValue(
                             newArray.getElementDef(), value));
        }

        return newArray;
    }

    /*
     * Local methods
     */

    public void clear() {
        array.clear();
    }

    public void remove(int pos) {
        array.remove(pos);
    }

    /*
     * These next 3 exist to consolidate valid insertions.
     */
    private ArrayValue addScalar(FieldValue value) {
        assert ((FieldDefImpl)value.getDefinition()).isSubtype(getElementDef());
        /* turn float to double */
        if (value.isFloat() && getElementDef().isJson()) {
            value = FieldDefImpl.doubleDef.createDouble(value.asFloat().get());
        }
        trackHomogeneousType(value);
        array.add(value);
        return this;
    }

    private ArrayValue addScalar(int index, FieldValue value) {
        assert ((FieldDefImpl)value.getDefinition()).isSubtype(getElementDef());
        /* turn float to double */
        if (value.isFloat() && getElementDef().isJson()) {
            value = FieldDefImpl.doubleDef.createDouble(value.asFloat().get());
        }
        trackHomogeneousType(value);
        array.add(index, value);
        return this;
    }

    private ArrayValue setScalar(int index, FieldValue value) {
        assert ((FieldDefImpl)value.getDefinition()).isSubtype(getElementDef());
        /* turn float to double */
        if (value.isFloat() && getElementDef().isJson()) {
            value = FieldDefImpl.doubleDef.createDouble(value.asFloat().get());
        }
        trackHomogeneousType(value);
        array.set(index, value);
        return this;
    }

    List<FieldValue> getArrayInternal() {
        return array;
    }

    public FieldDefImpl getElementDef() {
        return ((ArrayDefImpl)fieldDef).getElement();
    }

    FieldDefImpl getHomogeneousType() {
        return homogeneousType;
    }

    void setHomogeneousType(FieldDefImpl def) {
        homogeneousType = def;
    }

    boolean isHomogeneous() {
        return homogeneousType != null;
    }

    public void addInternal(FieldValue value) {
        array.add(value);
    }

    /**
     * This is used by index deserialization.  The format for enums is an
     * integer.
     */
    ArrayValue addEnum(int value) {
        add(((EnumDefImpl)getElementDef()).createEnum(value));
        return this;
    }

    /**
     * This method tracks the type of the elements in the array handling
     * transitions to/from wildcard types.
     */
    private void trackHomogeneousType(FieldValue value) {

        FieldDefImpl elemDef = getElementDef();

        if (!elemDef.isWildcard()) {
            return;
        }

        FieldDefImpl valDef = (FieldDefImpl)value.getDefinition();

        if (size() == 0) {
            /*
             * transition from empty wildcard array to homogenous wildcard
             * array.
             */
            assert(homogeneousType == null);

            if (valDef.isAtomic()) {
                homogeneousType = valDef;
            }

        } else if (homogeneousType != null &&
                   homogeneousType.getType() != valDef.getType()) {
            /* transition from homogenous wildcard to heterogenous wildcard */
            homogeneousType = null;
        }
    }

    private void clearHomogeneousType() {
        homogeneousType = null;
    }

    @Override
    public ArrayValueSerializer asArrayValueSerializer() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<FieldValueSerializer> iterator() {
        final List<?> values = toList();
        return (Iterator<FieldValueSerializer>)values.iterator();
    }
}

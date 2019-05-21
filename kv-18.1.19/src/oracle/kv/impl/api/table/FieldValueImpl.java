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
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

import oracle.kv.impl.api.table.ValueSerializer.ArrayValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.MapValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.RecordValueSerializer;

import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.BinaryValue;
import oracle.kv.table.BooleanValue;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FixedBinaryValue;
import oracle.kv.table.FloatValue;
import oracle.kv.table.IndexKey;
import oracle.kv.table.IntegerValue;
import oracle.kv.table.LongValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.NumberValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.StringValue;
import oracle.kv.table.TimestampValue;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.persist.model.Persistent;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectWriter;

/**
 * FieldValueImpl represents a value of a single field.  A value may be simple
 * or complex (single-valued vs multi-valued).  FieldValue is the building
 * block of row values in a table.
 *<p>
 * The FieldValueImpl class itself has no state and serves as an abstract base
 * for implementations of FieldValue and its sub-interfaces.
 */
@Persistent(version=1)
public abstract class FieldValueImpl
    implements FieldValue, FieldValueSerializer, Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    @Override
    public FieldValueImpl clone() {
        try {
            return (FieldValueImpl) super.clone();
        } catch (CloneNotSupportedException ignore) {
        }
        return null;
    }

    @Override
    public int compareTo(FieldValue o) {
        throw new IllegalArgumentException
            ("FieldValueImpl objects must implement compareTo");
    }

    @Override
    public abstract FieldDefImpl getDefinition();

    @Override
    public BinaryValue asBinary() {
        throw new ClassCastException
            ("Field is not a Binary: " + getClass());
    }

    @Override
    public NumberValue asNumber() {
        throw new ClassCastException
            ("Field is not a Number: " + getClass());
    }

    @Override
    public BooleanValue asBoolean() {
        throw new ClassCastException
            ("Field is not a Boolean: " + getClass());
    }

    @Override
    public DoubleValue asDouble() {
        throw new ClassCastException
            ("Field is not a Double: " + getClass());
    }

    @Override
    public FloatValue asFloat() {
        throw new ClassCastException
            ("Field is not a Float: " + getClass());
    }

    @Override
    public IntegerValue asInteger() {
        throw new ClassCastException
            ("Field is not an Integer: " + getClass());
    }

    @Override
    public LongValue asLong() {
        throw new ClassCastException
            ("Field is not a Long: " + getClass());
    }

    @Override
    public StringValue asString() {
        throw new ClassCastException
            ("Field is not a String: " + getClass());
    }

    @Override
    public TimestampValue asTimestamp() {
        throw new ClassCastException
            ("Field is not a Timestamp: " + getClass());
    }

    @Override
    public EnumValue asEnum() {
        throw new ClassCastException
            ("Field is not an Enum: " + getClass());
    }

    @Override
    public FixedBinaryValue asFixedBinary() {
        throw new ClassCastException
            ("Field is not a FixedBinary: " + getClass());
    }

    @Override
    public ArrayValue asArray() {
        throw new ClassCastException
            ("Field is not an Array: " + getClass());
    }

    @Override
    public MapValue asMap() {
        throw new ClassCastException
            ("Field is not a Map: " + getClass());
    }

    @Override
    public RecordValue asRecord() {
        throw new ClassCastException
            ("Field is not a Record: " + getClass());
    }

    @Override
    public Row asRow() {
        throw new ClassCastException
            ("Field is not a Row: " + getClass());
    }

    @Override
    public PrimaryKey asPrimaryKey() {
        throw new ClassCastException
            ("Field is not a PrimaryKey: " + getClass());
    }

    @Override
    public IndexKey asIndexKey() {
        throw new ClassCastException
            ("Field is not an IndexKey: " + getClass());
    }

    @Override
    public boolean isBinary() {
        return false;
    }

    @Override
    public boolean isNumber() {
        return false;
    }

    @Override
    public boolean isBoolean() {
        return false;
    }

    @Override
    public boolean isDouble() {
        return false;
    }

    @Override
    public boolean isFloat() {
        return false;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public boolean isFixedBinary() {
        return false;
    }

    @Override
    public boolean isLong() {
        return false;
    }

    @Override
    public boolean isString() {
        return false;
    }

    @Override
    public boolean isTimestamp() {
        return false;
    }

    @Override
    public boolean isEnum() {
        return false;
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public boolean isRecord() {
        return false;
    }

    @Override
    public boolean isRow() {
        return false;
    }

    @Override
    public boolean isPrimaryKey() {
        return false;
    }

    @Override
    public boolean isIndexKey() {
        return false;
    }

    @Override
    public boolean isJsonNull() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isAtomic() {
        return false;
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public boolean isComplex() {
        return false;
    }

    /**
     * Subclasses can override this but it will do a pretty good job of output
     */
    @Override
    public String toJsonString(boolean pretty) {
        if (pretty) {
            ObjectWriter writer = JsonUtils.createWriter(pretty);
            try {
                return writer.writeValueAsString(toJsonNode());
            }
            catch (IOException ioe) {
                return ioe.toString();
            }
        }
        StringBuilder sb = new StringBuilder(128);
        toStringBuilder(sb);
        return sb.toString();
    }

    /*
     * Local methods
     */
    public boolean isTuple() {
        return false;
    }

    /*
     * Check whether this is an EmptyValueImpl.
     * Note: captials are used for EMPTY to distinguish this method from
     * RecordValueImpl.isEmpty().
     */
    @Override
    public boolean isEMPTY() {
        return false;
    }

    static boolean isSpecialValue(FieldValueImpl val) {
        return val.isNull() || val.isEMPTY() || val.isJsonNull();
    }

    public int size() {
        throw new ClassCastException(
            "Value is not complex (array, map, or record): " + getClass());
    }

    public Map<String, FieldValue> getMap() {
        throw new ClassCastException(
            "Value is not a record or map: " + getClass());
    }

    @Override
    public int getInt() {
        throw new ClassCastException(
            "Value is not an integer or subtype: " + getClass());
    }

    public void setInt(@SuppressWarnings("unused")int v) {
        throw new ClassCastException(
            "Value is not an integer or subtype: " + getClass());
    }

    @Override
    public long getLong() {
        throw new ClassCastException(
            "Value is not a long or subtype: " + getClass());
    }

    public void setLong(@SuppressWarnings("unused")long v) {
        throw new ClassCastException(
            "Value is not a long or subtype: " + getClass());
    }

    @Override
    public float getFloat() {
        throw new ClassCastException(
            "Value is not a float or subtype: " + getClass());
    }

    public void setFloat(@SuppressWarnings("unused")float v) {
        throw new ClassCastException(
            "Value is not a float or subtype: " + getClass());
    }

    public void setDecimal(@SuppressWarnings("unused")BigDecimal v) {
        throw new ClassCastException(
            "Value is not a Number or subtype: " + getClass());
    }

    public BigDecimal getDecimal() {
        throw new ClassCastException(
            "Value is not a double or subtype: " + getClass());
    }

    @Override
    public double getDouble() {
        throw new ClassCastException(
            "Value is not a double or subtype: " + getClass());
    }

    public void setDouble(@SuppressWarnings("unused")double v) {
        throw new ClassCastException(
            "Value is not a double or subtype: " + getClass());
    }

    @Override
    public String getString() {
        throw new ClassCastException(
            "Value is not a string or subtype: " + getClass());
    }

    public void setString(@SuppressWarnings("unused")String v) {
        throw new ClassCastException(
            "Value is not a String or subtype: " + getClass());
    }

    @Override
    public String getEnumString() {
        throw new ClassCastException(
            "Value is not an enum or subtype: " + getClass());
    }

    public void setEnum(@SuppressWarnings("unused")String v) {
        throw new ClassCastException(
            "Value is not an enum or subtype: " + getClass());
    }

    @Override
    public boolean getBoolean() {
        throw new ClassCastException(
            "Value is not a boolean: " + getClass());
    }

    public void setBoolean(@SuppressWarnings("unused")boolean v) {
        throw new ClassCastException(
            "Value is not a boolean or subtype: " + getClass());
    }

    @Override
    public byte[] getBytes() {
        throw new ClassCastException(
            "Value is not a binary: " + getClass());
    }

    public void setTimestamp(@SuppressWarnings("unused") Timestamp timestamp) {
        throw new ClassCastException("Value is not a timestamp: " + getClass());
    }

    public Timestamp getTimestamp() {
        throw new ClassCastException("Value is not a timestamp: " + getClass());
    }

    public FieldValueImpl getElement(int index) {

        if (isArray()) {
            return ((ArrayValueImpl)this).get(index);
        }

        if (isRecord()) {
            return ((RecordValueImpl)this).get(index);
        }

        if (isTuple()) {
            return ((TupleValue)this).get(index);
        }

        throw new ClassCastException(
            "Value is not an array or record: " + getClass());
    }

    public FieldValueImpl getElement(String fieldName) {

        if (isMap()) {
            return ((MapValueImpl)this).get(fieldName);
        }

        if (isRecord()) {
            return ((RecordValueImpl)this).get(fieldName);
        }

        if (isTuple()) {
            return ((TupleValue)this).get(fieldName);
        }

        throw new ClassCastException(
            "Value is not a map or a record: " + getClass());
    }

    public FieldValueImpl getFieldValue(String fieldName) {

        if (isMap()) {
            return ((MapValueImpl)this).get(fieldName);
        }

        if (isRecord()) {
            return ((RecordValueImpl)this).get(fieldName);
        }

        if (isTuple()) {
            return ((TupleValue)this).get(fieldName);
        }

        throw new ClassCastException(
            "Value is not a map or a record: " + getClass());
    }

    /**
     * Return the "next" legal value for this type in terms of comparison
     * purposes.  That is value.compareTo(value.getNextValue()) is &lt; 0 and
     * there is no legal value such that value &lt; cantHappen &lt;
     * value.getNextValue().
     *
     * This method is only called for indexable fields and is only
     * implemented for types for which FieldDef.isValidIndexField() returns true.
     */
    FieldValueImpl getNextValue() {
        throw new IllegalArgumentException
            ("Type does not implement getNextValue: " +
             getClass().getName());
    }

    /**
     * Return the minimum legal value for this type in terms of comparison
     * purposes such that there is no value V where value.compareTo(V) &gt; 0.
     *
     * This method is only called for indexable fields and is only
     * implemented for types for which FieldDef.isValidIndexField() returns true.
     */
    FieldValueImpl getMinimumValue() {
        throw new IllegalArgumentException
            ("Type does not implement getMinimumValue: " +
             getClass().getName());
    }

    /**
     * Return a Jackson JsonNode for the instance.
     */
    public abstract JsonNode toJsonNode();

    /**
     *
     * @param sb
     */
    abstract public void toStringBuilder(StringBuilder sb);

    /**
     * Construct a FieldValue from an Java Object.
     */
    static FieldValue fromJavaObjectValue(FieldDef def, Object o) {

        switch (def.getType()) {
        case INTEGER:
            return def.createInteger((Integer)o);
        case LONG:
            return def.createLong((Long)o);
        case DOUBLE:
            return def.createDouble((Double)o);
        case FLOAT:
            return def.createFloat((Float)o);
        case NUMBER:
            return def.createNumber((BigDecimal)o);
        case STRING:
            return def.createString((String)o);
        case BINARY:
            return def.createBinary((byte[])o);
        case FIXED_BINARY:
            return def.createFixedBinary((byte[])o);
        case BOOLEAN:
            return def.createBoolean((Boolean)o);
        case ENUM:
            return def.createEnum((String)o);
        case TIMESTAMP:
            return def.createTimestamp((Timestamp)o);
        case RECORD:
            return RecordValueImpl.fromJavaObjectValue(def, o);
        case ARRAY:
            return ArrayValueImpl.fromJavaObjectValue(def, o);
        case MAP:
            return MapValueImpl.fromJavaObjectValue(def, o);
        default:
            throw new IllegalArgumentException
                ("Complex classes must override fromJavaObjectValue");
        }
    }

    /**
     * Return a String representation of the value suitable for use as part of
     * a primary key.  This method must work for any value that can participate
     * in a primary key.  The key string format may be different than a more
     * "natural" string format and may not be easily human readable.  It is
     * defined so that primary key fields sort and compare correctly and
     * consistently.
     */
    @SuppressWarnings("unused")
    public String formatForKey(FieldDef field, int storageSize) {
        throw new IllegalArgumentException
            ("Key components must be atomic types");
    }

    String formatForKey(FieldDef field) {
        return formatForKey(field, 0);
    }

    /**
     * Returns the FieldValue associated with a given field path. The path
     * steps may be identifiers or the special "keys()", "values()", or "[]"
     * steps.
     *
     * If the path crosses an array, the arrayIndex param is used to select
     * a single item from that array. However, if arrayIndex is -1, the path
     * is not supposed to cross an array, and an IllegalArgumentException is
     * thrown in this case.
     *
     * If the path crosses a map, the mapKey param is used to select a single
     * entry from that map.
     *
     * If the ValidValidator is not null, then its ValidValidator.check()
     * method is called to check if the field value is valid for the step
     * level.
     *
     * So, the method will always return at most one value. The method will
     * never return the java null, but it may return one of the 3 special
     * values: NULL, json null, or EMPTY.
     *
     * This method is used by the indexing code only, so the path conforms
     * to the kind of paths used in CREATE index statements.
     */
    FieldValueImpl findFieldValue(TablePath path,
                                  int arrayIndex,
                                  String mapKey) {
        return findFieldValue(path, 0, arrayIndex, mapKey);
    }

    private FieldValueImpl findFieldValue(TablePath path,
                                          int pathPos,
                                          int arrayIndex,
                                          String mapKey) {

        if (pathPos >= path.numSteps()) {
            throw new IllegalStateException(
                "Unexpected end of index path: " + path);
        }

        String next = path.getStep(pathPos++);

        if (!isComplex()) {

            /*
             * Handle the case where the index path is a.b.c[], but in the
             * doc the path a.b.c is an atomic. In this case, c should be
             * viewed as an array containing the single c value.
             */
            if (TableImpl.BRACKETS.equals(next) && pathPos >= path.numSteps()) {
                return this;
            }

            /*
             * Handle the case where the index path is a.b.c, but in the
             * doc the path a.b is an atomic.
             */
            return EmptyValueImpl.getInstance();
        }

        switch (getType()) {
        case RECORD: {
            RecordValueImpl rec = (RecordValueImpl)this;
            FieldValueImpl fv = rec.get(next);

            if (fv == null) {
                throw new IllegalStateException(
                    "Unexpected null field value in path " + path +
                    " at step " + next + " in record :\n" + rec);
            }

            if (pathPos >= path.numSteps()) {
                return fv;
            }

            if (fv.isNull()) {
                return fv;
            }

            return fv.findFieldValue(path, pathPos, arrayIndex, mapKey);
        }
        case MAP: {
            /*
             * Handle the case where the index path expects an array, but
             * the doc is a map instead. In this case, the map should be
             * treated as if it was an array containing this map as its
             * single element.
             */
            if (path.isBracketsStep(pathPos - 1)) {
                if (pathPos >= path.numSteps()) {
                    return this;
                }
                next =  path.getStep(pathPos++);
            }

            if (path.isKeysStep(pathPos - 1)) {
                return FieldDefImpl.stringDef.createString(mapKey);
            }

            if (path.isValuesStep(pathPos - 1)) {
                next = mapKey;
            }

            MapValueImpl map = (MapValueImpl)this;
            FieldValueImpl fv = map.get(next);

            if (fv == null) {
                return EmptyValueImpl.getInstance();
            }

            if (pathPos >= path.numSteps()) {
                return fv;
            }

            return fv.findFieldValue(path, pathPos, arrayIndex, mapKey);
        }
        case ARRAY: {
            if (arrayIndex == -1) {
                throw new IllegalArgumentException(
                    "Unexpected array in path " + path +
                    " at step " + next + " Array value =\n" + this);
            }

            ArrayValueImpl arr = (ArrayValueImpl)this;
            FieldValueImpl fv = arr.get(arrayIndex);

            /*
             * Peek at the current component. If it is [], consume it,
             * and keep going. This allows operations to target the element
             * itself vs a field contained in the element.
             */
            if (path.isBracketsStep(pathPos - 1)) {
                if (pathPos >= path.numSteps()) {
                    return fv;
                }
            } else {
                --pathPos;
            }

            if (fv.isAtomic()) {
                return EmptyValueImpl.getInstance();
            }

            /*
             * We should not encounter any other array after this one, so we
             * pass -1 as the arrayIndex param, to enfore this constraint.
             */
            return fv.findFieldValue(path, pathPos, -1, mapKey);
        }
        default:
            return null;
        }
    }

    /**
     * The type is passed explicitly for the case where it may be JSON.
     * The FieldDef is only needed for Timestamp.
     */
    protected static Object readTuple(FieldDef.Type type,
                                      FieldDef def,
                                      TupleInput in) {
        switch (type) {
        case INTEGER:
            return in.readSortedPackedInt();
        case STRING:
            return in.readString();
        case LONG:
            return in.readSortedPackedLong();
        case DOUBLE:
            return in.readSortedDouble();
        case FLOAT:
            return in.readSortedFloat();
        case NUMBER:
            return NumberUtils.readTuple(in);
        case ENUM:
            return in.readSortedPackedInt();
        case BOOLEAN:
            return in.readBoolean();
        case TIMESTAMP: {
            assert def != null;
            byte[] buf = new byte[((TimestampDefImpl)def).getNumBytes()];
            in.read(buf);
            return buf;
        }
        default:
            throw new IllegalStateException
                ("Type not supported in indexes: " + type);
        }
    }

    /**
     * Compares 2 FieldValue instances.
     *
     * For null(java null) or NULL(NullValue) value, they are compared based on
     * "null last" rule:
     *     null &gt; not null
     *     NULL &gt; NOT NULL
     */
    public static int compareFieldValues(FieldValue val1, FieldValue val2) {

        FieldValueImpl v1 = (FieldValueImpl)val1;
        FieldValueImpl v2 = (FieldValueImpl)val2;

        if (v1 != null) {
            if (v2 == null) {
                return -1;
            }
            if (isSpecialValue(v1) || isSpecialValue(v2)) {
                if (!isSpecialValue(v1)) {
                    return -1;
                }
                if (!isSpecialValue(v2)) {
                    return 1;
                }
                return compareSpecialValues(v1, v2);
            }
            return v1.compareTo(v2);
        } else if (v2 != null) {
            return 1;
        }
        return 0;
    }

    /**
     * The order of 3 kinds of special NULL values is:
     *  Empty < JSON null < SQL null
     */
    private static int compareSpecialValues(FieldValueImpl v1,
                                            FieldValueImpl v2)  {

        if (v1.isEMPTY()) {
            return v2.isEMPTY() ? 0 : -1;
        }
        if (v1.isJsonNull()) {
            return v2.isJsonNull() ? 0 : (v2.isEMPTY() ? 1 : -1);
        }
        return v2.isNull() ? 0 : 1;
    }

    /**
     * If schema is for a union, returns the union memeber schema that matches
     * the given type. Otherwise, returns the schema itself.
     */
    static Schema getUnionSchema(Schema schema, Schema.Type type) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() == type) {
                    return s;
                }
            }
            throw new IllegalArgumentException
                ("Cannot find type in union schema: " + type);
        }
        return schema;
    }

    /**
     * Casts the value to int, possibly with loss of information about
     * magnitude, precision or sign.
     */
    public int castAsInt() {
        throw new ClassCastException(
            "Value can not be cast to an integer: " + getClass());
    }

    /**
     * Casts the value to long, possibly with loss of information about
     * magnitude, precision or sign.
     */
    public long castAsLong() {
        throw new ClassCastException(
            "Value can not be cast to a long: " + getClass());
    }

    /**
     * Casts the value to float, possibly with loss of information about
     * magnitude, precision or sign.
     */
    public float castAsFloat() {
        throw new ClassCastException(
            "Value can not be cast to a float: " + getClass());
    }

    /**
     * Casts the value to double, possibly with loss of information about
     * magnitude, precision or sign.
     */
    public double castAsDouble() {
        throw new ClassCastException(
            "Value can not be cast to a double: " + getClass());
    }

    /**
     * Casts the value to BigDecimal.
     */
    public BigDecimal castAsDecimal() {
        throw new ClassCastException(
            "Value can not be cast to a Number: " + getClass());
    }

    public String castAsString() {
        throw new ClassCastException(
            "Value can not be cast to a String: " + getClass());
    }

    FieldValue castToSuperType(FieldDefImpl targetDef) {

        if (isNull()) {
            return this;
        }

        FieldDefImpl valDef = getDefinition();

        if (targetDef.isWildcard() ||
            targetDef.equals(valDef)) {
            return this;
        }

        assert(valDef.isSubtype(targetDef));

        switch (getType()) {

        case INTEGER: {
            /* target must be long or number */
            return (targetDef.isLong() ?
                    targetDef.createLong(asInteger().get()) :
                    targetDef.createNumber(asInteger().get()));
        }

        case LONG: {
            /* target must be number */
            assert targetDef.isNumber();
            return targetDef.createNumber(asLong().get());
        }

        case FLOAT: {
            /* target must be double or number */
            return (targetDef.isDouble() ?
                    targetDef.createDouble(asFloat().get()) :
                    targetDef.createNumber(asFloat().get()));
        }

        case DOUBLE: {
            /* target must be number */
            assert targetDef.isNumber();
            return targetDef.createNumber(asDouble().get());
        }

        case ARRAY: {
            FieldDefImpl elemDef = ((ArrayDefImpl)targetDef).getElement();
            ArrayValueImpl arr = (ArrayValueImpl)this;
            ArrayValueImpl newarr = ((ArrayDefImpl)targetDef).createArray();

            for (FieldValue e : arr.getArrayInternal()) {
                FieldValueImpl elem = (FieldValueImpl)e;
                newarr.addInternal(elem.castToSuperType(elemDef));
            }
            return newarr;
        }

        case MAP: {
            FieldDefImpl targetElemDef = ((MapDefImpl)targetDef).getElement();
            MapValueImpl map = (MapValueImpl)this;
            MapValueImpl newmap = ((MapDefImpl)targetDef).createMap();

            for (Map.Entry<String, FieldValue> entry : map.getMap().entrySet()) {
                String key = entry.getKey();
                FieldValueImpl elem = (FieldValueImpl)entry.getValue();
                newmap.put(key, elem.castToSuperType(targetElemDef));
            }
            return newmap;
        }

        case RECORD: {
            RecordValueImpl rec = (RecordValueImpl)this;
            RecordValueImpl newrec = ((RecordDefImpl)targetDef).createRecord();
            int numFields = rec.getNumFields();
            RecordDefImpl recTargetDef = (RecordDefImpl)targetDef;
            for (int i = 0; i < numFields; ++i) {
                FieldValueImpl fval = rec.get(i);
                if (fval != null) {
                    FieldDefImpl targetFieldDef = recTargetDef.getFieldDef(i);
                    newrec.put(i, fval.castToSuperType(targetFieldDef));
                }
            }
            return newrec;
        }

        /* these have no super types */
        case NUMBER:
        case STRING:
        case ENUM:
        case BOOLEAN:
        case BINARY:
        case FIXED_BINARY:
        case TIMESTAMP: {
            return this;
        }

        default:
            throw new IllegalStateException("Unexpected type: " + getType());
        }
    }

    @Override
    public byte[] getFixedBytes() {
        throw new ClassCastException(
            "Value is not a Fixed binary: " + getClass());
    }

    @Override
    public byte[] getNumberBytes() {
        throw new ClassCastException(
            "Value is not a Number: " + getClass());
    }

    @Override
    public byte[] getTimestampBytes() {
        throw new ClassCastException(
            "Value is not a Timestamp: " + getClass());
    }

    @Override
    public RecordValueSerializer asRecordValueSerializer() {
        throw new ClassCastException
        ("Field is not a Record: " + getClass());
    }

    @Override
    public MapValueSerializer asMapValueSerializer() {
        throw new ClassCastException
        ("Field is not a Map: " + getClass());
    }

    @Override
    public ArrayValueSerializer asArrayValueSerializer() {
        throw new ClassCastException
        ("Field is not an Array: " + getClass());
    }

    /**
     * This method is used by IndexImpl.rowFromIndexKey(), which creates a
     * partially filled table Row from a binary index key. putComplex() takes
     * as input a fieldPath and an atomic value. The fieldPath is supposed to
     * be a path that defines an index field.
     *
     * The method puts the given value deep into "this", implicitly creating
     * intermediate fields/elements along the given fieldPath, if they do not
     * already exist.
     *
     * The "jsonArrayPathPos" argument is the path index of nested array of JSON
     * field, e.g. json.users[].name, jsonArrayPathPos is 1, the path index of
     * "users". Use a negative value for non-JSON field path or JSON field
     * doesn't contain array.
     *
     * Note: the method is recursive and applies to FieldValues of different
     * kinds, but the initial, non-recursive, invocation is always on an a Row
     * instance.
     */
    void putComplex(TablePath path,
                    FieldValueImpl value,
                    String keyForKeysField,
                    int jsonArrayPathPos) {

        putComplex(path, 0, value, keyForKeysField, jsonArrayPathPos);
    }

    private FieldValueImpl putComplex(TablePath path,
                                      int pathPos,
                                      FieldValueImpl value,
                                      String keyForKeysField,
                                      int jsonArrayPathPos) {

        switch (getType()) {

        case RECORD: {

            if (isTuple()) {
                throw new QueryStateException(
                    "Cannot add fields to a TupleValue : " + this +
                    "\nIndex path : " + path);
            }

            RecordValueImpl rec = (RecordValueImpl)this;

            String fname = path.getStep(pathPos);
            int pos = rec.getFieldPos(fname);
            FieldValueImpl fval;

            if (pathPos == path.numSteps() - 1) {
                fval = value;
            } else {
                fval = rec.get(pos);

                if (fval == null) {
                    FieldDef elemDef = rec.getFieldDef(pos);
                    fval = createComplexValue(elemDef,
                                              isJsonArrayPath(elemDef,
                                                              jsonArrayPathPos,
                                                              pathPos));
                }

                if (fval.isNull()) {
                    return fval;
                }

                fval = fval.putComplex(path, ++pathPos, value, keyForKeysField,
                                       jsonArrayPathPos);
            }

            if (fval.isEMPTY()) {
                throw new QueryStateException(
                    "Cannot insert EMPTY value at field " + fname +
                    " of record:\n" + rec + "\nIndex path : " + path +
                    " path pos = " + pathPos);
            }

            rec.putInternal(pos, fval);
            return this;
        }
        case ARRAY: {
            ArrayValueImpl arr = (ArrayValueImpl)this;
            FieldDefImpl elemDef = arr.getElementDef();

            String fname = path.getStep(pathPos);

            if (!path.isBracketsStep(pathPos)) {
                throw new QueryStateException(
                    "Unexpected step " + fname + " in index path : " + path);
            }

            /*
             * If the index entry is EMPTY, either (a) the whole array in the
             * associated row is be empty or (b) the current path is empty.
             * For the query it does not matter anyway, so, just return the
             * array without putting anything in it.
             */
            if (value.isEMPTY()) {
                return this;
            }

            if (pathPos == path.numSteps() - 1) {

                /*
                 * If the index entry is NULL, the whole array in the associated
                 * row must be NULL.
                 */
                if (value.isNull()) {
                    return value;
                }

                if (arr.size() == 0) {
                    arr.add(value);
                } else {
                    arr.set(0, value);
                }

                return this;
            }

            /*
             * The array contains records or maps. If value.isNull() at this
             * point, either (a) the whole array is NULL, or (b) the field
             * indexed inside a contained rec/map is NULL. We cannot
             * differentiate between (a) and (b). We will assume (b) is true,
             * which means that we will try to insert the NULL somewhere
             * downstream in the path. What if we are wrong and (a) is actually
             * true? In some cases it doesn't matter, but in other its does.
             * Consider the following 2 examples:
             *
             * create table foo (
             *    id integer,
             *    rec record(a integer,
             *               arr1 array(map(long)),
             *               arr2 array(map(record(b long, c long))))
             * )
             *
             * create index idx1 on foo (rec.arr1[].foo)
             * create index idx2 on foo (rec.arr2[].foo.b rec.arr2[].foo.c )
             *
             *
             * For idx2, where all the indexed fields are record fields, it is
             * ok to assume (b), because even if (a) is true, then the row we
             * construct from the index entry will contain NULL for all the
             * indexed fields, and if the index is used to evaluate a path expr
             * that matches an index path, the result will be the same with both
             * (a) and (b).
             *
             * For idx1, the recursive invocation of putComplex() below, will
             * try to insert a NULL in the map that is created inside this
             * array. This is not allowed of course, and the recursive
             * invocation will return NULL. This implies that (a) was actually
             * true. To correct the wrong assumption that (b) was true, we
             * check below whether the result of the recursive call is NULL,
             * and if so, we just return NULL instead of trying to insert the
             * NULL in this array( this array is essentially thrown away).
             */

            FieldValueImpl elem = null;

            if (arr.size() == 0) {
                /*
                 * Array can contains map or record, so use false for
                 * isJsonAsArray.
                 */
                elem = createComplexValue(elemDef, false /* jsonIsArray */);
                arr.add(elem);
            } else {
                if (!(arr.get(0) instanceof ComplexValueImpl)) {
                    throw new QueryStateException(
                        "Invalid attempt to overwrite an atomic element " +
                        "with a complex element in an array encountered at " +
                        "step " + fname + " in index path : " + path +
                        "\nArray value : \n" + this);
                }

                elem = arr.get(0);
            }

            elem = elem.putComplex(path, ++pathPos, value, keyForKeysField, -1);

            if (elem.isNull()) {
                return elem;
            }

            return this;
        }

        case MAP: {
            /*
             * There are 3 different kinds of index paths on maps:
             * 1. index on the map's key : map.keys()
             * 2. index on the map's value : map.values()....
             * 3. index on the values of a specific map key : map.someKey
             * An index can mix combinations of the above paths, the only
             * restriction being that in an index entry that contains multiple
             * "keys()" and/or "values()" fields, the values of these fields
             * all come from the same index entry in the original map inside
             * the associated table row.
             */
            MapValueImpl map = (MapValueImpl)this;

            /*
             * If the index entry is EMPTY, either (a) the whole map in the
             * associated row is be empty or (b) the current path is empty.
             * For the query it does not matter anyway, so, just return the
             * map without putting anything in it.
             */
            if (value.isEMPTY()) {
                return this;
            }

            FieldDefImpl elemDef = map.getElementDef();

            String mapKey;

            /* Case 1 */
            if (path.isKeysStep(pathPos)) {

                /*
                 * If the index entry is NULL, the whole map in the associated
                 * row must be NULL.
                 */
                if (value.isNull()) {
                    return value;
                }

                mapKey = ((StringValueImpl)value).get();
                assert(keyForKeysField != null);
                assert(keyForKeysField.equals(mapKey));

                /*
                 * If any "values()" index fields have been processed already,
                 * the map contains an entry E with "values()" as its key. We
                 * must replace E with an entry whose key is the mapKey and
                 * whose value is the E.value. Otherwise, we insert a new entry
                 * with mapKey as its key and NULL as its value.
                 */
                FieldValue mapVal = map.get(TableImpl.VALUES);
                if (mapVal != null) {
                    map.remove(TableImpl.VALUES);
                    map.put(mapKey, mapVal);
                } else {
                    map.putNull(mapKey);
                }

                return this;
            }

            /*
             * If the current path is a "values()" path and we have already
             * processed a "keys()" path, the values of these 2 paths come from
             * the same entry of the original map, so they must also be in the
             * same entry of the recreated map. So, use the key from the
             * "keys()" path as the mapKey.
             */
            if (path.isValuesStep(pathPos)) {
                mapKey = (keyForKeysField != null ?
                          keyForKeysField : TableImpl.VALUES);
            } else {
                mapKey = path.getStep(pathPos);
            }

            /*
             * NOTE: at this point mapKey may be a normal key string or the
             * "values()" string.  In the latter case an entry will be created
             * using "values()" as the map key.
             */

            if (pathPos == path.numSteps() - 1) {

                /*
                 * If the index entry is NULL, the whole map in the associated
                 * row must be NULL.
                 */
                if (value.isNull()) {
                    return value;
                }

                /*
                 * There are no more components, which implies that the map
                 * values are atomics. Put a map entry using mapKey as the
                 * key and the given value as the value.
                 */
                map.put(mapKey, value);
                return this;
            }

            /*
             * if value.isNull(), the comment in the ARRAY case above
             * applies here as well
             */

            /* Create the field if it's not been created. */
            FieldValueImpl elem = map.get(mapKey);

            if (elem == null || elem.isNull()) {
                elem = createComplexValue(elemDef,
                                          isJsonArrayPath(elemDef,
                                                          jsonArrayPathPos,
                                                          pathPos));
                map.put(mapKey, elem);
            }

            elem = elem.putComplex(path, ++pathPos, value, keyForKeysField,
                                   jsonArrayPathPos);

            if (elem.isNull()) {
                return elem;
            }

            return this;
        }
        default:
            throw new QueryStateException(
                "Cannot put a value in an atomic value");
        }
    }

    private boolean isJsonArrayPath(FieldDef def,
                                    int jsonArrayPathPos,
                                    int pathPos) {

        return def.isJson() &&
               (jsonArrayPathPos >= 0 && pathPos == jsonArrayPathPos);
    }

    static private ComplexValueImpl createComplexValue(FieldDef def,
                                                       boolean jsonIsArray) {
        switch (def.getType()) {
        case MAP:
            return (ComplexValueImpl) def.createMap();
        case RECORD:
            return (ComplexValueImpl) def.createRecord();
        case ARRAY:
            return (ComplexValueImpl) def.createArray();
        case JSON:
            return (ComplexValueImpl) ((jsonIsArray) ?
                                       def.createArray() : def.createMap());
        default:
            throw new IllegalArgumentException(
                "Not a complex type: " + def.getType());
        }
    }
}

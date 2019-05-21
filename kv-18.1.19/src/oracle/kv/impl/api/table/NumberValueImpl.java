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

import java.math.BigDecimal;
import java.util.Arrays;

import oracle.kv.impl.util.SortableString;
import oracle.kv.table.NumberValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DecimalNode;

/**
 * This class represents a BigDecimal value.
 */
public class NumberValueImpl extends FieldValueImpl implements NumberValue {

    private static final long serialVersionUID = 1L;

    private byte[] value;

    NumberValueImpl(long value) {
        this.value = NumberUtils.serialize(value);
    }

    NumberValueImpl(BigDecimal value) {
        this.value = NumberUtils.serialize(value);
    }

    public NumberValueImpl(byte[] value) {
        this.value = value;
    }

    /**
     * This constructor creates DecimalValueImpl from the String format used for
     * sorted keys.
     */
    NumberValueImpl(String keyValue) {
        value = SortableString.bytesFromSortable(keyValue);
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public NumberValueImpl clone() {
        return new NumberValueImpl(value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof NumberValueImpl) {
            return Arrays.equals(value, ((NumberValueImpl) other).value);
        }
        return false;
    }

    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof NumberValueImpl) {
            return IndexImpl.compareUnsignedBytes
                    (value, ((NumberValueImpl)other).value);
        } else if (other.isNumeric()){
            BigDecimal otherVal = null;
            switch (other.getType()) {
            case INTEGER:
                otherVal = BigDecimal.valueOf(other.asInteger().get());
                break;
            case LONG:
                otherVal = BigDecimal.valueOf(other.asLong().get());
                break;
            case FLOAT:
                otherVal = BigDecimal.valueOf(other.asFloat().get());
                break;
            case DOUBLE:
                otherVal = BigDecimal.valueOf(other.asDouble().get());
                break;
            default:
                break;
            }
            assert(otherVal != null);
            return get().compareTo(otherVal);
        }
        throw new ClassCastException("Object is not a numeric type");
    }

    @Override
    public String toString() {
        return get().toString();
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.NUMBER;
    }

    @Override
    public NumberDefImpl getDefinition() {
        return FieldDefImpl.numberDef;
    }

    @Override
    public NumberValue asNumber() {
        return this;
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    @Override
    public boolean isNumeric() {
        return true;
    }

    /*
     * Public api methods from NumberValue
     */

    @Override
    public BigDecimal get() {
        return getDecimal();
    }

    /*
     * FieldValueImpl internal api methods
     */
    @Override
    public BigDecimal getDecimal() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return BigDecimal.valueOf(((Integer)val).intValue());
        }
        if (val instanceof Long) {
            return BigDecimal.valueOf(((Long)val).longValue());
        }

        assert(val instanceof BigDecimal);
        return (BigDecimal)val;
    }

    @Override
    public void setDecimal(BigDecimal v) {
        value = NumberUtils.serialize(v);
    }

    @Override
    public int castAsInt() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return ((Integer)val).intValue();
        }
        if (val instanceof Long) {
            return ((Long)val).intValue();
        }

        assert(val instanceof BigDecimal);
        return ((BigDecimal)val).intValue();
    }

    @Override
    public long castAsLong() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return ((Integer)val).longValue();
        }
        if (val instanceof Long) {
            return ((Long)val).longValue();
        }

        assert(val instanceof BigDecimal);
        return getDecimal().longValue();
    }

    @Override
    public float castAsFloat() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return ((Integer)val).floatValue();
        }
        if (val instanceof Long) {
            return ((Long)val).floatValue();
        }

        assert(val instanceof BigDecimal);
        return ((BigDecimal)val).floatValue();
    }

    @Override
    public double castAsDouble() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return ((Integer)val).doubleValue();
        }
        if (val instanceof Long) {
            return ((Long)val).doubleValue();
        }

        assert(val instanceof BigDecimal);
        return ((BigDecimal)val).doubleValue();
    }

    @Override
    public BigDecimal castAsDecimal() {
        return getDecimal();
    }

    @Override
    public String castAsString() {
        Object val = getNumericValue();
        return val.toString();
    }

    private Object getNumericValue() {
        return NumberUtils.deserialize(value);
    }

    @Override
    public String formatForKey(FieldDef field1, int storageSize) {
        return toKeyString(value);
    }

    @Override
    FieldValueImpl getNextValue() {
        return new NumberValueImpl(NumberUtils.nextUp(value));
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return new NumberValueImpl(NumberUtils.getNegativeInfinity());
    }

    @Override
    public JsonNode toJsonNode() {
        return new DecimalNode(getDecimal());
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append(toString());
    }

    @Override
    public byte[] getBytes() {
        return value;
    }

    static String toKeyString(byte[] bytes) {
        return SortableString.toSortable(bytes);
    }

    @Override
    public byte[] getNumberBytes() {
        return getBytes();
    }
}

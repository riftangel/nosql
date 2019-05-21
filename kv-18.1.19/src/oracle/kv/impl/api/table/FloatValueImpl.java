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

import java.io.IOException;

import oracle.kv.impl.util.SortableString;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FloatValue;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.node.ValueNode;

import com.sleepycat.persist.model.Persistent;

@Persistent(version=1)
public class FloatValueImpl extends FieldValueImpl implements FloatValue {

    private static final long serialVersionUID = 1L;

    protected float value;

    FloatValueImpl(float value) {
        this.value = value;
    }

    /**
     * This constructor creates FloatValueImpl from the String format used for
     * sorted keys.
     */
    FloatValueImpl(String keyValue) {
        this.value = SortableString.floatFromSortable(keyValue);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private FloatValueImpl() {
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public FloatValueImpl clone() {
        return new FloatValueImpl(value);
    }

    @Override
    public int hashCode() {
        return ((Float) value).hashCode();
    }

    @Override
    public boolean equals(Object other) {

        /* == doesn't work for the various Float constants */
        if (other instanceof FloatValueImpl) {
            return Double.compare(value, ((FloatValueImpl)other).get()) == 0;
        }
        return false;
    }

    /**
     * Allow comparison against Double to succeed
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof FloatValueImpl) {
            return Double.compare(value, ((FloatValueImpl)other).get());
        }
        throw new ClassCastException("Object is not an DoubleValue");
    }

    @Override
    public String toString() {
        return Float.toString(value);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.FLOAT;
    }

    @Override
    public FloatDefImpl getDefinition() {
        return FieldDefImpl.floatDef;
    }

    @Override
    public FloatValue asFloat() {
        return this;
    }

    @Override
    public boolean isFloat() {
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
     * Public api methods from FloatValue
     */

    @Override
    public float get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public float getFloat() {
        return value;
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public void setFloat(float v) {
        value = v;
    }

    @Override
    public int castAsInt() {
        return (int)value;
    }

    @Override
    public long castAsLong() {
        return (long)value;
    }

    @Override
    public float castAsFloat() {
        return value;
    }

    @Override
    public double castAsDouble() {
        return value;
    }

    @Override
    public BigDecimal castAsDecimal() {
        return new BigDecimal(value);
    }

    @Override
    public String castAsString() {
        return Float.toString(value);
    }

    @Override
    public FieldValueImpl getNextValue() {
        return new FloatValueImpl(Math.nextUp(value));
    }

    @Override
    public FieldValueImpl getMinimumValue() {
        return new FloatValueImpl(Float.MIN_VALUE);
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        return toKeyString(value);
    }

    /**
     * Jackson does not have a FloatNode for the object node representation.
     * There is a FloatNode implementation below that works for serializing
     * the value to JSON.
     */
    @Override
    public JsonNode toJsonNode() {
        return new FloatNode(value);
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append(toString());
    }

    static String toKeyString(float value) {
        return SortableString.toSortable(value);
    }
    /*
     * local methods
     */

    /**
     * Jackson 1.9 does not have a FloatNode.  The implementation of
     * toJsonString() uses toJsonNode() which requires a JsonNode
     * implementation for this type.  This class is a minimal implementation
     * that works for serialization of a Float value as a string for JSON
     * output.
     *
     * It is only used by toJsonNode(), which is only used
     * for FieldValue.toJsonString().  If another mechanism is used for
     * that output this can go away.  Also, if an upgrade to Jackson
     * 2.x is done it can go away, as that version has a FloatNode.
     *
     * TODO: implement another way to do toJsonString()
     */
    private static final class FloatNode extends ValueNode {
        private final float value;

        FloatNode(float value) {
            this.value = value;
        }

        /**
         * This is the only method that matters.  The others exist because they
         * are abstract in the base.
         */
        @Override
        public final void serialize(JsonGenerator jg, SerializerProvider provider)
            throws IOException, JsonProcessingException
        {
            jg.writeNumber(value);
        }

        @Override
        public JsonToken asToken() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            return false;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public String getValueAsText() {
            return asText();
        }

        @Override
        public String asText() {
            return ((Float)value).toString();
        }
    }
}

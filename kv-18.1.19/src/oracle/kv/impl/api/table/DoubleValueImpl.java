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

import oracle.kv.impl.util.SortableString;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DoubleNode;

import com.sleepycat.persist.model.Persistent;

@Persistent(version=1)
public class DoubleValueImpl extends FieldValueImpl implements DoubleValue {

    private static final long serialVersionUID = 1L;

    protected double value;

    DoubleValueImpl(double value) {
        this.value = value;
    }

    /**
     * This constructor creates DoubleValueImpl from the String format used for
     * sorted keys.
     */
    DoubleValueImpl(String keyValue) {
        this.value = SortableString.doubleFromSortable(keyValue);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private DoubleValueImpl() {
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public DoubleValueImpl clone() {
        return new DoubleValueImpl(value);
    }

    @Override
    public int hashCode() {
        return ((Double) value).hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof DoubleValueImpl) {
            /* == doesn't work for the various Double constants */
            return Double.compare(value,((DoubleValueImpl)other).get()) == 0;
        }
        return false;
    }

    /**
     * Allow comparison against Float to succeed.
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof DoubleValueImpl) {
            return Double.compare(value, ((DoubleValueImpl)other).get());
        }
        throw new ClassCastException("Object is not an DoubleValue");
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.DOUBLE;
    }

    @Override
    public DoubleDefImpl getDefinition() {
        return FieldDefImpl.doubleDef;
    }

    @Override
    public DoubleValue asDouble() {
        return this;
    }

    @Override
    public boolean isDouble() {
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
     * Public api methods from DoubleValue
     */

    @Override
    public double get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public void setDouble(double v) {
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
        return (float)value;
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
        return Double.toString(value);
    }

    @Override
    public FieldValueImpl getNextValue() {
        return new DoubleValueImpl(Math.nextUp(value));
    }

    @Override
    public FieldValueImpl getMinimumValue() {
        return new DoubleValueImpl(Double.MIN_VALUE);
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        return toKeyString(value);
    }

    @Override
    public JsonNode toJsonNode() {
        return new DoubleNode(value);
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append(toString());
    }

    static String toKeyString(double value) {
        return SortableString.toSortable(value);
    }
}

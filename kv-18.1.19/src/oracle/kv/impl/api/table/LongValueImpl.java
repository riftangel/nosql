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
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.LongValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.LongNode;

import com.sleepycat.persist.model.Persistent;

@Persistent(version=1)
public class LongValueImpl extends FieldValueImpl implements LongValue {

    private static final long serialVersionUID = 1L;

    public static LongValueImpl ZERO = new LongValueImpl();

    protected long value;

    LongValueImpl(long value) {
        this.value = value;
    }

    /**
     * This constructor creates LongValueImpl from the String format used for
     * sorted keys.
     */
    LongValueImpl(String keyValue) {
        this.value = SortableString.longFromSortable(keyValue);
    }

    /* DPL */
    private LongValueImpl() {
        value = 0;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public LongValueImpl clone() {
        return new LongValueImpl(value);
    }

    @Override
    public int hashCode() {
        return ((Long) value).hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof LongValueImpl) {
            return value == ((LongValueImpl)other).get();
        }
        return false;
    }

    /**
     * Allow comparison to IntegerValue succeed.
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof LongValueImpl) {
            return compare(value, ((LongValueImpl)other).value);
        } else if (other instanceof IntegerValueImpl) {
            return compare(value, ((IntegerValueImpl)other).get());
        }

        throw new ClassCastException("Value is not comparable to LongValue");
    }

    public static int compare(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.LONG;
    }

    @Override
    public LongDefImpl getDefinition() {
        return FieldDefImpl.longDef;
    }

    @Override
    public LongValue asLong() {
        return this;
    }

    @Override
    public boolean isLong() {
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
     * Public api methods from LongValue
     */

    @Override
    public long get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public long getLong() {
        return value;
    }

    @Override
    public void setLong(long v) {
        value = v;
    }

    @Override
    public int castAsInt() {
        return (int)value;
    }

    @Override
    public long castAsLong() {
        return value;
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
        return Long.toString(value);
    }

    @Override
    FieldValueImpl getNextValue() {
        if (value == Long.MAX_VALUE) {
            return null;
        }
        return new LongValueImpl(value + 1L);
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return new LongValueImpl(Long.MIN_VALUE);
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        return toKeyString(value, field);
    }

    @Override
    public JsonNode toJsonNode() {
        return new LongNode(value);
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append(toString());
    }

    static String toKeyString(long value, FieldDef field) {
        int len = (field != null) ? ((LongDefImpl)field).getEncodingLength() : 0;
        return SortableString.toSortable(value, len);
    }

}

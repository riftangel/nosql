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

import oracle.kv.table.BooleanValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import com.sleepycat.persist.model.Persistent;

@Persistent(version=1)
public class BooleanValueImpl extends FieldValueImpl implements BooleanValue {

    private static final long serialVersionUID = 1L;

    /*
     * Use simple "t" and "f" as primary key serialization. See formatForKey()
     */
    static final String trueKeyString = "t";
    static final String falseKeyString = "f";

    public static final BooleanValueImpl trueValue =
        new BooleanValueImpl(true);

    public static final BooleanValueImpl falseValue =
        new BooleanValueImpl(false);

    private boolean value;

    private BooleanValueImpl(boolean value) {
        this.value = value;
    }

    /* DPL */
    private BooleanValueImpl() {
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public BooleanValueImpl clone() {
        return create(value);
    }

    @Override
    public int hashCode() {
        return ((Boolean) value).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof BooleanValueImpl) {
            return value == ((BooleanValueImpl)other).get();
        }
        return false;
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof BooleanValueImpl) {
            /* java 7
            return Boolean.compare(value, ((BooleanValueImpl)other).value);
            */
            return ((Boolean)value).compareTo(((BooleanValueImpl)other).value);
        }
        throw new ClassCastException("Object is not an BooleanValue");
    }

    @Override
    public String toString() {
        return Boolean.toString(value);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.BOOLEAN;
    }

    @Override
    public BooleanDefImpl getDefinition() {
        return FieldDefImpl.booleanDef;
    }

    @Override
    public BooleanValue asBoolean() {
        return this;
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    /*
     * Public api methods from BooleanValue
     */

    @Override
    public boolean get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public boolean getBoolean() {
        return value;
    }

    @Override
    public void setBoolean(boolean v) {
        value = v;
    }

    @Override
    public JsonNode toJsonNode() {
        return (value ? BooleanNode.TRUE : BooleanNode.FALSE);
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append(toString());
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        return toKeyString(value);
    }

    @Override
    FieldValueImpl getNextValue() {
        if (!value) {
            return trueValue;
        }
        return null;
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return falseValue;
    }

    /*
     * local methods
     */

    public static BooleanValueImpl create(boolean value) {
        return (value ? trueValue : falseValue);
    }

    static BooleanValueImpl create(String value) {
        if (value.equals(trueKeyString)) {
            return trueValue;
        }
        if (value.equals(falseKeyString)) {
            return falseValue;
        }
        throw new IllegalArgumentException("Invalid string for boolean type: " +
                                           value);
    }

    static String toKeyString(boolean value) {
        return value ? trueKeyString : falseKeyString;
    }
}

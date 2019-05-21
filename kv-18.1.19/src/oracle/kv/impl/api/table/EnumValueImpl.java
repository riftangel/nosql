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

import oracle.kv.impl.util.SortableString;
import oracle.kv.table.EnumDef;
import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;
import org.codehaus.jackson.util.CharTypes;

import com.sleepycat.persist.model.Persistent;

/**
 * A single value in an enumeration is represented as a string.  Only strings
 * that are part of the enumeration are allowed to be set in this object.
 * Validation is performed on construction and setting of the value.
 * Construction requires the {@link EnumDef} that defines the valid strings for
 * the enumeration.
 */
@Persistent(version=1)
public class EnumValueImpl extends FieldValueImpl implements EnumValue {

    private static final long serialVersionUID = 1L;

    private final EnumDefImpl field;

    private String value;


    EnumValueImpl(EnumDef field, String value) {
        this.field = (EnumDefImpl) field;
        this.value = value;
        validate();
    }

    /* DPL */
    @SuppressWarnings("unused")
    private EnumValueImpl() {
        field = null;
        value = null;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public EnumValueImpl clone() {
        return new EnumValueImpl(field, value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof EnumValueImpl) {
            EnumValueImpl otherVal = (EnumValueImpl) other;
            EnumDefImpl def = field;
            EnumDefImpl otherDef = otherVal.getDefinition();
            /*
             * Avoid calling EnumDefImpl.equals() because it will
             * result in a recursive calling circle.
             */
            return (def.valuesEqual(otherDef) &&
                    value.equals(otherVal.get()));
        }
        return false;
    }

    /**
     * compareTo compares based on order of the specified enumeration values
     * in the enum and not the string values.
     */
    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof EnumValueImpl) {
            int thisIndex = indexOf(value);
            int otherIndex = indexOf(((EnumValueImpl)other).value);
            return ((Integer)thisIndex).compareTo(otherIndex);
        }
        throw new ClassCastException
            ("Object is not an IntegerValue");
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.ENUM;
    }

    @Override
    public EnumDefImpl getDefinition() {
        return field;
    }

    @Override
    public EnumValue asEnum() {
        return this;
    }

    @Override
    public boolean isEnum() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    /*
     * Public api methods from EnumValue
     */

    @Override
    public String get() {
        return value;
    }

    @Override
    public int getIndex() {
        return (field).indexOf(value);
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public String getEnumString() {
        return value;
    }

    @Override
    public void setEnum(String v) {
        value = v;
    }

    @Override
    public String castAsString() {
        return value;
    }

    /**
     * Add one to the index of this value in the enum and return a new
     * EnumValueImpl based on that index.  If there is no value with the
     * next index, return null.
     */
    @Override
    public EnumValueImpl getNextValue() {
        int index = getIndex();
        EnumDefImpl def = field;
        if (def.isValidIndex(index + 1)) {
            return def.createEnum(index + 1);
        }
        return null;
    }

    /**
     * Minimum value for any enum is 0
     */
    @Override
    public FieldValueImpl getMinimumValue() {
        return (field).createEnum(0);
    }

    /**
     * In order to sort correctly keys from an enumeration value must be the
     * value's index in the declaration.
     */
    @Override
    public String formatForKey(FieldDef field1, int storageSize) {
        return toKeyString(field, getIndex());
    }

    @Override
    public JsonNode toJsonNode() {
        return new TextNode(value);
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        if (value == null) {
            sb.append("null");
            return;
        }

        sb.append('\"');
        CharTypes.appendQuoted(sb, value);
        sb.append('\"');
    }

    /*
     * local methods
     */

    int indexOf(String enumValue) {
        return (field).indexOf(enumValue);
    }

    private void validate() {
        if (field != null && value != null) {
            (field).validateValue(value);
            return;
        }
        throw new IllegalArgumentException
            ("Value not valid for enumeration: " + value);
    }

    static EnumValueImpl createFromKey(EnumDef field, String indexString) {
        EnumDefImpl def = (EnumDefImpl)field;
        int index = SortableString.intFromSortable(indexString);
        return def.createEnum(index);
    }

    static String toKeyString(EnumDefImpl enumDef, int index) {
        return SortableString.toSortable(index, enumDef.getEncodingLen());
    }
}

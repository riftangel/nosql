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

import static oracle.kv.impl.api.table.TableJsonUtils.MAX;
import static oracle.kv.impl.api.table.TableJsonUtils.MIN;

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.IntegerDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * IntegerDefImpl is an implementation of the IntegerDef interface.
 */
@Persistent(version=1)
public class IntegerDefImpl extends FieldDefImpl implements IntegerDef {

    private static final long serialVersionUID = 1L;

    /*
     * min and max are inclusive
     */
    private Integer min;
    private Integer max;
    private int encodingLength;

    /**
     * Constructor requiring all fields.
     */
    IntegerDefImpl(String description, Integer min, Integer max) {
        super(Type.INTEGER, description);
        this.min = min;
        this.max = max;
        validate();
    }

    /**
     * Constructor requiring just the description.
     */
    IntegerDefImpl(String description) {
        this(description, null, null);
    }

    /**
     * This constructor defaults most fields.
     */
    IntegerDefImpl() {
        super(Type.INTEGER);
        min = null;
        max = null;
        encodingLength = 0; /* 0 means full-length */
    }

    private IntegerDefImpl(IntegerDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
        encodingLength = impl.encodingLength;
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public IntegerDefImpl clone() {

        if (this == FieldDefImpl.integerDef) {
            return this;
        }

        return new IntegerDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof IntegerDefImpl;
    }

    @Override
    public boolean isValidKeyField() {
        return true;
    }

    @Override
    public boolean isValidIndexField() {
        return true;
    }

    @Override
    public IntegerDef asInteger() {
        return this;
    }

    @Override
    public IntegerValueImpl createInteger(int value) {

        return (hasMin() || hasMax() ?
                new IntegerRangeValue(value, this) :
                new IntegerValueImpl(value));
    }

    @Override
    IntegerValueImpl createInteger(String value) {

        return (hasMin() || hasMax() ?
                new IntegerRangeValue(value, this) :
                new IntegerValueImpl(value));
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.TABLE_API_VERSION;
    }

    /*
     * Public api methods from IntegerDef
     */

    @Override
    public Integer getMin() {
        return min;
    }

    @Override
    public Integer getMax() {
        return max;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    int getEncodingLength() {
        return encodingLength;
    }

    @Override
    public boolean hasMin() {
        return min != null;
    }

    @Override
    public boolean hasMax() {
        return max != null;
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isInteger() ||
            superType.isLong() ||
            superType.isNumber() ||
            superType.isAny() ||
            superType.isAnyJsonAtomic() ||
            superType.isAnyAtomic() ||
            superType.isJson()) {
            return true;
        }

        return false;
    }

    @Override
    void toJson(ObjectNode node) {
        super.toJson(node);
        if (min != null) {
            node.put(MIN, min);
        }
        if (max != null) {
            node.put(MAX, max);
        }
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isInt()) {
            throw new IllegalArgumentException
                ("Default value for type INTEGER is not int");
        }
        return createInteger(node.getIntValue());
    }

    /*
     * local methods
     */

    private void validate() {

        if (min != null && max != null) {
            if (min > max) {
                throw new IllegalArgumentException
                    ("Invalid min or max value");
            }
        }
        encodingLength = SortableString.encodingLength(min, max);
    }

    /**
     * Validates the value against the range if one exists.
     * min/max are inclusive.
     */
    void validateValue(int val) {

        if ((min != null && val < min) || (max != null && val > max)) {
            StringBuilder sb = new StringBuilder();
            sb.append("Value, ");
            sb.append(val);
            sb.append(", is outside of the allowed range");
            throw new IllegalArgumentException(sb.toString());
        }
    }
}

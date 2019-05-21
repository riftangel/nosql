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
import oracle.kv.table.FloatDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * FloatDefImpl implements the FloatDef interface.
 */
@Persistent(version=1)
public class FloatDefImpl extends FieldDefImpl implements FloatDef {

    private static final long serialVersionUID = 1L;
    /*
     * These are not final to allow for schema evolution.
     */
    private Float min;
    private Float max;

    /**
     * Constructor requiring all fields.
     */
    FloatDefImpl(String description, Float min, Float max) {
        super(Type.FLOAT, description);
        this.min = min;
        this.max = max;
        validate();
    }

    FloatDefImpl(String description) {
        this(description, null, null);
    }

    /**
     * This constructor defaults most fields.
     */
    FloatDefImpl() {
        super(Type.FLOAT);
        min = null;
        max = null;
    }

    private FloatDefImpl(FloatDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public FloatDefImpl clone() {

        if (this == FieldDefImpl.floatDef) {
            return this;
        }

        return new FloatDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FloatDefImpl;
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
    public FloatDef asFloat() {
        return this;
    }

    @Override
    public FloatValueImpl createFloat(float value) {

        return (hasMin() || hasMax() ?
                new FloatRangeValue(value, this) :
                new FloatValueImpl(value));
    }

    @Override
    FloatValueImpl createFloat(String value) {

        return (hasMin() || hasMax() ?
                new FloatRangeValue(value, this) :
                new FloatValueImpl(value));
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.TABLE_API_VERSION;
    }

    /*
     * Public api methods from FloatDef
     */

    @Override
    public Float getMin() {
        return min;
    }

    @Override
    public Float getMax() {
        return max;
    }

    /*
     * FieldDefImpl internal api methods
     */

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

        if (superType.isFloat() ||
            superType.isDouble() ||
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

        /*
         * Add min, max
         */
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

        /*
         * Jackson does not have a FloatNode in the version being used, so
         * no further validation is possible.
         */
        if (!node.isNumber()) {
            throw new IllegalArgumentException
                ("Default value for type FLOAT is not a number");
        }
        return createFloat(Float.valueOf(node.asText()));
    }

    /*
     * local methods
     */

    private void validate() {

        /* Make sure min <= max */
        if (min != null && max != null) {
            if (min > max) {
                throw new IllegalArgumentException
                    ("Invalid min or max value");
            }
        }
    }

    /**
     * Validates the value against the range if one exists.
     */
    void validateValue(float val) {

        /* min/max are inclusive */
        if ((min != null && val < min) ||
            (max != null && val > max)) {
            StringBuilder sb = new StringBuilder();
            sb.append("Value, ");
            sb.append(val);
            sb.append(", is outside of the allowed range");
            throw new IllegalArgumentException(sb.toString());
        }
    }
}

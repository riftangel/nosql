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
import oracle.kv.table.DoubleDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * DoubleDefImpl implements the DoubleDef interface.
 */
@Persistent(version=1)
public class DoubleDefImpl extends FieldDefImpl implements DoubleDef {

    private static final long serialVersionUID = 1L;

    private Double min;
    private Double max;

    /**
     * Constructor requiring all fields.
     */
    DoubleDefImpl(String description, Double min, Double max) {
        super(Type.DOUBLE, description);
        this.min = min;
        this.max = max;
        validate();
    }

    DoubleDefImpl(String description) {
        this(description, null, null);
    }

    /**
     * This constructor defaults most fields.
     */
    DoubleDefImpl() {
        this(null, null, null);
    }

    private DoubleDefImpl(DoubleDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public DoubleDefImpl clone() {

        if (this == FieldDefImpl.doubleDef) {
            return this;
        }

        return new DoubleDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DoubleDefImpl;
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
    public DoubleDef asDouble() {
        return this;
    }

    @Override
    public DoubleValueImpl createDouble(double value) {

        return (hasMin() || hasMax() ?
                new DoubleRangeValue(value, this) :
                new DoubleValueImpl(value));
    }

    @Override
    DoubleValueImpl createDouble(String value) {

        return (hasMin() || hasMax() ?
                new DoubleRangeValue(value, this) :
                new DoubleValueImpl(value));
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.TABLE_API_VERSION;
    }

    /*
     * Public api methods from DoubleDef
     */

    @Override
    public Double getMin() {
        return min;
    }

    @Override
    public Double getMax() {
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

        if (superType.isDouble() ||
            superType.isNumber() ||
            superType.isAny() ||
            superType.isAnyAtomic() ||
            superType.isAnyJsonAtomic() ||
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
        if (!node.isDouble()) {
            throw new IllegalArgumentException
                ("Default value for type DOUBLE is not double");
        }
        return createDouble(node.getDoubleValue());
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
     * min/max are inclusive
     */
    void validateValue(double val) {
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

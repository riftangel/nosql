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
import static oracle.kv.impl.api.table.TableJsonUtils.MAX_INCL;
import static oracle.kv.impl.api.table.TableJsonUtils.MIN;
import static oracle.kv.impl.api.table.TableJsonUtils.MIN_INCL;

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.StringDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * StringDefImpl implements the StringDef interface.
 */
@Persistent(version=1)
public class StringDefImpl extends FieldDefImpl implements StringDef {

    private static final long serialVersionUID = 1L;

    private String min;
    private String max;
    private Boolean minInclusive;
    private Boolean maxInclusive;

    StringDefImpl(
        String description,
        String min,
        String max,
        Boolean minInclusive,
        Boolean maxInclusive) {

        super(Type.STRING, description);
        this.min = min;
        this.max = max;
        this.minInclusive = minInclusive;
        this.maxInclusive = maxInclusive;
        validate();
    }

    StringDefImpl(String description) {
        this(description, null, null, null, null);
    }

    StringDefImpl() {
        super(Type.STRING);
        min = null;
        max = null;
        minInclusive = null;
        maxInclusive = null;
    }

    private StringDefImpl(StringDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
        minInclusive = impl.minInclusive;
        maxInclusive = impl.maxInclusive;
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public StringDefImpl clone() {

        if (this == FieldDefImpl.stringDef) {
            return this;
        }

        return new StringDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof StringDefImpl;
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
    public StringDef asString() {
        return this;
    }

    @Override
    public StringValueImpl createString(String value) {

        return (hasMin() || hasMax() ?
                new StringRangeValue(value, this) :
                new StringValueImpl(value));
    }

    /*
     * Public api methods from StringDef
     */

    @Override
    public String getMin() {
        return min;
    }

    @Override
    public String getMax() {
        return max;
    }

    @Override
    public boolean isMinInclusive() {
        /* Default value of inclusive is true */
        return (minInclusive != null ? minInclusive : true);
    }

    @Override
    public boolean isMaxInclusive() {
        /* Default value of inclusive is true */
        return (maxInclusive != null ? maxInclusive : true);
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

        if (superType.isString() ||
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
            node.put(MIN_INCL, minInclusive);
        }
        if (max != null) {
            node.put(MAX, max);
            node.put(MAX_INCL, maxInclusive);
        }
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isTextual()) {
            throw new IllegalArgumentException
                ("Default value for type STRING is not a string");
        }
        return createString(node.asText());
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.TABLE_API_VERSION;
    }

    /*
     * local methods
     */

    private void validate() {
        /* Make sure min <= max */
        if (min != null && max != null) {
            if (min.compareTo(max) > 0 ) {
                throw new IllegalArgumentException
                    ("Invalid min or max value");
            }
        }
    }

    void validateValue(String val) {
        if (val == null) {
            throw new IllegalArgumentException
                ("String values cannot be null");
        }
        if ((min != null &&
             ((isMinInclusive() && min.compareTo(val) > 0) ||
              (!isMinInclusive() && min.compareTo(val) >= 0))) ||
            (max != null &&
             ((isMaxInclusive() && max.compareTo(val) < 0) ||
              (!isMaxInclusive() && max.compareTo(val) <= 0)))) {

            StringBuilder sb = new StringBuilder();
            sb.append("Value, ");
            sb.append(val);
            sb.append(", is outside of the allowed range");
            if (min != null && isMinInclusive()) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            if (min != null) {
                sb.append(min);
            } else {
                sb.append("-INF");
            }
            if (max != null) {
                sb.append(max);
            } else {
                sb.append("+INF");
            }
            if (max != null && isMaxInclusive()) {
                sb.append("]");
            } else {
                sb.append(")");
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }
}

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

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.StringValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.util.CharTypes;

@Persistent(version=1)
public class StringValueImpl extends FieldValueImpl implements StringValue {

    private static final long serialVersionUID = 1L;

    protected String value;

    private static final char MIN_VALUE_CHAR = ((char) 1);

    StringValueImpl(String value) {
        this.value = value;
    }

    /* DPL */
    @SuppressWarnings("unused")
    private StringValueImpl() {
        value = null;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public StringValueImpl clone() {
        return new StringValueImpl(value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof StringValueImpl) {
            return value.equals(((StringValueImpl)other).get());
        }
        return false;
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof StringValueImpl) {
            return value.compareTo(((StringValueImpl)other).value);
        }
        throw new ClassCastException("Object is not an StringValue");
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.STRING;
    }

    @Override
    public StringDefImpl getDefinition() {
        return FieldDefImpl.stringDef;
    }

    @Override
    public StringValue asString() {
        return this;
    }

    @Override
    public boolean isString() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    /*
     * Public api methods from StringValue
     */

    @Override
    public String get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public String getString() {
        return value;
    }

    @Override
    public void setString(String v) {
        value = v;
    }

    @Override
    public String castAsString() {
        return value;
    }

    /**
     * The "next" value, lexicographically, is this string with a
     * minimum character (1) added.
     */
    @Override
    FieldValueImpl getNextValue() {
        return new StringValueImpl(incrementString(value));
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return new StringValueImpl("");
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        return toKeyString(value);
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
     * Local methods
     */

    public static StringValueImpl create(String value) {
        return new StringValueImpl(value);
    }

    static String incrementString(String value) {
        return value + MIN_VALUE_CHAR;
    }

    static String toKeyString(String value) {
        return value;
    }
}

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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import com.sleepycat.persist.model.Persistent;

/**
 * A class to encapsulate an explicitly null value for a field.  This class is
 * implemented as a singleton object and is never directly constructed or
 * accessed by applications.
 */
@Persistent(version=1)
public class NullValueImpl extends FieldValueImpl {

    private static final long serialVersionUID = 1L;

    private static NullValueImpl instanceValue = new NullValueImpl();

    public static NullValueImpl getInstance() {
        return instanceValue;
    }

    /* DPL */
    NullValueImpl() {
    }

    @Override
    public FieldDef.Type getType() {
        throw new UnsupportedOperationException
            ("Cannot get type from NullNode");
    }

    @Override
    public FieldDefImpl getDefinition() {
        throw new UnsupportedOperationException
            ("Cannot get type from NullNode");
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public JsonNode toJsonNode() {
        return NullNode.getInstance();
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append("null");
    }

    @Override
    public NullValueImpl clone() {
        return getInstance();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof NullValueImpl;
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof NullValueImpl) {

            /* all null values are the same, and equal */
            return 0;
        }
        throw new ClassCastException
            ("Object is not a NullValue");
    }

    @Override
    public String toString() {
        return "NullValue";
    }
}

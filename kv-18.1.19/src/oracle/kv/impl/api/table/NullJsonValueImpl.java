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

/**
 * A class to encapsulate an explicitly null value for a field.  This class is
 * implemented as a singleton object and is never directly constructed or
 * accessed by applications.
 */
public class NullJsonValueImpl extends FieldValueImpl {

    private static final long serialVersionUID = 1L;

    private static NullJsonValueImpl instanceValue = new NullJsonValueImpl();

    public static NullJsonValueImpl getInstance() {
        return instanceValue;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.JSON;
    }

    @Override
    public FieldDefImpl getDefinition() {
        return FieldDefImpl.jsonDef;
    }

    @Override
    public boolean isJsonNull() {
        return true;
    }

    @Override
    public boolean isAtomic() {
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
    public NullJsonValueImpl clone() {
        return getInstance();
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof NullJsonValueImpl) {

            /* all null values are the same, and equal */
            return 0;
        }
        throw new ClassCastException
            ("Object is not a NullJsonValue");
    }

    @Override
    public String toString() {
        return "null";
    }
}

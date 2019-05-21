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

import java.util.Arrays;

import oracle.kv.table.BinaryValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import com.sleepycat.persist.model.Persistent;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BinaryNode;

@Persistent(version=1)
public class BinaryValueImpl extends FieldValueImpl implements BinaryValue {

    private static final long serialVersionUID = 1L;

    final private byte[] value;

    BinaryValueImpl(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException
                ("Binary values cannot be null");
        }
        this.value = value;
    }

    /* DPL */
    @SuppressWarnings("unused")
    private BinaryValueImpl() {
        value = null;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public BinaryValueImpl clone() {
        return new BinaryValueImpl(value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof BinaryValueImpl) {
            return Arrays.equals(value, ((BinaryValueImpl)other).get());
        }
        return false;
    }

    /**
     * Returns 0 if the two values are equal in terms of length and byte
     * content, otherwise it returns -1.
     */
    @Override
    public int compareTo(FieldValue otherValue) {
        return (equals(otherValue) ? 0 : -1);
    }

   @Override
    public String toString() {
        return TableJsonUtils.encodeBase64(value);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.BINARY;
    }

    @Override
    public BinaryDefImpl getDefinition() {
        return FieldDefImpl.binaryDef;
    }

    @Override
    public BinaryValue asBinary() {
        return this;
    }

    @Override
    public boolean isBinary() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    /*
     * Public api methods from BinaryValue
     */

    @Override
    public byte[] get() {
        return value;
    }

    @Override
    public JsonNode toJsonNode() {
        return new BinaryNode(value);
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append("\"");
        sb.append(TableJsonUtils.encodeBase64(value));
        sb.append("\"");
    }

    /*
     * Methods from FieldValueImpl
     */

    @Override
    public byte[] getBytes() {
        return value;
    }

    /*
     * local methods
     */
    public static BinaryValueImpl create(byte[] value) {
        return new BinaryValueImpl(value);
    }
}

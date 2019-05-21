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

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FixedBinaryValue;

import org.codehaus.jackson.Base64Variants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BinaryNode;

import com.sleepycat.persist.model.Persistent;

@Persistent(version=1)
public class FixedBinaryValueImpl extends FieldValueImpl
    implements FixedBinaryValue {

    private static final long serialVersionUID = 1L;

    private byte[] value;

    private final FixedBinaryDefImpl def;

    FixedBinaryValueImpl(byte[] value, FixedBinaryDefImpl def) {
        this.value = value;
        this.def = def;
    }

    /* DPL */
    @SuppressWarnings("unused")
    private FixedBinaryValueImpl() {
        def = null;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public FixedBinaryValueImpl clone() {
        return new FixedBinaryValueImpl(value, def);
    }

   @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof FixedBinaryValueImpl) {
            FixedBinaryValueImpl otherImpl = (FixedBinaryValueImpl)other;
            return (def.equals(otherImpl.def) &&
                    Arrays.equals(value, otherImpl.get()));
        }
        return false;
    }

    /**
     * TODO: maybe use JE comparator algorithm.
     * For now, all binary is equal
     */
    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof FixedBinaryValueImpl) {
            return 0;
        }
        throw new ClassCastException
            ("Object is not an FixedBinaryValue");
    }

    @Override
    public String toString() {
        return Base64Variants.getDefaultVariant().encode(value, false);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.FIXED_BINARY;
    }

    @Override
    public FixedBinaryDefImpl getDefinition() {
        return def;
    }

    @Override
    public FixedBinaryValue asFixedBinary() {
        return this;
    }

    @Override
    public boolean isFixedBinary() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    /*
     * Public api methods from FixedBinaryValue
     */

    @Override
    public byte[] get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */
    @Override
    public byte[] getBytes() {
        return value;
    }

    @Override
    public JsonNode toJsonNode() {
        return new BinaryNode(value);
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append(Base64Variants.getDefaultVariant().encode(value, true));
    }

    @Override
    public byte[] getFixedBytes() {
        return getBytes();
    }
}

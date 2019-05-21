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

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.AnyDef;
import oracle.kv.table.BinaryValue;
import oracle.kv.table.BooleanValue;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.FloatValue;
import oracle.kv.table.IntegerValue;
import oracle.kv.table.LongValue;
import oracle.kv.table.StringValue;

public class AnyDefImpl extends FieldDefImpl implements AnyDef {

    private static final long serialVersionUID = 1L;

    AnyDefImpl() {
        super(Type.ANY, "");
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public AnyDefImpl clone() {
        return FieldDefImpl.anyDef;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof AnyDefImpl;
    }

    @Override
    public AnyDef asAny() {
        return this;
    }

    @Override
    public BinaryValue createBinary(byte[] value) {
        return binaryDef.createBinary(value);
    }

    @Override
    public BooleanValue createBoolean(boolean value) {
        return booleanDef.createBoolean(value);
    }

    @Override
    public DoubleValue createDouble(double value) {
        return doubleDef.createDouble(value);
    }

    @Override
    public FloatValue createFloat(float value) {
        return floatDef.createFloat(value);
    }

    @Override
    public IntegerValue createInteger(int value) {
        return integerDef.createInteger(value);
    }

    @Override
    public LongValue createLong(long value) {
        return longDef.createLong(value);
    }

    @Override
    public StringValue createString(String value) {
        return stringDef.createString(value);
    }

    @Override
    public ArrayValueImpl createArray() {
        return new ArrayValueImpl(FieldDefImpl.arrayAnyDef);
    }

    @Override
    public MapValueImpl createMap() {
        return new MapValueImpl(FieldDefImpl.mapAnyDef);
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return false;
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {
        return superType.getType() == Type.ANY;
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.QUERY_VERSION;
    }
}

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

/*
 * A factory to create instances of FieldDefImpl subclasses. Such instances
 * need to be created by the query packages, and instead of making the
 * constructors of the FieldDefImpl subclasses public, we use this factory.
 */
public class FieldDefFactory {

    public static IntegerDefImpl createIntegerDef() {
        return FieldDefImpl.integerDef;
    }

    public static LongDefImpl createLongDef() {
        return FieldDefImpl.longDef;
    }

    public static FloatDefImpl createFloatDef() {
        return FieldDefImpl.floatDef;
    }

    public static JsonDefImpl createJsonDef() {
        return FieldDefImpl.jsonDef;
    }

    public static DoubleDefImpl createDoubleDef() {
        return FieldDefImpl.doubleDef;
    }

    public static NumberDefImpl createNumberDef() {
        return FieldDefImpl.numberDef;
    }

    public static StringDefImpl createStringDef() {
        return FieldDefImpl.stringDef;
    }

    public static EnumDefImpl createEnumDef(String[] values) {
        return new EnumDefImpl(values, null/*descr*/);
    }

    public static BooleanDefImpl createBooleanDef() {
        return FieldDefImpl.booleanDef;
    }

    public static BinaryDefImpl createBinaryDef() {
        return FieldDefImpl.binaryDef;
    }

    public static FixedBinaryDefImpl createFixedBinaryDef(int size) {
        return new FixedBinaryDefImpl(size, null/*descr*/);
    }

    public static TimestampDefImpl createTimestampDef(int precision) {
        return FieldDefImpl.timestampDefs[precision];
    }

    public static RecordDefImpl createRecordDef(
        FieldMap fieldMap,
        String descr) {
        return new RecordDefImpl(fieldMap, descr);
    }

    public static ArrayDefImpl createArrayDef(FieldDefImpl elemType) {
        return createArrayDef(elemType, null);
    }

    public static ArrayDefImpl createArrayDef(
        FieldDefImpl elemType,
        String descr) {

        if (descr == null) {
            if (elemType.isJson()) {
                return FieldDefImpl.arrayJsonDef;
            }

            if (elemType.isAny()) {
                return FieldDefImpl.arrayAnyDef;
            }
        }

        return new ArrayDefImpl(elemType, descr);
    }

    public static MapDefImpl createMapDef(FieldDefImpl elemType) {
        return createMapDef(elemType, null);
    }

    public static MapDefImpl createMapDef(
        FieldDefImpl elemType,
        String descr) {

        if (descr == null) {
            if (elemType.isJson()) {
                return FieldDefImpl.mapJsonDef;
            }

            if (elemType.isAny()) {
                return FieldDefImpl.mapAnyDef;
            }
        }

        return new MapDefImpl(elemType, descr);
    }

    public static AnyDefImpl createAnyDef() {
        return FieldDefImpl.anyDef;
    }

    public static AnyAtomicDefImpl createAnyAtomicDef() {
        return FieldDefImpl.anyAtomicDef;
    }

    public static AnyJsonAtomicDefImpl createAnyJsonAtomicDef() {
        return FieldDefImpl.anyJsonAtomicDef;
    }

    public static AnyRecordDefImpl createAnyRecordDef() {
        return FieldDefImpl.anyRecordDef;
    }

    public static FieldDefImpl createAtomicTypeDef(FieldDef.Type type) {
        switch (type) {
        case STRING:
            return createStringDef();
        case INTEGER:
            return createIntegerDef();
        case LONG:
            return createLongDef();
        case DOUBLE:
            return createDoubleDef();
        case FLOAT:
            return createFloatDef();
        case NUMBER:
            return createNumberDef();
        case BINARY:
            return createBinaryDef();
        case BOOLEAN:
            return createBooleanDef();
        default:
            throw new IllegalArgumentException(
                "Cannot create an atomic field def of type " + type);
        }
    }
}

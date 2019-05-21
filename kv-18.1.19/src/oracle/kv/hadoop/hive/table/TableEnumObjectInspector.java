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

package oracle.kv.hadoop.hive.table;

import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldValue;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.ENUM to Hive column type STRING.
 */
public class TableEnumObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableStringObjectInspector {

    /*
     * Note: with respect to the 'get' methods defined by this ObjectInspector,
     * when the Hive infrastructure invokes those methods during a given
     * query, the data Object input to those methods may be an instance of
     * FieldValue (specifically, an EnumValue) or may be an instance of
     * the corresponding Java class (that is, a String). As a result, each
     * such method must be prepared to handle both cases.
     *
     * With respect to the 'create/set' methods, this class defaults to
     * the same behavior as the corresponding 'create/set' methods of the
     * JavaStringObjectInspector class; which always returns a Java String
     * representing the String value of an enum), instead of an EnumValue.
     */

    TableEnumObjectInspector() {
        super(TypeInfoFactory.stringTypeInfo);
    }

    @Override
    public Text getPrimitiveWritableObject(Object o) {
        return new Text(getPrimitiveJavaObject(o));
    }

    @Override
    public String getPrimitiveJavaObject(Object o) {

        if (o == null) {
            return TableFieldTypeEnum.TABLE_FIELD_UNKNOWN_TYPE.toString();
        }

        if (o instanceof FieldValue) {
            final EnumValue enumValue = ((FieldValue) o).asEnum();
            return enumValue.get();
        }
        return o.toString();
    }

    @Override
    public Object create(Text value) {
        return value == null ? null : value.toString();
    }

    @Override
    public Object set(Object o, Text value) {
        return value == null ? null : value.toString();
    }

    @Override
    public Object create(String value) {
        return value;
    }

    @Override
    public Object set(Object o, String value) {
        return value;
    }
}

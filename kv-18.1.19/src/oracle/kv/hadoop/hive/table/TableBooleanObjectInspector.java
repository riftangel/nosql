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

import oracle.kv.table.FieldValue;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.BOOLEAN to Hive column type BOOLEAN.
 */
public class TableBooleanObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableBooleanObjectInspector {

    /*
     * Implementation note: this class is a clone (with modifications)
     * of the JavaBooleanObjectInspector class from the Hive package
     * org.apache.hadoop.hive.serde2.objectinspector.primitive. Although
     * it would be preferable to subclass JavaBooleanObjectInspector
     * and then override the 'get' methods (and inherit the 'settable'
     * methods) of that class, this unfortunately cannot be done;
     * because the constructor for JavaBooleanObjectInspector is defined
     * with default package access rather than public or protected access.
     * Because of this, cloning JavaBooleanObjectInspector is the only way
     * to provide the necessary functionality and avoid the scoping related
     * compilation errors that would result from subclassing.
     *
     * With respect to the 'get' methods defined by this ObjectInspector,
     * when the Hive infrastructure invokes those methods during a given
     * query, the data Object input to those methods may be an instance of
     * FieldValue (specifically, a BooleanValue) or may be an instance of
     * the corresponding Java class (that is, a Boolean). As a result, each
     * such method must be prepared to handle both cases.
     *
     * With respect to the 'create/set' methods, this class defaults to
     * the same behavior as the corresponding 'create/set' methods of the
     * JavaBooleanObjectInspector class.
     */

    private static boolean DEFAULT_VALUE = false;

    TableBooleanObjectInspector() {
        super(TypeInfoFactory.booleanTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        return o == null ? null : new BooleanWritable(get(o));
    }

    @Override
    public boolean get(Object o) {

        if (o == null) {
            return DEFAULT_VALUE;
        }

        if (o instanceof Boolean) {
            return ((Boolean) o).booleanValue();
        } else if (o instanceof FieldValue) {
            if (((FieldValue) o).isNull()) {
                return DEFAULT_VALUE;
            }
            return ((FieldValue) o).asBoolean().get();
        }
        throw new IllegalArgumentException(
                      "invalid object type: must be Boolean or BooleanValue");
    }

    @Override
    public Object create(boolean value) {
        return value ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override
    public Object set(Object o, boolean value) {
        return value ? Boolean.TRUE : Boolean.FALSE;
    }
}

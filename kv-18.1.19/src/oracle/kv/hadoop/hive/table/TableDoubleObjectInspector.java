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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.DoubleWritable;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.DOUBLE to Hive column type DOUBLE.
 */
public class TableDoubleObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableDoubleObjectInspector {

    /*
     * Implementation note: this class is a clone (with modifications)
     * of the JavaDoubleObjectInspector class from the Hive package
     * org.apache.hadoop.hive.serde2.objectinspector.primitive. Although
     * it would be preferable to subclass JavaDoubleObjectInspector
     * and then override the 'get' methods (and inherit the 'settable'
     * methods) of that class, this unfortunately cannot be done;
     * because the constructor for JavaDoubleObjectInspector is defined
     * with default package access rather than public or protected access.
     * Because of this, cloning JavaDoubleObjectInspector is the only way
     * to provide the necessary functionality and avoid the scoping related
     * compilation errors that would result from subclassing.
     *
     * With respect to the 'get' methods defined by this ObjectInspector,
     * when the Hive infrastructure invokes those methods during a given
     * query, the data Object input to those methods may be an instance of
     * FieldValue (specifically, a DoubleValue) or may be an instance of
     * the corresponding Java class (that is, a Double). As a result, each
     * such method must be prepared to handle both cases.
     *
     * With respect to the 'create/set' methods, this class defaults to
     * the same behavior as the corresponding 'create/set' methods of the
     * JavaDoubleObjectInspector class.
     */

    private static double DEFAULT_VALUE = 0.0d;

    TableDoubleObjectInspector() {
        super(TypeInfoFactory.doubleTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        return o == null ? null : new DoubleWritable(get(o));
    }

    @Override
    public double get(Object o) {

        if (o == null) {
            return DEFAULT_VALUE;
        }

        if (o instanceof Double) {
            return ((Double) o).doubleValue();
        } else if (o instanceof FieldValue) {
            if (((FieldValue) o).isNull()) {
                return DEFAULT_VALUE;
            }
            return ((FieldValue) o).asDouble().get();
        }
        throw new IllegalArgumentException(
                      "invalid object type: must be Double or DoubleValue");
    }

    @Override
    public Object create(double value) {
        return Double.valueOf(value);
    }

    @Override
    public Object set(Object o, double value) {
        return Double.valueOf(value);
    }
}

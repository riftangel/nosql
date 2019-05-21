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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.INTEGER to Hive column type INT.
 */
public class TableIntObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableIntObjectInspector {

    /*
     * Implementation note: this class is a clone (with modifications)
     * of the JavaIntObjectInspector class from the Hive package
     * org.apache.hadoop.hive.serde2.objectinspector.primitive. Although
     * it would be preferable to subclass JavaIntObjectInspector
     * and then override the 'get' methods (and inherit the 'settable'
     * methods) of that class, this unfortunately cannot be done;
     * because the constructor for JavaIntObjectInspector is defined
     * with default package access rather than public or protected access.
     * Because of this, cloning JavaIntObjectInspector is the only way
     * to provide the necessary functionality and avoid the scoping related
     * compilation errors that would result from subclassing.
     *
     * With respect to the 'get' methods defined by this ObjectInspector,
     * when the Hive infrastructure invokes those methods during a given
     * query, the data Object input to those methods may be an instance of
     * FieldValue (specifically, a IntegerValue) or may be an instance of
     * the corresponding Java class (that is, an Integer). As a result, each
     * such method must be prepared to handle both cases.
     *
     * With respect to the 'create/set' methods, this class defaults to
     * the same behavior as the corresponding 'create/set' methods of the
     * JavaIntObjectInspector class.
     */

    private static int DEFAULT_VALUE = 0;

    TableIntObjectInspector() {
        super(TypeInfoFactory.intTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        return o == null ? null : new IntWritable(get(o));
    }

    @Override
    public int get(Object o) {

        if (o == null) {
            return DEFAULT_VALUE;
        }

        if (o instanceof Integer) {
            return ((Integer) o).intValue();
        } else if (o instanceof FieldValue) {
            if (((FieldValue) o).isNull()) {
                return DEFAULT_VALUE;
            }
            return ((FieldValue) o).asInteger().get();
        }
        throw new IllegalArgumentException(
                      "invalid object type: must be Integer or IntegerValue");
    }

    @Override
    public Object create(int value) {
        return Integer.valueOf(value);
    }

    @Override
    public Object set(Object o, int value) {
        return Integer.valueOf(value);
    }
}

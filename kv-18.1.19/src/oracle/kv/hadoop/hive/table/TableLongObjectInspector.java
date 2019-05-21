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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.LONG to Hive column type LONG.
 */
public class TableLongObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableLongObjectInspector {

    /*
     * Implementation note: this class is a clone (with modifications)
     * of the JavaLongObjectInspector class from the Hive package
     * org.apache.hadoop.hive.serde2.objectinspector.primitive. Although
     * it would be preferable to subclass JavaLongObjectInspector
     * and then override the 'get' methods (and inherit the 'settable'
     * methods) of that class, this unfortunately cannot be done;
     * because the constructor for JavaLongObjectInspector is defined
     * with default package access rather than public or protected access.
     * Because of this, cloning JavaLongObjectInspector is the only way
     * to provide the necessary functionality and avoid the scoping related
     * compilation errors that would result from subclassing.
     *
     * With respect to the 'get' methods defined by this ObjectInspector,
     * when the Hive infrastructure invokes those methods during a given
     * query, the data Object input to those methods may be an instance of
     * FieldValue (specifically, a LongValue) or may be an instance of
     * the corresponding Java class (that is, a Long). As a result, each
     * such method must be prepared to handle both cases.
     *
     * With respect to the 'create/set' methods, this class defaults to
     * the same behavior as the corresponding 'create/set' methods of the
     * JavaLongObjectInspector class.
     */

    private static long DEFAULT_VALUE = 0L;

    TableLongObjectInspector() {
        super(TypeInfoFactory.longTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        return o == null ? null : new LongWritable(get(o));
    }

    @Override
    public long get(Object o) {

        if (o == null) {
            return DEFAULT_VALUE;
        }

        if (o instanceof Long) {
            return ((Long) o).longValue();
        } else if (o instanceof FieldValue) {
            if (((FieldValue) o).isNull()) {
                return DEFAULT_VALUE;
            }
            return ((FieldValue) o).asLong().get();
        }
        throw new IllegalArgumentException(
                      "invalid object type: must be Long or LongValue");
    }

    @Override
    public Object create(long value) {
        return Long.valueOf(value);
    }

    @Override
    public Object set(Object o, long value) {
        return Long.valueOf(value);
    }
}

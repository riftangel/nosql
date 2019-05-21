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

import java.util.Arrays;

import oracle.kv.table.BinaryValue;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FixedBinaryValue;

import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of either type FieldDef.Type.BINARY or type FieldDef.Type.FIXED_BINARY
 * to Hive column type BINARY (byte[]).
 */
public class TableBinaryObjectInspector
                 extends AbstractPrimitiveJavaObjectInspector
                 implements SettableBinaryObjectInspector {

    /*
     * Implementation note: this class is a clone (with modifications)
     * of the JavaBinaryObjectInspector class from the Hive package
     * org.apache.hadoop.hive.serde2.objectinspector.primitive. Although
     * it would be preferable to subclass JavaBinaryObjectInspector
     * and then override the 'get' methods (and inherit the 'settable'
     * methods) of that class, this unfortunately cannot be done;
     * because the constructor for JavaBinaryObjectInspector is defined
     * with default package access rather than public or protected access.
     * Because of this, cloning JavaBinaryObjectInspector is the only way
     * to provide the necessary functionality and avoid the scoping related
     * compilation errors that would result from subclassing.
     *
     * With respect to the 'get' methods defined by this ObjectInspector,
     * when the Hive infrastructure invokes those methods during a given
     * query, the data Object input to those methods may be an instance of
     * FieldValue (specifically, a BinaryValue or a FixedBinaryValue) or may
     * be an instance of the corresponding Java class (that is, a byte array).
     * As a result, each such method must be prepared to handle both cases.
     *
     * With respect to the 'create/set/copy' methods, this class defaults
     * to the same behavior as the corresponding 'create/set/copy' methods
     * of the JavaBinaryObjectInspector class.
     */

    TableBinaryObjectInspector() {
        super(TypeInfoFactory.binaryTypeInfo);
    }

    @Override
    public byte[] copyObject(Object o) {

        if (null == o) {
            return null;
        }
        final byte[] incoming = (byte[]) o;
        final byte[] outgoing = new byte[incoming.length];
        System.arraycopy(incoming, 0, outgoing, 0, incoming.length);
        return outgoing;
    }

    @Override
    public BytesWritable getPrimitiveWritableObject(Object o) {

        if (o == null) {
            return null;
        }

        if (o instanceof byte[]) {
            return new BytesWritable((byte[]) o);
        }

        if (o instanceof FieldValue) {
            if (((FieldValue) o).isNull()) {
                return null;
            }
            if (o instanceof BinaryValue) {
                final BinaryValue binaryValue = (BinaryValue) o;
                return new BytesWritable(binaryValue.get());
            }
            if (o instanceof FixedBinaryValue) {
                final FixedBinaryValue fixedBinaryValue = (FixedBinaryValue) o;
                return new BytesWritable(fixedBinaryValue.get());
            }
        }
        throw new IllegalArgumentException(
                      "invalid input object: must be byte[], " +
                      "BinaryValue, or FixedBinaryValue");
    }

    @Override
    public byte[] getPrimitiveJavaObject(Object o) {

        if (o == null) {
            return null;
        }

        if (o instanceof byte[]) {
            return (byte[]) o;

        }

        if (o instanceof FieldValue) {
            if (((FieldValue) o).isNull()) {
                return null;
            } 
            if (o instanceof BinaryValue) {
                final BinaryValue binaryValue = (BinaryValue) o;
                return binaryValue.get();
            }
            if (o instanceof FixedBinaryValue) {
                final FixedBinaryValue fixedBinaryValue = (FixedBinaryValue) o;
                return fixedBinaryValue.get();
            }
        }
        throw new IllegalArgumentException(
                      "invalid input object: must be byte[], " +
                      "BinaryValue, or FixedBinaryValue");
    }

    @Override
    public byte[] set(Object o, byte[] bb) {

        return bb == null ? null : Arrays.copyOf(bb, bb.length);
    }

    @Override
    public byte[] set(Object o, BytesWritable bw) {

        return bw == null ? null :  LazyUtils.createByteArray(bw);
    }

    @Override
    public byte[] create(byte[] bb) {
        return bb == null ? null : Arrays.copyOf(bb, bb.length);
    }

    @Override
    public byte[] create(BytesWritable bw) {
        return bw == null ? null : LazyUtils.createByteArray(bw);
    }
}

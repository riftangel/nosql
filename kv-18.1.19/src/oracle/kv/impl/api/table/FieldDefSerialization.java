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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FixedBinaryDef;
import oracle.kv.table.MapDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.TimestampDef;

/**
 * Methods to serialize and deserialize FieldDefImpl instances. It is assumed
 * that ranges and default values for types that support them need not be
 * included. This is because FieldDef instances are only (currently)
 * serialized when serializing a query plan, which will not contain these.
 *
 * If that assumption changes, then ranges and defaults and perhaps even
 * comments will be added.
 *
 * @see #writeFieldDef FastExternalizable format
 */
public class FieldDefSerialization {

    /*******************************************************************
     *
     * Serialization methods
     *
     *******************************************************************/

    /**
     * Writes a {@link FieldDef} to the output stream.  Format:
     * <ol>
     * <li> ({@link Type}) {@link FieldDef#getType def.getType()}
     * <li> Additional data for these types:
     *   <ol type="a">
     *   <li> {@link Type#FIXED_BINARY FIXED_BINARY}: {@link
     *        #writeFixedBinary writeFixedBinary(def)}
     *   <li> {@link Type#ENUM ENUM}: {@link #writeEnum writeEnum(def)}
     *   <li> {@link Type#TIMESTAMP TIMESTAMP}: {@link #writeTimestamp
     *        writeTimestamp(def)}
     *   <li> {@link Type#RECORD RECORD}: {@link #writeRecord
     *        writeRecord(def)}
     *   <li> {@link Type#MAP MAP}: {@link #writeMap writeMap(def)}
     *   <li> {@link Type#ARRAY ARRAY}: {@link #writeArray writeArray(def)}
     *   </ol>
     * </ol>
     */
    public static void writeFieldDef(FieldDef def,
                                     DataOutput out,
                                     short serialVersion) throws IOException {

        /*
         * The type of the value
         */
        def.getType().writeFastExternal(out, serialVersion);

        switch (def.getType()) {
        case INTEGER:
        case LONG:
        case DOUBLE:
        case FLOAT:
        case STRING:
        case BINARY:
        case BOOLEAN:
        case NUMBER:
        case ANY:
        case ANY_ATOMIC:
        case ANY_RECORD:
        case EMPTY:
        case JSON:
        case ANY_JSON_ATOMIC:
            break;
        case FIXED_BINARY:
            writeFixedBinary(def.asFixedBinary(), out, serialVersion);
            break;
        case ENUM:
            writeEnum(def.asEnum(), out, serialVersion);
            break;
        case TIMESTAMP:
            writeTimestamp(def.asTimestamp(), out, serialVersion);
            break;
        case RECORD:
            writeRecord((RecordDefImpl) def, out, serialVersion);
            break;
        case MAP:
            writeMap(def.asMap(), out, serialVersion);
            break;
        case ARRAY:
            writeArray(def.asArray(), out, serialVersion);
            break;
        default:
            throw new AssertionError();
        }
    }

    /**
     * Writes a {@link FixedBinaryDef} to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) {@link FixedBinaryDef#getSize def.getSize()}
     * </ol>
     */
    public static void writeFixedBinary(
        FixedBinaryDef def,
        DataOutput out,
        @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        /*
         * Write the (fixed) size of the binary. Fixed binary can only
         * be null or full-sized, so the size of its byte array is the
         * same as the defined size.
         */
        int size = def.getSize();
        SerializationUtil.writeNonNullSequenceLength(out, size);
    }

    /**
     * Writes a {@link RecordDef} to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeString String}) {@link
     *      RecordDef#getName name}
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) {@link RecordDef#getNumFields numFields}
     * <li> For each field:
     *   <ol>
     *   <li> ({@link SerializationUtil#writeString String}) <i>field name</i>
     *   <li> {@link #writeFieldDef writeFieldDef(<i>field def</i>)}
     *   <li> ({@link DataOutput#writeBoolean boolean}) {@linkplain
     *        RecordDef#isNullable(String) <i>whether field is nullable</i>}
     *   <li> <i>[Optional]</i> {@link FieldValueSerialization#writeFieldValue
     *        writeFieldValue(} {@linkplain RecordDef#getDefaultValue(String)
     *        <i>default value</i>)} // if field is not nullable
     *   </ol>
     * </ol>
     */
    public static void writeRecord(RecordDefImpl def,
                                   DataOutput out,
                                   short serialVersion) throws IOException {

        SerializationUtil.writeString(out, serialVersion, def.getName());
        SerializationUtil.writeNonNullSequenceLength(out, def.getNumFields());

        for (FieldMapEntry fme : def.getFieldProperties()) {
            String fname = fme.getFieldName();
            FieldDefImpl fdef = fme.getFieldDef();
            SerializationUtil.writeString(out, serialVersion, fname);
            writeFieldDef(fdef, out, serialVersion);
            boolean nullable = fme.isNullable();
            out.writeBoolean(nullable);
            if (!nullable) {
                FieldValue defVal = fme.getDefaultValue();
                assert(defVal != null);
                FieldValueSerialization.writeFieldValue(
                    defVal, fdef.isWildcard() /* writeValDef */,
                    out, serialVersion);
            }
        }
    }

    /**
     * Writes a {@link MapDef} to the output stream.  Format:
     * <ol>
     * <li> {@link #writeFieldDef writeFieldDef(} {@link MapDef#getElement
     *      def.getElement())}
     * </ol>
     */
    public static void writeMap(MapDef def,
                                DataOutput out,
                                short serialVersion) throws IOException {

        writeFieldDef(def.getElement(), out, serialVersion);
    }

    /**
     * Writes an {@link ArrayDef} to the output stream.  Format:
     * <ol>
     * <li> {@link #writeFieldDef writeFieldDef(} {@link ArrayDef#getElement
     *      def.getElement())}
     * </ol>
     */
    public static void writeArray(ArrayDef def,
                                  DataOutput out,
                                  short serialVersion) throws IOException {

        writeFieldDef(def.getElement(), out, serialVersion);
    }

    /**
     * Writes an {@link EnumDef} to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and greater:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) <i>values length</i>
     * <li> ({@link SerializationUtil#writeString String}{@code []}) {@link
     *      EnumDef#getValues def.getValues()}
     * </ol>
     */
    public static void writeEnum(EnumDef def,
                                 DataOutput out,
                                 short serialVersion)
        throws IOException {

        String[] values = def.getValues();
        SerializationUtil.writeNonNullSequenceLength(out, values.length);
        for (String value : values) {
            SerializationUtil.writeString(out, serialVersion, value);
        }
    }

    /**
     * Writes a {@link TimestampDef} to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writePackedInt packed int}) {@link
     *      TimestampDef#getPrecision def.getPrecision()}
     * </ol>
     */
    public static void writeTimestamp(
        TimestampDef def,
        DataOutput out,
        @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        SerializationUtil.writePackedInt(out, def.getPrecision());
    }

    /*******************************************************************
     *
     * Deserialization methods
     *
     *******************************************************************/

    public static FieldDefImpl readFieldDef(DataInput in,
                                            short serialVersion)
            throws IOException {

        FieldDef.Type type = FieldDef.Type.readFastExternal(in, serialVersion);
        switch (type) {
        case INTEGER:
            return FieldDefImpl.integerDef;
        case LONG:
            return FieldDefImpl.longDef;
        case DOUBLE:
            return FieldDefImpl.doubleDef;
        case FLOAT:
            return FieldDefImpl.floatDef;
        case STRING:
            return FieldDefImpl.stringDef;
        case BINARY:
            return FieldDefImpl.binaryDef;
        case BOOLEAN:
            return FieldDefImpl.booleanDef;
        case NUMBER:
            return FieldDefImpl.numberDef;
        case FIXED_BINARY:
            int size = SerializationUtil.readNonNullSequenceLength(in);
            return new FixedBinaryDefImpl(size, null);
        case ENUM:
            return readEnum(in, serialVersion);
        case TIMESTAMP:
            return readTimestamp(in, serialVersion);
        case RECORD:
            return readRecord(in, serialVersion);
        case MAP:
            return readMap(in, serialVersion);
        case ARRAY:
            return readArray(in, serialVersion);
        case ANY:
            return FieldDefImpl.anyDef;
        case ANY_ATOMIC:
            return FieldDefImpl.anyAtomicDef;
        case ANY_JSON_ATOMIC:
            return FieldDefImpl.anyJsonAtomicDef;
        case ANY_RECORD:
            return FieldDefImpl.anyRecordDef;
        case EMPTY:
            return FieldDefImpl.emptyDef;
        case JSON:
            return FieldDefImpl.jsonDef;
        default:
            throw new IllegalStateException("Unknown type code: " + type);
        }
    }

    static RecordDefImpl readRecord(DataInput in, short serialVersion)
            throws IOException {

        String name = SerializationUtil.readString(in, serialVersion);
        int size = SerializationUtil.readNonNullSequenceLength(in);

        FieldMap fieldMap = new FieldMap();

        for (int i = 0; i < size; i++) {
            String fname = SerializationUtil.readString(in, serialVersion);
            FieldDefImpl fdef = readFieldDef(in, serialVersion);
            boolean nullable = in.readBoolean();
            FieldValueImpl defVal = null;
            if (!nullable) {
                defVal = (FieldValueImpl)
                    FieldValueSerialization.readFieldValue(
                        (fdef.isWildcard() ? null : fdef),
                        in, serialVersion);
            }
            fieldMap.put(fname, fdef, nullable, defVal);
        }

        if (name == null) {
            return new RecordDefImpl(fieldMap, null/*description*/);
        }

        return new RecordDefImpl(name, fieldMap);
    }

    /*
     * A map just has its element.
     */
    static MapDefImpl readMap(DataInput in, short serialVersion)
            throws IOException {

        return FieldDefFactory.createMapDef(readFieldDef(in, serialVersion));
    }

    /*
     * An array just has its element.
     */
    static ArrayDefImpl readArray(DataInput in, short serialVersion)
            throws IOException {

        return FieldDefFactory.createArrayDef(readFieldDef(in, serialVersion));
    }

    static EnumDefImpl readEnum(DataInput in, short serialVersion)
            throws IOException {

        int numValues = SerializationUtil.readNonNullSequenceLength(in);
        String[] values = new String[numValues];
        for (int i = 0; i < numValues; i++) {
            values[i] = SerializationUtil.readString(in, serialVersion);
        }
        return new EnumDefImpl(values, null);
    }

    @SuppressWarnings("unused")
    static TimestampDefImpl readTimestamp(DataInput in, short serialVersion)
        throws IOException {

        return new TimestampDefImpl(SerializationUtil.readPackedInt(in));
    }
}

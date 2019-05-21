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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_2;
import static oracle.kv.impl.util.SerialVersion.STD_UTF8_VERSION;
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import oracle.kv.impl.api.table.ValueSerializer.ArrayValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.MapValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.RecordValueSerializer;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TimestampDef;
import oracle.kv.table.TimestampValue;

/**
 * Methods to serialize and deserialize FieldValueImpl instances.
 *
 * @see #writeFieldValue FastExternalizable format
 */
public class FieldValueSerialization {

    /**
     * Represents a {@code null} value.
     */
    public static final int NULL_VALUE = -1;

    /**
     * Represents a {@code null} reference.
     */
    public static final int NULL_REFERENCE = -2;

    /**
     * Represents a {@code null} JSON value.
     */
    public static final int NULL_JSON_VALUE = -3;

    /**
     * Represents an {@code empty} JSON value.
     */
    public static final int EMPTY_VALUE = -4;

    /*******************************************************************
     *
     * Serialization methods
     *
     *******************************************************************/

    /**
     * Writes a possibly {@code null} field value.  The format is selected from
     * the following choices:
     * <ol>
     * <li> ({@code byte}) {@value #NULL_REFERENCE} // if {@code val} is {@code
     *     null}
     * <li> ({@code byte}) {@value #NULL_VALUE} // if {@link FieldValue#isNull
     *      val.isNull()} is {@code true}
     * <li> ({@code byte}) {@value #NULL_JSON_VALUE} // if {@link
     *      FieldValue#isJsonNull val.isJsonNull()} is {@code true}
     * <li> ({@code byte}) {@value #EMPTY_VALUE} // if {@link
     *      FieldValueImpl#isEMPTY val.isEMPTY()} is {@code true}
     * <li> Otherwise:
     *   <ol type="a">
     *   <li> ({@link Type}) {@link FieldValue#getType val.getType()}
     *   <li> {@link #writeNonNullFieldValue writeNonNullFieldValue(val,
     *        writeValDef, false)}
     *   </ol>
     * </ol>
     *
     * <p>If writeValDef is true, the deserializer does not have the FieldDef
     * for this value, or the FieldDef it knows about is a wildcard (in both of
     * these cases, the readFieldValue() method will be called with the def
     * param being null). In these cases, the serializer must serialize the
     * type as well as the value and the deserializer will read this type first
     * in order to parse the value bytes correctly.
     *
     * <p>This variant of writeFieldValue should be called when it is possible
     * that the given FieldValue is java null or one of the 3 special values:
     * SQL NULL, json null, or EMPTY. In this case, the method writes an extra
     * byte at the start of the serialized value, to indicate if the value is
     * indeed null or NullValue. If the value turns out to a "normal" one,
     * the extra byte will store the kind of the value (the enum returned by
     * val.getType()).
     *
     * <p>If neither null nor a special value are possible, it's better to call
     * the second variant below, passing true for the "writeValKind" param, to
     * indicate that the value kind has not been written already. In this case,
     * the value kind will be written only if needed, ie., only if the
     * writeValDef param is also true.
     *
     * @param val the field value
     * @param writeValDef whether to write the field definition
     * @param out the output stream
     * @param serialVersion the version of serialization format
     * @throws IOException if an I/O error occurs when writing to the stream
     */
    public static void writeFieldValue(
        FieldValue val,
        boolean writeValDef,
        DataOutput out,
        short serialVersion) throws IOException {

        writeFieldValueInternal((FieldValueSerializer)val, writeValDef, out,
            serialVersion);
    }

    static void writeFieldValueInternal(
        FieldValueSerializer val,
        boolean writeValDef,
        DataOutput out,
        short serialVersion) throws IOException {

        if (val == null) {
            out.writeByte(NULL_REFERENCE);
        } else if (val.isNull()) {
            out.writeByte(NULL_VALUE);
        } else if (val.isJsonNull()) {
            out.writeByte(NULL_JSON_VALUE);
        } else if (val.isEMPTY()) {
            out.writeByte(EMPTY_VALUE);
        } else {
            val.getType().writeFastExternal(out, serialVersion);
            writeNonNullFieldValueInternal(val, writeValDef, false, out,
                serialVersion);
        }
    }

    /**
     * Writes a non-null {@link FieldValue} to the output stream.  Format for
     * {@code serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and
     * greater:
     * <ol>
     * <li> <i>[Optional]</i> ({@code byte}) {@value #NULL_JSON_VALUE} // If
     *      {@code writeValDef}, {@code writeValKind}, and {@link
     *      FieldValue#isJsonNull val.isJsonNull()} are all true.  In this
     *      case, no other data is written.
     * <li> <i>[Optional]</i> ({@link Type}) {@link FieldValue#getType
     *      val.getType()} // If {@code writeValDef} and {@code writeValKind}
     *      are both true
     * <li> The additional data written depends on the value of {@link
     *      FieldValue#getType val.getType()}:
     *   <ol type="a">
     *   <li> {@link Type#INTEGER INTEGER}: ({@link
     *        SerializationUtil#writePackedInt packed int}) {@link
     *        FieldValueImpl#getInt val.getInt()}
     *   <li> {@link Type#LONG LONG}: ({@link SerializationUtil#writePackedLong
     *        packed long}) {@link FieldValueImpl#getLong val.getLong()}
     *   <li> {@link Type#DOUBLE DOUBLE}: ({@link DataOutput#writeDouble
     *        double}) {@link FieldValueImpl#getDouble val.getDouble()}
     *   <li> {@link Type#FLOAT FLOAT}: ({@link DataOutput#writeFloat float})
     *        {@link FieldValueImpl#getFloat val.getFloat()}
     *   <li> {@link Type#STRING STRING}: ({@link
     *        SerializationUtil#writeNonNullString non-null String}) {@link
     *        FieldValueImpl#getString val.getString()}
     *   <li> {@link Type#BOOLEAN BOOLEAN}: ({@link DataOutput#writeBoolean
     *        boolean}) {@link FieldValueImpl#getBoolean val.getBoolean()}
     *   <li> {@link Type#NUMBER NUMBER}: ({@link
     *        SerializationUtil#writeNonNullByteArray non-null byte array})
     *        {@link FieldValueImpl#getBytes val.getBytes()}
     *   <li> {@link Type#BINARY BINARY}: ({@link
     *        SerializationUtil#writeNonNullByteArray non-null byte array})
     *        {@link FieldValueImpl#getBytes val.getBytes()}
     *   <li> {@link Type#FIXED_BINARY FIXED_BINARY}: ({@link
     *        SerializationUtil#writeNonNullByteArray non-null byte array})
     *        {@link FieldValueImpl#getBytes val.getBytes()}
     *   <li> {@link Type#ENUM ENUM}: {@link #writeEnum writeEnum(val,
     *        writeValDef)}
     *   <li> {@link Type#TIMESTAMP TIMESTAMP}: {@link #writeTimestamp
     *        writeTimestamp(val, writeValDef)}
     *   <li> {@link Type#RECORD RECORD}: {@link #writeRecord writeRecord(val,
     *        writeValDef)}
     *   <li> {@link Type#MAP MAP}: {@link #writeMap writeMap(val,
     *        writeValDef)}
     *   <li> {@link Type#ARRAY ARRAY}: {@link #writeArray writeArray(val,
     *        writeValDef)}
     *   </ol>
     * </ol>
     *
     * @param val the field value
     * @param writeValDef whether to write the field definition
     * @param writeValKind whether to write the field type
     * @param out the output stream
     * @param serialVersion the version of serialization format
     * @throws IllegalStateException if val is null or represents a null value
     * @throws IOException if an I/O error occurs when writing to the stream
     */
    public static void writeNonNullFieldValue(
        FieldValue val,
        boolean writeValDef,
        boolean writeValKind,
        DataOutput out,
        short serialVersion) throws IOException {

        writeNonNullFieldValueInternal((FieldValueSerializer)val, writeValDef,
            writeValKind, out, serialVersion);
    }

    private static void writeNonNullFieldValueInternal(
        FieldValueSerializer value,
        boolean writeValDef,
        boolean writeValKind,
        DataOutput out,
        short serialVersion) throws IOException {

        if (value == null || value.isNull() || value.isEMPTY()) {

            throw new IllegalStateException("Unexpected value: " + value);
        }

        FieldDefImpl valDef = (FieldDefImpl)value.getDefinition();

        /*
         * The following checks are valid under the following assumption:
         * RecordValues which are constructed by a record-constructor expr (not
         * yet implemented) will not have ANY_RECORD as their associated type.
         * Notice that a record-constructor expr will probably look like this:
         * "{" name_expr ":" value_expr ("," name_expr ":" value_expr)* "}"
         * If so, this assumption means that a RECORD type must be built on the
         * fly for each RecordValue constructed.
         */
        if (valDef.isWildcard() && !value.isJsonNull()) {
            throw new IllegalStateException(
                "An item cannot have a wildcard type\n" + value);
        }

        if (valDef.getType() != value.getType()) {
            throw new IllegalStateException(
                "Mismatch between value kind and associated type\n" +
                "Value kind : " + value.getType() + "\n" +
                "Type : " + valDef);
        }

        /*
         * Notice that we do NOT write the value kind if the receiver has type
         * info (i.e., if writeValDef == false). This has implications for the
         * query processor, and specifically for value-constructing exprs. For
         * example, if the static type of an array-constructor expr is
         * ARRAY(LONG), the constructed array must contain longs only, i.e.
         * it cannot contain integers. This means that if the static element
         * type of the array constructor is not a wildcard type, we must cast
         * every item produced by the input exprs of the array constructor to
         * that static element type. Furthermore, if the static type of the
         * top expr on the server side is, say, LONG, then we must cast each
         * item produced by that expr to LONG, before we serialized it and
         * ship it to the client. The check below enforces this restriction.
         */
        if (writeValDef && writeValKind) {
            if (value.isJsonNull()) {
                out.writeByte(NULL_JSON_VALUE);
                return;
            }
            value.getType().writeFastExternal(out, serialVersion);
        }
        switch (value.getType()) {
        case INTEGER:
            SerializationUtil.writePackedInt(out, value.getInt());
            break;
        case LONG:
            SerializationUtil.writePackedLong(out, value.getLong());
            break;
        case DOUBLE:
            out.writeDouble(value.getDouble());
            break;
        case FLOAT:
            out.writeFloat(value.getFloat());
            break;
        case STRING:
            if (serialVersion >= STD_UTF8_VERSION) {
                SerializationUtil.writeNonNullString(
                    out, serialVersion, value.getString());
            } else {
                out.writeUTF(value.getString());
            }
            break;
        case BOOLEAN:
            out.writeBoolean(value.getBoolean());
            break;
        case NUMBER:
            writeNonNullByteArray(out, value.getNumberBytes());
            break;
        case BINARY:
            writeNonNullByteArray(out, value.getBytes());
            break;
        case FIXED_BINARY:
            /*
             * Write the (fixed) size of the binary. Fixed binary can only
             * be null or full-sized, so the size of its byte array is the
             * same as the defined size.
             */
            final byte[] bytes = value.getFixedBytes();
            final int size = value.getDefinition().asFixedBinary().getSize();
            if (size != bytes.length) {
                throw new IllegalStateException(
                    "Definition size " + size +
                    " is different from bytes length " + bytes.length);
            }
            writeNonNullByteArray(out, bytes);
            break;
        case ENUM:
            writeEnumInternal(value, writeValDef, out, serialVersion);
            break;
        case TIMESTAMP:
            writeTimestampInternal(value, writeValDef, out, serialVersion);
            break;
        case RECORD:
            writeRecordInternal(value.asRecordValueSerializer(), writeValDef,
                out, serialVersion);
            break;
        case MAP:
            writeMapInternal(value.asMapValueSerializer(), writeValDef, out,
                serialVersion);
            break;
        case ARRAY:
            writeArrayInternal(value.asArrayValueSerializer(), writeValDef, out,
                serialVersion);
            break;
        case ANY:
        case ANY_ATOMIC:
        case ANY_JSON_ATOMIC:
        case ANY_RECORD:
            throw new IllegalStateException
                ("ANY* types cannot be materialized as values");
        case JSON:
            throw new IllegalStateException
                ("JSON cannot be materialized as a value");
        case EMPTY:
            throw new IllegalStateException(
                "EMPTY type does not contain any values");
        }
    }

    /**
     * Writes an {@link EnumValue} to the output stream.  Format:
     * <ol>
     * <li> <i>[Optional]</i> {@link FieldDefSerialization#writeEnum
     *      writeEnum(} {@link EnumValue#getDefinition value.getDefinition())}
     *      // If {@code writeValDef} is {@code true}
     * <li> ({@link DataOutput#writeShort short}) {@link EnumValue#getIndex
     *      value.getIndex()}
     * </ol>
     *
     * @param value the enum value
     * @param writeValDef whether to write the field definition
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     */
    public static void writeEnum(EnumValueImpl value,
                                 boolean writeValDef,
                                 DataOutput out,
                                 short serialVersion)
        throws IOException {

        writeEnumInternal(value, writeValDef, out, serialVersion);
    }

    private static void writeEnumInternal(FieldValueSerializer value,
                                          boolean writeValDef,
                                          DataOutput out,
                                          short serialVersion)
        throws IOException {

        EnumDefImpl def = (EnumDefImpl)value.getDefinition();
        if (writeValDef) {
            FieldDefSerialization.writeEnum(def, out, serialVersion);
        }
        out.writeShort(def.indexOf(value.getEnumString()));
    }

    /**
     * Writes a {@link TimestampValue} to the output stream.  Format:
     * <ol>
     * <li> <i>[Optional]</i> {@link FieldDefSerialization#writeTimestamp
     *      writeTimestamp(} {@link FieldValue#getDefinition
     *      value.getDefinition())} // If {@code writeValDef} is {@code true}
     * <li> ({@code byte}) <i>timestamp length</i>
     * <li> ({@code byte[]}) {@link TimestampValueImpl#getBytes
     *      value.getBytes()}
     * </ol>
     */
    public static void writeTimestamp(TimestampValueImpl value,
                                      boolean writeValDef,
                                      DataOutput out,
                                      short serialVersion)
        throws IOException {

        writeTimestampInternal(value, writeValDef, out, serialVersion);
    }

    private static void writeTimestampInternal(FieldValueSerializer value,
                                               boolean writeValDef,
                                               DataOutput out,
                                               short serialVersion)
        throws IOException {

        if (writeValDef) {
            final TimestampDef def = value.getDefinition().asTimestamp();
            FieldDefSerialization.writeTimestamp(def, out, serialVersion);
        }
        final byte[] bytes = value.getTimestampBytes();
        if (bytes.length == 0) {
            throw new IllegalStateException("Bytes must not be empty");
        }
        if (bytes.length > Byte.MAX_VALUE) {
            throw new IllegalStateException("Too many bytes in timestamp: " +
                                            bytes.length);
        }
        out.writeByte(bytes.length);
        out.write(bytes);
    }

    /**
     * Writes a {@link RecordValueImpl} to the output stream.  Format:
     * <ol>
     * <li> <i>[Optional]</i> {@link FieldDefSerialization#writeRecord
     *      writeRecord(} {@link RecordValue#getDefinition
     *      record.getDefinition())} // If {@code writeValDef} is {@code true}
     * <li> For each field in the record, select one of:
     *   <ol type="a">
     *   <li> {@link #writeNonNullFieldValue writeNonNullFieldValue(field,
     *        wildcard, true)} // If the field cannot be null
     *   <li> {@link #writeFieldValue writeFieldValue(field, wildcard)} // If
     *        the field may be null
     *   </ol>
     * </ol>
     *
     * <p>There is an optimization to avoid writing the type byte for fields
     * that are not nullable, which means that there is no need to
     * differentiate between a null value and non-null value.
     *
     * <p>NOTE: it is unclear whether this optimization will be helpful or more
     * confusing to non-Java drivers when they must handle this format. If the
     * intent is to have these drivers treat data as *mostly* schemaless, as
     * they do with the JSON-based proxy, requiring them to understand nullable
     * vs not nullable fields may be excessive. Watch this space.
     */
    public static void writeRecord(
        RecordValueImpl record,
        boolean writeValDef,
        DataOutput out,
        short serialVersion) throws IOException {

        writeRecordInternal(record, writeValDef, out, serialVersion);
    }

    private static void writeRecordInternal(
        RecordValueSerializer record,
        boolean writeValDef,
        DataOutput out,
        short serialVersion) throws IOException {

        RecordDefImpl recordDef = (RecordDefImpl)record.getDefinition();

        if (writeValDef) {
            FieldDefSerialization.writeRecord(recordDef, out, serialVersion);
        }

        for (int pos = 0; pos < recordDef.getNumFields(); ++pos) {

            FieldDefImpl fdef = recordDef.getFieldDef(pos);
            FieldValueSerializer fval = record.get(pos);

            /*
             * If the field is not nullable, call the 3rd version of
             * writeFieldValue, passing true for "writevalKind."
             * This will avoid writing the type byte if possible.
             */
            if (!recordDef.isNullable(pos)) {
                writeNonNullFieldValueInternal(fval,
                                               fdef.isWildcard(), // writeValDef
                                               true,              // writeValKind
                                               out, serialVersion);
            } else {
                writeFieldValueInternal(fval,
                                        fdef.isWildcard(), // writeValDef
                                        out, serialVersion);
            }
        }
    }

    /**
     * Writes a {@link MapValue} to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and greater:
     * <ol>
     * <li> <i>[Optional]</i> {@link FieldDefSerialization#writeFieldDef
     *      writeFieldDef(} {@link MapValue#getDefinition map.getDefinition())}
     *      // If {@code writeValDef} is {@code true}
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) {@link MapValue#size map.size()}
     * <li> For each entry in the map:
     *   <ol type="a">
     *   <li> ({@link SerializationUtil#writeNonNullString non-null String})
     *        <i>entry key</i>
     *   <li> {@link #writeNonNullFieldValue writeNonNullFieldValue(value,
     *        wildcard, true)}
     *   </ol>
     * </ol>
     */
    public static void writeMap(
        MapValueImpl map,
        boolean writeValDef,
        DataOutput out,
        short serialVersion) throws IOException {

        writeMap(map, writeValDef, out, serialVersion);
    }

    private static void writeMapInternal(
        MapValueSerializer map,
        boolean writeValDef,
        DataOutput out,
        short serialVersion) throws IOException {

        MapDef mapDef = map.getDefinition();
        FieldDefImpl elemDef = (FieldDefImpl)mapDef.getElement();
        boolean wildcard = elemDef.isWildcard();

        if (writeValDef) {
            FieldDefSerialization.writeFieldDef(elemDef, out, serialVersion);
        }

        int size = map.size();
        SerializationUtil.writeNonNullSequenceLength(out, size);
        if (size == 0) {
            return;
        }

        Iterator<Entry<String, FieldValueSerializer>> iter = map.iterator();
        while(iter.hasNext()) {
            Entry<String, FieldValueSerializer> entry = iter.next();
            if (serialVersion >= STD_UTF8_VERSION) {
                SerializationUtil.writeNonNullString(
                    out, serialVersion, entry.getKey());
            } else {
                out.writeUTF(entry.getKey());
            }

            writeNonNullFieldValueInternal(entry.getValue(),
                                           wildcard, // writeValDef
                                           true, // writeValKind
                                           out, serialVersion);
        }
    }

    /**
     * Writes an {@link ArrayValue} to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and greater:
     * <ol>
     * <li> <i>[Optional]</i> {@link FieldDefSerialization#writeFieldDef
     *      writeFieldDef(} {@link ArrayValueImpl#getElementDef
     *      array.getElementDef())} // if {@code writeValDef} is {@code true}
     * <li> If the type is a {@link FieldDefImpl#isWildcard wildcard}, then
     *      write the following items:
     *   <ol type="a">
     *   <li> ({@link DataOutput#writeBoolean boolean}) {@linkplain
     *        ArrayValueImpl#getHomogeneousType <i>whether homogeneous type is
     *        present</i>}
     *   <li> <i>[Optional]</i> {@link FieldDefSerialization#writeFieldDef
     *        writeFieldDef(homogeneous type)} // If the homogeneous type is
     *        present
     *   </ol>
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) {@link ArrayValue#size array.size()}
     * <li> For each array element:
     *   <ol type="a">
     *   <li> {@link #writeNonNullFieldValue writeNonNullFieldValue(element,
     *        wildcard, true)} // Where {@code wildcard} is {@code true} if the
     *        type is a wildcard and the type is not homogeneous
     *   </ol>
     * </ol>
     */
    public static void writeArray(
        ArrayValueImpl array,
        boolean writeValDef,
        DataOutput out,
        short serialVersion) throws IOException {

        writeArrayInternal(array, writeValDef, out, serialVersion);
    }

    private static void writeArrayInternal(
        ArrayValueSerializer array,
        boolean writeValDef,
        DataOutput out,
        short serialVersion) throws IOException {

        ArrayDefImpl arrayDef = (ArrayDefImpl)array.getDefinition();
        FieldDefImpl elemDef = arrayDef.getElement();
        FieldDefImpl homogeneousType =
            (array instanceof ArrayValueImpl) ?
                ((ArrayValueImpl) array).getHomogeneousType() : null;
        boolean wildcard = elemDef.isWildcard();
        boolean homogeneous = (homogeneousType != null);

        if (writeValDef) {
            FieldDefSerialization.writeFieldDef(elemDef, out, serialVersion);
        }

        if (serialVersion >= QUERY_VERSION_2 && wildcard) {

            out.writeBoolean(homogeneous);

            if (homogeneous) {
                FieldDefSerialization.writeFieldDef(homogeneousType,
                                                    out, serialVersion);
                wildcard = false;
            }
        }

        int size = array.size();
        SerializationUtil.writeNonNullSequenceLength(out, size);

        Iterator<FieldValueSerializer> iter = array.iterator();
        while(iter.hasNext()) {
            FieldValueSerializer fieldVal = iter.next();
            writeNonNullFieldValueInternal(fieldVal,
                                           wildcard, // writeValDef
                                           true, // writeValKind
                                           out,
                                           serialVersion);
        }
    }

    /*******************************************************************
     *
     * Deserialization methods
     *
     *******************************************************************/

    public static FieldValue readFieldValue(
        FieldDef def,
        DataInput in,
        short serialVersion) throws IOException {

        ValueReader<FieldValueImpl> reader =
            new FieldValueReaderImpl<FieldValueImpl>();
        readFieldValue(reader, null, def, in, serialVersion);
        return reader.getValue();
    }

    public static FieldValue readNonNullFieldValue(
        FieldDef def,
        FieldDef.Type valKind,
        DataInput in,
        short serialVersion) throws IOException {

        ValueReader<FieldValueImpl> reader =
            new FieldValueReaderImpl<FieldValueImpl>();
        readNonNullFieldValue(reader, null, def, valKind, in, serialVersion);
        return reader.getValue();
    }

    static void readFieldValue(
        ValueReader<?> reader,
        String fieldName,
        FieldDef def,
        DataInput in,
        short serialVersion) throws IOException {

        int ordinal = in.readByte();

        if (ordinal == NULL_REFERENCE) {
            return;
        }

        if (ordinal == NULL_VALUE) {
            reader.readNull(fieldName);
            return;
        }

        if (ordinal == NULL_JSON_VALUE) {
            reader.readJsonNull(fieldName);
            return;
        }

        if (ordinal == EMPTY_VALUE) {
            reader.readEmpty(fieldName);
            return;
        }

        FieldDef.Type valKind = FieldDef.Type.valueOf(ordinal);

        readNonNullFieldValue(reader, fieldName, def, valKind, in,
            serialVersion);
    }

    static void readNonNullFieldValue(
        ValueReader<?> reader,
        String fieldName,
        FieldDef def,
        FieldDef.Type valKind,
        DataInput in,
        short serialVersion) throws IOException {

        if (def == null) {
            if (valKind == null) {
                int ordinal = in.readByte();

                if (ordinal == NULL_JSON_VALUE) {
                    reader.readJsonNull(fieldName);
                    return;
                }

                valKind = FieldDef.Type.valueOf(ordinal);
            }
        } else if (valKind == null) {
            valKind = def.getType();
        }

        switch (valKind) {

        case INTEGER: {
            int val = SerializationUtil.readPackedInt(in);
            reader.readInteger(fieldName, val);
            break;
        }
        case LONG: {
            long val = SerializationUtil.readPackedLong(in);
            reader.readLong(fieldName, val);
            break;
        }
        case DOUBLE: {
            double val = in.readDouble();
            reader.readDouble(fieldName, val);
            break;
        }
        case FLOAT: {
            float val = in.readFloat();
            reader.readFloat(fieldName, val);
            break;
        }
        case STRING: {
            String val = (serialVersion >= STD_UTF8_VERSION) ?
                SerializationUtil.readNonNullString(in, serialVersion) :
                in.readUTF();
            reader.readString(fieldName, val);
            break;
        }
        case BOOLEAN: {
            reader.readBoolean(fieldName, in.readBoolean());
            break;
        }
        case NUMBER: {
            final byte[] bytes = readNonNullByteArray(in);
            if (bytes.length == 0) {
                throw new IllegalStateException(
                    "Invalid zero length for number");
            }
            reader.readNumber(fieldName, bytes);
            break;
        }
        case BINARY: {
            final byte[] bytes = readNonNullByteArray(in);
            reader.readBinary(fieldName, bytes);
            break;
        }
        case FIXED_BINARY: {
            final byte[] bytes = readNonNullByteArray(in);
            reader.readFixedBinary(fieldName,
                new FixedBinaryDefImpl(bytes.length, null), bytes);
            break;
        }
        case ENUM: {
            EnumDefImpl enumDef =
                (def == null ?
                 FieldDefSerialization.readEnum(in, serialVersion) :
                 (EnumDefImpl) def);

            assert(enumDef != null);
            short index = in.readShort();
            reader.readEnum(fieldName, enumDef, index);
            break;
        }
        case TIMESTAMP: {
            TimestampDefImpl timestampDef =
                (def == null ?
                 FieldDefSerialization.readTimestamp(in, serialVersion) :
                 (TimestampDefImpl) def);

            assert(timestampDef != null);
            final int len = in.readByte();
            if (len <= 0) {
                throw new IOException("Invalid timestamp def length: " + len);
            }
            final byte[] bytes = new byte[len];
            in.readFully(bytes);
            reader.readTimestamp(fieldName, timestampDef, bytes);
            break;
        }
        case RECORD:
            readRecord(reader, fieldName, def, in, serialVersion);
            break;
        case MAP:
            readMap(reader, fieldName, def, in, serialVersion);
            break;
        case ARRAY:
            readArray(reader, fieldName, def, in, serialVersion);
            break;
        default:
            throw new IllegalStateException("Type not supported: " + valKind);
        }
    }

    static void readRecord(
        ValueReader<?> reader,
        String fieldName,
        FieldDef def,
        DataInput in,
        short serialVersion) throws IOException {

        RecordDefImpl recordDef =
            (def == null ?
             FieldDefSerialization.readRecord(in, serialVersion) :
             (RecordDefImpl)def);

        reader.startRecord(fieldName, recordDef);
        for (int pos = 0; pos < recordDef.getNumFields(); ++pos) {

            FieldDefImpl fdef = recordDef.getFieldDef(pos);
            if (fdef.isWildcard()) {
                fdef = null;
            }

            String name = recordDef.getFieldName(pos);
            /*
             * If the field is not a wildcard, and it's not nullable its type will
             * not have been written. Use a different variant of readFieldValue().
             */
            if (fdef != null && !recordDef.isNullable(pos)) {
                readNonNullFieldValue(reader, name, fdef, fdef.getType(),
                    in, serialVersion);
            } else {
                readFieldValue(reader, name, fdef, in, serialVersion);
            }
        }
        reader.endRecord();
    }

    /**
     * See writeMap for expected format
     */
    static void readMap(
        ValueReader<?> reader,
        String fieldName,
        FieldDef def,
        DataInput in,
        short serialVersion) throws IOException {

        FieldDefImpl elemDef = null;
        MapDef mapDef = null;

        if (def != null) {
            mapDef =  def.asMap();
            elemDef = (FieldDefImpl)mapDef.getElement();
        } else {
            elemDef = FieldDefSerialization.readFieldDef(in, serialVersion);
            mapDef = FieldDefFactory.createMapDef(elemDef);
        }

        reader.startMap(fieldName, mapDef);

        boolean wildcard = elemDef.isWildcard();

        if (wildcard) {
            elemDef = null;
        }

        int size = SerializationUtil.readNonNullSequenceLength(in);

        for (int i = 0; i < size; i++) {

            String fname = (serialVersion >= STD_UTF8_VERSION) ?
                SerializationUtil.readNonNullString(in, serialVersion) :
                in.readUTF();

            readNonNullFieldValue(reader, fname, elemDef, null, in,
                serialVersion);
        }
        reader.endMap();
    }

    /**
     * See writeArray for expected format
     */
    static void readArray(
        ValueReader<?> reader,
        String fieldName,
        FieldDef def,
        DataInput in,
        short serialVersion) throws IOException {

        ArrayDefImpl arrayDef = null;
        FieldDefImpl elemDef = null;
        boolean wildcard;

        if (def != null) {
            arrayDef = (ArrayDefImpl)def;
            elemDef = arrayDef.getElement();
            wildcard = elemDef.isWildcard();

        } else {
            elemDef = FieldDefSerialization.readFieldDef(in, serialVersion);
            arrayDef = FieldDefFactory.createArrayDef(elemDef);
            wildcard = elemDef.isWildcard();
        }

        /*
         * If this is a wildcard array, the sender includes info about whether
         * the array is actually a homogeneous one, and if so, what is the
         * homogeneous type.
         */
        boolean homogeneous = false;
        if (serialVersion >= QUERY_VERSION_2 && wildcard) {
            homogeneous = in.readBoolean();

            if (homogeneous) {
                elemDef = FieldDefSerialization.readFieldDef(in, serialVersion);
                wildcard = false;
            }
        }

        reader.startArray(fieldName, arrayDef, (homogeneous ? elemDef : null));

        if (wildcard) {
            /*
             * elemDef is passed as input to the readFieldValue() call below.
             * If it is a wildcard type, we set it to null, which means that we
             * don't have any type info for the elements, and we expect to find
             * such info in front of each element inside the serialized format.
             */
            elemDef = null;
        }

        int size = SerializationUtil.readNonNullSequenceLength(in);

        for (int i = 0; i < size; i++) {
            readNonNullFieldValue(reader, null, elemDef, null, in,
                serialVersion);
        }
        reader.endArray();
    }
}

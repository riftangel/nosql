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

package oracle.kv.impl.util;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.sleepycat.util.PackedInteger;

/**
 * Utility methods to facilitate serialization/deserialization
 */
public class SerializationUtil {

    /**
     * The name of the system property that controls the maximum sequence
     * length.  The value of the system property should be a valid positive
     * integer.  The methods in this file for reading sequences, whether
     * arrays, collections, or strings, will throw an IOException if the
     * serialized input attempts to create an object with more elements than
     * the maximum value.
     */
    public static final String MAX_SEQUENCE_LENGTH_PROPERTY =
        "oracle.kv.serial.max.sequence.length";

    /** The default maximum sequence length. */
    private static final int DEFAULT_MAX_SEQUENCE_LENGTH = Integer.MAX_VALUE;

    /** The maximum sequence length. */
    public static volatile int maxSequenceLength = Integer.getInteger(
        MAX_SEQUENCE_LENGTH_PROPERTY, DEFAULT_MAX_SEQUENCE_LENGTH);

    /* TODO: Use StandardCharsets version in Java 8 */
    private static final Charset utf8 = Charset.forName("UTF-8");

    public static final String EMPTY_STRING = new String();

    public static final byte[] EMPTY_BYTES = { };

    /** The TimeUnit values, for read-only use. */
    private static final TimeUnit[] TIMEUNIT_VALUES = TimeUnit.values();

    /**
     * Returns the object de-serialized from the specified byte array.
     *
     * @param <T> the type of the deserialized object
     *
     * @param bytes the serialized bytes
     * @param oclass the class associated with the deserialized object
     *
     * @return the deserialized object
     */
    @SuppressWarnings("unchecked")
    public static <T> T getObject(byte[] bytes, Class<T> oclass) {

        if (bytes == null) {
            return null;
        }

        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return (T)ois.readObject();
        } catch (IOException ioe) {
            throw new IllegalStateException
                ("Exception deserializing object: " + oclass.getName(), ioe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException
            ("Exception deserializing object: " + oclass.getName(), cnfe);
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException ioe) {
                    throw new IllegalStateException
                        ("Exception closing deserializing stream", ioe);
                }
            }
        }
    }

    /**
     * Returns a byte array containing the serialized form of this object.
     *
     * @param object the object to be serialized
     *
     * @return the serialized bytes
     */
    public static byte[] getBytes(Object object) {
        ObjectOutputStream oos = null;
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.close();
            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IllegalStateException
                ("Exception serializing object: " +
                 object.getClass().getName(),
                 ioe);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException ioe) {
                    throw new IllegalStateException
                        ("Exception closing serializing stream", ioe);
                }
            }
        }
    }

    /**
     * Reads a packed integer from the input and returns it.
     *
     * @param in the data input
     * @return the integer that was read
     */
    public static int readPackedInt(DataInput in) throws IOException {
        final byte[] bytes = new byte[PackedInteger.MAX_LENGTH];
        in.readFully(bytes, 0, 1);
        final int len = PackedInteger.getReadIntLength(bytes, 0);
        try {
            in.readFully(bytes, 1, len - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Invalid packed int", e);
        }
        return PackedInteger.readInt(bytes, 0);
    }

    /**
     * Writes a packed integer to the output.
     *
     * @param out the data output
     * @param value the integer to be written
     */
    public static void writePackedInt(DataOutput out, int value)
            throws IOException {
        final byte[] buf = new byte[PackedInteger.MAX_LENGTH];
        final int offset = PackedInteger.writeInt(buf, 0, value);
        out.write(buf, 0, offset);
    }

    /**
     * Reads a packed long from the input and returns it.
     *
     * @param in the data input
     * @return the long that was read
     */
    public static long readPackedLong(DataInput in) throws IOException {
        final byte[] bytes = new byte[PackedInteger.MAX_LONG_LENGTH];
        in.readFully(bytes, 0, 1);
        final int len = PackedInteger.getReadLongLength(bytes, 0);
        try {
            in.readFully(bytes, 1, len - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Invalid packed long", e);
        }
        return PackedInteger.readLong(bytes, 0);
    }

    /**
     * Writes a packed long to the output.
     *
     * @param out the data output
     * @param value the long to be written
     */
    public static void writePackedLong(DataOutput out, long value)
            throws IOException {
        final byte[] buf = new byte[PackedInteger.MAX_LONG_LENGTH];
        final int offset = PackedInteger.writeLong(buf, 0, value);
        out.write(buf, 0, offset);
    }

    /**
     * Reads a string written by {@link #writeString}, using standard UTF-8 or
     * Java's modified UTF-8 format, depending on the serial version.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @return a string or null
     * @throws IOException if an I/O error occurs, if the input UTF-8 encoding
     * is invalid, or if the String length is too large
     */
    public static String readString(DataInput in, short serialVersion)
        throws IOException {

        return (serialVersion >= SerialVersion.STD_UTF8_VERSION) ?
            readStdUTF8String(in) :
            readJavaString(in);
    }

    /**
     * Reads a non-null string written by {@link #writeNonNullString}, using
     * standard UTF-8 or Java's modified UTF-8 format, depending on the serial
     * version.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @return a string
     * @throws IOException if an I/O error occurs, if the input represents a
     * null value, if the input UTF-8 encoding is invalid, or if the String
     * length is too large
     */
    public static String readNonNullString(DataInput in, short serialVersion)
        throws IOException {

        final String result = readString(in, serialVersion);
        if (result == null) {
            throw new IOException("Found null value for non-null string");
        }
        return result;
    }

    /**
     * Writes a string for reading by {@link #readString}, using standard UTF-8
     * or Java's modified UTF-8 format, depending on the serial version.  The
     * string may be null or empty.  This code differentiates between the two,
     * maintaining the ability to round-trip null and empty string values.
     *
     * <p>This format is used rather than that of {@link DataOutput#writeUTF}
     * to allow packing of the size of the string. For shorter strings this
     * size savings is a significant percentage of the space used.
     *
     * <p>If {@code serialVersion} is 13 or earlier, the UTF-8 bytes are
     * written in the modified UTF-8 format used by {@link
     * DataOutput#writeUTF}.  For serial version 14 and later, to support
     * non-Java interoperability, the UTF-8 bytes are written using the
     * standard UTF-8 format documented by <a
     * href="http://www.ietf.org/rfc/rfc2279.txt">RFC 2279</a> and implemented
     * by the {@link Charset} class using the "UTF-8" standard charset.
     *
     * <p>Format:
     * <ol>
     * <li> ({@link #writePackedInt packed int}) <i>string length, or -1 for
     *      null</i>
     * <li> <i>[Optional]</i> ({@code byte[]}) <i>UTF-8 bytes</i>
     * </ol>
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param value the string or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeString(DataOutput out,
                                   short serialVersion,
                                   String value)
        throws IOException {

        if (serialVersion >= SerialVersion.STD_UTF8_VERSION) {
            writeStdUTF8String(out, value);
        } else {
            writeJavaString(out, value);
        }
    }

    /**
     * Writes a non-null string for reading by {@link #readNonNullString},
     * using the same format as {@link #writeString}, but not permitting a null
     * value to be written.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param value the string
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code value} is {@code null}
     */
    public static void writeNonNullString(DataOutput out,
                                          short serialVersion,
                                          String value)
        throws IOException {

        ObjectUtil.checkNull("value", value);
        writeString(out, serialVersion, value);
    }

    /**
     * Reads a string written by writeJavaString, using Java's modified UTF8
     * format. The returned string will be null if the original string was
     * null.
     *
     * @param in the data input
     * @return a string or null
     * @throws IOException if there is an I/O error, if the input UTF-8
     * encoding is invalid, or if the string length is too large
     */
    private static String readJavaString(DataInput in)
        throws IOException {

        final int len = readPackedInt(in);
        if (len < 0) {
            /* Negative means the original string was null */
            return null;
        }
        if (len == 0) {
            return EMPTY_STRING;
        }
        checkMaxSequenceLength(len);
        final byte[] bytes = new byte[len];
        in.readFully(bytes);
        try {
            return utfToString(bytes);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException("Invalid UTF-8 encoding", e);
        }
    }

    /** Throws an IOException if length is greater than maxSequenceLength. */
    private static void checkMaxSequenceLength(int length) throws IOException {
        if (length > maxSequenceLength) {
            throw new IOException("Sequence length " + length +
                                  " is greater than maximum sequence length " +
                                  maxSequenceLength);
        }
    }

    /**
     * Serializes a string using Java's modified UTF8 format, which can be read
     * by readJavaString. The string may be null or empty. This code
     * differentiates between the two, maintaining the ability to round-trip
     * null and empty string values.
     *
     * This format is used rather than that of DataOutput.writeUTF() to allow
     * packing of the size of the string. For shorter strings this size
     * savings is a significant percentage of the space used.
     *
     * @param out the data output
     * @param value the string to serialize or null
     * @throws IOException if an I/O error occurs
     */
    private static void writeJavaString(DataOutput out, String value)
        throws IOException {

        if (value == null) {
            writePackedInt(out, -1); /* -1 is encoded in a single byte */
            return;
        }

        final int utfLength = getUTFLength(value);

        writePackedInt(out, utfLength);

        if (utfLength > 0) {
            final byte[] bytes = stringToUTF(value, utfLength);
            out.write(bytes);
        }
    }

    /* -- The following methods are cribbed from JE's UtfOps and the JDK -- */

    /*
     * Gets the length of a UTF-8 byte array representation of the specified
     * string
     */
    private static int getUTFLength(String s) {
        int len = 0;
        final int length = s.length();
        for (int i = 0; i < length; i++) {
            int c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                len++;
            } else if (c > 0x07FF) {
                len += 3;
            } else {
                len += 2;
            }
        }
        return len;
    }

    /**
     * Converts the specified string into UTF-8 format
     */
    private static byte[] stringToUTF(String s, int utfLength) {

        final int length = s.length();
        assert length > 0;

        final byte[] bytes = new byte[utfLength];
        int byteOffset = 0;
        int i = 0;

        /* optimized loop for ascii */
        while (i < length) {
            int c = s.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            i++;
            bytes[byteOffset++] = (byte) c;
        }

        while (i < length) {
            int c = s.charAt(i++);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytes[byteOffset++] = (byte) c;
            } else if (c > 0x07FF) {
                bytes[byteOffset++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytes[byteOffset++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                bytes[byteOffset++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            } else {
                bytes[byteOffset++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                bytes[byteOffset++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
        }
        return bytes;
    }

    /**
     * Converts byte arrays encoded as UTF-8 into strings. It converts as
     * many bytes as are in the bytes array. If offsets are ever needed, this
     * can be added.
     */
    private static String utfToString(byte[] bytes)
        throws IOException {

        final int len = bytes.length;

        if (len == 0) {
            return EMPTY_STRING;
        }

        /*
         * this assumes that the number of chars in the final string is
         * <= the number of encoded bytes, which is true for UTF-8 encodings.
         * This assumption saves counting twice.
         */
        char[] chars = new char[len];
        int count = 0;
        int charArrayCount = 0;
        int char1, char2, char3;

        /*
         * Optimize for ascii
         */
        while (count < len) {
            char1 = bytes[count] & 0xff;
            if (char1 > 127) {
                break;
            }
            count++;
            chars[charArrayCount++] = (char) char1;
        }

        while (count < len) {
            char1 = bytes[count++] & 0xff;
            switch (char1 >> 4) {
            case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                /* 0xxxxxxx */
                chars[charArrayCount++] = (char) char1;
                break;
            case 12: case 13:
                /* 110x xxxx   10xx xxxx*/
                char2 = bytes[count++];
                if ((char2 & 0xC0) != 0x80) {
                    throw new IOException("Invalid UTF-8 encoding");
                }
                chars[charArrayCount++] = (char)(((char1 & 0x1F) << 6) |
                                                 (char2 & 0x3F));
                break;
            case 14:
                char2 = bytes[count++];
                char3 = bytes[count++];
                if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                    throw new IOException("Invalid UTF-8 encoding");
                }
                chars[charArrayCount++] = (char)(((char1 & 0x0F) << 12) |
                                                 ((char2 & 0x3F) << 6)  |
                                                 ((char3 & 0x3F) << 0));
                break;
            default:
                throw new IOException("Invalid UTF-8 encoding");
            }
        }
        return new String(chars, 0, charArrayCount);
    }

    /**
     * Writes a possibly null or empty string to an output stream using
     * standard UTF-8 format.
     *
     * <p>First writes a {@link #writePackedInt packedInt} representing the
     * length of the UTF-8 encoding of the string, or {@code -1} if the string
     * is null, followed by the UTF-8 encoding for non-empty strings.
     *
     * @param out the output stream
     * @param value the string or null
     * @throws IOException if an I/O error occurs
     */
    private static void writeStdUTF8String(DataOutput out, String value)
        throws IOException {

        if (value == null) {
            writePackedInt(out, -1);
            return;
        }
        final ByteBuffer buffer = utf8.encode(value);
        final int length = buffer.limit();
        writePackedInt(out, length);
        if (length > 0) {
            out.write(buffer.array(), 0, length);
        }
    }

    /**
     * Reads a possibly null string from an input stream in standard UTF-8
     * format.
     *
     * <p>First reads a {@link #readPackedInt packedInt} representing the
     * length of the UTF-8 encoding of the string, or a negative value for
     * null, followed by the string contents in UTF-8 format for a non-empty
     * string, if any.
     *
     * @param in the input stream
     * @return the string
     * @throws IOException if an I/O error occurs, if the input UTF-8 encoding
     * is invalid, or if the String length is too large
     */
    private static String readStdUTF8String(DataInput in)
        throws IOException {

        final int length = readPackedInt(in);
        if (length < 0) {
            return null;
        }
        if (length == 0) {
            return EMPTY_STRING;
        }
        checkMaxSequenceLength(length);
        final byte[] bytes = new byte[length];
        in.readFully(bytes);
        return utf8.decode(ByteBuffer.wrap(bytes)).toString();
    }

    /**
     * Reads the length of a possibly null sequence.  The length is represented
     * as a {@link #readPackedInt packed int}, with -1 interpreted as meaning
     * null, and other negative values not permitted.
     *
     * @param in the input stream
     * @return the sequence length or -1 for null
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if the sequence length is too large
     */
    public static int readSequenceLength(DataInput in)
        throws IOException {

        final int result = readPackedInt(in);
        if (result < -1) {
            throw new IOException("Invalid sequence length: " + result);
        }
        checkMaxSequenceLength(result);
        return result;
    }

    /**
     * Reads the length of a non-null sequence.  The length is represented as a
     * non-negative {@link #readPackedInt packed int}.
     *
     * @param in the input stream
     * @return the sequence length
     * @throws IOException if an I/O error occurs, if the input represents a
     * null sequence, if the input format is invalid, or if the sequence length
     * is too large
     */
    public static int readNonNullSequenceLength(DataInput in)
        throws IOException {

        final int length = readSequenceLength(in);
        if (length == -1) {
            throw new IOException("Read null length for non-null sequence");
        }
        return length;
    }

    /**
     * Writes the size of a possibly null collection.  The length is
     * represented as a {@link #readPackedInt packed int}, with -1 representing
     * null.  Although we don't enforce maximum sequence lengths yet, this
     * entrypoint provides a place to do that.
     *
     * @param out the output stream
     * @param collection the collection or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeCollectionLength(DataOutput out,
                                             Collection<?> collection)
        throws IOException {

        writeSequenceLength(
            out, (collection != null) ? collection.size() : -1);
    }

    /**
     * Writes the length of a possibly null array.  The length is represented
     * as a {@link #readPackedInt packed int}, with -1 representing null.
     * Although we don't enforce maximum sequence lengths yet, this entrypoint
     * provides a place to do that.
     *
     * @param out the output stream
     * @param array the array or null
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code array} is not an array or
     * {@code null}
     */
    public static void writeArrayLength(DataOutput out, Object array)
        throws IOException {

        writeSequenceLength(
            out, (array != null) ? Array.getLength(array) : -1);
    }

    /**
     * Writes a sequence length.  The length is represented as a {@link
     * #readPackedInt packed int}, with -1 representing null.  Although we
     * don't enforce maximum sequence lengths yet, this entrypoint provides a
     * place to do that.
     *
     * @param out the output stream
     * @param length the sequence length or -1
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if length is less than -1
     */
    public static void writeSequenceLength(DataOutput out, int length)
        throws IOException {

        if (length < -1) {
            throw new IllegalArgumentException(
                "Invalid sequence length: " + length);
        }
        writePackedInt(out, length);
    }

    /**
     * Writes the length of a non-null sequence.  The length is represented as
     * a non-negative {@link #readPackedInt packed int}.  Although we don't
     * enforce maximum sequence lengths yet, this entrypoint provides a place
     * to do that.
     *
     * @param out the output stream
     * @param length the sequence length
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if length is less than 0
     */
    public static void writeNonNullSequenceLength(DataOutput out, int length)
        throws IOException {

        if (length < 0) {
            throw new IllegalArgumentException(
                "Invalid non-null sequence length: " + length);
        }
        writePackedInt(out, length);
    }

    /**
     * Reads a possibly null byte array as a {@link #readSequenceLength
     * sequence length} followed by the array contents.
     *
     * @param in the input stream
     * @return array the array or null
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if the array length is too large
     */
    public static byte[] readByteArray(DataInput in)
        throws IOException {

        final int len = readSequenceLength(in);
        if (len == -1) {
            return null;
        }
        if (len == 0) {
            return EMPTY_BYTES;
        }
        final byte[] array = new byte[len];
        in.readFully(array);
        return array;
    }

    /**
     * Writes a possibly null byte array as a {@link #writeSequenceLength
     * sequence length} followed by the array contents.
     *
     * @param out the output stream
     * @param array the byte array or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeByteArray(DataOutput out, byte[] array)
        throws IOException {

        final int length = (array == null) ? -1 : Array.getLength(array);
        writeArrayLength(out, array);
        if (length > 0) {
            out.write(array);
        }
    }

    /**
     * Reads a non-null byte array as a {@link #readNonNullSequenceLength
     * non-null sequence length} followed by the array contents.
     *
     * @param in the input stream
     * @return array the array
     * @throws IOException if an I/O error occurs, if the input represents a
     * null array, if the input format is invalid, or if the array length is
     * too large
     */
    public static byte[] readNonNullByteArray(DataInput in)
        throws IOException {

        final byte[] array = readByteArray(in);
        if (array == null) {
            throw new IOException("Read unexpected null array");
        }
        return array;
    }

    /**
     * Writes a non-null byte array as a {@link #writeNonNullSequenceLength
     * non-null sequence length} followed by the array contents.
     *
     * @param out the output stream
     * @param array the byte array
     * @throws IOException if an I/O error occurs
     */
    public static void writeNonNullByteArray(DataOutput out, byte[] array)
        throws IOException {

        checkNull("array", array);
        writeByteArray(out, array);
    }

    /**
     * Serializes a non-null collection of non-null FastExternalizable elements
     * as a {@link #writeNonNullSequenceLength non-null sequence length}
     * followed by the serialized forms of the elements.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param collection the collection
     * @throws IOException if an I/O error occurs
     */
    public static void writeNonNullCollection(
        DataOutput out,
        short serialVersion,
        Collection<? extends FastExternalizable> collection)
        throws IOException {

        checkNull("collection", collection);
        writeNonNullSequenceLength(out, collection.size());
        for (final FastExternalizable e : collection) {
            e.writeFastExternal(out, serialVersion);
        }
    }

    /**
     * Serializes a non-null array of non-null {@link FastExternalizable}
     * elements as a {@link #writeNonNullSequenceLength non-null sequence
     * length} followed by the serialized forms of the elements.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param array the array
     * @throws IOException if an I/O error occurs
     */
    public static void writeNonNullArray(DataOutput out,
                                         short serialVersion,
                                         FastExternalizable[] array)
        throws IOException {

        checkNull("array", array);
        writeNonNullSequenceLength(out, array.length);
        for (final FastExternalizable e : array) {
            e.writeFastExternal(out, serialVersion);
        }
    }

    /**
     * Reads a possibly null byte array with {@link #readByteArray}, but using
     * a short as the length format if {@code serialVersion} is less than
     * {@link SerialVersion#STD_UTF8_VERSION}.  This method should only be used
     * to support backward compatibility for old methods.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @return array the array or null
     * @throws IOException if an I/O error occurs, if the input format is
     * invalid, or if the array length is too large
     */
    public static byte[] readByteArrayOldShortLength(DataInput in,
                                                     short serialVersion)
        throws IOException {

        if (serialVersion >= SerialVersion.STD_UTF8_VERSION) {
            return readByteArray(in);
        }
        final short len = in.readShort();
        if (len == -1) {
            return null;
        }
        if (len == 0) {
            return EMPTY_BYTES;
        }
        checkMaxSequenceLength(len);
        final byte[] array = new byte[len];
        in.readFully(array);
        return array;
    }

    /**
     * Writes a possibly null byte array, using {@link #writeByteArray}, but
     * using a short as the length format if {@code serialVersion} is less than
     * {@link SerialVersion#STD_UTF8_VERSION}.  This method should only be used
     * to support backward compatibility for old methods.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param array the byte array or null
     * @throws IOException if an I/O error occurs
     */
    public static void writeByteArrayOldShortLength(DataOutput out,
                                                    short serialVersion,
                                                    byte[] array)
        throws IOException {

        if (serialVersion >= SerialVersion.STD_UTF8_VERSION) {
            SerializationUtil.writeByteArray(out, array);
        } else if (array == null) {
            out.writeShort(-1);
        } else {
            out.writeShort(array.length);
            out.write(array);
        }
    }

    /**
     * Reads a non-null byte array, using {@link #readNonNullByteArray}, but
     * using a short as the length format if {@code serialVersion} is less than
     * {@link SerialVersion#STD_UTF8_VERSION}.  This method should only be used
     * to support backward compatibility for old methods.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @return array the array or null
     * @throws IOException if an I/O error occurs, if the array value is null,
     * if the input format is invalid, or if the array length is too large
     */
    public static byte[] readNonNullByteArrayOldShortLength(
        DataInput in, short serialVersion)
        throws IOException {

        final byte[] array = readByteArrayOldShortLength(in, serialVersion);
        if (array == null) {
            throw new IOException("Expected non-null array");
        }
        return array;
    }

    /**
     * Writes a non-null byte array, using {@link #writeByteArray}, but using a
     * short as the length format if {@code serialVersion} is less than {@link
     * SerialVersion#STD_UTF8_VERSION}.  This method should only be used to
     * support backward compatibility for old methods.
     *
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param array the byte array
     * @throws IOException if an I/O error occurs
     */
    public static void writeNonNullByteArrayOldShortLength(DataOutput out,
                                                           short serialVersion,
                                                           byte[] array)
        throws IOException {

        checkNull("array", array);
        writeByteArrayOldShortLength(out, serialVersion, array);
    }

    /**
     * Serializes a FastExternalizable object which may be null.  First stores
     * a boolean, which represents if the object is present, followed, if true,
     * by the data for the object.
     *
     * @param <T> the type of the object
     * @param out the output stream
     * @param serialVersion the version of the serialization format
     * @param object the object or null
     * @throws IOException if there is an I/O error or a problem writing the
     * object
     */
    public static <T extends FastExternalizable>
        void writeFastExternalOrNull(DataOutput out,
                                     short serialVersion,
                                     T object)
        throws IOException {

        if (object != null) {
            out.writeBoolean(true);
            object.writeFastExternal(out, serialVersion);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Writes a non-null {@link TimeUnit} to the output stream as a {@link
     * #writePackedInt packed int} using the ordinal value.
     *
     * @param out the output stream
     * @param timeUnit the time unit
     * @throws IOException if there is an I/O error
     */
    public static void writeTimeUnit(DataOutput out,
                                     TimeUnit timeUnit)
        throws IOException {

        checkNull("timeUnit", timeUnit);
        writePackedInt(out, timeUnit.ordinal());
    }

    /**
     * Reads a {@link TimeUnit} from the input stream as a {@link
     * #readPackedInt packed int} using the ordinal value.
     *
     * @param in the input stream
     * @throws IOException if there is an I/O error or a problem with the input
     * format
     */
    public static TimeUnit readTimeUnit(DataInput in)
        throws IOException {

        final int ordinal = readPackedInt(in);
        try {
            return TIMEUNIT_VALUES[ordinal];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException("Illegal TimeUnit ordinal: " + ordinal);
        }
    }
}

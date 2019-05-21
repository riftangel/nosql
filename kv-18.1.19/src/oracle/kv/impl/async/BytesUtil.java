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

package oracle.kv.impl.async;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * Utility tools for a collection of bytes.
 */
public class BytesUtil {

    public final static int NUM_BYTES_PER_LINE = 16;

    /**
     * Returns a string of hex bytes for a byte array.
     *
     * @param array the byte array
     * @param offset the offset to start
     * @param len the number of bytes
     *
     * @return the string
     */
    public static String bytesToHex(byte[] array, int offset, int len) {
        char[] hexArray = new char[len * 3];
        for (int i = 0; i < len; ++i) {
            int v = array[i + offset] & 0xFF;
            hexArray[i * 3] = Character.forDigit(v >>> 4, 16);
            hexArray[i * 3 + 1] = Character.forDigit(v & 0x0F, 16);
            if ((i + 1) % NUM_BYTES_PER_LINE == 0) {
                hexArray[i * 3 + 2] = '\n';
            } else {
                hexArray[i * 3 + 2] = ' ';
            }
        }
        return new String(hexArray);
    }

    /**
     * Returns an integer from four bytes in a byte array.
     *
     * @param array the byte array
     * @param offset the offset to start
     *
     * @return the integer
     */
    public static int bytesToInt(byte[] array, int offset) {
        return (array[offset] << 24) +
            ((array[offset + 1] & 0xff) << 16) +
            ((array[offset + 2] & 0xff) << 8) +
            (array[offset + 3] & 0xff);
    }

    /**
     * Returns a string of hex bytes for a byte buffer.
     *
     * @param buf the byte buffer
     * @param offset the offset to start
     * @param len the number of bytes
     *
     * @return the string
     */
    public static String bytebufToHex(ByteBuffer buf, int offset, int len) {
        char[] hexArray = new char[len * 3];
        for (int i = 0; i < len; ++i) {
            int v = buf.get(i + offset) & 0xFF;
            hexArray[i * 3] = Character.forDigit(v >>> 4, 16);
            hexArray[i * 3 + 1] = Character.forDigit(v & 0x0F, 16);
            if ((i + 1) % NUM_BYTES_PER_LINE == 0) {
                hexArray[i * 3 + 2] = '\n';
            } else {
                hexArray[i * 3 + 2] = ' ';
            }
        }
        return new String(hexArray);
    }

    /**
     * Returns the hex string representation of a byte array.
     *
     * @param array the byte array
     * @param offset the offset to start
     * @param len the number of bytes
     *
     * @return the string
     */
    public static String toString(byte[] array, int offset, int len) {
        if (array == null) {
            return "[/]";
        }
        len = Math.min(len, array.length - offset);
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        builder.append("(").append(offset).
            append(","). append(len).append(")");
        if (offset > 0) {
            builder.append("...");
        }
        builder.append(bytesToHex(array, offset, len));
        if (offset + len < array.length) {
            builder.append("...");
        }
        builder.append("]");
        return builder.toString();
    }

    /**
     * Returns the hex string representation of a byte array.
     *
     * @param buf the byte buffer
     * @param len the number of bytes to show from index 0 and position
     *
     * @return the string
     */
    public static String toString(ByteBuffer buf, int len) {
        if (buf == null) {
            return "[/]";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        int pos = buf.position();
        int lim = buf.limit();
        int cap = buf.capacity();
        builder.append("(").append(pos).append(",").
            append(lim).append(",").append(cap).append(")");
        int lenZero = Math.min(pos, len);
        int lenPos = Math.min(lim - pos, len);
        builder.append(bytebufToHex(buf, 0, lenZero));
        if (lenZero < pos) {
            builder.append("...");
        }
        builder.append(bytebufToHex(buf, pos, lenPos));
        if (lenPos < lim - pos) {
            builder.append("...");
        }
        builder.append("]");
        return builder.toString();
    }

    /**
     * Returns the string representation of an array of byte buffers.
     *
     * @param buffers the byte buffers
     * @param offset the offset of buffers
     * @param nbufs the number of buffers
     * @param len the number of bytes to show for each buf
     *
     * @return the string
     */
    public static String toString(ByteBuffer[] buffers,
                                  int offset,
                                  int nbufs,
                                  int len) {
        if (buffers == null) {
            return "{/}";
        }
        nbufs = Math.min(buffers.length - offset, nbufs);
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("(").append(offset).append(")");
        if (offset > 0) {
            builder.append("...");
        }
        for (int i = offset; i < offset + nbufs; ++i) {
            builder.append(toString(buffers[i], len));
        }
        if (offset + nbufs < buffers.length) {
            builder.append("...");
        }
        builder.append("}");
        return builder.toString();
    }

    /**
     * Returns the string representation of a collection of byte buffers.
     *
     * @param bufs the byte buffers
     * @param offset the offset of buffers
     * @param nbufs the number of buffers
     * @param len the number of bytes to show for each buf
     *
     * @return the string
     */
    public static String toString(Collection<ByteBuffer> bufs,
                                  int offset,
                                  int nbufs,
                                  int len) {
        if (bufs == null) {
            return "{/}";
        }
        int cnt0 = offset;
        int cnt1 = Math.min(bufs.size(), nbufs);
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("(").append(offset).append(")");
        if (offset > 0) {
            builder.append("...");
        }
        for (ByteBuffer buf : bufs) {
            if ((--cnt0) > 0) {
                continue;
            }
            builder.append(toString(buf, len));
            if ((--cnt1) <= 0) {
                break;
            }
        }
        if (offset + nbufs < bufs.size()) {
            builder.append("...");
        }
        builder.append("}");
        return builder.toString();
    }
}


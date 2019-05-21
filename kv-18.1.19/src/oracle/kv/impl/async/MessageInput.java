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

import java.io.DataInput;
import java.io.EOFException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * A {@code DataInput} stream for input dialog messages.
 *
 * <p>Methods in this class are not thread-safe. We expect the message input is
 * read from only one thread.
 */
public class MessageInput implements DataInput {

    private final Queue<BytesInput> inputs = new LinkedList<BytesInput>();
    /* Buffer for reading long. */
    private byte[] readBuffer = new byte[8];

    /**
     * Creates an instance of this class.
     */
    public MessageInput() {

    }

    /**
     * Reads some bytes from an input stream and stores them into the buffer
     * array {@code b}.
     */
    @Override
    public void readFully(byte[] b) throws EOFException {
        readFully(b, 0, b.length);
    }

    /**
     *
     * Reads {@code len} bytes from an input stream.
     */
    @Override
    public void readFully(byte[] b, int off, int len) throws EOFException {
        if (len == 0) {
            return;
        }
        BytesInput input;
        int remaining;
        while (true) {
            input = inputs.peek();
            if (input == null) {
                throw new EOFException();
            }
            remaining = input.remaining();
            int n = Math.min(len, remaining);
            input.readFully(b, off, n);
            off += n;
            len -= n;
            pollIfConsumed();
            if (len == 0) {
                break;
            }
        }
    }

    /**
     * Makes an attempt to skip over {@code n} bytes of data from the input
     * stream, discarding the skipped bytes.
     */
    @Override
    public int skipBytes(int len) {
        int skipped = 0;
        BytesInput input;
        int remaining;
        while (true) {
            input = inputs.peek();
            if (input == null) {
                return skipped;
            }
            remaining = input.remaining();
            int n = Math.min(remaining, len);
            try {
                input.skipBytes(n);
            } catch (EOFException e) {
                /* We already checked the input has this many data. */
                throw new AssertionError();
            }
            len -= n;
            skipped += n;
            pollIfConsumed();
            if (len == 0) {
                return skipped;
            }
        }
    }

    /**
     * Reads one input byte and returns {@code true} if that byte is nonzero,
     * {@code false} if that byte is zero.
     */
    @Override
    public boolean readBoolean() throws EOFException {
         byte b = readByte();
         return (b != 0);
    }

    /**
     * Reads and returns one input byte.
     */
    @Override
    public byte readByte() throws EOFException {
        final BytesInput input = inputs.peek();
        if (input == null) {
            throw new EOFException();
        }
        byte b = input.readByte();
        pollIfConsumed();
        return b;
    }

    /**
     * Reads one input byte, zero-extends it to type {@code int}, and returns
     * the result, which is therefore in the range {@code 0} through {@code
     * 255}.
     */
    @Override
    public int readUnsignedByte() throws EOFException {
        return readByte() & 0xff;
    }

    /**
     * Reads two input bytes and returns a {@code short} value.
     */
    @Override
    public short readShort() throws EOFException {
        byte b0 = readByte();
        byte b1 = readByte();
        return (short) ((b0 << 8) | (b1 & 0xff));
    }

    /**
     * Reads two input bytes and returns an {@code int} value in the range
     * {@code 0} through {@code 65535}.
     */
    @Override
    public int readUnsignedShort() throws EOFException {
        byte b0 = readByte();
        byte b1 = readByte();
        return (((b0 & 0xff) << 8) | (b1 & 0xff));
    }

    /**
     * Reads two input bytes and returns a {@code char} value.
     */
    @Override
    public char readChar() throws EOFException {
        byte b0 = readByte();
        byte b1 = readByte();
        return (char) ((b0 << 8) | (b1 & 0xff));
    }

    /**
     * Reads four input bytes and returns an {@code int} value.
     */
    @Override
    public int readInt() throws EOFException {
        byte b0 = readByte();
        byte b1 = readByte();
        byte b2 = readByte();
        byte b3 = readByte();
        return ((b0 << 24) |
                ((b1 & 0xff) << 16) |
                ((b2 & 0xff) <<  8) |
                ((b3 & 0xff) <<  0));
    }

    /**
     * Reads eight input bytes and returns a {@code long} value.
     */
    @Override
    public long readLong() throws EOFException {
        readFully(readBuffer, 0, 8);
        return (((long)readBuffer[0] << 56) +
                ((long)(readBuffer[1] & 0xff) << 48) +
                ((long)(readBuffer[2] & 0xff) << 40) +
                ((long)(readBuffer[3] & 0xff) << 32) +
                ((long)(readBuffer[4] & 0xff) << 24) +
                ((readBuffer[5] & 0xff) << 16) +
                ((readBuffer[6] & 0xff) <<  8) +
                ((readBuffer[7] & 0xff) <<  0));
    }

    /**
     * Reads four input bytes and returns a {@code float} value.
     */
    @Override
    public float readFloat() throws EOFException {
        return Float.intBitsToFloat(readInt());
    }

    /**
     * Reads eight input bytes and returns a {@code double} value.
     */
    @Override
    public double readDouble() throws EOFException {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * Reads the next line of text from the input stream.
     */
    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads in a string that has been encoded using a modified UTF-8 format.
     */
    @Override
    public String readUTF() {
        throw new UnsupportedOperationException(
                "Use oracle.kv.impl.util.SerializationUtil.readString");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("[");
        for (BytesInput input : inputs) {
            sb.append(input.toString());
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Adds a {@link BytesInput} to the message input.
     *
     * @param input the bytes input
     */
    public void add(BytesInput input) {
        if (input == null) {
            throw new NullPointerException();
        }
        if (input.remaining() == 0) {
            return;
        }
        inputs.add(input);
    }

    /**
     * Poll the first input and release if there is no readable bytes
     * remaining.
     */
    private void pollIfConsumed() {
        BytesInput input = inputs.peek();
        if (input == null) {
            return;
        }
        if (input.remaining() == 0) {
            inputs.poll();
            input.release();
        }
    }

}

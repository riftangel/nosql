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

import java.io.DataOutput;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;

/**
 * A {@code DataOutput} stream for output dialog messages.
 *
 * <p>Methods in this class are not thread-safe. We expect the message output
 * is written from only one thread.
 *
 * @see java.io.DataOutput
 */
public class MessageOutput implements DataOutput {

    private final static int BUFFER_SIZE = 4096;

    private final int bufferSize;
    /* A buffer used for non-array-like data */
    private ByteBuffer buffer;
    /*
     * The position in buffer representing the start of new data that needs to
     * be added to outputs.
     */
    private int pos = 0;
    /* The output data in the form of a byte buffer list */
    private final List<ByteBuffer> outputs = new ArrayList<ByteBuffer>();
    /* Size of the output */
    private volatile int nbytesTotal = 0;
    /*
     * Indicates whether the output frames are already polled. The frames are
     * only allowed to be polled once and if polled, no write can be performed.
     * This is used to make sure that the caller should not write to the
     * message output after it calls DialogContext#write with the message.
     * Using volatile for thread-safety should be sufficient.
     */
    private volatile boolean framesPolled = false;

    public MessageOutput() {
        this(BUFFER_SIZE);
    }

    /* Make bufferSize adjustable for testing. */
    public MessageOutput(int bufferSize) {
        if ((bufferSize < 8)) {
            throw new IllegalArgumentException();
        }
        this.bufferSize = bufferSize;
        this.buffer = ByteBuffer.allocate(bufferSize);
    }

    /**
     * Writes to the output stream the eight low-order bits of the argument
     * <code>b</code>.
     */
    @Override
    public void write(int b) {
        ensureFramesNotPolled();
        allocIfLessThan(1);
        buffer.put((byte) b);
        nbytesTotal ++;
    }

    /**
     * Writes to the output stream all the bytes in array <code>b</code>.
     */
    @Override
    public void write(byte[] b) {
        ensureFramesNotPolled();
        write(b, 0, b.length);
    }

    /**
     * Writes <code>len</code> bytes from array <code>b</code>, in order,  to
     * the output stream.
     */
    @Override
    public void write(byte[] b, int off, int len) {
        ensureFramesNotPolled();
        /*
         * Check for zero length, the ByteBuffer.wrap will do the rest of
         * argument check.
         */
        if (len == 0) {
            return;
        }
        appendBufferedToOutput();
        outputs.add(ByteBuffer.wrap(b, off, len));
        nbytesTotal += len;
    }


    /**
     * Writes a <code>boolean</code> value to this output stream.
     */
    @Override
    public void writeBoolean(boolean v) {
        ensureFramesNotPolled();
        write(v ? 1 : 0);
    }

    /**
     * Writes to the output stream the eight low- order bits of the argument
     * <code>v</code>.
     */
    @Override
    public void writeByte(int v) {
        ensureFramesNotPolled();
        write(v);
    }

    /**
     * Writes two bytes to the output stream to represent the value of the
     * argument.
     */
    @Override
    public void writeShort(int v) {
        ensureFramesNotPolled();
        allocIfLessThan(2);
        buffer.putShort((short) v);
        nbytesTotal += 2;
    }

    /**
     * Writes a <code>char</code> value, which is comprised of two bytes, to
     * the output stream.
     */
    @Override
    public void writeChar(int v) {
        ensureFramesNotPolled();
        allocIfLessThan(2);
        buffer.putChar((char) v);
        nbytesTotal += 2;
    }

    /**
     * Writes an <code>int</code> value, which is comprised of four bytes, to
     * the output stream.
     */
    @Override
    public void writeInt(int v) {
        ensureFramesNotPolled();
        allocIfLessThan(4);
        buffer.putInt(v);
        nbytesTotal += 4;
    }

    /**
     * Writes a <code>long</code> value, which is comprised of eight bytes, to
     * the output stream.
     */
    @Override
    public void writeLong(long v) {
        ensureFramesNotPolled();
        allocIfLessThan(8);
        buffer.putLong(v);
        nbytesTotal += 8;
    }

    /**
     * Writes a <code>float</code> value, which is comprised of four bytes, to
     * the output stream.
     */
    @Override
    public void writeFloat(float v) {
        ensureFramesNotPolled();
        allocIfLessThan(4);
        buffer.putFloat(v);
        nbytesTotal += 4;
    }

    /**
     * Writes a <code>double</code> value, which is comprised of eight bytes,
     * to the output stream.
     */
    @Override
    public void writeDouble(double v) {
        ensureFramesNotPolled();
        allocIfLessThan(8);
        buffer.putDouble(v);
        nbytesTotal += 8;
    }

    /**
     * Writes a string to the output stream.
     */
    @Override
    public void writeBytes(String s) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes every character in the string <code>s</code>, to the output
     * stream, in order, two bytes per character.
     */
    @Override
    public void writeChars(String s) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes two bytes of length information to the output stream, followed by
     * the modified UTF-8 representation of  every character in the string
     * <code>s</code>.
     */
    @Override
    public void writeUTF(String s) throws UTFDataFormatException {
        throw new UnsupportedOperationException(
                "Use oracle.kv.impl.util.SerializationUtil.writeString");
    }

    /**
     * Gets the size of this output.
     *
     * @return the size
     */
    public int size() {
        return nbytesTotal;
    }

    /**
     * Creates frames according to frameSize and returns the frames.
     *
     * This method should be called only once. After the method is called, all
     * methods in this class will throw {@link IllegalStateException}.
     *
     * @param frameSize the frame size
     * @return list of frames
     */
    public Queue<List<ByteBuffer>> pollFrames(int frameSize) {
        if (frameSize <= 0) {
            throw new IllegalArgumentException();
        }

        ensureFramesNotPolled();
        framesPolled = true;

        appendBufferedToOutput();

        final LinkedList<List<ByteBuffer>> frames =
            new LinkedList<List<ByteBuffer>>();

        if (outputs.isEmpty()) {
            return frames;
        }

        List<ByteBuffer> curr = new ArrayList<ByteBuffer>();
        int size = 0;
        frames.add(curr);

        for (final ByteBuffer buf : outputs) {
            int inc = buf.remaining();

            /* Add to the current frame if small enough */
            if (size + inc <= frameSize) {
                curr.add(buf);
                size += inc;
                continue;
            }

            /*
             * Keep filling the current frame with chunks of the buf until we
             * consume all.
             */
            while (true) {
                int remaining = buf.remaining();
                if (remaining == 0) {
                    break;
                }

                if ((size == frameSize)) {
                    curr = new ArrayList<ByteBuffer>();
                    size = 0;
                    frames.add(curr);
                }

                inc = Math.min(remaining, frameSize - size);
                int newPos = buf.position() + inc;

                ByteBuffer chunk = buf.duplicate();
                chunk.limit(newPos);
                curr.add(chunk);
                size += inc;

                buf.position(newPos);
            }
        }
        outputs.clear();
        return frames;
    }

    /**
     * Allocate a new buffer if the remaining of current is less than a certain
     * value. Add frames of the current buffer before allocate the new.
     */
    private void allocIfLessThan(int val) {
        if (val > bufferSize) {
            throw new AssertionError();
        }
        if (buffer.remaining() >= val) {
            return;
        }
        appendBufferedToOutput();
        buffer = ByteBuffer.allocate(Math.max(val, bufferSize));
        pos = 0;
    }

    /**
     * Append a part between pos and the current position of buffer as a chunk
     * to the frame.
     */
    private void appendBufferedToOutput() {
        int currpos = buffer.position();
        assert (buffer.limit() == buffer.capacity() && (pos <= currpos));
        if (pos == currpos) {
            return;
        }

        ByteBuffer chunk = buffer.duplicate();
        chunk.position(pos);
        chunk.limit(currpos);
        outputs.add(chunk);

        pos = currpos;
    }

    /**
     * Ensure the frames are not polled.
     */
    private void ensureFramesNotPolled() {
        if (framesPolled) {
            throw new AssertionError("Frames polled");
        }
    }
}


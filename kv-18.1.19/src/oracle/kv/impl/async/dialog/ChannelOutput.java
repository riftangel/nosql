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

package oracle.kv.impl.async.dialog;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.sleepycat.util.PackedInteger;

/**
 * An output class used for writing protocol messages.
 *
 * The class implements the funcitonality of writing protocol messages chunks.
 * Inheriting will implement the flushing funcitonality.
 *
 * Each message must be written to the channel output atomically, i.e., bytes
 * cannot be interleaved for different messages. Therefore, the channel output
 * is operated with chunks. To write, start with beginChunk and finish with
 * Chunk#done. The implementation of Chunk#done should be atomic.
 *
 * Note that there is no atomicity support in this class among chunks.
 * Therefore, the happen-before relationship between two chunks cannot be
 * defined using only methods from this class, for example, to achieve the
 * requirement of no DialogFrame after DialogAbort. Extra locking is needed for
 * that purpose.
 */
public class ChannelOutput {

    /* TODO: Use StandardCharsets version in Java 8 */
    private static final Charset utf8 = Charset.forName("UTF-8");

    /*
     * A queue staging the written chunks which will be flushed to the
     * transport.
     */
    private final Queue<Chunk> chunkQueue = new ConcurrentLinkedQueue<Chunk>();

    public Queue<Chunk> getChunkQueue() {
        return chunkQueue;
    }

    /**
     * Begin a message chunk.
     *
     * Must call {@link Chunk#done} after finished writing to the chunk.
     *
     * @param maxSizeNoBuf the maximum size of data that is not held
     * inside byte buffers.
     * @param last {@code true} if the chunk is the last valid chunk. Chunks
     * after the last will not be written to the transport.
     *
     * @return the chunk
     */
    public Chunk beginChunk(int maxSizeNoBuf, boolean last) {
        return new Chunk(maxSizeNoBuf, last);
    }

    /**
     * Close the output and release the resources.
     */
    public void close() {
        chunkQueue.clear();
    }

    /**
     * A chunk for writing message fields that writes to a list of byte
     * buffers.
     *
     * The class is not thread safe. All operations to the chunk should appear
     * in one thread.
     */
    public class Chunk {

        private final byte[] bytesForNoBuf;
        private final ByteBuffer bufForNoBuf;
        private int addBufPos = 0;
        /* The list of buffers holding all byte buffers of this chunk */
        private final List<ByteBuffer> chunkbuf;
        private final boolean last;

        private volatile ByteBuffer[] chunkArray = null;

        public Chunk(int maxSizeNoBuf, boolean last) {
            this.bytesForNoBuf = new byte[maxSizeNoBuf];
            this.bufForNoBuf = ByteBuffer.wrap(bytesForNoBuf);
            this.chunkbuf = new ArrayList<ByteBuffer>();
            this.last = last;
        }

        /**
         * Writes a byte.
         *
         * @param v value to write
         */
        public void writeByte(byte v) {
            ensureChunkArrayNull();
            bufForNoBuf.put(v);
        }

        /**
         * Writes a packed integer.
         *
         * The format follows com.sleepycat.util.PackedInteger#writeLong
         *
         * @param v value to write
         */
        public void writePackedLong(long v) {
            ensureChunkArrayNull();
            int pos = bufForNoBuf.position();
            int newPos = PackedInteger.writeLong(bytesForNoBuf, pos, v);
            bufForNoBuf.position(newPos);
        }

        /**
         * Writes a string with standard UTF-8 encoding, truncating the output
         * if it exceeds the specified maximum length.
         *
         * @param s the string
         * @param maxLength the max length of the encoded string
         */
        public void writeUTF8(String s, int maxLength) {
            ensureChunkArrayNull();
            CharBuffer chbuf = CharBuffer.wrap(s);
            final ByteBuffer buf = utf8.encode(chbuf);
            int length = buf.limit();
            /* Truncate the output if it is too large. */
            if (length > maxLength) {
                buf.limit(maxLength);
                length = buf.limit();
            }
            writePackedLong(length);
            addBuf();
            chunkbuf.add(buf);
        }

        /**
         * Writes an array of byte buffers.
         *
         * @param buflist the byte buffer list
         */
        public void writeBytes(List<ByteBuffer> buflist) {
            ensureChunkArrayNull();
            addBuf();
            chunkbuf.addAll(buflist);
        }

        /**
         * Mark the writing chunk done.
         *
         * Writing methods of this class will have no effect after this method
         * is called.
         */
        public void done() {
            ensureChunkArrayNull();
            addBuf();
            chunkArray = chunkbuf.toArray(new ByteBuffer[chunkbuf.size()]);
            chunkQueue.add(this);
            chunkbuf.clear();
        }

        /**
         * Returns the chunk array.
         */
        public ByteBuffer[] chunkArray() {
            return chunkArray;
        }

        /**
         * Returns if the chunk is the last.
         */
        public boolean last() {
            return last;
        }

        private void ensureChunkArrayNull() {
            if (chunkArray != null) {
                throw new IllegalStateException(
                        "Operation on chunk after it is done");
            }
        }

        private void addBuf() {
            int pos = bufForNoBuf.position();
            assert addBufPos <= pos;
            if (addBufPos == pos) {
                return;
            }

            final ByteBuffer buf = bufForNoBuf.duplicate();
            buf.position(addBufPos);
            buf.limit(pos);
            chunkbuf.add(buf);

            addBufPos = pos;
        }
    }

}


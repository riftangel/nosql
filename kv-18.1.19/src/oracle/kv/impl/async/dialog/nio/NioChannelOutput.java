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

package oracle.kv.impl.async.dialog.nio;

import java.nio.ByteBuffer;

import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.dialog.ChannelOutput;

/**
 * An output used for writing protocol messages and flushing to an NIO channel.
 *
 * The output is used as follows:
 *
 * NioChannelOutput.Bufs bufs = output.getBufs();
 * socketChannel.write(bufs.array(), bufs.offset(), bufs.length());
 * if (output.hasRemaining()) {
 *     // register for WRITE_INTEREST if necessary
 * }
 *
 * The above methods for flushing are not thread safe. Callers should acquire a
 * flush lock while calling these methods.
 */
class NioChannelOutput extends ChannelOutput {

    private static final int ARRAY_LENGTH = 128;

    private final Bufs bufsToFlush;
    private final int arrayLength;

    /*
     * The flag indicating we have flushed the chunk that marked last. Thread
     * safety is not an issue since flush is guaranteed to acquire a flush lock
     * by the caller.
     */
    private boolean lastChunkFetched;

    NioChannelOutput() {
        this(ARRAY_LENGTH);
    }

    NioChannelOutput(int arrayLength) {
        this.arrayLength = arrayLength;
        this.bufsToFlush = new Bufs();
    }

    /**
     * Wrapper class for an array of buffers.
     */
    class Bufs {
        private ByteBuffer[] array = new ByteBuffer[arrayLength];
        private int offset = 0;
        private int length = 0;
        /* The pos for the first array in chunkQueue to fetch. */
        private int posInFirstOfQueue = 0;

        ByteBuffer[] array() {
            return array;
        }

        int offset() {
            return offset;
        }

        int length() {
            return length;
        }

        /**
         * Updates the offset and length so that offset points to the first
         * buffer that has data.
         */
        void update() {
            final int off = offset;
            final int len = length;
            boolean empty = true;
            for (int i = off; i < off + len; ++i) {
                if (array[i].remaining() != 0) {
                    length = off + len - i;
                    offset = i;
                    empty = false;
                    break;
                }
            }
            if (empty) {
                offset = 0;
                length = 0;
            }
        }

        /**
         * Trims the array so that the first buffer has data.
         */
        void trim() {
            if (offset == 0) {
                return;
            }
            if (length == 0) {
                offset = 0;
                length = 0;
                return;
            }
            System.arraycopy(array, offset, array, 0, length);
            offset = 0;
        }

        /**
         * Fetches from the chunkQueue and fills the array.
         */
        void fetch() {

            update();

            if (lastChunkFetched) {
                return;
            }

            if (offset > array.length / 2) {
                trim();
            }

            while (true) {
                Chunk nextChunk = getChunkQueue().peek();
                if (nextChunk == null) {
                    break;
                }
                ByteBuffer[] nextBufs = nextChunk.chunkArray();
                int actualLenInNextBufs = nextBufs.length - posInFirstOfQueue;
                int numSlotsInArray = array.length - offset - length;
                int copyLen = Math.min(actualLenInNextBufs, numSlotsInArray);
                System.arraycopy(nextBufs, posInFirstOfQueue,
                        array, offset + length, copyLen);
                posInFirstOfQueue += copyLen;
                length += copyLen;
                if (posInFirstOfQueue == nextBufs.length) {
                    getChunkQueue().poll();
                    if (nextChunk.last()) {
                        lastChunkFetched = true;
                        break;
                    }
                    posInFirstOfQueue = 0;
                }
                if (offset + length == array.length) {
                    break;
                }
            }
        }
    }

    /**
     * Returns the buffers to flush to the channel.
     */
    Bufs getBufs() {
        bufsToFlush.fetch();
        return bufsToFlush;
    }

    /**
     * Returns {@code true} if there is remaining data to flush.
     */
    boolean hasRemaining() {
        return (!getChunkQueue().isEmpty()) || (bufsToFlush.length != 0);
    }

    /**
     * Return the string representation.
     *
     * The operation is not thread safe.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ChannelOutput");
        builder.append(":A");
        builder.append("(").append(bufsToFlush.offset()).append(",").
            append(bufsToFlush.length()).append(")");
        builder.append(BytesUtil.toString(
                    bufsToFlush.array(), bufsToFlush.offset(),
                    bufsToFlush.length(), 32));
        builder.append("Q");
        builder.append("(").append(getChunkQueue().size()).append(")");
        builder.append(BytesUtil.toString(
                    getChunkQueue().peek().chunkArray(), 0, 4, 32));
        return builder.toString();
    }

}


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
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import oracle.kv.impl.async.BytesInput;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.dialog.ChannelInput;

import com.sleepycat.util.PackedInteger;

/**
 * Input used for reading from an NIO channel and feeding to protocol read.
 *
 * This input provides buffer management for both socket channel reads and
 * protocol reader. For socket channel reads, to fully utilize the gathering
 * read feature and avoid reading into a byte buffer that has few remaining
 * space, we always prepare two buffers. For protocol reads, we must support
 * the mark-and-reset functionality as the current data may not be complete for
 * a protocol message.
 *
 * The data structure is as follows:
 * |----------------------------------------------|
 * |             protoBufList                     |
 * |----------------------------------------------|
 * | ...| protoBuf | ................ |  chnlBuf0 | chnlBuf1
 * |----------------------------------------------|
 *         ^  ^    ^                           ^
 *         |  |   protoIter                    |
 *         |  |                                |
 *         |  markPos, where protocol reader   |
 *         |           marks for a later reset |
 *         |                                   |
 *         |                              chnlPos, where socket reads start
 *     protoPos, where protocol read starts
 *
 * The instances of this class is intended to be used inside the single-thread
 * channel exector and is not intended to be thread safe.
 *
 * The input is used as follows:
 *
 * while (true) {
 *     ByteBuffer[] bufs = input.flipToChannelRead()
 *     socketChannel.read(bufs);
 *     input.flipToProtocolRead();
 *     while (true) {
 *         // ...
 *         input.mark();
 *         if (notEnoughData(input)) {
 *             input.reset();
 *             break;
 *         }
 *         // read data from input
 *     }
 * }
 */
class NioChannelInput implements ChannelInput {

    /* TODO: Use StandardCharsets version in Java 8 */
    private static final Charset utf8 = Charset.forName("UTF-8");
    private static final int BUFFER_SIZE = 4096;

    private final int bufferSize;
    /* Byte buffer array for channel read. */
    private final ByteBuffer[] chnlArray = new ByteBuffer[2];
    /* Two byte buffers for channel read. */
    private ByteBuffer chnlBuf0;
    private ByteBuffer chnlBuf1;
    /*
     * The pos in chnlBuf0 for a socket channel read. Data before chnlPos is
     * put during previos channel read and is ready for protocol read.
     */
    private int chnlPos;
    /*
     * All buffers that are ready for the protocol read including chnlBuf0. The
     * chnlBuf0 is pushed into the list when it is assigned. When flipping to
     * the protocol-read mode, if chnlBuf0 is filled with protocol-ready data,
     * chnlBuf1 becomes chnlBuf0 and pushed into the list while a new chnllBuf1
     * is allocated.
     */
    private LinkedList<ByteBuffer> protoBufList;
    /*
     * An iterator to quickly locate the next proto buf when protoBuf is
     * consumed during protocol read.
     */
    private ListIterator<ByteBuffer> protoIter;
    /*
     * The buffer for protocol read. The protoIter points to the item after the
     * protoBuf inside the list.
     */
    private ByteBuffer protoBuf;
    /*
     * The pos of the protoBuf to start read. Used the save the position of
     * protoBuf when flipping to the channel read mode. This is necessary
     * because chnlBuf0 and protoBuf can be the same byte buffer.
     */
    private int protoPos;
    /*
     * The marked pos that will be reset to later. The marked buffer is always
     * the first byte buffer of protoBufList, i.e., buffers before it are
     * always considered consumed and thrown away since the caller will have no
     * way to refer to them again.
     */
    private int markPos;
    /* Number of bytes between protoBuf#position() to chnlBuf0#chnlPos. */
    private int readableBytes = 0;

    /* Byte array used for reading packed long. */
    private final byte[] packedLongBytes =
        new byte[PackedInteger.MAX_LONG_LENGTH];

    NioChannelInput() {
        this(BUFFER_SIZE);
    }

    NioChannelInput(int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException();
        }
        this.bufferSize = bufferSize;
        this.chnlBuf0 = ByteBuffer.allocate(bufferSize);
        this.chnlBuf1 = ByteBuffer.allocate(bufferSize);
        this.chnlArray[0] = chnlBuf0;
        this.chnlArray[1] = chnlBuf1;
        this.chnlPos = 0;
        this.protoBufList = new LinkedList<ByteBuffer>();
        this.protoBufList.add(chnlBuf0);
        this.protoBuf = chnlBuf0;
        this.protoPos = 0;
        this.markPos = 0;
    }

    /**
     * Sets the mark at the current position.
     */
    @Override
    public void mark() {
        /* Since we mark again, everything until protoBuf can be removed. */
        pollUntilProtoBuf();
        markPos = protoBuf.position();
    }

    /**
     * Resets the position to a previously marked position.
     */
    @Override
    public void reset() {
        /*
         * Resets the buffers to the state of when we mark, i.e., all buffers
         * except for the marked buffer having their position to be 0 and the
         * marked buffer having its position to the markPos.
         */

        /*
         * Resets all the buffers to position 0. We only need to loop until the
         * protoBuf since buffers after already have their position to be 0.
         */
        Iterator<ByteBuffer> iter = protoBufList.iterator();
        while (iter.hasNext()) {
            ByteBuffer buf = iter.next();
            readableBytes += buf.position();
            buf.position(0);
            if (buf == protoBuf) {
                break;
            }
        }

        protoIter = protoBufList.listIterator();

        protoBuf = protoIter.next();
        protoBuf.position(markPos);

        readableBytes -= markPos;
    }

    /**
     * Gets the number of readable bytes in the input.
     */
    @Override
    public int readableBytes() {
        return readableBytes;
    }

    /**
     * Reads a byte from the input.
     */
    @Override
    public byte readByte() {
        ensureProtoBufNotConsumed();
        byte b = protoBuf.get();
        readableBytes --;
        return b;
    }

    /**
     * Reads {@code len} bytes from the input.
     */
    @Override
    public BytesInput readBytes(int len) {
        if (len == 0) {
            return new NioBytesInput(0, null);
        }
        LinkedList<ByteBuffer> bufs = new LinkedList<ByteBuffer>();
        int n = len;
        while (true) {
            int chunkLen = Math.min(n, protoBuf.remaining());
            int newPos = protoBuf.position() + chunkLen;

            ByteBuffer chunk = protoBuf.duplicate();
            chunk.limit(newPos);

            bufs.add(chunk);
            n -= chunkLen;

            protoBuf.position(newPos);
            if (n == 0) {
                break;
            }
            ensureProtoBufNotConsumed();
        }
        readableBytes -= len;
        return new NioBytesInput(len, bufs);
    }

    /**
     * Peeks at the input to see if there is enough data for {@code
     * readPackedLong}.
     */
    @Override
    public boolean canReadPackedLong() {
        if (readableBytes == 0) {
            return false;
        }
        return (readableBytes >= peekPackedLongLength());
    }

    /**
     * Reads a packed long.
     */
    @Override
    public long readPackedLong() {
        /* Get the length. */
        final int len = peekPackedLongLength();
        /* Get the bytes from current protoBuf and the next if necessary */
        final int rest = protoBuf.remaining();
        if (len <= rest) {
            protoBuf.get(packedLongBytes, 0, len);
        } else {
            protoBuf.get(packedLongBytes, 0, rest);
            ensureProtoBufNotConsumed();
            protoBuf.get(packedLongBytes, rest, len - rest);
        }
        readableBytes -= len;
        return PackedInteger.readLong(packedLongBytes, 0);
    }

    private int peekPackedLongLength() {
        ensureProtoBufNotConsumed();
        packedLongBytes[0] = protoBuf.get(protoBuf.position());
        return PackedInteger.getReadLongLength(packedLongBytes, 0);
    }

    /**
     * Reads a {@code String} in standard UTF8 format.
     */
    @Override
    public String readUTF8(int length) {
        if (readableBytes < length) {
            return null;
        }
        /* Read the UTF8 bytes */
        final int len = length;
        final byte[] bytes = new byte[length];
        int offset = 0;
        while (true) {
            int n = Math.min(length, protoBuf.remaining());
            protoBuf.get(bytes, offset, n);
            length -= n;
            offset += n;
            if (length == 0) {
                break;
            }
            ensureProtoBufNotConsumed();
        }
        readableBytes -= len;
        return utf8.decode(ByteBuffer.wrap(bytes)).toString();
    }

    /**
     * Close the input and release resources.
     */
    @Override
    public void close() {
        protoBufList.clear();
    }

    /**
     * Return the string representation.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ChannelInput");
        builder.append(" chnlPos=").append(chnlPos);
        builder.append(" markPos=").append(markPos);
        builder.append(" protoPos=").append(protoPos);
        builder.append(" readable=").append(readableBytes);
        builder.append(" bufs=");
        Iterator<ByteBuffer> iter = protoBufList.iterator();
        while (iter.hasNext()) {
            ByteBuffer buf = iter.next();
            if (buf == protoBuf) {
                builder.append("Pr");
            }
            if (buf == chnlBuf0) {
                builder.append("Ch");
            }
            builder.append(BytesUtil.toString(buf, buf.limit()));
        }
        builder.append(" Ch1");
        builder.append(BytesUtil.toString(chnlBuf1, chnlBuf1.limit()));
        return builder.toString();
    }

    /**
     * Flip to the mode of reading from channel.
     *
     * Gets the input ready for a socket channel read after we have completed a
     * protocol read. See the class description for how the method is used.
     *
     * @return byte array for channel reading.
     */
    ByteBuffer[] flipToChannelRead() {
        /*
         * Clean up and save the protocol read states.
         * - Reset the mark position
         * - Remove buffers until protoBuf
         * - Set the protoPos
         */
        markPos = 0;
        pollUntilProtoBuf();
        protoPos = protoBuf.position();
        /* Make chnlBuf0 ready for channel read. */
        chnlBuf0.position(chnlPos);
        chnlBuf0.limit(chnlBuf0.capacity());
        return chnlArray;
    }

    /**
     * Flip to the mode of reading by the protocol.
     *
     * Gets the input ready for protocol reader after we have completed a
     * socket channel read. See the class description for how the method is
     * used.
     */
    void flipToProtocolRead() {
        /* Do accounting first */
        readableBytes += chnlBuf0.position() - chnlPos;
        readableBytes += chnlBuf1.position();
        /* Step1, allocate new buffer if necessary. */
        while (chnlBuf0.remaining() == 0) {
            /*
             * Make the all data in chnlBuf0 readable for protocol, if it is
             * only partially readable, then it is also the protoBuf, the
             * position will be set in Step 3.
             */
            chnlBuf0.limit(chnlBuf0.capacity());
            chnlBuf0.position(0);
            /*
             * Add the chnlBuf1 as chnlBuf0 into the protocol buffers. Its
             * positions for protocol will be set in the next iteration or in
             * Step2
             */
            protoBufList.add(chnlBuf1);
            /* Allocate a new buffer. */
            chnlBuf0 = chnlBuf1;
            chnlBuf1 = ByteBuffer.allocate(bufferSize);
            chnlArray[0] = chnlBuf0;
            chnlArray[1] = chnlBuf1;
        }
        /*
         * Step2, save channel read postion and Make chnlBuf0 ready for
         * protocol read.
         */
        chnlPos = chnlBuf0.position();
        chnlBuf0.limit(chnlPos);
        chnlBuf0.position(0);
        /*
         * Step3, set up the protocol read buffers. This will also set chnlBuf0
         * to the correct position if protoBuf == chnlbuf.
         */
        pollUntilProtoBuf();
        protoBuf.position(protoPos);
    }

    /**
     * Removes everything until protoBuf.
     */
    private void pollUntilProtoBuf() {
        while (true) {
            ByteBuffer buf = protoBufList.peek();
            if ((buf == null) || (buf == protoBuf)) {
                break;
            }
            protoBufList.poll();
        }
        protoIter = protoBufList.listIterator();
        protoBuf = protoIter.next();
    }

    /**
     * Ensures the protoBuf has remaining.
     */
    private void ensureProtoBufNotConsumed() {
        while (true) {
            if (protoBuf.remaining() > 0) {
                return;
            }
            if (!protoIter.hasNext()) {
                throw new IllegalStateException(
                        String.format(
                            "There is not enough data, " +
                            "should check readableBytes before read, " +
                            "input=%s",
                            toString()));
            }
            protoBuf = protoIter.next();
        }
    }


}

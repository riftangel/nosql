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

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Queue;

import oracle.kv.impl.async.BytesInput;

/**
 * An input that consumes chunks of bytes.
 *
 * The instances of this class is intended to be created and called inside the
 * single-thread channel executor and thus is not intended to be thread safe.
 */
class NioBytesInput implements BytesInput {

    private int remaining;
    private final Queue<ByteBuffer> bufQueue;

    NioBytesInput(int remaining, Queue<ByteBuffer> bufQueue) {
        this.remaining = remaining;
        this.bufQueue = bufQueue;
    }

    /**
     * Fills the byte array with {@code len} bytes.
     */
    @Override
    public void readFully(byte[] b, int off, int len) throws EOFException {
        if (len == 0) {
            return;
        }
        if (remaining < len) {
            throw new EOFException();
        }
        Iterator<ByteBuffer> iter = bufQueue.iterator();
        while (iter.hasNext()) {
            ByteBuffer buf = iter.next();
            int n = Math.min(len, buf.remaining());
            buf.get(b, off, n);
            len -= n;
            off += n;
            remaining -= n;
            if (buf.remaining() == 0) {
                iter.remove();
            }
            if (len == 0) {
                break;
            }
        }
    }

    /**
     * Skips {@code len} bytes.
     */
    @Override
    public void skipBytes(int n) throws EOFException {
        if (remaining < n) {
            throw new EOFException();
        }
        Iterator<ByteBuffer> iter = bufQueue.iterator();
        while (iter.hasNext()) {
            ByteBuffer buf = iter.next();
            int toskip = Math.min(n, buf.remaining());
            buf.position(buf.position() + toskip);
            n -= toskip;
            remaining -= toskip;
            if (buf.remaining() == 0) {
                iter.remove();
            }
            if (n == 0) {
                break;
            }
        }
    }

    /**
     * Reads a byte.
     */
    @Override
    public byte readByte() throws EOFException {
        if (remaining == 0) {
            throw new EOFException();
        }
        Iterator<ByteBuffer> iter = bufQueue.iterator();
        while (iter.hasNext()) {
            ByteBuffer buf = iter.next();
            if (buf.remaining() != 0) {
                remaining --;
                return buf.get();
            }
            iter.remove();
        }
        throw new EOFException();
    }

    /**
     * Returns the number of remaining bytes.
     */
    @Override
    public int remaining() {
        return remaining;
    }

    /**
     * Release the resource associated with this input.
     */
    @Override
    public void release() {
        bufQueue.clear();
    }

}

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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArrayOldShortLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArrayOldShortLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.UserDataControl;

/**
 * An operation that applies to a single key, from which the partition is
 * derived.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class SingleKeyOperation extends InternalOperation {

    /**
     * The key.
     */
    private final byte[] keyBytes;

    /**
     * Construct an operation with a single key.
     */
    public SingleKeyOperation(OpCode opCode, byte[] keyBytes) {
        super(opCode);
        checkNull("keyBytes", keyBytes);
        this.keyBytes = keyBytes;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    SingleKeyOperation(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);
        keyBytes = readNonNullByteArrayOldShortLength(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and greater:
     * <ol>
     * <li> ({@link InternalOperation}) {@code super}
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getKeyBytes keyBytes}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeNonNullByteArrayOldShortLength(out, serialVersion, keyBytes);
    }

    /**
     * Returns the byte array of the Key associated with the operation.
     */
    public byte[] getKeyBytes() {
        return keyBytes;
    }

    @Override
    public String toString() {
        return super.toString() + " Key: " +
               UserDataControl.displayKey(keyBytes);
    }
}

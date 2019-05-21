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

import static oracle.kv.impl.util.SerialVersion.STD_UTF8_VERSION;
import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.Depth;
import oracle.kv.KeyRange;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * A multi-delete operation deletes records in the KV Store.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class MultiDelete extends MultiKeyOperation {

    /** LOB suffix is present at version 2 and greater. */
    private static final short LOB_SERIAL_VERSION = 2;

    /**
     * The LOB suffix bytes sent to the RN, so the RN can use them to ensure
     * that LOB objects in the range are not deleted.  Is null when an R1
     * client sends the request.
     */
    private final byte[] lobSuffixBytes;

    /**
     * Constructs a multi-delete operation.
     */
    public MultiDelete(byte[] parentKey,
                       KeyRange subRange,
                       Depth depth,
                       byte[] lobSuffixBytes) {
        super(OpCode.MULTI_DELETE, parentKey, subRange, depth);
        this.lobSuffixBytes = lobSuffixBytes;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiDelete(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_DELETE, in, serialVersion);

        if (serialVersion >= STD_UTF8_VERSION) {
            lobSuffixBytes = readByteArray(in);
        } else if (serialVersion >= LOB_SERIAL_VERSION) {
            final int suffixLen = in.readShort();
            if (suffixLen == 0) {
                lobSuffixBytes = null;
            } else {
                lobSuffixBytes = new byte[suffixLen];
                in.readFully(lobSuffixBytes);
            }
        } else {
            lobSuffixBytes = null;
        }
    }

    byte[] getLobSuffixBytes() {
        return lobSuffixBytes;
    }

    /**
     * Writes this object to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and higher:
     * <ol>
     * <li> ({@link MultiKeyOperation}) {@code super}
     * <li> ({@link SerializationUtil#writeByteArray byte array)} {@link
     *      #getLobSuffixBytes lobSuffixBytes}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        if (serialVersion >= STD_UTF8_VERSION) {
            writeByteArray(out, lobSuffixBytes);
        } else if (serialVersion >= LOB_SERIAL_VERSION) {
            if ((lobSuffixBytes != null) && (lobSuffixBytes.length > 0)) {
                out.writeShort(lobSuffixBytes.length);
                out.write(lobSuffixBytes);
            } else {
                out.writeShort(0);
            }
        }
    }

    @Override
    public boolean performsWrite() {
        return true;
    }

    @Override
    public boolean isDelete() {
        return true;
    }
}

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
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArrayOldShortLength;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeCollectionLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArrayOldShortLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.Depth;
import oracle.kv.KeyRange;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.UserDataControl;

/**
 * This is an intermediate class for multi-get-batch iterate operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
abstract class MultiGetBatchIterateOperation extends MultiKeyOperation {

    private final List<byte[]> parentKeys;
    private final int batchSize;
    private final byte[] resumeKey;

    public MultiGetBatchIterateOperation(OpCode opCode,
                                         List<byte[]> parentKeys,
                                         byte[] resumekey,
                                         KeyRange subRange,
                                         Depth depth,
                                         int batchSize) {

        super(opCode, parentKeys.get(0), subRange, depth);

        this.parentKeys = parentKeys;
        this.resumeKey = resumekey;
        this.batchSize = batchSize;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    protected MultiGetBatchIterateOperation(OpCode opCode,
                                            DataInput in,
                                            short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);
        final int nkeys = (serialVersion >= STD_UTF8_VERSION) ?
            readSequenceLength(in) :
            in.readShort();
        if (nkeys >= 0) {
            parentKeys = new ArrayList<byte[]>(nkeys);
            for (int i = 0; i < nkeys; i++) {
                parentKeys.add(
                    readNonNullByteArrayOldShortLength(in, serialVersion));
            }
        } else {
            parentKeys = null;
        }
        if (serialVersion >= STD_UTF8_VERSION) {
            resumeKey = readByteArray(in);
        } else {
            int len = in.readShort();
            if (len > 0) {
                resumeKey = new byte[len];
                in.readFully(resumeKey);
            } else {
                resumeKey = null;
            }
        }
        batchSize = in.readInt();
    }

    List<byte[]> getParentKeys() {
        return parentKeys;
    }

    int getBatchSize() {
        return batchSize;
    }

    byte[] getResumeKey() {
        return resumeKey;
    }

    /**
     * Writes this object to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} or greater:
     * <ol>
     * <li> ({@link MultiKeyOperation}) {@code super}
     * <li> ({@link SerializationUtil#writeCollectionLength sequence length})
     *      <i>number of parentKeys</i>
     * <li> <i>[Optional]</i> <i>Repeat for each parent key</i>
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeNonNullByteArray non-null
     *         byte array}) <i>key</i>
     *    </ol>
     * <li> ({@link SerializationUtil#writeByteArray byte array})
     *      {@link #getResumeKey resumeKey}
     * <li> ({@link DataOutput#writeInt int}) {@link #getBatchSize batchSize}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        if (serialVersion >= STD_UTF8_VERSION) {
            writeCollectionLength(out, parentKeys);
        } else {
            out.writeShort((parentKeys != null) ? parentKeys.size() : -1);
        }
        if (parentKeys != null) {
            for (byte[] key: parentKeys) {
                writeNonNullByteArrayOldShortLength(out, serialVersion, key);
            }
        }
        if (serialVersion >= STD_UTF8_VERSION) {
            writeByteArray(out, resumeKey);
        } else if (resumeKey != null && resumeKey.length > 0) {
            out.writeShort(resumeKey.length);
            out.write(resumeKey);
        } else {
            out.writeShort(-1);
        }
        out.writeInt(batchSize);
    }

    @Override
    public String toString() {
        return "parentKeys: " + parentKeys.size() +
            " resumeKey: " + UserDataControl.displayKey(resumeKey) +
            " subRange: " + UserDataControl.displayKeyRange(getSubRange()) +
            " depth: " + getDepth();
    }
}

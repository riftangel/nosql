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
import static oracle.kv.impl.util.SerialVersion.STD_UTF8_VERSION;
import static oracle.kv.impl.util.SerializationUtil.readByteArrayOldShortLength;
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArrayOldShortLength;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArrayOldShortLength;
import static oracle.kv.impl.util.SerializationUtil.writeCollectionLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArrayOldShortLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.KeyRange;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * This is an intermediate class for a multi-batch-get table operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
abstract class MultiGetBatchTableOperation extends MultiGetTableOperation {

    private final List<byte[]> keys;
    private final int batchSize;
    private final byte[] resumeKey;

    public MultiGetBatchTableOperation(OpCode opCode,
                                       List<byte[]> parentKeys,
                                       byte[] resumeKey,
                                       TargetTables targetTables,
                                       KeyRange subRange,
                                       int batchSize) {

        super(opCode, parentKeys.get(0), targetTables, subRange);
        for (final byte[] key : parentKeys) {
            checkNull("parentKeys element", key);
        }
        this.keys = parentKeys;
        this.resumeKey= resumeKey;
        this.batchSize = batchSize;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    protected MultiGetBatchTableOperation(OpCode opCode,
                                          DataInput in,
                                          short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);
        final int nkeys = (serialVersion >= STD_UTF8_VERSION) ?
            readSequenceLength(in) :
            in.readShort();
        if (nkeys >= 0) {
            keys = new ArrayList<byte[]>(nkeys);
            for (int i = 0; i < nkeys; i++) {
                keys.add(
                    readNonNullByteArrayOldShortLength(in, serialVersion));
            }
        } else {
            keys = null;
        }
        resumeKey = readByteArrayOldShortLength(in, serialVersion);
        batchSize = in.readInt();
    }

    List<byte[]> getParentKeys() {
        return keys;
    }

    int getBatchSize() {
        return batchSize;
    }

    byte[] getResumeKey() {
        return resumeKey;
    }

    /**
     * Writes this object to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and greater:
     * <ol>
     * <li> ({@link MultiGetTableOperation}) {@code super}
     * <li> ({@link SerializationUtil#writeCollectionLength sequence length})
     *      <i>number of keys</i>
     * <li> <i>[Optional]</i> For each element of {@link #getParentKeys keys}:
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *    array}) <i>key</i>
     *    </ol>
     * <li> ({@link SerializationUtil#writeByteArray byte array}) {@link
     *      #getResumeKey resumeKey}
     * <li> ({@link DataOutput#writeInt int}) {@link #getBatchSize batchSize}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        final boolean useStdUTF8 = serialVersion >= STD_UTF8_VERSION;
        if (useStdUTF8) {
            writeCollectionLength(out, keys);
        } else {
            out.writeShort((keys != null) ? keys.size() : -1);
        }
        if (keys != null) {
            for (byte[] key: keys) {
                writeNonNullByteArrayOldShortLength(out, serialVersion, key);
            }
        }
        writeByteArrayOldShortLength(out, serialVersion, resumeKey);
        out.writeInt(batchSize);
    }
}

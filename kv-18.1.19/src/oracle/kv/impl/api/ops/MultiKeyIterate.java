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
import static oracle.kv.impl.util.SerializationUtil.readByteArrayOldShortLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArrayOldShortLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * A multi-key iterate operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
abstract public class MultiKeyIterate extends MultiKeyOperation {

    private final Direction direction;
    private final int batchSize;
    private final byte[] resumeKey;

    /**
     * Construct a multi-key iterate operation.
     */
    MultiKeyIterate(OpCode opCode,
                    byte[] parentKey,
                    KeyRange subRange,
                    Depth depth,
                    Direction direction,
                    int batchSize,
                    byte[] resumeKey) {
        super(opCode, parentKey, subRange, depth);
        checkNull("direction", direction);
        this.direction = direction;
        this.batchSize = batchSize;
        this.resumeKey = resumeKey;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiKeyIterate(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        direction = Direction.readFastExternal(in, serialVersion);
        batchSize = in.readInt();

        resumeKey = readByteArrayOldShortLength(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} or greater:
     * <ol>
     * <li> ({@link MultiKeyOperation}) {@code super}
     * <li> ({@link Direction}) {@link #getDirection direction}
     * <li> ({@link DataOutput#writeInt int}) {@link #getBatchSize batchSize}
     * <li> ({@link SerializationUtil#writeByteArray byte array}) {@link
     *      #getResumeKey resultKey}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        direction.writeFastExternal(out, serialVersion);
        out.writeInt(batchSize);

        writeByteArrayOldShortLength(out, serialVersion, resumeKey);
    }

    Direction getDirection() {
        return direction;
    }

    int getBatchSize() {
        return batchSize;
    }

    byte[] getResumeKey() {
        return resumeKey;
    }
}

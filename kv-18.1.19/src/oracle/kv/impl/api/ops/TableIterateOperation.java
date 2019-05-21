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
import static oracle.kv.impl.util.SerialVersion.MAXKB_ITERATE_VERSION;
import static oracle.kv.impl.util.SerializationUtil.readByteArrayOldShortLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArrayOldShortLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * This is an intermediate class for a table iteration where the records
 * may or may not reside on the same partition.
 *
 * @see #writeFastExternal FastExternalizable format
 */
abstract class TableIterateOperation extends MultiTableOperation {

    private final boolean majorComplete;
    private final Direction direction;
    private final int batchSize;
    private final int maxReadKB;
    private final byte[] resumeKey;

    protected TableIterateOperation(OpCode opCode,
                                    StoreIteratorParams sip,
                                    TargetTables targetTables,
                                    boolean majorComplete,
                                    byte[] resumeKey,
                                    int emptyReadFactor) {
        this(opCode,
             sip.getParentKeyBytes(),
             targetTables,
             sip.getPartitionDirection(),
             sip.getSubRange(),
             majorComplete,
             sip.getBatchSize(),
             resumeKey,
             sip.getMaxReadKB(),
             emptyReadFactor);
    }

    /*
     * Internal use constructor that avoids StoreIteratorParams
     * construction.
     */
    protected TableIterateOperation(OpCode opCode,
                                    byte[] parentKeyBytes,
                                    TargetTables targetTables,
                                    Direction direction,
                                    KeyRange range,
                                    boolean majorComplete,
                                    int batchSize,
                                    byte[] resumeKey,
                                    int maxReadKB,
                                    int emptyReadFactor) {
        super(opCode, parentKeyBytes, targetTables, range, emptyReadFactor);
        checkNull("direction", direction);
        this.majorComplete = majorComplete;
        this.direction = direction;
        this.batchSize = batchSize;
        this.resumeKey = resumeKey;
        this.maxReadKB = maxReadKB;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    TableIterateOperation(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);
        majorComplete = in.readBoolean();
        direction = Direction.readFastExternal(in, serialVersion);

        int tmpBatchSize = in.readInt();
        maxReadKB = (serialVersion >= MAXKB_ITERATE_VERSION)? in.readInt() : 0;
        resumeKey = readByteArrayOldShortLength(in, serialVersion);

        /*
         * When doing a scan that includes the parent key the parent is handled
         * separately from the descendants. The parent key does not make a
         * valid resume key, so if the batch size is 1, increase it to ensure
         * that the parent key is not the resume key. This is mostly not a
         * problem for table scans, but it does not hurt.
         */
        if (getResumeKey() == null && tmpBatchSize == 1) {
            batchSize = 2;
        } else {
            batchSize = tmpBatchSize;
        }

    }
    /**
     * Writes this object to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and greater:
     * <ol>
     * <li> ({@link MultiTableOperation}) {@code super}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getMajorComplete
     *      majorComplete}
     * <li> ({@link Direction}) {@link #getDirection direction}
     * <li> ({@link DataOutput#writeInt int}) {@link #getBatchSize batchSize}
     * <li> ({@link SerializationUtil#writeByteArray byte array}) {@link
     *      #getResumeKey resumeKey}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        out.writeBoolean(majorComplete);
        direction.writeFastExternal(out, serialVersion);
        out.writeInt(batchSize);
        if (serialVersion >= MAXKB_ITERATE_VERSION) {
            out.writeInt(maxReadKB);
        }

        writeByteArrayOldShortLength(out, serialVersion, resumeKey);
    }

    Direction getDirection() {
        return direction;
    }

    int getBatchSize() {
        return batchSize;
    }

    int getMaxReadKB() {
        return maxReadKB;
    }

    byte[] getResumeKey() {
        return resumeKey;
    }

    boolean getMajorComplete() {
        return majorComplete;
    }
}

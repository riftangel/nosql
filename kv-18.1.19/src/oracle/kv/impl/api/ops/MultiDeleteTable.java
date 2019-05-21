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

import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import oracle.kv.KeyRange;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.util.SerialVersion;

/**
 * A multi-delete table operation over table(s) in the same partition.
 * This code is shared between normal client multiDelete operations and
 * table data removal code which is internal to RepNodes.  In the latter
 * case there are options that don't apply to client operations:
 * <ol>
 * <li> batch size is allowed
 * <li> resume key used for batching (returned by a single iteration instance)
 * <li> major key may be incomplete which is not possible in the client
 * operation
 * </ol>
 * This state is added to the object but is only set and used by a separate
 * direct object constructor.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class MultiDeleteTable extends MultiTableOperation {

    private final byte[] resumeKey;
    private final boolean majorPathComplete;
    private final int batchSize;
    private final int maxWriteKB;

    /*
     * This is only used on the server side by table data removal to track the
     * last key deleted in order to use it as the resumeKey for batch deletes.
     */
    private transient byte[] lastDeleted;

    /**
     * Construct a multi-delete operation, used by client.
     */
    public MultiDeleteTable(byte[] parentKey,
                            TargetTables targetTables,
                            KeyRange subRange) {
        this(parentKey, targetTables, subRange, null, 0);
    }

    /**
     * Constructs a multi-delete operation, used by client.
     */
    public MultiDeleteTable(byte[] parentKey,
                            TargetTables targetTables,
                            KeyRange subRange,
                            byte[] resumeKey,
                            int maxWriteKB) {
        super(OpCode.MULTI_DELETE_TABLE, parentKey, targetTables, subRange);
        this.resumeKey = resumeKey;
        this.maxWriteKB = maxWriteKB;
        this.majorPathComplete = true;
        this.batchSize = 0;
    }

    /**
     * FastExternalizable constructor.
     */
    protected MultiDeleteTable(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_DELETE_TABLE, in, serialVersion);

        this.majorPathComplete = true;
        this.batchSize = 0;
        if (serialVersion >= SerialVersion.MULTIDELTBL_WRITEKB_RESUMEKEY) {
            this.resumeKey = readByteArray(in);
            this.maxWriteKB = in.readInt();
        } else {
            this.resumeKey = null;
            this.maxWriteKB = 0;
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        if (serialVersion >= SerialVersion.MULTIDELTBL_WRITEKB_RESUMEKEY) {
            writeByteArray(out, resumeKey);
            out.writeInt(maxWriteKB);
        }
    }

    /**
     * Construct a MultiDeleteTable operation for internal use by table data
     * removal.  This constructor requires only a single key and target
     * table but also requires batchSize, resumeKey, and majorPathComplete
     * state.  KeyRange does not apply.  Note: KeyRange might
     * be used by a more general-purpose delete mechanism if ever exposed.
     */
    public MultiDeleteTable(byte[] parentKey,
                            long targetTableId,
                            boolean majorPathComplete,
                            int batchSize,
                            byte[] resumeKey) {
        super(OpCode.MULTI_DELETE_TABLE, parentKey,
              new TargetTables(targetTableId), null);
        this.majorPathComplete = majorPathComplete;
        this.batchSize = batchSize;
        this.resumeKey = resumeKey;
        this.maxWriteKB = 0;
    }

    byte[] getResumeKey() {
        return resumeKey;
    }

    boolean getMajorPathComplete() {
        return majorPathComplete;
    }

    int getBatchSize() {
        return batchSize;
    }

    public byte[] getLastDeleted() {
        return lastDeleted;
    }

    void setLastDeleted(byte[] lastDeleted) {
        this.lastDeleted = lastDeleted;
    }

    int getMaxWriteKB() {
        return maxWriteKB;
    }
}

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

package oracle.kv.impl.pubsub;

import oracle.kv.pubsub.StreamOperation;
import oracle.kv.table.Row;

/**
 * Object represents a put operation in NoSQL Stream
 */
public class StreamPutEvent implements StreamOperation.PutEvent {

    private final SequenceId sequenceId;

    private final int repGroupId;

    private final Row row;

    /**
     * Constructs a put operation
     *
     * @param row         row of put
     * @param sequenceId  unique sequence id
     * @param repGroupId  shard id of the deletion
     */
    StreamPutEvent(Row row, SequenceId sequenceId, int repGroupId) {
        this.row = row;
        this.sequenceId = sequenceId;
        this.repGroupId = repGroupId;
    }

    /**
     * Returns the row associated with the put operation.
     *
     * @return row associated with the put operation.
     */
    @Override
    public Row getRow() {
        return row;
    }

    /**
     * Returns the unique sequence id associated with this operation.
     *
     * @return the unique sequence id associated with this operation.
     */
    @Override
    public SequenceId getSequenceId() {
        return sequenceId;
    }

    /**
     * Returns the shard id of this operation
     *
     * @return the shard id of this operation
     */
    @Override
    public int getRepGroupId() {
        return repGroupId;
    }

    /**
     * Returns {@link oracle.kv.pubsub.StreamOperation.Type#PUT}.
     */
    @Override
    public Type getType() {
        return Type.PUT;
    }

    /**
     * Returns this operation.
     */
    @Override
    public PutEvent asPut() {
        return this;
    }

    /**
     * Throws IllegalArgumentException.
     */
    @Override
    public DeleteEvent asDelete() {
        throw new IllegalArgumentException("This operation is not a delete");
    }

    @Override
    public String toString() {
        return "PUT OP [seq: " + ((StreamSequenceId)sequenceId).getSequence() +
               ", shard id: " + repGroupId +
               ", row: " + (row == null ? "null" : row.toJsonString(true)) +
               "]";
    }
}

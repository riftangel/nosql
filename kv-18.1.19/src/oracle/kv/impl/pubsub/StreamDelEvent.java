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
import oracle.kv.table.PrimaryKey;

/**
 * Object represents a delete operation in NoSQL Stream
 */
public class StreamDelEvent implements StreamOperation.DeleteEvent {

    private final PrimaryKey key;

    private final SequenceId sequenceId;

    private final int repGroupId;

    /**
     * Constructs a delete operation
     *
     * @param key         primary key of the deleted row
     * @param sequenceId  unique sequence id
     * @param repGroupId  shard id of the deletion
     */
    StreamDelEvent(PrimaryKey key, SequenceId sequenceId, int repGroupId) {
        this.key = key;
        this.sequenceId = sequenceId;
        this.repGroupId = repGroupId;
    }

    /**
     * Returns the key associated with the delete operation.
     *
     * @return  the key associated with the delete operation.
     */
    @Override
    public PrimaryKey getPrimaryKey() {
        return key;
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
     * Returns {@link oracle.kv.pubsub.StreamOperation.Type#DELETE}.
     */
    @Override
    public Type getType() {
        return Type.DELETE;
    }

    /**
     * Throws IllegalArgumentException.
     */
    @Override
    public PutEvent asPut() {
        throw new IllegalArgumentException("This operation is not a put");
    }

    /**
     * Returns this operation.
     */
    @Override
    public DeleteEvent asDelete() {
        return this;
    }

    @Override
    public String toString() {
        return "Del OP [seq: " + ((StreamSequenceId)sequenceId).getSequence() +
               ", shard id: " + repGroupId +
               ", primary key: " +
               (key == null ? "null" : key.toJsonString(true)) + "]";
    }

}

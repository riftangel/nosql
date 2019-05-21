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

package oracle.kv.pubsub;

import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

/**
 * The operation (Put, Delete) that was delivered over the NoSQL stream.
 */
public interface StreamOperation {

    /**
     * Returns the unique sequence id associated with this operation.
     */
    SequenceId getSequenceId();

    /**
     * Returns the shard id associated with this operation.
     */
    int getRepGroupId();

    /**
     * The type of the operation.
     */
    enum Type {
        /**
         * A {@link PutEvent} operation.
         */
        PUT,

        /**
         * A {@link DeleteEvent} operation.
         */
        DELETE
    }

    /**
     * Returns the type of this operation.
     *
     * @return the type of this operation
     */
    Type getType();

    /**
     * Converts this operation to a {@link PutEvent}.
     *
     * @return this operation as a Put
     * @throws IllegalArgumentException if this operation is not a Put
     */
    PutEvent asPut();

    /**
     * Converts this operation to a {@link DeleteEvent}.
     *
     * @return this operation as a Delete
     * @throws IllegalArgumentException if this operation is not a Delete
     */
    DeleteEvent asDelete();

    /**
     * Used to signal a Put operation
     */
    interface PutEvent extends StreamOperation {

        /**
         * Returns the Row associated with the put operation.
         * <p>
         * Note that TTL information can be obtained from the Row.
         */
        Row getRow();
    }

    /**
     * Used to signal a Delete operation
     */
    interface DeleteEvent extends StreamOperation {
        /**
         * Returns the primary key associated with the delete operation.
         */
        PrimaryKey getPrimaryKey();
    }

    /**
     * A SequenceId uniquely identifies a stream operation associated with a
     * Publisher.
     * <p>
     * It can also be used to sequence operations on the same key. Note that
     * subscription API provides no guarantees about the order of operations
     * beyond the single key. That is, only for any single key, the subscription
     * API guarantees the order of events on that particular key received by
     * subscriber is the same order these operations applied in NoSQL DB.
     * Therefore it is the application's responsibility to ensure that the
     * comparison is only used in the context of the same key, since the key
     * is not part of its state.
     * <p>
     * If compareTo is called to compare the sequenceId of two instances of
     * different implementing classes, ClassCastException will be thrown
     * because the sequenceIds from different classes are not directly
     * comparable.
     */
    interface SequenceId extends Comparable<SequenceId> {

        /*
         * Implementation Note: SequenceId is simply an encapsulation of the
         * Store VLSN at the mean time. The sequence id may extend to cross
         * multiple shards in future when pub/sub API is adaptive to elastic
         * operations.
         */

        /**
         * Returns a byte representation suitable for saving in some other
         * data source. The byte array representation can also be used as a for
         * comparisons.
         */
        byte[] getBytes();
    }
}

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

import java.util.Arrays;

import com.sleepycat.je.utilint.VLSN;

/**
 * Object to represent an operation from source kvstore via replication
 * stream. Each entry is reconstructed from a single message in replication
 * stream. Received entries will be queued in a FIFO queue to be processed
 * at granularity of transaction. Currently two types of operations can be
 * constructed from replication stream: 1) a data operation representing a
 * write (e.g., put or delete) operation in kvstore; 2) a transactional
 * operation (e.g, commit or abort). All other type of messages shall be
 * filtered out either by feeder filter or the client-side replication
 * stream callback that consumes incoming messages.
 */
class DataEntry {

    /* type of data entry */
    private final Type type;
    /* vlsn associated with the entry */
    private final VLSN vlsn;
    /* txn id associated with the entry */
    private final long txnId;
    /* key part of the entry */
    private final byte[] key;
    /* value part of the entry, null for deletion */
    private final byte[] value;

    /**
     * Builds a data entry
     *
     * @param type     type of entry
     * @param vlsn     vlsn of operation
     * @param txnId    txn id
     * @param key      key of the entry
     * @param value    value of the entry
     */
    DataEntry(Type type, VLSN vlsn, long txnId, byte[] key, byte[] value) {

        this.type = type;
        this.vlsn = vlsn;
        this.txnId = txnId;
        this.key = key;
        this.value = value;
    }

    /**
     * Gets entry type
     *
     * @return type of entry
     */
    Type getType() {
        return type;
    }

    /**
     * Gets the VLSN associated with the entry
     *
     * @return VLSN of the entry
     */
    VLSN getVLSN(){
        return vlsn;
    }

    /**
     * Gets the TXN id associated with the item if it is transactional
     *
     * @return txn id of the item
     */
    long getTxnID() {
        return txnId;
    }

    /**
     * Gets the key in the item if exists
     *
     * @return key as byte array, null if does not exist
     */
    byte[] getKey() {
        return key;
    }

    /**
     * Gets the value in the item if exists
     *
     * @return value as byte array, null if it does not exist, e.g., in
     * delete operation.
     */
    byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder msg  = new StringBuilder();

        switch(type) {
            case TXN_COMMIT:
                msg.append("txn commit, ")
                   .append("vlsn: ").append(vlsn)
                   .append(", txn id: ").append(txnId);
                break;

            case TXN_ABORT:
                msg.append("txn abort, ")
                   .append("vlsn: ").append(vlsn)
                   .append(", txn id: ").append(txnId);
                break;

            case DELETE:
                msg.append("delete op, ")
                   .append("vlsn: ").append(vlsn)
                   .append(", key:").append(Arrays.toString(getKey()))
                   .append(", txn id: ").append(txnId);
                break;

            case PUT:
                msg.append("put op, ")
                   .append("vlsn: ").append(vlsn)
                   .append(", key:").append(Arrays.toString(getKey()))
                   .append(", txn id: ").append(txnId);
                break;

            default:
                break;
        }
        msg.append("\n");
        return msg.toString();
    }

    /**
     * Type of messages supported in publisher
     */
    enum Type {

        /* txn commit */
        TXN_COMMIT,

        /* txn abort */
        TXN_ABORT,

        /* msg represents a deletion of a key */
        DELETE,

        /* msg represents a put (insert or update) of a key */
        PUT;

        private static final Type[] VALUES = values();

        /**
         * Gets the OP corresponding to the specified ordinal.
         */
        public static Type get(int ordinal) {
            return VALUES[ordinal];
        }
    }
}

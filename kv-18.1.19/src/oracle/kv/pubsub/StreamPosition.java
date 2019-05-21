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

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import oracle.kv.impl.topo.RepGroupId;

import com.sleepycat.je.utilint.VLSN;

/**
 * A StreamPosition represents a position in a subscription stream.
 */
public class StreamPosition implements Serializable {

    private static final long serialVersionUID = 1L;

    /* name of the source kvstore. */
    private final String storeName;

    /*
     * a unique id associated with the KVStore. It, in conjunction with the
     * store name, is used to ensure consistent use of the StreamPosition with
     * respect to its store.
     *
     * Implementation note: using the topology id of the source kvstore.
     */
    private final long storeId;

    /*
     * Stream positions for a set of shards, need be thread safe. Since shard
     * numbers run from 0 to max with no gaps, an array or List of
     * AtomicReference<ShardPosition> would probably be better.
     */
    private final ConcurrentHashMap<Integer,ShardPosition> allShardPos;

    /**
     * @hidden
     *
     * Creates an empty StreamPosition. Subsequent setShardPosition calls,
     * one for each shard in the store establish the store wide stream position.
     *
     * @param storeName name of the store
     * @param storedId the unique id associated with the store
     */
    public StreamPosition(String storeName, long storedId) {
        super();

        if (storeName == null || storeName.isEmpty()) {
            throw new NullPointerException("null or empty store name");
        }

        this.storeName = storeName;
        this.storeId = storedId;

        allShardPos = new ConcurrentHashMap<>();
    }


    /**
     * @hidden
     *
     * Makes a copy a stream position
     *
     * @param pos  stream position to copy
     */
    public StreamPosition(StreamPosition pos) {
        storeName = pos.getStoreName();
        storeId = pos.storeId;

        allShardPos = new ConcurrentHashMap<>();
        for (ShardPosition p : pos.getAllShardPos()) {
            allShardPos.put(p.getRepGroupId(),
                            new ShardPosition(p.getRepGroupId(),
                                              p.getVLSN()));

        }
    }

    /**
     * Gets store name
     *
     * @return store name
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * Gets store id
     *
     * @return  store id
     */
    public long getStoreId() {
        return storeId;
    }

    /**
     * @hidden
     *
     * Gets a ShardPosition pertaining to the specific shard.
     *
     * @param shardId the shard ID
     *
     * @return a StreamPosition pertaining to the shard id, or null if
     * StreamPosition does not contain the a ShardPosition pertaining to the
     * shard id.
     */
    public ShardPosition getShardPosition(int shardId) {
        return allShardPos.get(shardId);
    }

    /**
     * @hidden
     *
     * Sets StreamPosition pertaining to the specific shard.
     *
     * @param shardId id of shard
     * @param vlsn    vlsn position to update
     */
    public void setShardPosition(int shardId, VLSN vlsn) {

        final ShardPosition curr = allShardPos.get(shardId);
        if (curr == null) {
            allShardPos.put(shardId, new ShardPosition(shardId, vlsn));
            return;
        }

        curr.setVlsn(vlsn);
    }

    /**
     * @hidden
     *
     * Internal use in test only
     *
     * Gets all shard positions
     *
     * @return a map with all shard positions
     */
    public Collection<ShardPosition> getAllShardPos() {
        return allShardPos.values();
    }

    /**
     * @hidden
     *
     * Creates an init stream position from very beginning for each shard
     *
     * @param storeName    name of kvstore
     * @param storeID      id of kvstore
     * @param shards       set of shard ids
     *
     * @return stream position from the beginning
     */
    public static StreamPosition getInitStreamPos(String storeName,
                                                  long storeID,
                                                  Set<RepGroupId> shards) {
        final StreamPosition initPos = new StreamPosition(storeName, storeID);
        for (RepGroupId id : shards) {
            initPos.setShardPosition(id.getGroupId(), VLSN.FIRST_VLSN);
        }
        return initPos;
    }

    /**
     * @hidden
     *
     * Returns a new instance for the next stream position.
     */
    public synchronized StreamPosition nextStreamPosition() {
        final StreamPosition ret = new StreamPosition(this);

        for (ShardPosition p : ret.getAllShardPos()) {
            p.setVlsn(p.getVLSN().getNext());
        }
        return ret;
    }

    /**
     * @hidden
     *
     * Returns a new instance for the previous stream position.
     */
    public synchronized StreamPosition prevStreamPosition() {
        final StreamPosition ret = new StreamPosition(this);

        for (ShardPosition p : ret.getAllShardPos()) {
            p.setVlsn(p.getVLSN().getPrev());
        }
        return ret;
    }

    /**
     * @hidden
     *
     * In unit test only, returns true if stream positions matches
     */
    public boolean match(StreamPosition o) {

        if (!storeName.equals(o.getStoreName())) {
            return false;
        }

        if (!(storeId == o.getStoreId())) {
            return false;
        }

        if (allShardPos.size() != o.getAllShardPos().size()) {
            return false;
        }

        for (ShardPosition pos : allShardPos.values()) {
            if (!pos.equals(o.getShardPosition(pos.getRepGroupId()))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return "{" + storeName + "(id=" + storeId + "): " +
               allShardPos.values() + "}";
    }

    /**
     * @hidden
     *
     * Represents the shard position. The set of all shard positions for the
     * NoSQL store constitutes the NoSQL stream position.
     */
    public static class ShardPosition implements Serializable {
        private static final long serialVersionUID = 1L;

        /* shard id */
        private final int gid;

        /* shard vlsn */
        private volatile VLSN vlsn;

        ShardPosition(int gid, VLSN vlsn) {

            super();
            this.gid = gid;
            this.vlsn = vlsn;
        }

        public int getRepGroupId() {
            return gid;
        }

        public VLSN getVLSN() {
            return vlsn;
        }

        public void setVlsn(VLSN vlsn) {
            this.vlsn = vlsn;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + gid;
            result = 31 * result + vlsn.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || !(other instanceof ShardPosition)) {
                return false;
            }
            final ShardPosition o = (ShardPosition)other;
            return (gid == o.gid) && vlsn.equals(o.vlsn);
        }

        @Override
        public String toString() {
            return "rg" + gid + "(vlsn=" + vlsn + ")";
        }
    }
}

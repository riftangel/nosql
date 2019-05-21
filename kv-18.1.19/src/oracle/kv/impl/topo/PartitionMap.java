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

package oracle.kv.impl.topo;

import java.io.DataInput;
import java.io.IOException;

import oracle.kv.impl.map.HashKeyToPartitionMap;
import oracle.kv.impl.topo.ResourceId.ResourceType;

import com.sleepycat.persist.model.Persistent;

/**
 * @see #writeFastExternal FastExternalizable format
 */
@Persistent
public class PartitionMap extends
    ComponentMap<PartitionId, Partition> {

    private static final long serialVersionUID = 1L;

    transient HashKeyToPartitionMap keyToPartitionMap;

    public PartitionMap(Topology topology) {
        super(topology);
    }

    PartitionMap(Topology topology, DataInput in, short serialVersion)
        throws IOException {

        super(topology, in, serialVersion);
    }

    @SuppressWarnings("unused")
    private PartitionMap() {
        super();
    }

    /**
     * Returns the number of partitions in the partition map.
     */
    public int getNPartitions() {
        return cmap.size();
    }

    /**
     * Returns the partition id for the environment that contains the
     * replicated partition database associated with the given key.
     *
     * @param keyBytes the key used to identify the partition.
     *
     * @return the partition id that contains the key.
     */
    PartitionId getPartitionId(byte[] keyBytes) {
        if ((keyToPartitionMap == null) ||
            (keyToPartitionMap.getNPartitions() != size())) {
            /* Initialize transient field on demand. */
            keyToPartitionMap = new HashKeyToPartitionMap(size());
        }
        return keyToPartitionMap.getPartitionId(keyBytes);
    }

    /**
     * Returns the rep group id for the environment that contains the
     * replicated partition database associated with the given partition. If
     * the partition is not present null is returned. This may be due to the
     * map not being initialized before this method is called.
     *
     * @param partitionId the partitionId.
     *
     * @return the id of the RepGroup that contains the partition or null
     */
    public RepGroupId getRepGroupId(PartitionId partitionId) {
        final Partition p = cmap.get(partitionId);
        return (p == null) ? null : p.getRepGroupId();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#nextId()
     */
    @Override
    PartitionId nextId() {
        return new PartitionId(nextSequence());
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#getResourceType()
     */
    @Override
    ResourceType getResourceType() {
        return ResourceType.PARTITION;
    }

    @Override
    Class<Partition> getComponentClass() {
        return Partition.class;
    }
}

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

import oracle.kv.impl.topo.ResourceId.ResourceType;

import com.sleepycat.persist.model.Persistent;

/**
 * Defines the RepGroup internal map used collect its arb nodes
 *
 * @see #writeFastExternal FastExternalizable format
 */
@Persistent
public class ArbNodeComponentMap extends ComponentMap<ArbNodeId, ArbNode> {

    private static final long serialVersionUID = 1L;

    /* The owning rep group. */
    private final RepGroup repGroup;

    public ArbNodeComponentMap(RepGroup repGroup, Topology topology) {
        super(topology);
        this.repGroup = repGroup;
    }

    ArbNodeComponentMap(RepGroup repGroup,
                        Topology topology,
                        DataInput in,
                        short serialVersion)
        throws IOException {

        super(topology, in, serialVersion);
        this.repGroup = repGroup;
    }

    /**
     * Empty constructor to satisfy DPL. Though this class is never persisted,
     * because it is referenced from existing persistent classes it must be
     * annotated and define an empty constructor.
     */
    @SuppressWarnings("unused")
    private ArbNodeComponentMap() {
        throw new IllegalStateException("Should not be invoked");
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#nextId()
     */
    @Override
    ArbNodeId nextId() {
        return  new ArbNodeId(this.repGroup.getResourceId().getGroupId(),
                              nextSequence());
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#getResourceType()
     */
    @Override
    ResourceType getResourceType() {
       return ResourceType.ARB_NODE;
    }

    @Override
    Class<ArbNode> getComponentClass() {
        return ArbNode.class;
    }
}

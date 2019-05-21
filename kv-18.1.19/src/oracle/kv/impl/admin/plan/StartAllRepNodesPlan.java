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

package oracle.kv.impl.admin.plan;

import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Restart all RepNodes in the store.
 */
@Persistent
public class StartAllRepNodesPlan extends StartRepNodesPlan {

    private static final long serialVersionUID = 1L;

    StartAllRepNodesPlan(String name,
                         Planner planner,
                         Topology topology) {
        super(name, planner, topology, topology.getRepNodeIds());
    }
    /* DPL */
    protected StartAllRepNodesPlan() {
    }

    @Override
    public boolean isExclusive() {
        return false;
    }
}

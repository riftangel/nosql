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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class for methods related to {@link Topology}.
 */
public class TopologyUtil {

    /**
     * Return a map of replication groups to partitions Ids.
     */
    public static Map<RepGroupId, List<PartitionId>>
        getRGIdPartMap(Topology topology) {

        final Map<RepGroupId, List<PartitionId>> map =
            new HashMap<RepGroupId, List<PartitionId>>();

        for (Partition p : topology.getPartitionMap().getAll()) {
            List<PartitionId> list = map.get(p.getRepGroupId());

            if (list == null) {
                list = new ArrayList<PartitionId>();
                map.put(p.getRepGroupId(), list);
            }

            list.add(p.getResourceId());
        }

        return map;
    }

    /**
     * Returns the number of repNodes can be used for read operations.
     */
    public static int getNumRepNodesForRead(Topology topology,
                                            int[] readZoneIds) {
        final List<Integer> readZoneIdsLst;
        if (readZoneIds != null) {
            readZoneIdsLst = new ArrayList<Integer>(readZoneIds.length);
            for (int id : readZoneIds) {
                readZoneIdsLst.add(id);
            }
        } else {
            readZoneIdsLst = null;
        }

        final Collection<Datacenter> datacenters =
            topology.getDatacenterMap().getAll();
        int num = 0;
        for (Datacenter dc: datacenters) {
            if (readZoneIdsLst != null) {
                final int dcId = dc.getResourceId().getDatacenterId();
                if (!readZoneIdsLst.contains(dcId)) {
                    continue;
                }
            }
            num += dc.getRepFactor();
        }
        final int nShards = topology.getRepGroupMap().size();
        return num * nShards;
    }
}

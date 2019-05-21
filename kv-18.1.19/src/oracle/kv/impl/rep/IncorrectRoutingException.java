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

package oracle.kv.impl.rep;

import oracle.kv.impl.topo.PartitionId;

/**
 * Exception to indicating that a key based operation was routed to a RepNode
 * that does not own the key. This exception typically indicates that the
 * rep node making the request has a Topology definition that's obsolete
 * and needs to be updated.
 */
public class IncorrectRoutingException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final PartitionId partitionId;

    public IncorrectRoutingException(String message,
                                     PartitionId partitionId) {
        super(message);
        this.partitionId = partitionId;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }
}

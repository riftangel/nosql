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

package oracle.kv.impl.sna;

import java.io.Serializable;

import oracle.kv.KVVersion;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * StorageNodeStatus represents the current status of a running StorageNode.
 * It includes ServiceStatus and Version.
 */
public class StorageNodeStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Used during testing: A non-null value overrides the current version. */
    private static volatile KVVersion testCurrentKvVersion;

    private final ServiceStatus status;
    private final KVVersion kvVersion;

    public StorageNodeStatus(ServiceStatus status) {
        this.status = status;
        this.kvVersion = getCurrentKVVersion();

        /* Initialize release info */
        this.kvVersion.getReleaseDate();
    }

    /** Gets the current version. */
    private static KVVersion getCurrentKVVersion() {
        return (testCurrentKvVersion != null) ?
            testCurrentKvVersion :
            KVVersion.CURRENT_VERSION;
    }

    /**
     * Set the current version to a different value, for testing.  Specifying
     * {@code null} reverts to the standard value.
     */
    public static void setTestKVVersion(final KVVersion testKvVersion) {
        testCurrentKvVersion = testKvVersion;
    }

    public ServiceStatus getServiceStatus() {
        return status;
    }

    public KVVersion getKVVersion() {
        return kvVersion;
    }

    @Override
    public String toString() {
        return status + "," + status;
    }
}

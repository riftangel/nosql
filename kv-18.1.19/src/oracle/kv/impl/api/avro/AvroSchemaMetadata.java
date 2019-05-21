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

package oracle.kv.impl.api.avro;

import java.io.Serializable;

/**
 * Holds the metadata that is stored along with a schema, including the schema
 * status that is part of the key.
 *
 * This serializable class is used in the admin CommandService RMI interface.
 * Fields may be added without adding new remote methods.
 */
public class AvroSchemaMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AvroSchemaStatus status;
    private final long timeModified;
    private final String byUser;
    private final String fromMachine;

    public AvroSchemaMetadata(AvroSchemaStatus status,
                              long timeModified,
                              String byUser,
                              String fromMachine) {
        this.status = status;
        this.timeModified = timeModified;
        this.byUser = byUser;
        this.fromMachine = fromMachine;
    }
    
    public AvroSchemaStatus getStatus() {
        return status;
    }

    public long getTimeModified() {
        return timeModified;
    }
    
    public String getByUser() {
        return byUser;
    }

    public String getFromMachine() {
        return fromMachine;
    }
}

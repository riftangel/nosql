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

package oracle.kv.impl.admin.param;

import java.io.Serializable;

import oracle.kv.impl.topo.DatacenterId;

import com.sleepycat.persist.model.Persistent;

/**
 * Datacenter attributes.
 */
@Persistent
public class DatacenterParams implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    
    /* @deprecated as of R2 */
    @SuppressWarnings("unused")
    @Deprecated
    private String comment;
    private DatacenterId datacenterId;

    /* No-arg constructor for use by DPL */
    public DatacenterParams() {
    }

    public DatacenterParams(DatacenterId id, String name) {
        this.datacenterId = id;
        this.name = name;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the datacenterId
     */
    public DatacenterId getDatacenterId() {
        return datacenterId;
    }

    /**
     * @return a String value
     */
    @Override
	public String toString() {
        String result = name + " Id=" + datacenterId;
        return result;
    }
}

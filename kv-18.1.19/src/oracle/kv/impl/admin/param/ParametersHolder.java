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

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * The Entity that leads to a Parameters object. We need this indirection so
 * that Parameters can be referenced from other persistent objects like Plans.
 */
@Entity
public class ParametersHolder {

    private final static String PARAMETERS_KEY = "Parameters";

    @PrimaryKey
    private final String topologyKey = PARAMETERS_KEY;

    private Parameters parameters;

    @SuppressWarnings("unused")
	private ParametersHolder() {
    }

    public ParametersHolder(Parameters parameters) {
        this.parameters = parameters;
    }

    public static String getKey() {
        return PARAMETERS_KEY;
    }

    public Parameters getParameters() {
        return parameters;
    }
}

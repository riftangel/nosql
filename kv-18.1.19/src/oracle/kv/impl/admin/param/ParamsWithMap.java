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

import oracle.kv.impl.param.ParameterMap;

/**
 * Implemented by parameters classes that have an underlying map.
 */
public interface ParamsWithMap {

    /**
     * Returns the parameter map underlying these parameters.
     *
     * @return the underlying parameter map
     */
    ParameterMap getMap();
}

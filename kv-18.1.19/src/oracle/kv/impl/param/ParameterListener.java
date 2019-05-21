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

package oracle.kv.impl.param;

/**
 * This is a simple interface that is implemented by services or portions of
 * services that want to be notified of parameter changes at runtime.
 *
 * See the admin and rep packages for examples of usage.
 */
public interface ParameterListener {

    /**
     * Notify listener of new parameters.
     *
     * @param oldMap the parameters being replaced
     *
     * @param newMap the new parameter values
     */
    public void newParameters(ParameterMap oldMap, ParameterMap newMap);
}

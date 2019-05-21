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

package oracle.kv.table;

/**
 * FloatDef is an extension of {@link FieldDef} to encapsulate the Float type.
 * It adds a minimum and maximum value range and a default value.
 * Minimum and maximum are inclusive.
 *
 * @since 3.0
 */
public interface FloatDef extends FieldDef {

    /**
     * @return the minimum value for the instance if defined, otherwise null
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on Float types. A storage size argument can be specified on
     * a Float type when used in a primary key.
     */
    @Deprecated
    Float getMin();

    /**
     * @return the maximum value for the instance if defined, otherwise null
     *
     * @deprecated as of release 4.0 it is no longer possible to specify
     * ranges on Float types. A storage size argument can be specified on
     * a Float type when used in a primary key.
     */
    @Deprecated
    Float getMax();

    /**
     * @return a deep copy of this object
     */
    @Override
    public FloatDef clone();
}

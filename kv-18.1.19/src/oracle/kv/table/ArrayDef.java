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
 * ArrayDef is an extension of {@link FieldDef} to encapsulate an array
 * of values.  It defines an array of values of the same type.
 *
 * @since 3.0
 */
public interface ArrayDef extends FieldDef {

    /**
     * @return the FieldDef of the array element
     */
    FieldDef getElement();

    /**
     * @return a deep copy of this object
     */
    @Override
    public ArrayDef clone();
}

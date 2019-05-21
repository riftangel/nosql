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

package oracle.kv.impl.query.compiler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.FastExternalizable;

/**
 * The order-by clause, for each sort expression allows for an optional
 * "sort spec", which specifies the relative order of NULLs (less than or
 * greater than all other values) and whether the values returned by the
 * sort expr should be sorted in ascending or descending order.
 *
 * The SortSpec class stores these two pieces of information.
 */
public class SortSpec implements FastExternalizable {

    public final boolean theIsDesc;

    public final boolean theNullsFirst;

    SortSpec(boolean isDesc, boolean nullsFirst) {
        theIsDesc = isDesc;
        theNullsFirst = nullsFirst;
    }

    public SortSpec(
        DataInput in,
        @SuppressWarnings("unused") short serialVersion) throws IOException {
        theIsDesc = in.readBoolean();
        theNullsFirst = in.readBoolean();
    }

    @Override
    public void writeFastExternal(
        DataOutput out,
        short serialVersion)  throws IOException {
        out.writeBoolean(theIsDesc);
        out.writeBoolean(theNullsFirst);
    }
}

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

package oracle.kv.impl.api.ops;

import java.io.DataInput;
import java.io.IOException;

import oracle.kv.Depth;
import oracle.kv.KeyRange;

/**
 * A multi-get operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class MultiGet extends MultiKeyOperation {

    /**
     * Construct a multi-get operation.
     */
    public MultiGet(byte[] parentKey, KeyRange subRange, Depth depth) {
        super(OpCode.MULTI_GET, parentKey, subRange, depth);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiGet(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_GET, in, serialVersion);
    }
}

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
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;

/**
 * A NOP request used internally by the <code>RequestDispatcher</code> to
 * obtain just the "status" information associated with the returned result.
 *
 * @see RepNodeStateUpdateThread
 * @see #writeFastExternal FastExternalizable format
 */
public class NOP extends InternalOperation {

    public NOP() {
        super(OpCode.NOP);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    public NOP(DataInput in, short serialVersion) {
        super(OpCode.NOP, in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link InternalOperation}) {@code super}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }
}

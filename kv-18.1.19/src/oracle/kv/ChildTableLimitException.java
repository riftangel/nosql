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

package oracle.kv;

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerializationUtil;

/**
 * Thrown when an attempt is made to exceeded a table's child limit.
 * 
 * @hiddensee {@link #writeFastExternal FastExternalizable format}
 */
public class ChildTableLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    private final int childTableLimit;

    /**
     * Constructs an instance of <code>ChildTableLimitException</code>
     * with the specified table, child limit, and detail message.
     *
     * @param tableName the table name
     * @param childTableLimit the child table limit
     * @param msg the detail message
     * 
     * @hidden For internal use only
     */
    public ChildTableLimitException(String tableName,
                                    int childTableLimit,
                                    String msg) {
        super(tableName, msg);
        assert tableName != null;
        this.childTableLimit = childTableLimit;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public ChildTableLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        childTableLimit = readPackedInt(in);
    }

    /**
     * Gets the child table limit at the time of the exception.
     *
     * @return the child table limit
     * 
     * @hidden For internal use only
     */
    public int getChildTableLimit() {
        return childTableLimit;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceLimitException}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getChildTableLimit childTableLimit}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, childTableLimit);
    }
}

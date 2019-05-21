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
 * Thrown when an attempt is made to exceeded a table's index limit.
 * 
 * @hiddensee {@link #writeFastExternal FastExternalizable format}
 */
public class IndexLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    private final int indexLimit;

    /**
     * Constructs an instance of <code>IndexLimitExceededException</code> with
     * the specified table, index limit, and detail message.
     *
     * @param tableName the table name
     * @param indexLimit the index limit
     * @param msg the detail message
     * 
     * @hidden For internal use only
     */
    public IndexLimitException(String tableName, int indexLimit, String msg) {
        super(tableName, msg);
        assert tableName != null;
        this.indexLimit = indexLimit;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public IndexLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        indexLimit = readPackedInt(in);
    }

    /**
     * Gets the index limit at the time of the exception.
     *
     * @return the child table limit
     * 
     * @hidden For internal use only
     */
    public int getIndexLimit() {
        return indexLimit;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceLimitException}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getIndexLimit indexLimit}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, indexLimit);
    }
}

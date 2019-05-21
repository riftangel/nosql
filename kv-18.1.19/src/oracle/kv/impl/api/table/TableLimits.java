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

package oracle.kv.impl.api.table;

import java.io.IOException;
import java.io.Serializable;

import oracle.kv.KVVersion;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Container for table limits.
 *
 */
public class TableLimits implements Serializable {

    private static final long serialVersionUID = 1L;

    /* KV Version where limits was introduced */
    public static final KVVersion TABLE_LIMITS_VERSION = KVVersion.R18_1;

    /* Initial implementation */
    private static final int V1 = 1;
    
    private static final int CURRENT_VERSION = V1;

    /* Schema version of this instance */
    private final int version = CURRENT_VERSION;

    /**
     * Limit value to indicate that no limit is to be enforced
     */
    public static final int NO_LIMIT = Integer.MAX_VALUE;

    /*
     * Limit value to indicate that a limit is not to be changed
     */
    public static final int NO_CHANGE = -1;

    /**
     * TableLimit instance to set a table read-only.
     */
    public static TableLimits READ_ONLY =
                        new TableLimits(NO_CHANGE, 0, NO_CHANGE);

    /**
     * TableLimit instance to disable all read and write accesses to a table
     */
    public static TableLimits NO_ACCESS = new TableLimits(0, 0, NO_CHANGE);

    /**
     * The limit values. A value less than NO_LIMIT indicates that a limit
     * is present and should be enforced. A value of NO_CHANGE indicates that
     * that limit should not be changed. Limit objects with NO_CHANGE values
     * are used to set values through the API and should not be used for
     * enforcement before init() is called.
     */

    /* KB/sec */
    private int readLimit;
    private int writeLimit;
    /* GB */
    private int sizeLimit;
    /* # of indexes */
    private int indexLimit;
    /* # of child tables */
    private int childTableLimit;
    /* index key size limit in bytes */
    private int indexKeySizeLimit;

    /**
     * Constructs a table limit object. The read and write limits are specified
     * in KB/second. Size limit is in GB. A value of NO_LIMIT indicates that
     * no limit is enforced. A value of NO_CHANGE will indicate that a limit
     * is not changed when this object is applied to an existing table with
     * limits.
     *
     * @param readLimit the read limit in KB/second
     * @param writeLimit the write limit in KB/second
     * @param sizeLimit the table size limit in GB
     * @param maxIndexes the maximum number of indexes
     * @param maxChildren the maximum number child tables
     * @param indexKeySizeLimit the maximum size of an index key in bytes
     */
    public TableLimits(int readLimit, int writeLimit,
                       int sizeLimit, int maxIndexes,
                       int maxChildren, int indexKeySizeLimit) {
        this.readLimit = readLimit;
        this.writeLimit = writeLimit;
        this.sizeLimit = sizeLimit;
        this.indexLimit = maxIndexes;
        this.childTableLimit = maxChildren;
        this.indexKeySizeLimit = indexKeySizeLimit;
    }

    /** TODO - Temporary until cloud updates to new constructor */
    @Deprecated
    public TableLimits(int readLimit, int writeLimit,
                       int sizeLimit, int maxIndexes,
                       int maxChildren) {
        this(readLimit, writeLimit, sizeLimit,
             maxIndexes, maxChildren, NO_LIMIT);
    }

    /**
     * A convenience constructor for a table limit object with only the
     * read, write, and size limits. Limits on indexes, child
     * tables, or index key size are not changed.
     *
     * @param readLimit the read limit in KB/second
     * @param writeLimit the write limit in KB/second
     * @param sizeLimit the table size limit in GB
     */
    public TableLimits(int readLimit, int writeLimit, int sizeLimit) {
        this(readLimit, writeLimit, sizeLimit, NO_CHANGE, NO_CHANGE, NO_CHANGE);
    }

    /*
     * Initializes this instance. If any values have not been set (-1)
     * they are initialized to Integer.MAX_VALUE or the value from the old
     * limits if non-null.
     */
    void init(TableLimits oldLimits) {
        if (readLimit < 0) {
            readLimit = (oldLimits == null) ? NO_LIMIT :
                                              oldLimits.getReadLimit();
        }
        if (writeLimit < 0) {
            writeLimit = (oldLimits == null) ? NO_LIMIT :
                                               oldLimits.getWriteLimit();
        }
        if (sizeLimit < 0) {
            sizeLimit = (oldLimits == null) ? NO_LIMIT :
                                              oldLimits.getSizeLimit();
        }
        if (indexLimit < 0) {
            indexLimit = (oldLimits == null) ? NO_LIMIT :
                                               oldLimits.getIndexLimit();
        }
        if (childTableLimit < 0) {
            childTableLimit = (oldLimits == null) ? NO_LIMIT :
                                                 oldLimits.getChildTableLimit();
        }
        if (indexKeySizeLimit < 0) {
            indexKeySizeLimit = (oldLimits == null) ? NO_LIMIT :
                                               oldLimits.getIndexKeySizeLimit();
        }
    }

    /**
     * Returns true if this any limits need to be enforced.
     * @return true if this any limits need to be enforced
     */
    public boolean hasLimits() {
        return hasThroughputLimits() ||
               hasSizeLimit() ||
               hasIndexLimit() ||
               hasChildTableLimit() ||
               hasIndexKeySizeLimit();
    }

    /* -- Throughput Limits -- */

    /**
     * Gets the read throughput limit.
     *
     * @return the read throughput limit
     */
    public int getReadLimit() {
        assert readLimit >= 0;
        return readLimit;
    }

    /**
     * Gets the write throughput limit.
     *
     * @return the write throughput limit
     */
    public int getWriteLimit() {
        assert writeLimit >= 0;
        return writeLimit;
    }

    public boolean isReadAllowed() {
        return readLimit > 0;
    }

    public boolean isWriteAllowed() {
        return writeLimit > 0;
    }

    /**
     * Returns true if either read or write throughput limits are set..
     *
     * @return true if either read or write throughput limits are set
     */
    public boolean hasThroughputLimits() {
        assert readLimit >= 0;
        assert writeLimit >= 0;
        return readLimit < Integer.MAX_VALUE ||
               writeLimit < Integer.MAX_VALUE;
    }

    /**
     * Returns true if either of the specified read or write rates have
     * exceeded the corresponding limits.
     *
     * @return true if either of the read or write limits have been exceeded.
     */
    public boolean throughputExceeded(long localReadRate, long localWriteRate) {
        assert readLimit >= 0;
        assert writeLimit >= 0;
        return localReadRate > readLimit || localWriteRate > writeLimit;
    }

    /* -- Size Limit -- */

    public boolean hasSizeLimit() {
        assert sizeLimit >= 0;
        return sizeLimit < Integer.MAX_VALUE;
    }

    /**
     * Gets the size limit.
     *
     * @return the size limit
     */
    public int getSizeLimit() {
        assert sizeLimit >= 0;
        return sizeLimit;
    }

    /* -- Index Limit -- */

    boolean hasIndexLimit() {
        assert indexLimit >= 0;
        return indexLimit < Integer.MAX_VALUE;
    }

    int getIndexLimit() {
        assert indexLimit >= 0;
        return indexLimit;
    }

    /* -- Child Table Limit -- */

    boolean hasChildTableLimit() {
        assert childTableLimit >= 0;
        return childTableLimit < Integer.MAX_VALUE;
    }

    int getChildTableLimit() {
        assert childTableLimit >= 0;
        return childTableLimit;
    }

    /* -- Index Key Size Limit -- */

    public boolean hasIndexKeySizeLimit() {
        assert indexKeySizeLimit >= 0;
        return indexKeySizeLimit < Integer.MAX_VALUE;
    }

    public int getIndexKeySizeLimit() {
        assert indexKeySizeLimit >= 0;
        return indexKeySizeLimit;
    }

    void putLimits(ObjectNode node) {
        if (!hasLimits()) {
            return;
        }
        final ArrayNode array = node.putArray("limits");
        final ObjectNode fnode = array.addObject();
        if (hasThroughputLimits()) {
            fnode.put("readLimit", readLimit);
            fnode.put("writeLimit", writeLimit);
        }
        if (hasSizeLimit()) {
            fnode.put("sizeLimit", sizeLimit);
        }
        if (hasIndexLimit()) {
            fnode.put("indexLimit", indexLimit);
        }
        if (hasChildTableLimit()) {
            fnode.put("childTableLimit", childTableLimit);
        }
        if (hasIndexKeySizeLimit()) {
            fnode.put("indexKeySizeLimit", indexKeySizeLimit);
        }
    }

    @SuppressWarnings("unused")
    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        /**
         * Checks whether this is a newer version which we don't support. This
         * should not happen as the Admin should prevent sending a new
         * version out until the store has been upgraded.
         * 
         * Since version is final, the Eclipse thinks the throw is dead code,
         * requiring the @SuppressWarnings above.
         */
        if (version > CURRENT_VERSION) {
            throw new IOException("Unknown version: " + version +
                                  ", current version is " + CURRENT_VERSION);
        }
    }

    @Override
    public String toString() {
        return "TableLimits[" + parseLimit(readLimit) + ", " +
                                parseLimit(writeLimit) + ", " +
                                parseLimit(sizeLimit) + ", " +
                                parseLimit(indexLimit) + ", " +
                                parseLimit(childTableLimit) + ", " +
                                parseLimit(indexKeySizeLimit) + "]";
    }

    private String parseLimit(int value) {
        return (value < 0) ? "NO_CHANGE" :
                       (value < Integer.MAX_VALUE ? Integer.toString(value) :
                                                    "NO_LIMIT");
    }
}

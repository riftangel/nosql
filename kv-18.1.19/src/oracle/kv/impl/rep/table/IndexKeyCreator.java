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

package oracle.kv.impl.rep.table;

import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;

import oracle.kv.IndexKeySizeLimitException;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableLimits;

/**
 *
 */
public class IndexKeyCreator implements SecondaryKeyCreator,
                                        SecondaryMultiKeyCreator {

    private volatile IndexImpl index;
    
    /*
     * Index key size limit. If there is no limit, set to Integer.MAX_VALUE
     */
    private volatile int keySizeLimit;
    
    /*
     * Keep this state to make access faster
     */
    private final boolean keyOnly;
    private final boolean isMultiKey;

    IndexKeyCreator(IndexImpl index) {
        setIndex(index);
        this.keyOnly = index.isKeyOnly();
        this.isMultiKey = index.isMultiKey();
    }

    boolean primaryKeyOnly() {
        return keyOnly;
    }

    boolean isMultiKey() {
        return isMultiKey;
    }

    /**
     * Refreshes the index reference. It should be refreshed when the table
     * metadata is updated.
     * 
     * @param newIndex the new index object.
     */
    final void setIndex(IndexImpl newIndex) {
        index = newIndex;
        final TableLimits tl =
                    index.getTableImpl().getTopLevelTable().getTableLimits();
        
        /* Set to Integer.MAX_VALUE if no limit */
        keySizeLimit = ((tl != null) && tl.hasIndexKeySizeLimit()) ?
                                  tl.getIndexKeySizeLimit() : Integer.MAX_VALUE;
    }

    /* -- From SecondaryKeyCreator -- */

    @Override
    public boolean createSecondaryKey(SecondaryDatabase secondaryDb,
                                      DatabaseEntry key,
                                      DatabaseEntry data,
                                      DatabaseEntry result) {
        byte[] res =
            index.extractIndexKey(key.getData(),
                                  (data != null ? data.getData() : null),
                                  keyOnly);
        if (res != null) {
            checkKeySizeLimit(res.length);
            result.setData(res);
            return true;
        }
        return false;
    }

    /* -- From SecondaryMultiKeyCreator -- */

    @Override
    public void createSecondaryKeys(SecondaryDatabase secondaryDb,
                                    DatabaseEntry key,
                                    DatabaseEntry data,
                                    Set<DatabaseEntry> results) {

        /*
         * Ideally we'd pass the results Set to index.extractIndexKeys but
         * IndexImpl is client side as well and DatabaseEntry is not currently
         * in the client classes pulled from JE.  DatabaseEntry is simple, but
         * also references other JE classes that are not client side.  It is a
         * slippery slope.
         *
         * If the extra object allocations show up in profiling then something
         * can be done.
         */
        List<byte[]> res = index.extractIndexKeys(key.getData(),
                                                  data.getData(),
                                                  keyOnly);
        if (res != null) {
            int length = 0;
            for (byte[] bytes : res) {
                length += bytes.length;
                results.add(new DatabaseEntry(bytes));
            }
            checkKeySizeLimit(length);
        }
    }
    
    /**
     * Throws IndexKeySizeLimitException if an index key size limit is set for
     * the index and the key length is greater than the limit.
     * 
     * @param length key length to check
     */
    private void checkKeySizeLimit(int length) {
        if (length > keySizeLimit) {
            throw new IndexKeySizeLimitException(index.getTable().getFullName(),
                                                 index.getName(),
                                                 keySizeLimit,
                                                 "index key of " + length +
                                                 " bytes exceeded limit of " +
                                                 keySizeLimit);
        }
    }

    @Override
    public String toString() {
        return "IndexKeyCreator[" + index.getName() + "]";
    }
}

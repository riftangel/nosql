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

package oracle.kv.impl.pubsub;

import static com.sleepycat.je.log.LogEntryType.LOG_DEL_LN;
import static com.sleepycat.je.log.LogEntryType.LOG_DEL_LN_TRANSACTIONAL;
import static com.sleepycat.je.log.LogEntryType.LOG_INS_LN;
import static com.sleepycat.je.log.LogEntryType.LOG_INS_LN_TRANSACTIONAL;
import static com.sleepycat.je.log.LogEntryType.LOG_TRACE;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_ABORT;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_COMMIT;
import static com.sleepycat.je.log.LogEntryType.LOG_UPD_LN;
import static com.sleepycat.je.log.LogEntryType.LOG_UPD_LN_TRANSACTIONAL;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import oracle.kv.Key;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.table.TableManager.IDBytesComparator;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.OutputWireRecord;
import com.sleepycat.util.UtfOps;

/**
 * Object represents a feeder filter that will be constructed, serialized
 * and sent to the source feeder over the wire by NoSQLPublisher. At feeder
 * side, the filter is deserialized and rebuilt.
 *
 * Following entries will be filtered out by the feeder:
 * - entry from an internal db;
 * - entry from a db supporting duplicates;
 * - entry from any non-subscribed tables (table-level subscription filtering)
 */
class NoSQLStreamFeederFilter implements FeederFilter, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Set of table ID names for subscribed tables, or null for all tables.
     */
    private final Set<String> tableIds;

    /**
     * Match keys for subscribed tables, indexed by the root table id string,
     * or null for all tables.
     */
    private final Map<String, List<MatchKey>> tableMatchKeys;

    /**
     * Match keys with indexed by root table ID bytes, or null for all
     * tables.
     */
    private transient volatile Map<byte[], List<MatchKey>> tableMatchKeysBytes;

    /*- statistics -*/

    /* number of internal and dup db blocked by the filter */
    private long numIntDupDBFiltered;

    /* number of txn entries passing the filter */
    private long numTxnEntries;

    /* number of non-data entry blocked by the filter */
    private long numNonDataEntry;

    /* number of rows passing the filter */
    private long numRowsPassed;

    /* number of rows blocked by the filter */
    private long numRowsBlocked;

    /*
     * Cached internal and dup db id, to save filtering cost. Starting from an
     * empty set, it would cache internal db id and duplicate db id whenever
     * we saw a record from these dbs. Its overhead should be small since
     * there are not many internal dbs or duplicate dbs in JE env.
     */
    private final Map<Long, Boolean> cachedIntDupDbIds;

    private NoSQLStreamFeederFilter(Set<TableImpl> tables) {
        super();

        if ((tables == null) || tables.isEmpty()) {
            tableIds = null;
        } else {
            tableIds = new HashSet<>(tables.size());
            for (final TableImpl table : tables) {
                tableIds.add(table.getIdString());
            }
        }

        /* init map from string root table id to list of match keys */
        tableMatchKeys = getMatchKeys(tables);

        /* convert to map with byte[] table id as key */
        tableMatchKeysBytes = convertToBytesKey(tableMatchKeys);

        numRowsPassed = 0;
        numRowsBlocked = 0;
        numTxnEntries = 0;
        numNonDataEntry = 0;
        numIntDupDBFiltered = 0;
        cachedIntDupDbIds = new HashMap<>();
    }

    /* convert the map with string table id key to a map with byte[] key */
    private static TreeMap<byte[], List<MatchKey>>
    convertToBytesKey(Map<String, List<MatchKey>> tableMatchKeys) {

        if (tableMatchKeys == null) {
            return null;
        }

        final TreeMap<byte[], List<MatchKey>> ret =
            new TreeMap<>(new IDBytesComparator());
        for (Entry<String, List<MatchKey>> entry : tableMatchKeys.entrySet()) {

            ret.put(UtfOps.stringToBytes(entry.getKey()), entry.getValue());
        }

        return ret;
    }

    /**
     * Stores the information about a table key needed to determine if an entry
     * key matches a table.  The information includes the number of initial key
     * components to skip, corresponding to parent table IDs and primary key
     * components, when looking for the table ID component of the key, and the
     * table ID of the matching table.  Note that this scheme does not check
     * all parent table IDs, only the root table ID, so it may generate false
     * positive matches, but only in rare circumstances.  Those incorrect
     * entries, if any, will be filtered out by the publisher.
     */
    private static class MatchKey implements Serializable {
        private static final long serialVersionUID = 1;

        /** The table ID of the table, represented as a string */
        final String tableId;

        /** The byte array form of the table ID. */
        transient volatile byte[] tableIdBytes;

        /**
         * The number of primary key components associated with just this
         * table, not parent tables.  This value is used to determine if the
         * key contains additional components beyond the one for this table,
         * meaning it is for a child of this table.
         */
        final int keyCount;

        /**
         * The number of key components to skip to find the table ID relative
         * to the start of the key.  Set to 0 if the table is a root table.
         */
        final int skipCount;

        /**
         * The table ID of the root table for the table, represented as a
         * string.
         */
        final String rootTableId;

        /** The byte array form of the root table ID. */
        transient volatile byte[] rootTableIdBytes;

        /** Return a match key for the specified table. */
        MatchKey(TableImpl table) {
            tableId = table.getIdString();
            tableIdBytes = table.getIDBytes();
            final TableImpl firstParent = (TableImpl) table.getParent();
            keyCount = (firstParent == null) ?
                table.getPrimaryKeySize() :
                table.getPrimaryKeySize() - firstParent.getPrimaryKeySize();

            /*
             * Count primary key components and table IDs to skip to find the
             * child table ID, and find the root table
             */
            int count = (firstParent == null) ?
                0 :
                /*
                 * The number of primary key components in the immediate
                 * parent, which includes components for any higher parents.
                 */
                firstParent.getPrimaryKeySize();
            TableImpl rootTable = table;

            for (TableImpl t = firstParent;
                 t != null;
                 t = (TableImpl) t.getParent()) {

                /* Skip the table ID component */
                count++;

                rootTable = t;
            }
            skipCount = count;
            rootTableId = rootTable.getIdString();
            rootTableIdBytes = rootTable.getIDBytes();
        }

        /** Returns whether the key matches the table for this instance. */
        boolean matches(byte[] key) {

            final int rootIdLen = getRootTableIdLength(key);

            /* the key does not have a valid root table id*/
            if (rootIdLen == 0) {
                return false;
            }

            /* check if root table id match */
            if (!equalsBytes(rootTableIdBytes, 0, rootTableIdBytes.length,
                             key, 0, rootIdLen)) {
                return false;
            }

            /* root table id must be followed by a delimiter */
            if (!Key.isDelimiter(key[rootIdLen])) {
                return false;
            }

            /* if subscribed a root table, check key count after table id */
            if (skipCount == 0) {
                return checkKeyCount(key, rootIdLen + 1, keyCount);
            }

            /* find the child table id from key */
            int start = rootIdLen + 1;
            /* Skip any additional parent components */
            for (int i = 1; i < skipCount; i++) {
                final int e = Key.findNextComponent(key, start);
                if (e == -1) {
                    return false;
                }
                start = e + 1;
            }

            /* finish skipping, now find child table id */
            final int end = Key.findNextComponent(key, start);
            if (end == -1) {
                return false;
            }

            /* now we have a valid child id, check if a match */
            if (!equalsBytes(tableIdBytes, 0, tableIdBytes.length,
                             key, start, end)) {
                return false;
            }

            /*
             * If a match, need ensure that the key components needed for
             * this table are present, but no more than that, since that
             * would mean a child table.
             */
            return checkKeyCount(key, end + 1, keyCount);
        }

        /** Initialize the tableIdBytes and rootTableIdBytes fields. */
        private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {

            in.defaultReadObject();
            tableIdBytes = UtfOps.stringToBytes(tableId);
            rootTableIdBytes = UtfOps.stringToBytes(rootTableId);
        }

        /**
         * Returns true if starting from offset, key byte[] has exact number
         * of key counts as expected, false otherwise.
         */
        private static boolean checkKeyCount(byte[] key,
                                             int offset,
                                             int expKeyCount) {

            /* offset must be in range [0, length - 1] */
            if (offset < 0 || offset > key.length - 1) {
                return false;
            }

            for (int i = 0; i < expKeyCount; i++) {
                final int e = Key.findNextComponent(key, offset);
                if (e == -1) {
                    return false;
                }
                offset = e + 1;
            }

            /* adjust to make start at the delimiter or end of array */
            offset = offset - 1;
            /* Should be no more components, at the end of key */
            return (key.length == offset);
        }
    }

    /**
     * Returns a map from root table ID strings to lists of match keys for the
     * specified tables, or null to match all tables.
     */
    private static Map<String, List<MatchKey>> getMatchKeys(
        Set<TableImpl> tables) {

        if ((tables == null) || tables.isEmpty()) {
            return null;
        }

        final Map<String, List<MatchKey>> map = new HashMap<>();
        for (final TableImpl table : tables) {
            final String rootTblIdStr = table.getTopLevelTable().getIdString();
            List<MatchKey> matchKeys = map.get(rootTblIdStr);
            if (matchKeys == null) {
                matchKeys = new ArrayList<>(tables.size());
                map.put(rootTblIdStr, matchKeys);
            }
            matchKeys.add(new MatchKey(table));
        }

        return map;
    }

    /**
     * Gets a feeder filter with given set of subscribed tables
     *
     * @param tables  subscribed tables
     *
     * @return  a feeder filter with given set of subscribed tables
     */
    static NoSQLStreamFeederFilter getFilter(Set<TableImpl> tables) {
        return new NoSQLStreamFeederFilter(tables);
    }

    /**
     * Gets a feeder filter without subscribed table. The feeder filter will
     * pass all rows from all tables, except the internal db and dup db entries.
     *
     * @return  a feeder filter allowing all updates to all tables
     */
    static NoSQLStreamFeederFilter getFilter() {
        return new NoSQLStreamFeederFilter(null);
    }

    @Override
    public OutputWireRecord execute(final OutputWireRecord record,
                                    final RepImpl repImpl) {
        /* block all internal or duplicate db entry */
        if (isInternalDuplicate(record, repImpl)) {
            numIntDupDBFiltered++;
            return null;
        }

        final LogEntryType type = LogEntryType.findType(record.getEntryType());

        /* allow txn boundary entry pass */
        if (LOG_TXN_COMMIT.equals(type) || LOG_TXN_ABORT.equals(type)) {
            numTxnEntries++;
            return record;
        }

        /* allow trace entry for debugging or testing */
        if (LOG_TRACE.equals(type)) {
            return record;
        }

        /* block all non-data type entry */
        if (!isDataEntry(type)) {
            numNonDataEntry++;
            return null;
        }

        /* finally filter out all non-subscribed tables */
        return filter(record);
    }

    @Override
    public String[] getTableIds() {
        if (tableIds == null) {
            return null;
        }
        return tableIds.toArray(new String[tableIds.size()]);
    }

    @Override
    public String toString() {
        return ((tableIds == null || tableIds.isEmpty()) ?
            " all user tables." :
            " ids " + Arrays.toString(tableIds.toArray()));
    }

    public long getNumIntDupDBFiltered() {
        return numIntDupDBFiltered;
    }

    public long getNumTxnEntries() {
        return numTxnEntries;
    }

    public long getNumNonDataEntry() {
        return numNonDataEntry;
    }

    public long getNumRowsPassed() {
        return numRowsPassed;
    }

    public long getNumRowsBlocked() {
        return numRowsBlocked;
    }

    /* filters entries, should be as efficient as possible */
    private OutputWireRecord filter(final OutputWireRecord record) {

        final LogEntry entry = record.instantiateEntry();
        final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;

        /*
         * All duplicate db entries have been filtered before reaching here.
         * Looks for now postFetchInit() does nothing but return for
         * non-duplicate db entry, it is probably safer to still call it here
         * in case implementation of postFetchInit() changes in future.
         */
        lnEntry.postFetchInit(false);

        final byte[] key = lnEntry.getKey();

        /* check if match for a valid root table id in key */
        if (getRootTableIdLength(key) > 0 /* a valid root table id */  &&
            matchKey(key) /* find a match */) {
            numRowsPassed++;
            return record;
        }

        numRowsBlocked++;
        return null;
    }

    /* returns true if key byte[] matches any MatchKey in the list */
    private boolean matchKey(byte[] key) {

        if (tableMatchKeysBytes == null || tableMatchKeysBytes.isEmpty()) {
             /*
              * If no subscribed tables are specified in filter, we allow
              * all rows with a valid table id string to pass the filter.
              */
            return true;
        }

        final List<MatchKey> mkeys = tableMatchKeysBytes.get(key);
        if (mkeys != null) {
            for (MatchKey matchKey : mkeys) {
                if (matchKey.matches(key)) {
                    /* get a match! */
                    return true;
                }
            }
        }
        return false;
    }

    /* returns true for internal and duplicate db entry */
    private boolean isInternalDuplicate(OutputWireRecord record,
                                        RepImpl repImpl) {

        final DatabaseId dbId = record.getReplicableDBId();
        /* internal or duplicate db entry must have a db id */
        if (dbId == null) {
            return false;
        }

        final long id = dbId.getId();
        /* check if this id is an internal or dup db id we saw before */
        final Boolean cachedValue = cachedIntDupDbIds.get(id);
        if (cachedValue != null) {
            return cachedValue;
        }

        final DbTree dbTree = repImpl.getDbTree();
        final DatabaseImpl impl = dbTree.getDb(dbId);
        try {
            final boolean ret =
                (impl != null) &&
                (impl.getSortedDuplicates() || impl.isInternalDb());

            /* cache result for future records */
            cachedIntDupDbIds.put(id, ret);

            return ret;
        } finally {
            dbTree.releaseDb(impl);
        }
    }

    /* returns true for a data entry, e.g., put or delete */
    private static boolean isDataEntry(LogEntryType type) {

        /*
         * it looks in JE, in many places log entry types are compared via
         * equals() instead of '==', so follow the convention
         */
        return LOG_INS_LN.equals(type)               ||
               LOG_UPD_LN.equals(type)               ||
               LOG_DEL_LN.equals(type)               ||
               LOG_INS_LN_TRANSACTIONAL.equals(type) ||
               LOG_UPD_LN_TRANSACTIONAL.equals(type) ||
               LOG_DEL_LN_TRANSACTIONAL.equals(type);
    }

    /* Compares two non-null byte[] from start inclusively to end exclusively */
    private static boolean equalsBytes(byte[] a1, int s1, int e1,
                                       byte[] a2, int s2, int e2) {
        /* must be non-null */
        if (a1 == null || a2 == null) {
            return false;
        }

        /* no underflow */
        if (s1 < 0 || s2 < 0) {
            return false;
        }

        /* no overflow */
        if (a1.length < e1 || a2.length < e2) {
            return false;
        }

        /* end must be greater than start */
        final int len = e1 - s1;
        if (len < 0) {
            return false;
        }

        /* must have same length */
        if (len != (e2 - s2)) {
            return false;
        }

        for (int i = 0; i < len; i++) {
            if (a1[s1 + i] != a2[s2 + i]) {
                return false;
            }
        }

        return true;
    }

    /*
     * Returns length of the root table id in key, or 0 if the key does not
     * have a valid root table id
     */
    private static int getRootTableIdLength(byte[] key) {

        if (key == null) {
            return 0;
        }
        return Key.findNextComponent(key, 0);
    }

    /** Initialize the tableMatchKeysBytes field. */
    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();
        tableMatchKeysBytes = convertToBytesKey(tableMatchKeys);
    }
}

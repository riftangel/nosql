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

package oracle.kv.impl.query.runtime.server;

import static oracle.kv.impl.api.ops.InternalOperationHandler.MIN_READ;
import static oracle.kv.impl.api.ops.InternalOperationHandler.getStorageSize;
import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_READ_COMMITTED;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_6;

import java.util.HashSet;
import java.util.logging.Level;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.impl.api.ops.IndexKeysIterate;
import oracle.kv.impl.api.ops.IndexKeysIterateHandler;
import oracle.kv.impl.api.ops.IndexScanner;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.MultiGetTableKeys;
import oracle.kv.impl.api.ops.MultiGetTableKeysHandler;
import oracle.kv.impl.api.ops.MultiTableOperationHandler;
import oracle.kv.impl.api.ops.MultiTableOperationHandler.OperationTableInfo;
import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.api.ops.Scanner;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.table.BinaryValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexKeyImpl;
import oracle.kv.impl.api.table.IndexRange;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.FieldRange;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.Version;

/**
 * This class serves 2 purposes:
 * (a) It stores some operational context that cannot be stored in the RCB
 *     because the associated java classes are server-only code.
 * (b) Serves as a factory for 2 classes (PrimaryTableScanner and
 *     SecondaryTableScanner) that perform scans over the primary and a
 *     secondary index, respectively.
 *
 * The scanner classes provide a common api to ServerTableIter. Both classes
 * scan their associated index using dirty reads and key-only access and make
 * the scanned index entries available to the ServerTableIter via the
 * nextIndexRow() api. Then, if needed, the ServerTableIter can lock and get
 * the full table row via the currentTableRow() method. This 2-step table
 * access allows filtering based on index entry fields to be done before
 * fetching the full rows, thus avoiding fetching rows that do not survive
 * the filtering.
 *
 * An instance of this class is created in the ServerTableIter constructor,
 * before the server-side query operation starts.
 */
class TableScannerFactory {

    /*
     * The interface for scanners returned by this factory.
     */
    public interface TableScanner {

        /**
         * If forTable is null, the method returns the next index entry or null
         * if there is no next index entry. forTable may be not null only for
         * the primary table scanner and only is the ServerTableIter implements
         * a NESTED TABLES clause. In this case, the method returns the next
         * primary index entry whose number of key components is no more than
         * those of the forTable (or null if no such index entry).
         *
         * In both cases, the index entry is deserialized and returned as a
         * RecordValueImpl.
         */
        public RecordValueImpl nextIndexRow(TableImpl forTable)
            throws SizeLimitException;

        /**
         * Locks the current index entry. This interface may only be called
         * after nextIndexRow() has returned non-null.
         * @return true if the enry is locked, false if it cannot be locked,
         * which means that the entry has been deleted.
         */
        public boolean lockIndexRow();

        /**
         * Locks and returns the full row associated with the "current" key.
         * This interface may only be called after nextIndexEntry() has returned
         * non-null.
         * @return the complete row or null if the row has been deleted.
         */
        public RowImpl currentTableRow() throws SizeLimitException;

        public InternalOperation getOp();

        /**
         * Returns the table associated with the current index entry. The
         * result is valid only if this method is called after nextIndexRow()
         * has been called.
         */
        public TableImpl getTable();

        /**
         * Returns the current, deserialized index entry. The result is valid
         * only if this method is called after nextIndexRow() has been called.
         */
        public RecordValueImpl getIndexRow();

        /**
         * Returns the current primary key in binary format. The result is valid
         * only if this method is called after nextIndexRow() has been called.
         */
        public byte[] getPrimKeyBytes();

        /**
         * Returns the current secondary key in binary format, if the scanner
         * scans a secondary index, or null otherwise. The result is valid only
         * if this method is called after nextIndexRow() has been called.
         */
        public byte[] getSecKeyBytes();

        /**
         * Return the expiration time of the current row. The result is valid
         * only if this method is called after currentTableRow() has been
         * called.
         */
        public long expirationTime();

        /**
         * Return the version of the current row. The result is valid only if
         * this method is called after currentTableRow() has been called.
         */
        public Version rowVersion();

        /**
         * Closes the scanner. This must be called to avoid resource leaks.
         */
        public void close();
    }

    private final RuntimeControlBlock theRCB;

    private final ResumeInfo theResumeInfo;

    private final Transaction theTxn;

    private final PartitionId thePid;

    private final OperationHandler theHandlersManager;

    public TableScannerFactory(
        final RuntimeControlBlock rcb,
        final Transaction txn,
        final PartitionId pid,
        final OperationHandler oh) {

        theRCB = rcb;
        theResumeInfo = rcb.getResumeInfo();
        theTxn = txn;
        thePid = pid;
        theHandlersManager = oh;
    }

    /*
     * Returns a TableScanner. This is an index scanner if indexKey is not null,
     * otherwise it is a primary key scanner. In both cases the object must be
     * closed to avoid leaking resources and/or leaving records locked.
     */
    public TableScanner getTableScanner(
        Direction dir,
        TableImpl[] tables,
        int numAncestors,
        PrimaryKey[] primaryKeys,
        IndexKey[] indexKeys,
        FieldRange[] ranges,
        boolean eliminateDups,
        boolean isUpdate,
        boolean lockIndexEntries,
        short version) {

        assert primaryKeys == null || indexKeys == null;

        if (indexKeys != null) {

            if (tables.length > numAncestors + 1) {
                return new CompositeTableScanner(dir, eliminateDups,
                                                 tables, numAncestors,
                                                 indexKeys, ranges);
            }

            if (version >= QUERY_VERSION_6) {
                return new SecondaryTableScanner(dir, eliminateDups,
                                                 tables, numAncestors,
                                                 indexKeys, ranges,
                                                 lockIndexEntries);
            }

            return new OldSecondaryTableScanner(dir, eliminateDups,
                                                indexKeys, ranges);
        }

        return new PrimaryTableScanner(dir, isUpdate,
                                       tables, numAncestors,
                                       primaryKeys, ranges,
                                       lockIndexEntries, false);
    }

    AncestorScanner getAncestorScanner(InternalOperation op) {
        return new AncestorScanner(op);
    }

    /**
     * This is a "scanner" that is actually used to do an exact key lookup.
     * It is used by the ServerTaleIter to retrieve the ancestor keys and/or
     * rows for the current target-table row. It is also used during resume
     * to read the rows in the resume join path.
     *
     * Modeling it as a scanner allows some code to be reused in
     * ServerTableIter.
     *
     * theOp:
     * This is the InternalOperation used by the "main" scanner of the
     * ServerTableIter (either a PrimaryTableScanner or SecondaryTableScanner).
     * We put a reference in "this" so that read consumption is tracked in
     * one place only.
     */
    class AncestorScanner implements TableScanner {

        InternalOperation theOp;

        TableImpl theTable;

        IndexImpl theIndex;

        byte[] thePrimKey;

        byte[] theSecKey;

        Database theDB;

        Cursor theCursor;

        SecondaryDatabase theSecDB;

        SecondaryCursor theSecCursor;

        OperationResult theGetResult;

        boolean theMoreElements;

        RowImpl theTableRow;

        RecordValueImpl theIndexRow;

        final DatabaseEntry theKeyEntry;

        final DatabaseEntry theNoDataEntry;

        final DatabaseEntry theDataEntry;

        AncestorScanner(InternalOperation op) {

            theOp = op;

            if (thePid.getPartitionId() >= 0) {
                theDB = theHandlersManager.getRepNode().getPartitionDB(thePid);
            }

            theKeyEntry = new DatabaseEntry();
            theDataEntry = new DatabaseEntry();
            theNoDataEntry = new DatabaseEntry();
            theNoDataEntry.setPartial(0, 0, true);
        }

        void init(TableImpl table, IndexImpl index, byte[] pkey, byte[] ikey) {

            theTable = table;
            theIndex = index;
            thePrimKey = pkey;
            theSecKey = ikey;
            theMoreElements = true;
            theTableRow = null;
            theIndexRow = null;

            if (index == null) {

                if (theDB == null) {
                    theDB = theHandlersManager.getRepNode().getPartitionDB(pkey);
                }

                theCursor = theDB.openCursor(theTxn, CURSOR_READ_COMMITTED);

            } else {

                if (theSecDB == null) {
                    theSecDB = theHandlersManager.getRepNode().
                               getIndexDB(table.getNamespace(),
                                          index.getName(),
                                          table.getFullName());
                }

                theSecCursor = theSecDB.openCursor(theTxn, CURSOR_READ_COMMITTED);
            }
        }

        @Override
        public InternalOperation getOp() {
            return theOp;
        }

        @Override
        public TableImpl getTable() {
            return theTable;
        }

        @Override
        public RecordValueImpl getIndexRow() {
            return (theIndex != null ? theIndexRow : theTableRow);
        }

        @Override
        public byte[] getPrimKeyBytes() {
            return thePrimKey;
        }

        @Override
        public byte[] getSecKeyBytes() {
            return theSecKey;
        }

        @Override
        public long expirationTime() {
            return theGetResult.getExpirationTime();
        }

        @Override
        public Version rowVersion() {
            Cursor cursor = (theIndex == null ? theCursor : theSecCursor);
            return theHandlersManager.getVersion(cursor);
        }

        @Override
        public void close() {
            if (theCursor != null) {
                TxnUtil.close(theCursor);
            }
            if (theSecCursor != null) {
                TxnUtil.close(theSecCursor);
            }
        }

        @Override
        public RecordValueImpl nextIndexRow(TableImpl forTable)
            throws SizeLimitException {

            if (theMoreElements) {
                moveToNextIndexEntry();
                return (theIndex != null ? theIndexRow : theTableRow);
            }

            return null;
        }

        @Override
        public boolean lockIndexRow() {
            return true;
        }

        @SuppressWarnings("resource")
        @Override
        public RowImpl currentTableRow() throws SizeLimitException {

            Cursor cursor = (theIndex == null ? theCursor : theSecCursor);

            OperationResult result =
                cursor.get(theKeyEntry, theDataEntry, Get.CURRENT,
                           LockMode.READ_UNCOMMITTED.toReadOptions());

            if (result == null) {
                return null;
            }

            theOp.addReadBytes(getStorageSize(cursor));

            checkSizeLimit(theRCB, theOp, getStorageSize(cursor));

            byte[] data = theDataEntry.getData();

            if (!MultiTableOperationHandler.isTableData(data, null)) {
                return null;
            }

            if (data == null || data.length == 0) {

                /*
                 * A key-only row, no data to fetch. However, the table may
                 * have evolved and it now contains non-prim-key columns as
                 * well. So, we must fill the missing columns with their
                 * default values.
                 */
                if (theTableRow.getNumFields() > theTable.getPrimaryKeySize()) {
                    theTableRow.addMissingFields();
                }

                theTableRow.setExpirationTime(expirationTime());
                theTableRow.setVersion(rowVersion());
                return theTableRow;
            }

            if (theIndex == null) {
                Value.Format format = Value.Format.fromFirstByte(data[0]);

                if (!Value.Format.isTableFormat(format)) {
                    return null;
                }

                if (!theTable.initRowFromByteValue(theTableRow,
                                                   data,
                                                   format,
                                                   1/*offset*/)) {

                    return null;
                }
            } else {
                theTableRow = theTable.createRow();

                if (!theTable.initRowFromBytes(thePrimKey,data, theTableRow)) {
                    return null;
                }
            }

            theTableRow.setExpirationTime(expirationTime());
            theTableRow.setVersion(rowVersion());
            return theTableRow;
        }

        private void moveToNextIndexEntry()
            throws SizeLimitException {

            if (!theMoreElements) {
                theTableRow = null;
                theIndexRow = null;
                return;
            }

            theMoreElements = false;

            checkSizeLimit(theRCB, theOp, MIN_READ);

            if (theIndex == null) {

                if (theRCB.getTraceLevel() >= 2) {
                    theRCB.trace("Searching for anc key : " +
                                 PlanIter.printKey(thePrimKey));
                }

                theKeyEntry.setData(thePrimKey);

                theGetResult = theCursor.get(theKeyEntry, theNoDataEntry,
                                             Get.SEARCH,
                                             LockMode.DEFAULT.toReadOptions());

                theOp.addReadBytes(MIN_READ);

                if (theGetResult == null) {
                    return;
                }

                TableImpl table = theTable.findTargetTable(thePrimKey);

                if (table == null) {

                    /*
                     * This should not be possible unless there is a non-table key
                     * in the btree.
                     */
                    String msg = "Key is not in a table: "  +
                        Key.fromByteArray(thePrimKey);
                    theHandlersManager.getLogger().log(Level.INFO, msg);
                    return;
                }

                if (theTable.getId() != table.getId()) {
                    return;
                }

                theTableRow = theTable.createRow();

                if (!theTable.initRowFromKeyBytes(thePrimKey, -1, /*initPos*/
                                                  theTableRow)) {
                    theTableRow = null;
                    return;
                }

                if (theRCB.getTraceLevel() >= 3) {
                    theRCB.trace("Produced key row : " + theTableRow);
                }
            } else {

                theKeyEntry.setData(theSecKey);
                theDataEntry.setData(thePrimKey);

                theGetResult = theSecCursor.get(
                                   theKeyEntry,
                                   theDataEntry,
                                   theNoDataEntry,
                                   Get.SEARCH_BOTH,
                                   LockMode.DEFAULT.toReadOptions());

                theOp.addReadBytes(MIN_READ);

                if (theGetResult == null) {
                    return;
                }

                theIndexRow = theIndex.getIndexEntryDef().createRecord();

                theIndex.rowFromIndexEntry(theIndexRow, thePrimKey, theSecKey);

                if (theRCB.getTraceLevel() >= 3) {
                    theRCB.trace("Produced key row : " + theIndexRow);
                }
            }
        }
    }

    /*
     * theScanner:
     * The underlying Scanner used by PrimaryTableScanner. It uses
     * DIRTY_READ_ALL lockmode (unless theLockIndexEntries is true) and does
     * a key-only scan.
     *
     * theTableRow:
     * A RowImpl where the current binary primary-index key is deserialized
     * into. If the full record is needed by the query, the associated LN will
     * also be deserialized into this RowImpl.
     *
     * theDataEntry:
     * Used to retrieve the LN associated with the current index key.
     */
    private class PrimaryTableScanner implements TableScanner {

        final TargetTables theTargetTables;

        final boolean theLockIndexEntries;

        final Direction theDirection;

        final boolean theIsUpdate;

        final PrimaryKey[] theKeys;

        final FieldRange[] theRanges;

        int theCurrentIndexRange;

        MultiGetTableKeys theOp;

        final MultiGetTableKeysHandler theOpHandler;

        OperationTableInfo theTableInfo;

        Scanner theScanner;

        boolean theMoreElements;

        byte[] theBinaryPrimKey;

        TableImpl theTable;

        RowImpl theTableRow;

        final DatabaseEntry theDataEntry;

        PrimaryTableScanner(
            Direction dir,
            boolean isUpdate,
            TableImpl[] tables,
            int numAncestors,
            PrimaryKey[] keys,
            FieldRange[] ranges,
            boolean lockIndexEntries,
            boolean isComposite) {

            theTable = tables[numAncestors];
            theTargetTables =  new TargetTables(tables, numAncestors);
            theLockIndexEntries = lockIndexEntries;
            theDirection = dir;
            theIsUpdate = isUpdate;
            theKeys = keys;
            theRanges = ranges;

            theCurrentIndexRange = theResumeInfo.getCurrentIndexRange();

            theOpHandler = (MultiGetTableKeysHandler)
                theHandlersManager.getHandler(OpCode.MULTI_GET_TABLE_KEYS);

            theMoreElements = true;
            theDataEntry = new DatabaseEntry();

            theTableRow = theTable.createRow();

            if (!isComposite) {
                initIndexRange();
            }
        }

        void initIndexRange() {

            theTable = (TableImpl) theKeys[0].getTable();
            PrimaryKey key = theKeys[theCurrentIndexRange];
            FieldRange range = theRanges[theCurrentIndexRange];

            TableKey tableKey = TableKey.createKey(theTable, key, true);
            assert(tableKey != null);

            TableQuery queryOp = theRCB.getQueryOp();
            theOp = new MultiGetTableKeys(tableKey.getKeyBytes(),
                                          theTargetTables,
                                          TableAPIImpl.createKeyRange(range,
                                                                      true),
                                          queryOp.getEmptyReadFactor());
            /*
             * Configures the ThroughputTracker of MultiGetTableKeys op with
             * the ThroughputTracker of TableQuery.
             */
            theOp.setThroughputTracker(queryOp);

            theTableInfo = new OperationTableInfo();
            theTableInfo.setTopLevelTable(theTable.getTopLevelTable());

            theOpHandler.verifyTableAccess(theOp);

            if (theRCB.getTraceLevel() >= 2) {
                theRCB.trace("Initializing index scan with resume key: " +
                             PlanIter.printKey(theResumeInfo.getPrimResumeKey()) +
                             " move after resume key = " +
                             theResumeInfo.getMoveAfterResumeKey());
            }

            /*
             * Create a key-only scanner using dirty reads. This means that in
             * order to use the record, it must be locked, and if the data is
             * required, it must be fetched.
             */
            theScanner = theOpHandler.getScanner(
                theOp,
                theTableInfo,
                theTxn,
                thePid,
                tableKey.getMajorKeyComplete(),
                theDirection,
                theResumeInfo.getPrimResumeKey(),
                theResumeInfo.getMoveAfterResumeKey(),
                CURSOR_READ_COMMITTED,
                (theIsUpdate ?
                 LockMode.RMW :
                 (theLockIndexEntries ?
                  LockMode.DEFAULT :
                  LockMode.READ_UNCOMMITTED_ALL)),
                true); /* use a key-only scanner; fetch data in the "next" call */

            /*
             * Disable charging the cost of reading key in the scanner, see more
             * details on charging key read cost in moveToNextIndexEntry().
             */
            theScanner.setChargeKeyRead(false);
        }

        @Override
        public InternalOperation getOp() {
            return theOp;
        }

        @Override
        public TableImpl getTable() {
            return theTable;
        }

        @Override
        public RecordValueImpl getIndexRow() {
            return theTableRow;
        }

        @Override
        public byte[] getPrimKeyBytes() {
            return theBinaryPrimKey;
        }

        @Override
        public byte[] getSecKeyBytes() {
            return null;
        }

        @Override
        public long expirationTime() {
            return theScanner.getExpirationTime();
        }

        @Override
        public Version rowVersion() {
            return theHandlersManager.getVersion(theScanner.getCursor());
        }

        @Override
        public void close() {

            /*
             * Tally the read KB of MultiGetTableKeys op to
             * RuntimeControlBlock.readKB.
             */
            theRCB.tallyReadKB(getOp().getReadKB());
            if (theScanner != null) {
                theScanner.close();
            }
            theTableRow = null;
            theBinaryPrimKey = null;
        }

        @Override
        public RecordValueImpl nextIndexRow(TableImpl forTable)
            throws SizeLimitException {

            moveToNextIndexEntry(forTable);

            if (theMoreElements) {
                return theTableRow;
            }

            return null;
        }

        @Override
        public boolean lockIndexRow() {
            return theScanner.getCurrent();
        }

        @Override
        public RowImpl currentTableRow() throws SizeLimitException {

            checkSizeLimit(theRCB, getOp(), theScanner.getCurrentStorageSize());

            if (!theScanner.getLockedData(theDataEntry)) {
                return null;
            }

            byte[] data = theDataEntry.getData();

            if (!MultiTableOperationHandler.isTableData(data, null)) {
                return null;
            }

            if (data == null || data.length == 0) {

                /*
                 * A key-only row, no data to fetch. However, the table may
                 * have evolved and it now contains non-prim-key columns as
                 * well. So, we must fill the missing columns with their
                 * default values.
                 */
                if (theTableRow.getNumFields() > theTable.getPrimaryKeySize()) {
                    theTableRow.addMissingFields();
                }

                theTableRow.setExpirationTime(theScanner.getExpirationTime());
                theTableRow.setVersion(rowVersion());
                return theTableRow;
            }

            Value.Format format = Value.Format.fromFirstByte(data[0]);

            if (!Value.Format.isTableFormat(format)) {
                return null;
            }

            if (theTable.initRowFromByteValue(theTableRow,
                                              data,
                                              format,
                                              1/*offset*/)) {
                theTableRow.setExpirationTime(theScanner.getExpirationTime());
                theTableRow.setVersion(rowVersion());
                return theTableRow;
            }

            return null;
        }

        void moveToNextIndexEntry(TableImpl forTable)
            throws SizeLimitException {

            /*
             * The cost of reading key is disabled in the scanner, the cost of
             * reading key will be charged after key check by keyInTargetTable()
             *   - match > 0, valid key, charge min. read.
             *   - match < 0, no key found, charge empty read.
             *   - match = 0, invalid key for the target table and continue
             *                to next key, no charge.
             */
            while (theCurrentIndexRange < theKeys.length) {

                while (theMoreElements && theScanner.next()) {

                    theBinaryPrimKey = theScanner.getKey().getData();
                    theResumeInfo.setPrimResumeKey(theBinaryPrimKey);

                    if (theRCB.getTraceLevel() >= 4) {
                        theRCB.trace("Produced binary index entry in " +
                                     thePid + " : " +
                                     PlanIter.printKey(theBinaryPrimKey));
                    }

                    if (theRCB.getTraceLevel() >= 3 && forTable != null) {
                        theRCB.trace("moveToNextIndexEntry for table " +
                                     forTable.getFullName());
                    }

                    int match = MultiGetTableKeysHandler.
                        keyInTargetTable(theHandlersManager.getLogger(),
                                         theOp,
                                         theTargetTables,
                                         theTableInfo,
                                         (forTable != null ?
                                          forTable.getNumKeyComponents() :
                                          -1),
                                         theScanner.getKey(),
                                         theScanner.getData(),
                                         theScanner.getCursor(),
                                         theScanner.getLockMode(),
                                         false /* chargeReadCost */);

                    theBinaryPrimKey = theScanner.getKey().getData();
                    theResumeInfo.setPrimResumeKey(theBinaryPrimKey);

                    if (match <= 0) {
                        if (match < 0) {
                            theMoreElements = false;
                            /* No matched key found, charge empty read */
                            theOp.addEmptyReadCharge();
                        }
                        continue;
                    }

                    /* Charge min. read for reading the matched key */
                    theOp.addMinReadCharge();

                    checkSizeLimit(theRCB, theOp);

                    if (theTargetTables.hasChildTables() ||
                        theTargetTables.hasAncestorTables()) {
                        theTable = theTableInfo.getCurrentTable();
                        theTableRow = theTable.createRow();
                    }

                    if (!theTable.initRowFromKeyBytes(theBinaryPrimKey,
                                                      -1, /*initPos*/
                                                      theTableRow)) {
                        continue;
                    }

                    if (theRCB.getTraceLevel() >= 3) {
                        theRCB.trace("Produced key row : " + theTableRow);
                    }

                    return;
                }

                ++theCurrentIndexRange;

                theResumeInfo.setCurrentIndexRange(theCurrentIndexRange);

                if (theCurrentIndexRange < theKeys.length) {
                    theResumeInfo.setPrimResumeKey(null);
                    theMoreElements = true;
                    initIndexRange();
                }
            }

            theResumeInfo.setPrimResumeKey(null);
            theMoreElements = false;
        }
    }

    /**
     * theScanner:
     * The underlying IndexScanner used by SecondaryTableScanner. It uses
     * DIRTY_READ_ALL lockmode and does a key-only scan.
     *
     * thTableeRow:
     * The table Row that stores the record pointed to by the current index
     * entry (the one that the scanner is positioned on).
     *
     * theDataEntry:
     * A DataEntry used to retrieve the data portion of the record pointed to
     * by the current index entry.
     */
    private class SecondaryTableScanner implements TableScanner {

        final TableImpl theTable;

        final IndexImpl theIndex;

        final TargetTables theTargetTables;

        final boolean theLockIndexEntries;

        final Direction theDirection;

        final boolean theEliminateDups;

        final IndexKey[] theKeys;

        final FieldRange[] theRanges;

        final IndexKeysIterateHandler theOpHandler;

        IndexKeysIterate theOp;

        int theCurrentIndexRange;

        IndexScanner theScanner;

        boolean theMoreElements;

        byte[] theBinaryPrimKey;

        byte[] theBinaryIndexKey;

        final RecordValueImpl theIndexRow;

        final RowImpl theTableRow;

        final DatabaseEntry theDataEntry;

        final HashSet<BinaryValueImpl> thePrimKeysSet;

        /*
         * Flag to record the state if the current read cost exceeds the size
         * limit.
         */
        private boolean theExceededSizeLimit;

        SecondaryTableScanner(
            Direction dir,
            boolean eliminateDups,
            TableImpl[] tables,
            int numAncestors,
            IndexKey[] keys,
            FieldRange[] ranges,
            boolean lockIndexEntries) {

            theIndex = (IndexImpl) keys[0].getIndex();
            theTable = (TableImpl) theIndex.getTable();

            if (tables == null) {
                tables = new TableImpl[1];
                tables[0] = theTable;
            }

            assert(theTable == tables[numAncestors]);
            theTargetTables =  new TargetTables(tables, numAncestors);
            theLockIndexEntries = lockIndexEntries;
            theDirection = dir;
            theEliminateDups = eliminateDups;
            theKeys = keys;
            theRanges = ranges;

            theOpHandler = (IndexKeysIterateHandler)
                theHandlersManager.getHandler(OpCode.INDEX_KEYS_ITERATE);

            theCurrentIndexRange = theResumeInfo.getCurrentIndexRange();

            theMoreElements = true;
            theDataEntry = new DatabaseEntry();

            if (eliminateDups) {
                thePrimKeysSet = new HashSet<>(1000);
            } else {
                thePrimKeysSet = null;
            }

            initIndexRange();

            theTableRow = theTable.createRow();
            theIndexRow = theIndex.getIndexEntryDef().createRecord();
        }

        void initIndexRange() {

            if (theScanner != null) {
                theScanner.close();
            }

            IndexKeyImpl key = (IndexKeyImpl)theKeys[theCurrentIndexRange];
            FieldRange range = theRanges[theCurrentIndexRange];

            if (theRCB.getTraceLevel() >= 2) {
                theRCB.trace("Initializing IndexScan: \nKey = " + key +
                             "\nRange = " + range + "\ndup elim = " +
                             theEliminateDups + "\nResumeKey =\n" +
                             theResumeInfo.getSecResumeKey() + "\n" +
                             theResumeInfo.getPrimResumeKey());
            }

            /*
             * Create an IndexOperation for a single target table
             */
            IndexRange indexRange = new IndexRange(key, range, theDirection);

            TableQuery queryOp = theRCB.getQueryOp();
            theOp = new IndexKeysIterate(theIndex.getName(),
                                         theTargetTables,
                                         indexRange,
                                         theResumeInfo.getSecResumeKey(),
                                         theResumeInfo.getPrimResumeKey(),
                                         0 /* batch size not needed */,
                                         0 /* maxReadKB */,
                                         queryOp.getEmptyReadFactor());

            /*
             * Configures the ThroughputTracker of IndexKeysIterate op with
             * the ThroughputTracker of TableQuery.
             */
            theOp.setThroughputTracker(queryOp);

            theOpHandler.verifyTableAccess(theOp);

            /*
             * Create a key-only scanner using dirty reads. This means that
             * in order to use the record, it must be locked, and if the data
             * is required, it must be fetched.
             */
            theScanner = theOpHandler.getIndexScanner(
                theOp,
                theTxn,
                OperationHandler.CURSOR_READ_COMMITTED,
                (theLockIndexEntries ?
                 LockMode.DEFAULT :
                 LockMode.READ_UNCOMMITTED_ALL),
                true,
                theResumeInfo.getMoveAfterResumeKey());
        }

        @Override
        public InternalOperation getOp() {
            return theOp;
        }

        @Override
        public TableImpl getTable() {
            return theTable;
        }

        @Override
        public RecordValueImpl getIndexRow() {
            return theIndexRow;
        }

        @Override
        public byte[] getPrimKeyBytes() {
            return theBinaryPrimKey;
        }

        @Override
        public byte[] getSecKeyBytes() {
            return theBinaryIndexKey;
        }

        @Override
        public long expirationTime() {
            return theScanner.getExpirationTime();
        }

        @Override
        public Version rowVersion() {
            return theHandlersManager.getVersion(theScanner.getCursor());
        }

        @Override
        public void close() {

            /*
             * Tally read KB of IndexKeysIterate op to
             * RuntimeControlBlock.readKB.
             */
            theRCB.tallyReadKB(theOp.getReadKB());

            if (theScanner != null) {
                theScanner.close();
            }
            if (thePrimKeysSet != null) {
                thePrimKeysSet.clear();
            }
        }

        @Override
        public RecordValueImpl nextIndexRow(TableImpl forTable)
            throws SizeLimitException {

            /*
             * Throw SizeLimitException if theExceededSizeLimit is true, the
             * current entry has been read, set afterReadEntry of
             * SizeLimitException to true.
             */
            if (theExceededSizeLimit) {
                throw new SizeLimitException(true /* afterReadEntry */);
            }

            moveToNextIndexEntry();

            if (theMoreElements) {
                theIndex.rowFromIndexEntry(theIndexRow,
                                           theBinaryPrimKey,
                                           theBinaryIndexKey);

                if (theRCB.getTraceLevel() >= 3) {
                    theRCB.trace("Produced index row : " + theIndexRow);
                }

                return theIndexRow;
            }

            return null;
        }

        @Override
        public boolean lockIndexRow() {
            return theScanner.lockIndexEntry();
        }

        @Override
        public RowImpl currentTableRow() throws SizeLimitException {

            if (!theScanner.getLockedData(theDataEntry)) {
                return null;
            }

            /*
             * Check with size limit. For query on single table, if the current
             * read cost exceeds the size limit, return the current data entry
             * already-fetched and defer throwing SizeLimitException until
             * move to next index row.
             */
            try {
                checkSizeLimit(theRCB, theOp);
            } catch(SizeLimitException sle) {
                if (theTargetTables.hasAncestorTables()) {
                    throw sle;
                }
                assert !theExceededSizeLimit;
                theExceededSizeLimit = true;
            }

            byte[] data = theDataEntry.getData();

            if (data == null || data.length == 0) {

                /*
                 * A key-only row, no data to fetch. However, the table may
                 * have evolved and it now contains non-prim-key columns as
                 * well. So, we must fill the missing columns with their
                 * default values.
                 */

                if  (theTable.getRowDef().getNumFields() ==
                     theTable.getPrimaryKeySize()) {

                    throw new QueryStateException(
                        "currentRow() should never be called on a key-only " +
                        "table, because the index should be a covering one");
                }

                theTableRow.addMissingFields();
                theTableRow.setExpirationTime(theScanner.getExpirationTime());
                theTableRow.setVersion(rowVersion());
                return theTableRow;
            }

            if (!theTable.initRowFromBytes(theBinaryPrimKey,
                                           data,
                                           theTableRow)) {
                return null;
            }

            theTableRow.setExpirationTime(theScanner.getExpirationTime());
            theTableRow.setVersion(rowVersion());
            return theTableRow;
        }

        private void moveToNextIndexEntry()
            throws SizeLimitException {

            while (theCurrentIndexRange < theKeys.length) {

                while (theMoreElements && theScanner.next()) {

                    DatabaseEntry indexKey = theScanner.getIndexKey();
                    DatabaseEntry primaryKey = theScanner.getPrimaryKey();
                    assert(indexKey != null && primaryKey != null);

                    theBinaryPrimKey = primaryKey.getData();
                    theBinaryIndexKey = indexKey.getData();

                    theResumeInfo.setPrimResumeKey(theBinaryPrimKey);
                    theResumeInfo.setSecResumeKey(theBinaryIndexKey);

                    /* Must check size limit after the resume key is set */
                    checkSizeLimit(theRCB, theOp);

                    if (theEliminateDups) {
                        BinaryValueImpl primKeyVal =
                            FieldDefImpl.binaryDef.
                            createBinary(theBinaryPrimKey);

                        boolean added = thePrimKeysSet.add(primKeyVal);
                        if (!added) {
                            continue;
                        }
                    }

                    return;
                }

                ++theCurrentIndexRange;
                theResumeInfo.setCurrentIndexRange(theCurrentIndexRange);

                if (theCurrentIndexRange < theKeys.length) {
                    theResumeInfo.setPrimResumeKey(null);
                    theResumeInfo.setSecResumeKey(null);
                    initIndexRange();
                }
            }

            theResumeInfo.setPrimResumeKey(null);
            theResumeInfo.setSecResumeKey(null);
            theMoreElements = false;
        }
    }

    /*
     * This class is used only to support queries compiled with clients older
     * than verion 18.1
     */
    private class OldSecondaryTableScanner extends SecondaryTableScanner {

        RowImpl theRow;

        OldSecondaryTableScanner(
            Direction dir,
            boolean eliminateDups,
            IndexKey[] keys,
            FieldRange[] ranges) {

            super(dir, eliminateDups, null, 0, keys, ranges, false);
        }

        @Override
        public RecordValueImpl getIndexRow() {
            return theRow;
        }

        @Override
        public byte[] getSecKeyBytes() {
            throw new QueryStateException("Method should not be called");
        }

        @Override
        public RecordValueImpl nextIndexRow(TableImpl forTable) {
            getNextKey();
            return theRow;
        }

        /**
         * Fetches the data for the Row.
         */
        @Override
        public RowImpl currentTableRow() {

            if (!theScanner.getLockedData(theDataEntry)) {
                return null;
            }

            byte[] data = theDataEntry.getData();

            if (data == null || data.length == 0) {

                /*
                 * A key-only row, no data to fetch. However, the table may
                 * have evolved and it now contains non-prim-key columns as
                 * well. So, we must fill the missing columns with their
                 * default values.
                 */

                if  (theTable.getRowDef().getNumFields() ==
                     theTable.getPrimaryKeySize()) {

                    throw new QueryStateException(
                        "currentRow() should never be called on a key-only " +
                        "table, because the index should be a covering one");
                }

                theRow.addMissingFields();
                theRow.setExpirationTime(theScanner.getExpirationTime());
                theRow.setVersion(rowVersion());
                return theRow;
            }

            Value.Format format = Value.Format.fromFirstByte(data[0]);

            if (theTable.initRowFromByteValue(theRow, data, format, 1)) {
                theRow.setExpirationTime(theScanner.getExpirationTime());
                theRow.setVersion(rowVersion());
                return theRow;
            }

            return null;
        }

        private void getNextKey() {

            while (theCurrentIndexRange < theKeys.length) {

                while (theMoreElements && theScanner.next()) {
                    createKey();
                    if (theRow != null) {
                        return;
                    }
                }

                ++theCurrentIndexRange;
                theResumeInfo.setCurrentIndexRange(theCurrentIndexRange);

                if (theCurrentIndexRange < theKeys.length) {
                    theResumeInfo.setPrimResumeKey(null);
                    theResumeInfo.setSecResumeKey(null);
                    initIndexRange();
                }
            }

            theResumeInfo.setPrimResumeKey(null);
            theResumeInfo.setSecResumeKey(null);
            theRow = null;
            theMoreElements = false;
        }

        private void createKey() {

            DatabaseEntry indexKeyEntry = theScanner.getIndexKey();
            DatabaseEntry primaryKeyEntry = theScanner.getPrimaryKey();
            assert(indexKeyEntry != null && primaryKeyEntry != null);

            theBinaryPrimKey = primaryKeyEntry.getData();

            if (theEliminateDups) {
                BinaryValueImpl primKeyVal = 
                    FieldDefImpl.binaryDef.createBinary(theBinaryPrimKey);

                boolean added = thePrimKeysSet.add(primKeyVal);
                if (!added) {
                    theRow = null;
                    return;
                }
            }

            theResumeInfo.setPrimResumeKey(theBinaryPrimKey);
            theResumeInfo.setSecResumeKey(indexKeyEntry.getData());

            /* Create Row from primary key bytes */
            theRow = theTable.createRowFromKeyBytes(theBinaryPrimKey);

            if (theRCB.getTraceLevel() >= 3) {
                theRCB.trace("Produced prim-key row from index key : " +
                             theRow);
            }

            /* Add the index fields to the above row */
            theIndex.rowFromIndexKey(indexKeyEntry.getData(), theRow);

            if (theRCB.getTraceLevel() >= 3) {
                theRCB.trace("Produced prim+sec-key row from index key : " + 
                             theRow);
            }
        }
    }

    /**
     * A table scanner that scans the target table via a secondary index and
     * the descendant tables via a primary index scan.
     */
    private class CompositeTableScanner extends PrimaryTableScanner {

        final IndexImpl theIndex;

        final TableImpl theTargetTable;

        final boolean theEliminateDups;

        final IndexKey[] theSecKeys;

        final IndexKeysIterateHandler theSecOpHandler;

        IndexKeysIterate theSecOp;

        IndexScanner theSecScanner;

        byte[] theBinaryIndexKey;

        RecordValueImpl theIndexRow;

        final HashSet<BinaryValueImpl> thePrimKeysSet;

        CompositeTableScanner(
            Direction dir,
            boolean eliminateDups,
            TableImpl[] tables,
            int numAncestors,
            IndexKey[] keys,
            FieldRange[] ranges) {

            super(dir,
                  false, /* isUpdate */
                  tables,
                  numAncestors,
                  null,
                  ranges,
                  false, /*lockIndexEntries*/
                  true /*isComposite*/);

            theIndex = (IndexImpl) keys[0].getIndex();
            theTargetTable = tables[numAncestors];
            theEliminateDups = eliminateDups;
            theSecKeys = keys;

            theSecOpHandler = (IndexKeysIterateHandler)
                theHandlersManager.getHandler(OpCode.INDEX_KEYS_ITERATE);

            if (eliminateDups) {
                thePrimKeysSet = new HashSet<>(1000);
            } else {
                thePrimKeysSet = null;
            }

            initIndexRange();
        }

        @Override
        void initIndexRange() {

            if (theSecScanner != null) {
                theSecScanner.close();
            }

            if (theScanner != null) {
                theScanner.close();
            }

            IndexKeyImpl key = (IndexKeyImpl)theSecKeys[theCurrentIndexRange];
            FieldRange range = theRanges[theCurrentIndexRange];

            /*
             * Create an IndexOperation for a single target table
             */
            IndexRange indexRange = new IndexRange(key, range, theDirection);

            theSecOp = new IndexKeysIterate(theIndex.getName(),
                                            theTargetTables,
                                            indexRange,
                                            theResumeInfo.getSecResumeKey(),
                                            theResumeInfo.getPrimResumeKey(),
                                            0 /* batch size not needed */,
                                            0 /* maxReadKB */,
                                            1 /* emptyReadFactor */);

            theTableInfo = new OperationTableInfo();
            theTableInfo.setTopLevelTable(theTable.getTopLevelTable());

            /*
             * Configures the ThroughputTracker of IndexKeysIterate op with
             * the ThroughputTracker of TableQuery.
             */
            theSecOp.setThroughputTracker(theRCB.getQueryOp());

            theSecOpHandler.verifyTableAccess(theSecOp);

            /*
             * Create a key-only scanner using dirty reads. This means that
             * in order to use the record, it must be locked, and if the data
             * is required, it must be fetched.
             */
            theSecScanner = theSecOpHandler.getIndexScanner(
                theSecOp,
                theTxn,
                OperationHandler.CURSOR_READ_COMMITTED,
                LockMode.READ_UNCOMMITTED_ALL,
                true, /*keyonly*/
                false /*MoveAfterResumeKey*/); // ????

        }

        @Override
        public InternalOperation getOp() {
            return theSecOp;
        }

        @Override
        public RecordValueImpl getIndexRow() {
            return theIndexRow;
        }

        @Override
        public byte[] getSecKeyBytes() {
            return theBinaryIndexKey;
        }

        @Override
        public void close() {

            super.close();

            if (theSecScanner != null) {
                theSecScanner.close();
            }

            if (thePrimKeysSet != null) {
                thePrimKeysSet.clear();
            }
        }

        @Override
        public RecordValueImpl nextIndexRow(TableImpl forTable)
            throws SizeLimitException {

            moveToNextIndexEntry(forTable);

            if (!theMoreElements) {
                return null;
            }

            if (theTable.getId() == theTargetTable.getId()) {
                return theIndexRow;
            }

            return theTableRow;
        }

        @Override
        void moveToNextIndexEntry(TableImpl forTable)
            throws SizeLimitException {

            while (theCurrentIndexRange < theSecKeys.length) {

                if (moveToNextEntryInCurrentScan(forTable)) {
                    return;
                }

                ++theCurrentIndexRange;
                theResumeInfo.setCurrentIndexRange(theCurrentIndexRange);

                if (theCurrentIndexRange < theSecKeys.length) {
                    theResumeInfo.setPrimResumeKey(null);
                    theResumeInfo.setSecResumeKey(null);
                    theResumeInfo.setDescResumeKey(null);
                    theMoreElements = true;
                    initIndexRange();
                }
            }

            theResumeInfo.setPrimResumeKey(null);
            theResumeInfo.setSecResumeKey(null);
            theResumeInfo.setDescResumeKey(null);
            theMoreElements = false;
        }

        boolean moveToNextEntryInCurrentScan(TableImpl forTable)
            throws SizeLimitException {

            boolean newTargetTableRow = false;

            while (true) {
                while (theScanner == null) {

                    theMoreElements = theSecScanner.next();

                    if (!theMoreElements) {
                        break;
                    }

                    theBinaryIndexKey = theSecScanner.getIndexKey().getData();
                    theBinaryPrimKey = theSecScanner.getPrimaryKey().getData();
                    theResumeInfo.setPrimResumeKey(theBinaryPrimKey);
                    theResumeInfo.setSecResumeKey(theBinaryIndexKey);

                    checkSizeLimit(theRCB, theSecOp);

                    if (theEliminateDups) {
                        BinaryValueImpl primKeyVal = FieldDefImpl.binaryDef.
                            createBinary(theBinaryPrimKey);

                        boolean added = thePrimKeysSet.add(primKeyVal);
                        if (!added) {
                            continue;
                        }
                    }

                    /*
                     * Must create a new index row each time, because a ref to
                     * the returned index row may be stored in the join path.
                     */
                    theIndexRow = theIndex.getIndexEntryDef().createRecord();

                    theIndex.rowFromIndexEntry(theIndexRow,
                                               theBinaryPrimKey,
                                               theBinaryIndexKey);

                    if (theRCB.getTraceLevel() >= 2) {
                        theRCB.trace("Produced index row : " + theIndexRow +
                                     " with binary key " +
                                     PlanIter.printByteArray(theBinaryIndexKey));

                    }

                    newTargetTableRow = true;

                    PartitionId pid = theHandlersManager.getRepNode().
                        getPartitionId(theBinaryPrimKey);

                    theOpHandler.initTableLists(
                            theTargetTables,
                            theTableInfo,
                            theTxn,
                            Direction.FORWARD,
                            theResumeInfo.getDescResumeKey());

                    theScanner = new Scanner(
                            theSecOp,
                            theTxn,
                            pid,
                            theOpHandler.getRepNode(),
                            theBinaryPrimKey, /*parentKey*/
                            true, /* MajorKeyComplete */
                            null, /*range*/
                            Depth.PARENT_AND_DESCENDANTS,
                            Direction.FORWARD,
                            theResumeInfo.getDescResumeKey(), /*resumeKey*/
                            theResumeInfo.getMoveAfterResumeKey(),
                            CURSOR_READ_COMMITTED,
                            LockMode.READ_UNCOMMITTED_ALL,
                            true/*keyonly*/);
                }

                while (theMoreElements && theScanner.next()) {

                    theBinaryPrimKey = theScanner.getKey().getData();
                    theResumeInfo.setDescResumeKey(theBinaryPrimKey);

                    checkSizeLimit(theRCB, theSecOp);

                    int match = MultiGetTableKeysHandler.
                        keyInTargetTable(theHandlersManager.getLogger(),
                                         theSecOp,
                                         theTargetTables,
                                         theTableInfo,
                                         (forTable != null ?
                                          forTable.getNumKeyComponents() :
                                          -1),
                                         theScanner.getKey(),
                                         theScanner.getData(),
                                         theScanner.getCursor(),
                                         theScanner.getLockMode(),
                                         true /* chargeReadCost */);

                    theBinaryPrimKey = theScanner.getKey().getData();
                    theResumeInfo.setDescResumeKey(theBinaryPrimKey);

                    checkSizeLimit(theRCB, theSecOp);

                    if (match <= 0) {
                        if (match < 0) {
                            break;
                        }
                        continue;
                    }

                    theTable = theTableInfo.getCurrentTable();
                    theTableRow = theTable.createRow();

                    if (!theTable.initRowFromKeyBytes(theBinaryPrimKey,
                                                      -1, /*initPos*/
                                                      theTableRow)) {
                        continue;
                    }

                    if (theRCB.getTraceLevel() >= 2) {
                        theRCB.trace("Produced prim index row : " + theTableRow);
                    }

                    if (newTargetTableRow) {
                        newTargetTableRow = false;

                        /*
                         * Lock the target-table row and check if the sec-index
                         * entry still points to this row. If not, move to the
                         * next sec-index entry.
                         */
                        if (!theSecScanner.getCurrent()) {
                            theScanner.close();
                            theScanner = null;

                            return moveToNextEntryInCurrentScan(forTable);
                        }
                    }

                    return true;
                }

                if (theMoreElements) {
                    theScanner.close();
                    theScanner = null;
                    theResumeInfo.setDescResumeKey(null);
                    continue;
                }

                break;
            }

            return false;
        }
    }

    /**
     * Check if current read KB exceeds the read size limit or not, throw
     * SizeLimitException if true.
     */
    private static void checkSizeLimit(
        RuntimeControlBlock rcb,
        InternalOperation op) throws SizeLimitException {

        checkSizeLimit(rcb, op, 0);
    }

    /**
     * Check if reading nBytes will exceed the read size limit, throw
     * SizeLimitException if true.
     */
    private static void checkSizeLimit(
        RuntimeControlBlock rcb,
        InternalOperation op,
        int currentBytes) throws SizeLimitException {

        if (rcb.getMaxReadKB() == 0) {
            return;
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Checking Size Limit. Current size = " +
                      op.getReadKB() + " additional bytes = " +
                      InternalOperation.toKBytes(currentBytes) +
                      " Max Size = " + rcb.getCurrentMaxReadKB());
        }

        int inc = InternalOperation.toKBytes(currentBytes);

        if (op.getReadKB() + inc > rcb.getCurrentMaxReadKB()) {
            throw new SizeLimitException();
        }
    }

    /**
     * A utility exception used to indicate that the readKB of a operation
     * exceeds the size limit.
     */
    @SuppressWarnings("serial")
    static class SizeLimitException extends Exception {

        /* Ture if the exception is throw after read the current entry */
        private boolean afterReadEntry;

        SizeLimitException() {
            this(false);
        }

        SizeLimitException(boolean afterReadEntry) {
            this.afterReadEntry = afterReadEntry;
        }

        boolean getAfterReadEntry() {
            return afterReadEntry;
        }
    }
}

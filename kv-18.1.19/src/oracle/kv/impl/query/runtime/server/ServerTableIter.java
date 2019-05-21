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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_6;

import java.io.DataOutput;
import java.util.Arrays;
import java.util.HashSet;

import oracle.kv.impl.api.table.BinaryValueImpl;
import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexKeyImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.impl.query.runtime.BaseTableIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.PlanIterState;
import oracle.kv.impl.query.runtime.PlanIterState.StateEnum;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.query.runtime.server.TableScannerFactory.TableScanner;
import oracle.kv.impl.query.runtime.server.TableScannerFactory.AncestorScanner;
import oracle.kv.impl.query.runtime.server.TableScannerFactory.SizeLimitException;
import oracle.kv.table.FieldRange;
import oracle.kv.Key;

/**
 * ServerTableIter is used at runtime only, to execute a table scan (via the
 * primary or a secondary index) in the server context. This class exists so
 * that it can include code that uses server-only classes that are not available
 * on the client side. It is created by BaseTableIter during its open() call.
 * The BaseTableIter serves as a proxy for this implementation.
 *
 * Given that ServerTableIter is created during open(), it does not need to
 * store its dunamic state seprately; the ServerTableIter itself is "state".
 * So, ServerTableIter uses just the basic PlanIterState.
 *
 * ServerTableIter implements access to a single table as well as the
 * left-outer-join semantics of the NESTED TABLES clause.
 *
 * Some terminology for the NESTED TABLES case:
 *
 * - An "A" table is a table appearing in the ANCESTORS part of the NESTED
 *   TABLES.
 * - An "D" table is a table appearing in the DESCENDANTS part of the NESTED
 *   TABLES, or the target table.
 * - The "join tree" is a projection of the table hierarchy tree that contains
 *   the tables specified in the NESTED TABLES. For example, consider the
 *   following table hierarchy:
 *
 *                  A
 *               /  |  \
 *              /   |   \
 *            B     F     G
 *          /  \         /  \
 *         C    E       H    J
 *        /                   \
 *       D                     K
 *
 *  and this FROM clause: FROM NESTED TABLES(A descendatns(B, D, J)). Then the
 *  join tree is:
 *
 *                   A
 *                 /   \
 *               B      J
 *              /
 *             D
 *
 *   Here is another join tree from the same table hierarchy:
 *
 *                   A
 *                 /   \
 *               B      K
 *              /
 *             C
 *
 *   We will refer to the above join trees as "sample tree1" and "sample
 *   tree2" and use them as examples when we explain the NESTED TABLES
 *   algorithm below.
 *
 * - In this implementation, the terms "parent table" and "child table" will
 *   refer to parent/child  tables in the context of the join tree, not the
 *   table hierarchy tree. Similarly, the ancestor/descendant relationship is
 *   defined here with respect to the join tree.
 *
 * theFactory:
 * See javadoc for TableScannerFactory.
 *
 * theTables:
 * An array containing a TableImpl instance for each of the tables accessed
 * by "this". The position of the TableImpls within this array server as
 * local table ids. The tables are sorted in the order that they would be
 * encountered in a depth-first traversal of the table hierarchy.
 *
 * theTargetTable:
 * The target table of a NESTED TABLES clause, or the single table referenced
 * in the FROM clause.
 *
 * theTargetTablePos:
 * The position of the target table in theTables (it's equal to theNumAncestors)
 *
 * theLockIndexEntries:
 * This boolean field is used to implement a small optimization for the
 * following case: (a) "this" accesses a single table, and (b) the index used
 * to access the table is covering, and (c) there is no filtering predicate.
 * In this case, we should be locking the index entries during the scan, rather
 * than doing dirty reads and then locking the row (which, in case of secondary
 * indexes, involves a lookup of the primary index). Locking the index entries
 * upfront is ok because all of these entries need to be returned. In this case,
 * theLockIndexEntries will be set to true.
 *
 * Note that in case of a covering index with filtering, we do dirty reads as
 * we scan the index. If an index entry survives the filtering, then we lock
 * that entry (but not the associated table row). We don't need to recheck the
 * filtering predicate because as long as the JE cursor is on that entry, its
 *  key cannot change, it can only be marked as deleted, in which case the lock
 * call will fail and the entry will be skipped.
 *
 * theRTPrimKeys:
 * theRTSecKeys:
 * theRTRanges:
 * These 3 fields are the runtime versions of thePrimKeys, theSecKeys, and
 * theRanges fields of BaseTableIter. theRTPrimKeys and theRTSecKeys are
 * instances of PrimaryKeyImpl and IndexKeyImpl respectively (instead of
 * RecordValueImpl). Furthermore, any external-variable placeholders that
 * may appear in the compile-time versions of these 3 fields have been
 * replaced with actual values in the runtime versions.
 *
 * theIndexEntryDef:
 *
 * theAlwaysFalse:
 *
 * theJoinAncestors:
 * For each D-table T, the D-tables that are ancestors of T in the join tree.
 *
 * theJoinLeaves:
 * For each table, true/false if the table is/is-not a leaf in the join tree.
 *
 * theLinearJoin:
 * True if the join tree is a lineer path. If so, then if a table T in the path
 * does not satisfy the join or the ON predicates, we can skip all rows from
 * tables under T by using the max-key-components optimization (see
 * MultiTableOperationHandler.keyInTargetTable()).
 *
 * theScanner:
 * The scanner used to access the rows of the D tables. It scanns either the
 * primary index (which contains the rows of all tables), or a secondry index
 * of the target table (in which case, the target table is the only D table).
 * (see javadoc of TableScannerFactory). The row that theScanner is currently
 * positioned on is called the "current row", and its containing table is the
 * "current table".
 *
 * theJoinPathLength:
 * The current length of theJoinPath.
 *
 * theJoinPath:
 * A linear path in the join tree containing D-table rows and/or keys that
 * have been scanned already, but for which we don't know yet whether they will
 * participate in a result or not. All the rows in the path satisfy the join
 * and ON preds, so theJoinPath (together with theAncestorsPath) is essentially
 * storing a partial potential result.
 *
 * If the current row satisfies the join and ON preds, it is added to
 * theJoinPath. If the current table is a leaf in the join tree, theJoinPath
 * holds an actual result. We call this a "current result", because it
 * contains the current row (as well as rows from all the ancestors of the
 * current table) and is generated during the processing of the current row.
 * But theJoinPath may store an actual result even if its leaf node is not a
 * join-tree leaf; this is the case when we know (based on the position of the
 * current row relative to the rows in theJoinPath) that there are no rows that
 * may join with the leaf node in theJoinPath. We call this a "delayed result",
 * because it is generated after its participating rows have already been
 * scanned and processed. For example, consider the join tree shown above, and
 * let theJoinPath contain rows RA and RB from tables A and B respectively. If
 * the current row RJ is from table J, RB cannot match with any following rows,
 * and if RB did not also match with any preceding rows, the delayed result
 * [RA, RB, NULL, NULL] must be generated.
 *
 * theAncestorsPath:
 * It stores the A-table rows/keys for the current target-table row (the row at
 * the root of theJoinPath). Note that we don't keep track of the length of this
 * path, because all the A-rows are added together, so the path is always
 * "full".
 *
 * theSavedResult:
 * When theJoinPath has a delayed result, we cannot return that result to the
 * caller immediatelly, because we have to process the current row first (which
 * will not participate in theJoinPath result). In this case theSavedResult is
 * used to store theJoinPath result until we can actually return this result.
 * We have to do this because processing the current row may modify theJoinPath
 * (and the tuple registers).
 *
 * theNextTable:
 * Used to take advantage of the max-key-components optimization. When non-null
 * we know that no rows with more primary key components than those of
 * theNextTable will participate in the next result. So we can skip such rows.
 *
 * theHaveResult:
 * If true, the previous invocation of next() produced 2 results: a delayed one
 * and a "normal" one. Only the delayed result was returned, so in the current
 * invocation of next() we must return the normal result, which is stored in
 * thejoinPath. Here is an example that shows how 2 results may be produced:
 *
 * NESTED TABLES(A descendatns(B, C, G)).
 *
 * During a next() invocation, we get a row RG from G and theJoinPath contains
 * rows RA and RB from A abd B respectivelly. The RG row will cause a delayed
 * result to be produced: (RA, RB, NULL, NULL). However, (RA, NULL, NULL, RG)
 * is also a result.
 *
 * theInResume:
 * True while the ServerTableIter is in resume.
 *
 * thePrimKeysSet:
 * Used for duplicate row elimination.
 */
public class ServerTableIter extends BaseTableIter {

    private static class JoinPathNode {

        private int tablePos = -1;

        private byte[] primKeyBytes;

        private FieldValueImpl row;

        boolean matched;

        void reset(int pos, FieldValueImpl inRow, byte[] pk) {
            this.tablePos = pos;
            this.row = inRow;
            this.primKeyBytes = pk;
            this.matched = false;
        }

        @Override
        public String toString() {
            return ("tablePos = " + tablePos + " matched = " + matched +
                    " Row = \n" + row);
        }
    }

    static final FieldValueImpl theNULL = NullValueImpl.getInstance();

    /*
     * The following fields are created during the construction of "this"
     * and remain constant afterwards.
     */

    private final TableScannerFactory theFactory;

    private final TableImpl[] theTables;

    private final TableImpl theTargetTable;

    private final int theTargetTablePos;

    private final boolean theLockIndexEntries;

    private PrimaryKeyImpl[] theRTPrimKeys;

    private IndexKeyImpl[] theRTSecKeys;

    private FieldRange[] theRTRanges;

    protected RecordDefImpl theIndexEntryDef;

    private boolean theAlwaysFalse;

    private int[][] theJoinAncestors;

    private boolean[] theJoinLeaves;

    private boolean theLinearJoin;

    /*
     * The following fields store dynamic state used during the operation
     * of this iter.
     */

    private TableScanner theScanner;

    private int theJoinPathLength;

    private JoinPathNode[] theJoinPath;

    private byte[] theJoinPathSecKey;

    private JoinPathNode[] theAncestorsPath;

    private FieldValueImpl[] theSavedResult;

    private TableImpl theNextTable;

    private boolean theHaveResult;

    private boolean theInResume;

    final HashSet<BinaryValueImpl> thePrimKeysSet;

    ServerTableIter(
        RuntimeControlBlock rcb,
        ServerIterFactoryImpl opCtx,
        BaseTableIter parent) {

        super(parent);

        theFactory = new TableScannerFactory(rcb,
                                             opCtx.getTxn(),
                                             opCtx.getPartitionId(),
                                             opCtx.getOperationHandler());

        TableMetadataHelper md =  rcb.getMetadataHelper();

        int numTables = theTableNames.length;

        theTables = new TableImpl[numTables];

        for (int i = 0; i < theTableNames.length; ++i) {
            theTables[i] = md.getTable(theNamespace, theTableNames[i]);

            if (theTables[i] == null) {
                String name = TableMetadata.makeNamespaceName(theNamespace,
                                                              theTableNames[i]);
                throw new IllegalArgumentException(
                      "Table does not exist: " + name);
            }
        }

        theTargetTable = theTables[theNumAncestors];
        theTargetTablePos = theNumAncestors;

        theLockIndexEntries = (numTables == 1 &&
                               theUsesCoveringIndex[0] &&
                               thePredIters[0] == null);

        computeScanBoundatries(rcb);

        initJoins();

        if (theEliminateIndexDups && getTargetTablePred() != null) {
            thePrimKeysSet = new HashSet<BinaryValueImpl>(1000);
        } else {
            thePrimKeysSet = null;
        }
    }

    private void computeScanBoundatries(RuntimeControlBlock rcb) {

        TableImpl table = theTargetTable;
        IndexImpl index = (theIndexName != null ?
                           (IndexImpl)table.getIndex(theIndexName) :
                           null);
        int numRanges;

        if (index != null) {
            numRanges = theSecKeys.length;
            theRTSecKeys = new IndexKeyImpl[numRanges];

            for (int k = 0; k < numRanges; ++k) {
                theRTSecKeys[k] = index.
                    createIndexKeyFromFlattenedRecord(theSecKeys[k]);
            }
            theRTPrimKeys = null;
            theIndexEntryDef = index.getIndexEntryDef();
        } else {
            assert(thePrimKeys != null);
            numRanges = thePrimKeys.length;
            theRTPrimKeys = new PrimaryKeyImpl[numRanges];

            for (int k = 0; k < numRanges; ++k) {
                theRTPrimKeys[k] = table.createPrimaryKey(thePrimKeys[k]);
            }
            theRTSecKeys = null;
            theIndexEntryDef = table.getRowDef();
        }

        theRTRanges = new FieldRange[numRanges];

        for (int k = 0; k < numRanges; ++k) {

            PlanIter[] pushedExternals = thePushedExternals[k];

            if (pushedExternals == null ||
                pushedExternals.length == 0) {
                theRTRanges[k] = theRanges[k];
                continue;
            }

            FieldValueImpl val;
            int size = pushedExternals.length;

            /* Compute external expressions in the current FieldRange */
            if (theRanges[k] != null) {

                FieldRange range = theRanges[k].clone();

                PlanIter lowIter = pushedExternals[size-2];
                PlanIter highIter = pushedExternals[size-1];

                size -= 2;

                if (lowIter != null) {

                    val = computeExternalKey(rcb, lowIter, table, index,
                                             size, FuncCode.OP_GE);
                    if (theAlwaysFalse) {
                        return;
                    }

                    range.setStart(val, range.getStartInclusive(), false);
                }

                if (highIter != null) {

                    val = computeExternalKey(rcb, highIter, table, index,
                                             size, FuncCode.OP_LE);
                    if (theAlwaysFalse) {
                        return;
                    }

                    range.setEnd(val, range.getEndInclusive(), false);
                }

                if (!range.check()) {
                    theAlwaysFalse = true;
                    return;
                }

                if (range.getStart() != null || range.getEnd() != null) {
                    theRTRanges[k] = range;
                } else {
                    theRTRanges[k] = null;
                }

            } else {
                theRTRanges[k] = null;
            }

            /*
             * Compute external expressions in each components of the current
             * primary or index key
             */
            for (int i = 0; i < size; ++i) {

                PlanIter extIter = pushedExternals[i];

                if (extIter == null) {
                    continue;
                }

                val = computeExternalKey(rcb, extIter, table, index,
                                         i, FuncCode.OP_EQ);

                if (theAlwaysFalse) {
                    return;
                }

                if (index != null) {
                    theRTSecKeys[k].put(i, val);
                } else {
                    theRTPrimKeys[k].put(i, val);
                }
            }
        }
    }

    private FieldValueImpl computeExternalKey(
        RuntimeControlBlock rcb,
        PlanIter iter,
        TableImpl table,
        IndexImpl index,
        int keyPos,
        FuncCode compOp) {

        iter.open(rcb);
        iter.next(rcb);
        FieldValueImpl val = rcb.getRegVal(iter.getResultReg());
        iter.close(rcb);

        FieldValueImpl newVal = BaseTableIter.castValueToIndexKey(
            table, index, keyPos, val, compOp);

        if (newVal != val) {

            if (newVal == BooleanValueImpl.falseValue) {
                theAlwaysFalse = true;
                return newVal;
            }

            if (newVal == BooleanValueImpl.trueValue) {
                val = null;
            } else {
                val = newVal;
            }
        }

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("Computed external key: " + val);
        }

        return val;
    }

    private void initJoins() {

        int numTables = theTableNames.length;

        theJoinLeaves = new boolean[numTables];
        theJoinAncestors = new int[numTables][];
        theJoinAncestors[theTargetTablePos] = new int[0];
        theJoinLeaves[theTargetTablePos] = true;

        for (int i = theTargetTablePos + 1; i < numTables; ++i) {

            TableImpl table = theTables[i];
            theJoinLeaves[i] = true;

            int j;
            for (j = i-1; j >= theTargetTablePos; --j) {

                if (TableImpl.isAncestorOf(table, theTables[j])) {
                    theJoinLeaves[j] = false;
                    int numAncestorsOfParent = theJoinAncestors[j].length;
                    theJoinAncestors[i] = new int[1 + numAncestorsOfParent];
                    for (int k = 0; k < numAncestorsOfParent; ++k) {
                        theJoinAncestors[i][k] = theJoinAncestors[j][k];
                    }
                    theJoinAncestors[i][numAncestorsOfParent] = j;
                    break;
                }
            }

            assert(j >= theTargetTablePos);
        }

        int numLeaves = 0;

        for (int i = theTargetTablePos; i < numTables && numLeaves < 2; ++i) {
            if (theJoinLeaves[i]) {
                ++numLeaves;
            }
        }

        if (numLeaves == 1) {
            theLinearJoin = true;
        } else {
            theLinearJoin = false;
        }

        if (theNumDescendants > 0 || theNumAncestors > 0) {

            theSavedResult = new FieldValueImpl[numTables];
            theJoinPathLength = 0;
            theJoinPath = new JoinPathNode[theNumDescendants + 1];

            for (int i = 0; i <= theNumDescendants; ++i) {
                theJoinPath[i] = new JoinPathNode();
            }
        } else {
            theSavedResult = null;
            theJoinPath = null;
        }

        if (theNumAncestors > 0) {
            theAncestorsPath = new JoinPathNode[theNumAncestors];
            for (int i = 0; i < theNumAncestors; ++i) {
                theAncestorsPath[i] = new JoinPathNode();
            }
        } else {
            theAncestorsPath = null;
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion) {
        throwCannotCall("writeFastExternal");
    }

   private int findTablePos(TableImpl table, RecordValueImpl indexRow) {

        for (int i = 0; i < theTables.length; ++i) {
            if (theTables[i] == table) {
                return i;
            }
        }

        throw new QueryStateException(
            "Key does not belong to any table specified in " +
            " a NESTED TABLE clause. Row:\n" + indexRow);
    }

    private int getJoinParent(int table) {
        return theJoinAncestors[table][theJoinAncestors[table].length - 1];
    }

    @Override
    public int[] getTupleRegs() {
        throwCannotCall("getTupleRegs");
        return null;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        PlanIterState state = new PlanIterState();
        rcb.setState(theStatePos, state);

        TupleValue tuple = new TupleValue(
            theTypeDef, rcb.getRegisters(), theTupleRegs);

        rcb.setRegVal(theResultReg, tuple);

        if (theIndexTupleRegs != null) {
            TupleValue idxtuple = new TupleValue(theIndexEntryDef,
                                                 rcb.getRegisters(),
                                                 theIndexTupleRegs);

            rcb.setRegVal(theIndexResultReg, idxtuple);

            if (rcb.getTraceLevel() >= 4) {
                rcb.trace("Set result register for index var: " +
                          theIndexResultReg);
            }
        }

        for (int i = 0; i < thePredIters.length; ++ i) {
            if (thePredIters[i] != null) {
                thePredIters[i].open(rcb);
            }
        }

        if (theAlwaysFalse) {
            state.done();
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (theScanner == null && !theAlwaysFalse) {
            /*
             * This call will return an index scanner if secKey is not null,
             * otherwise it returns a primary key scanner.
             */
            theScanner = theFactory.getTableScanner(
                    theDirection,
                    theTables,
                    theNumAncestors,
                    theRTPrimKeys,
                    theRTSecKeys,
                    theRTRanges,
                    (theEliminateIndexDups && getTargetTablePred() == null),
                    theIsUpdate,
                    theLockIndexEntries,
                    theVersion);
        }

        boolean more;

        if (theNumDescendants > 0 || theNumAncestors > 0) {

            if (theHaveResult) {
                produceResult(rcb);
                theHaveResult = false;
                rcb.getResumeInfo().setMoveAfterResumeKey(true);
                more = true;
            } else {
                more = nestedTablesNext(rcb, state);
            }

            if (more) {

                /*
                 * Populate the regs with the results. Do the target table
                 * first. If it's using a covering secondary index, populate
                 * theIndexTupleRegs with the fields of the index entry.
                 * Otherwise populate theTupleRegs.
                 */
                if (theUsesCoveringIndex[theTargetTablePos] &&
                    thePrimKeys == null) {
                    RecordValueImpl idxRow = (RecordValueImpl)
                        theSavedResult[theTargetTablePos];
                    for (int i = 0; i < idxRow.getNumFields(); ++i) {
                        rcb.setRegVal(theIndexTupleRegs[i], idxRow.get(i));
                    }
                } else {
                    rcb.setRegVal(theTupleRegs[theTargetTablePos],
                                  theSavedResult[theTargetTablePos]);
                }

                for (int i = 0; i < theTables.length; ++i) {
                    if (i == theTargetTablePos) {
                        continue;
                    }
                    rcb.setRegVal(theTupleRegs[i], theSavedResult[i]);
                }
            }

            return more;
        }

        return simpleNext(rcb, state);
    }

    public boolean simpleNext(RuntimeControlBlock rcb, PlanIterState state) {

        RecordValueImpl indexRow = null;
        byte[] resumeKey = null;
        boolean firstCall = false;

        if (state.isOpen()) {
            state.setState(StateEnum.RUNNING);
            firstCall = true;
        }

        PlanIter filterIter = getTargetTablePred();

        int[] tupleRegs =
            (thePrimKeys == null && theVersion >= QUERY_VERSION_6 ?
             theIndexTupleRegs : theTupleRegs);

        try {
            indexRow = theScanner.nextIndexRow(null);

            while (indexRow != null) {

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Current index row :\n " + indexRow);
                }

                if (filterIter != null) {
                    /* Populate the appropriate registers */
                    for (int i = 0; i < indexRow.getNumFields(); ++i) {
                        rcb.setRegVal(tupleRegs[i], indexRow.get(i));
                    }

                    /* Compute the filter condition */
                    boolean match = filterIter.next(rcb);
                    boolean duplicate = false;

                    if (match) {
                        FieldValueImpl val =
                            rcb.getRegVal(filterIter.getResultReg());
                        match = (val.isNull() ? false : val.getBoolean());
                    }

                    filterIter.reset(rcb);

                    if (!match) {
                        indexRow = theScanner.nextIndexRow(theTargetTable);
                        continue;
                    }

                    /*
                     * If index filtering preds exist and duplicate elimination
                     * is required, do it after applying the filtering preds.
                     * Otherwise the following may happen: An index entry I1 is
                     * the 1st entry encountered with prim key K. So, I1 is not
                     * a duplicate. Assume I1 does not satisfy the filtering
                     * preds so it is skipped, but it has been inserted in the
                     * hash table. Then entry I2 is scanned and it also has prim
                     * key K. I2 will be eliminated as a duplicate, even though
                     * it may satisfy the filtering preds.
                     */
                    if (theEliminateIndexDups) {
                        BinaryValueImpl primKeyVal =
                            FieldDefImpl.binaryDef.
                            createBinary(theScanner.getPrimKeyBytes());

                        duplicate = !thePrimKeysSet.add(primKeyVal);
                    }

                    if (duplicate) {
                        indexRow = theScanner.nextIndexRow(theTargetTable);
                        continue;
                    }
                }

                if (theUsesCoveringIndex[0]) {
                    if (theLockIndexEntries || theScanner.lockIndexRow()) {

                        for (int i = 0; i < indexRow.getNumFields(); ++i) {
                            rcb.setRegVal(tupleRegs[i], indexRow.get(i));
                        }

                        return true;
                    }
                } else {
                    RowImpl tableRow = theScanner.currentTableRow();

                    if (tableRow != null) {

                        if (rcb.getTraceLevel() >= 2) {
                            rcb.trace("Produced row: " + tableRow);
                        }

                        if (!theIsUpdate) {

                            for (int i = 0; i < tableRow.getNumFields(); ++i) {
                                rcb.setRegVal(theTupleRegs[i], tableRow.get(i));
                            }

                            TupleValue tv = (TupleValue)
                                rcb.getRegVal(theResultReg);
                            tv.setExpirationTime(tableRow.getExpirationTime());
                            tv.setVersion(tableRow.getVersion());
                        } else {
                            rcb.setRegVal(theResultReg, tableRow);
                        }

                        return true;
                    }
                }

                indexRow = theScanner.nextIndexRow(theTargetTable);
            }
        } catch (SizeLimitException sle) {

            ResumeInfo ri = rcb.getResumeInfo();

            if (firstCall &&
                rcb.getMaxReadKB() == rcb.getCurrentMaxReadKB() &&
                Arrays.equals(ri.getPrimResumeKey(), resumeKey)) {
                throw new QueryException(
                    "Query cannot be executed further because the " +
                    "computation of a single result consumes " +
                    "more bytes than the maximum allowed.",
                    theLocation);
            }

            rcb.setReachedLimit(true);
            ri.setMoveAfterResumeKey(sle.getAfterReadEntry());
        }

        state.done();
        return false;
    }

    private boolean nestedTablesNext(
        RuntimeControlBlock rcb,
        PlanIterState state) {

        ResumeInfo ri = rcb.getResumeInfo();
        TableImpl table;
        RecordValueImpl indexRow = null;
        boolean producedResult = false;
        byte[] resumeKey = null;
        byte[] resumeKey2 = null;
        boolean firstCall = false;

        try {
            while (true) {

                producedResult = false;

                /*
                 * Step 1: Get the next row to consider. Let R be this row and
                 * T be its containing table.
                 */
                if (state.isOpen()) {
                    state.setState(StateEnum.RUNNING);
                    firstCall = true;
                    resumeKey = ri.getPrimResumeKey();
                    resumeKey2 = ri.getDescResumeKey();

                    if (ri.getPrimResumeKey() != null) {
                        indexRow = resume(rcb);
                    } else {
                        indexRow = theScanner.nextIndexRow(theTargetTable);
                    }
                } else {
                    indexRow = theScanner.nextIndexRow(theNextTable);
                }

                if (indexRow == null) {
                    break;
                }

                /* (Re)set the MoveAfterResumeKey flag to its "normal" value */
                ri.setMoveAfterResumeKey(true);

                theNextTable = null;
                boolean match = true;

                table = theScanner.getTable();
                int tablePos = findTablePos(table, indexRow);

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Current index row for table " +
                              table.getFullName() + " : \n" + indexRow);
                }

                /*
                 * Step 2: theJoinPath is empty. If T is not the target table,
                 * we can skip R and all subsequent rows until we get the next
                 * target-table row. Otherwise, if T is the target table, then
                 * (a) If there are any A tables, get the A rows associated
                 *     with R.
                 * (b) Apply the filtering pred, if any, on R. If pred not
                 *     satisfied skip R and all rows until next target-table
                 *     row. Else add R to theJoinPath and if there are no
                 *     other D tables, produce a result and return true.
                 *     Else, continue with next row.
                 *
                 * Note: we get the A rows before applying the filter pred
                 * in order to avoid deadlock. Otherwise, if the pred is
                 * satisfied, R will be locked when we go to get its A rows,
                 * so we will be holding 2 locks at a time (acquired in
                 * reverse order). TODO: optimize for the case when the
                 * filtering pred is not satisfied? To do this we need a JE
                 * api to release the lock held by a cursor, without closing
                 * the cursor.
                 */
                if (theJoinPathLength == 0) {

                    if (tablePos != theTargetTablePos) {
                        theNextTable = theTargetTable;
                        continue;
                    }

                    addAncestorValues(rcb, theScanner.getPrimKeyBytes());

                    match = addRowToJoinPath(rcb, theScanner, theJoinPath,
                                             0, tablePos, indexRow);

                    if (!match) {
                        theNextTable = table;
                    } else if (theJoinLeaves[tablePos]) {
                        produceResult(rcb);
                        return true;
                    }

                    continue;
                }

                /*
                 * Step 3: Find the closest ancestor to T that is in theJoinPath.
                 * Let AT be this table. Notice that if T is in theJoinPath
                 * already, AT is T itself. ancJPP is the position of AT within
                 * theJoinPath.
                 */
                int ancJPP = getAncestorPosInJoinPath(tablePos);
                int ancTablePos = theJoinPath[ancJPP].tablePos;
                TableImpl ancTable = theTables[ancTablePos];

                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("Join path ancestor for table " +
                              table.getFullName() + " is table " +
                              ancTable.getFullName() +
                              ", at join path pos = " + ancJPP +
                              ". Ancestor node = " + theJoinPath[ancJPP]);
                }

                /*
                 * Step 4: Produce delayed result, if necessary. Let L be the
                 * leaf node in the join path. If T is not a proper descendant
                 * of L table, the L row cannot match with R or any row after R,
                 * so if L has not matched with any preceding rows, theJoinPath
                 * stores a delayed result. We save this result until we are
                 * done processing R and then return it to the caller. To handle
                 * the case where this result turns out to be the last result of
                 * the current batch, we set the MoveAfterResumeKey flag to
                 * false so that the next batch will start with row R.
                 */
                if ((ancJPP < theJoinPathLength-1 || ancTablePos == tablePos) &&
                    !theJoinPath[theJoinPathLength-1].matched) {
                    produceResult(rcb);
                    producedResult = true;
                    ri.setMoveAfterResumeKey(false);
                }

                /*
                 * Step 5: If T is not a child of any table in theJoinPath, R
                 * does not join with any table, so we skip it. Furthermore,
                 * we truncate theJoinPath by throwing away any nodes below
                 * ancJPP, since these nodes cannot match with any row after R.
                 */
                if (tablePos != ancTablePos &&
                    getJoinParent(tablePos) != ancTablePos) {

                    theJoinPathLength = ancJPP + 1;

                    if (producedResult) {
                        return true;
                    }

                    continue;
                }

                /*
                 * Step 6: If T is a child of a table P in theJoinPath (T itself
                 * may or may not be in the join path) we check whether R and
                 * the P row satisfy the join predicate.
                 */
                if (tablePos != theTargetTablePos) {
                    match = doJoin(rcb,
                                   ancJPP - (ancTablePos == tablePos ? 1 : 0));
                }

                /*
                 * Step 7: If the join failed, R is skipped. However, we must
                 * first check for a delayed result. For example, consider our
                 * sample tree1, let theJoinPath contain rows RA and RB, and
                 * the current row be RD (R = RD, T = D). RD does not match
                 * with RB, and neither will any row after RD, so, if RB has
                 * not been matched already, we have a delayed result. This
                 * case is not captured in step 4 above.
                 *
                 * If T is a child of the target table, the fact that R does
                 * not match with the current target-table row means that we
                 * have moved past that row and all its descendant rows, so
                 * we can skip all rows until the next target-table row.
                 * Otherwise, if the join tree is just a linear path, we can
                 * skip all rows under T.
                 *
                 * Note: If the join is not linear, we cannot skip any rows
                 * using the max-key-components optimization. To see why,
                 * consider sample join tree2. Assume that we have a C row that
                 * does not match with the B row in theJoinPath. If we skip all
                 * rows having more key components than C, we may skip a K row
                 * that matches with the current A row in theJoinPath.
                 */
                if (!match) {

                    if (getJoinParent(tablePos) == theTargetTablePos) {
                        theNextTable = theTargetTable;
                    } else if (theLinearJoin) {
                        theNextTable = table;
                    }

                    if (!theJoinPath[theJoinPathLength-1].matched) {
                        produceResult(rcb);
                        producedResult = true;
                    }

                    if (producedResult) {
                        return true;
                    }

                    continue;
                }

                /*
                 * Step 8: If T is the target table, add the matching ancestor
                 * rows. As before, in order to avoid deadlocks, this is done
                 * before locking R and adding it to theJoinPath.
                 */
                if (tablePos == theTargetTablePos) {
                    addAncestorValues(rcb, theScanner.getPrimKeyBytes());
                }

                /*
                 * Step 9: Apply the ON/filtering pred, if any, on R. If the pred
                 * is not satisfied skip R, after returning any delayed result.
                 * Otherwise, lock and add R to theJoinPath.
                 *
                 * We can apply the max-key-componentes optimization to skip
                 * more rows than R if the join is linear, or T is the target
                 * table.
                 */
                match = addRowToJoinPath(rcb, theScanner, theJoinPath,
                                         ancJPP +
                                         (ancTablePos == tablePos ? 0 : 1),
                                         tablePos, indexRow);
                if (!match) {

                    if (tablePos == theTargetTablePos || theLinearJoin) {
                        theNextTable = table;
                    }

                    if (producedResult) {
                        return true;
                    }
                    continue;
                }

                /*
                 * Step 10: At this point R has been added to theJoinPath. If
                 * T is a leaf of the join tree, we have a "current" result,
                 * which we can return immediatelly.
                 */
                if (theJoinLeaves[tablePos]) {
                    assert(tablePos ==
                           theJoinPath[theJoinPathLength-1].tablePos);
                    if (!producedResult) {
                        produceResult(rcb);
                    } else {
                        theHaveResult = true;
                        ri.setMoveAfterResumeKey(false);
                    }

                    return true;
                }

                /*
                 * Step 11: Return any delayed rsult.
                 */
                if (producedResult) {
                    return true;
                }
            }

        } catch (SizeLimitException sle) {

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Resuming due to SizeLimitException");
            }

            /*
             * If an SLE occurs during the 1st call to next() for the current
             * incarnation of the query at a server, and the read limit is the
             * max allowed, and no progress has been done, throw QE.
             */
            if (firstCall &&
                rcb.getMaxReadKB() == rcb.getCurrentMaxReadKB() &&
                Arrays.equals(ri.getPrimResumeKey(), resumeKey) &&
                Arrays.equals(ri.getDescResumeKey(), resumeKey2)) {
                throw new QueryException(
                    "Query cannot be executed further because the " +
                    "computation of a single result consumes " +
                    "more bytes than the maximum allowed." +
                    theLocation);
            }

            rcb.setReachedLimit(true);
            state.done();

            /*
             * If the SLE occurred after a delayed result was recognized,
             * return that result now.
             */
            if (producedResult) {
                return true;
            }

            /*
             * If the SLE occurred while in resume(), don't change the resume
             * info (we will resume with the same continuation key). Otherwise,
             * save the new join path and set theMoveAfterResumeKey to false
             * so that we will resume with the current row R.
             */
            if (!theInResume) {
                ri.setMoveAfterResumeKey(false);
                saveJoinPath(rcb);
            }

            return false;
        }

        /*
         * We are done. However, anything that remains in theJoinPath is a
         * delayed result that must be returned now.
         */
        state.done();

        if (theJoinPathLength > 0 &&
            !theJoinPath[theJoinPathLength-1].matched) {
            produceResult(rcb);
            return true;
        }

        return false;
    }

    private int getAncestorPosInJoinPath(int tablePos) {

        for (int i = theJoinPathLength - 1; i >= 0; --i) {

            int joinTablePos = theJoinPath[i].tablePos;

            if (tablePos == joinTablePos) {
                return i;
            }

            int[] ancestors = theJoinAncestors[tablePos];
            for (int j = ancestors.length - 1; j >= 0; --j) {
                if (ancestors[j] == joinTablePos) {
                    return i;
                }
            }
        }

        throw new QueryStateException(
            "Table does not have an ancestor in the join path. Table: " +
            theTables[tablePos].getFullName() + "\njoin path length = " +
            theJoinPathLength);
    }

    /**
     * Join the current row with the row in the jpp position of theJoinPath.
     * Return true if the rows match; false otherwise.
     */
    private boolean doJoin(RuntimeControlBlock rcb, int jpp) {

        TableImpl outerTable = theTables[theJoinPath[jpp].tablePos];

        byte[] outerKey = theJoinPath[jpp].primKeyBytes;
        byte[] innerKey = theScanner.getPrimKeyBytes();
        innerKey = Key.getPrefixKey(innerKey, outerTable.getNumKeyComponents());

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Join at path pos " + jpp +
                      "\nouter row:\n" + theJoinPath[jpp].row +
                      "\ninner row:\n" + theScanner.getIndexRow());
        }

        boolean res = Arrays.equals(innerKey, outerKey);

        if (!res && rcb.getTraceLevel() >= 2) {
            rcb.trace("Join failed");
        }

        return res;
    }

    /**
     * Add a row/key to a given path, which is either theJoinPath or
     * theAncestorsPath. The row is added only if it can be locked successfully
     * and it satisfies the ON/filtering pred associated with its table, if
     * any. The row/key is added at a given position in the path, and in the
     * case of theJoinPath, any nodes after that position are removed from the
     * path.
     */
    private boolean addRowToJoinPath(
        RuntimeControlBlock rcb,
        TableScanner scanner,
        JoinPathNode[] path,
        int pathPos,
        int tablePos,
        RecordValueImpl indexRow) throws SizeLimitException {

        boolean lockedRow = false;

        /*
         * tableRow will actually be set to indexRow, if the index is covering.
         * Otherwise, it will be set to a true table row (i.e., a RowImpl).
         */
        RecordValueImpl tableRow = null;

        if (thePredIters.length > 0 && thePredIters[tablePos] != null) {

            /*
             * Populate the appropriate registers for evaluating the pred.
             */
            if (tablePos == theTargetTablePos) {

                if (thePrimKeys != null) {
                    rcb.setRegVal(theTupleRegs[tablePos], indexRow);
                } else {
                    for (int i = 0; i < indexRow.getNumFields(); ++i) {
                        rcb.setRegVal(theIndexTupleRegs[i], indexRow.get(i));
                    }
                }

            } else {

                if (theUsesCoveringIndex[tablePos]) {
                    if (scanner.lockIndexRow()) {
                        tableRow = indexRow;
                        lockedRow = true;
                    }
                } else {
                    tableRow = scanner.currentTableRow();
                    if (tableRow != null) {
                        lockedRow = true;
                    }
                }

                if (!lockedRow) {
                    return false;
                }

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Evaluating ON predicate on : " +
                              tableRow);
                }

                for (int i = 0; i < theNumAncestors; ++i) {
                    rcb.setRegVal(theTupleRegs[i], theAncestorsPath[i].row);
                }

                if (path == theJoinPath && theJoinPathLength > 0) {

                    if (thePrimKeys == null &&
                        theUsesCoveringIndex[theTargetTablePos]) {

                        RecordValueImpl targetIndexRow = (RecordValueImpl)
                            theJoinPath[0].row;

                        for (int i = 0; i < targetIndexRow.getNumFields(); ++i) {
                            rcb.setRegVal(theIndexTupleRegs[i],
                                          targetIndexRow.get(i));
                        }
                    } else {
                        rcb.setRegVal(theTupleRegs[theTargetTablePos],
                                      theJoinPath[0].row);
                    }

                    for (int i = 1; i < theJoinPathLength; ++i) {
                        rcb.setRegVal(theTupleRegs[theJoinPath[i].tablePos],
                                      theJoinPath[i].row);
                    }
                }

                rcb.setRegVal(theTupleRegs[tablePos], tableRow);
            }

            /*
             * Evaluate the pred. Return false if not satified.
             */
            PlanIter predIter = thePredIters[tablePos];

            boolean match = predIter.next(rcb);

            if (match) {
                FieldValueImpl val = rcb.getRegVal(predIter.getResultReg());
                match = (val.isNull() ? false : val.getBoolean());
            }

            predIter.reset(rcb);

            if (!match) {
                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("ON predicate failed on table : " +
                              theTableNames[tablePos]);
                }
                return false;
            }

            /*
             * Eliminate duplicate rows, if necessary (see comment in
             * simpleNext()).
             */
            if (tablePos == theTargetTablePos && theEliminateIndexDups) {
                BinaryValueImpl primKeyVal =
                    FieldDefImpl.binaryDef.
                    createBinary(scanner.getPrimKeyBytes());

                match = !thePrimKeysSet.add(primKeyVal);
            }

            if (!match) {
                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Target table row is a duplicate");
                }
                return false;
            }
        }

        /*
         * Lock and read the row, if not done already
         */
        if (!lockedRow) {
            if (theUsesCoveringIndex[tablePos]) {
                if (scanner.lockIndexRow()) {
                    tableRow = indexRow;
                    lockedRow = true;
                }
            } else {
                tableRow = scanner.currentTableRow();
                if (tableRow != null) {
                    lockedRow = true;
                }
            }

            if (!lockedRow) {
                return false;
            }
        }

        /*
         * Add row to the given path.
         */
        path[pathPos].reset(tablePos, tableRow, scanner.getPrimKeyBytes());

        if (path == theJoinPath) {

            theJoinPathLength = pathPos + 1;

            if (pathPos > 0) {
                path[pathPos - 1].matched = true;
            }

            if (tablePos == theTargetTablePos &&
                theIndexName != null) {
                theJoinPathSecKey = scanner.getSecKeyBytes();
            }
        }

        if (rcb.getTraceLevel() >= 2) {
            String pathstr = (path == theJoinPath ?
                              "join path" :
                              "ancestors path");
            rcb.trace("Added node to " + pathstr + " at position : " + pathPos +
                      ". Node : " + path[pathPos]);
        }

        return true;
    }

    private void produceResult(RuntimeControlBlock rcb) {

        for (int i = 0; i < theNumAncestors; ++i) {
            theSavedResult[i] = theAncestorsPath[i].row;

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Saved anestor path row: " + theAncestorsPath[i].row);
            }
        }

        for (int i = theNumAncestors; i < theTables.length; ++i) {
            theSavedResult[i] = theNULL;
        }

        theJoinPath[theJoinPathLength-1].matched = true;

        saveJoinPath(rcb);

        /* Remove the last node from theJoinPath; it's not needed any more */
        --theJoinPathLength;
    }

    /**
     * Save info from the join path to the RCB, to be used as resume info.
     */
    private void saveJoinPath(RuntimeControlBlock rcb) {

        int[] joinPathTables = null;
        byte[] joinPathLastKey = null;
        boolean joinPathMatched = true;

        if (theJoinPathLength > 0) {

            joinPathTables = new int[theJoinPathLength];
            joinPathLastKey = theJoinPath[theJoinPathLength-1].primKeyBytes;
            joinPathMatched = theJoinPath[theJoinPathLength-1].matched;

            for (int i = 0; i < theJoinPathLength; ++i) {

                int table = theJoinPath[i].tablePos;

                theSavedResult[table] = theJoinPath[i].row;

                joinPathTables[i] = table;

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Saved join path row: " + theJoinPath[i].row);
                }
            }
        }

        rcb.getResumeInfo().setJoinPath(joinPathTables,
                                        joinPathLastKey,
                                        theJoinPathSecKey,
                                        joinPathMatched);
    }

    private void addAncestorValues(
        RuntimeControlBlock rcb,
        byte[] targetKey) throws SizeLimitException {

        boolean match = false;

        AncestorScanner ancScanner =
            theFactory.getAncestorScanner(theScanner.getOp());

        for (int i = 0; i < theNumAncestors; ++i) {

            TableImpl ancTable = theTables[i];

            byte[] ancKey =
                Key.getPrefixKey(targetKey, ancTable.getNumKeyComponents());

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Adding ancestor row for ancestor table " +
                          ancTable.getFullName() + " at pos " + i);
            }

            if (theAncestorsPath[i].tablePos >= 0 &&
                Arrays.equals(ancKey, theAncestorsPath[i].primKeyBytes)) {

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Ancestor row is already in ancestors path " +
                              ". Ancestor index row =\n" +
                              theAncestorsPath[i].row);
                }

                continue;
            }

            try {
                ancScanner.init(ancTable, null, ancKey, null);

                RecordValueImpl ancIndexRow = ancScanner.nextIndexRow(null);

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Ancestor row retrieved. " +
                              "Ancestor index row =\n" + ancIndexRow);
                }

                if (ancIndexRow == null) {
                    if (rcb.getTraceLevel() >= 2) {
                        rcb.trace("Join failed");
                    }
                    theAncestorsPath[i].reset(i, theNULL, null);
                    continue;
                }

                match = addRowToJoinPath(rcb, ancScanner,
                                         theAncestorsPath,
                                         i,
                                         i,
                                         ancIndexRow);
                if (!match) {
                    if (rcb.getTraceLevel() >= 2) {
                        rcb.trace("ON predicate failed on ancestor row");
                    }
                    theAncestorsPath[i].reset(i, theNULL, null);
                }
            } finally {
                ancScanner.close();
            }
        }
    }

    private RecordValueImpl resume(RuntimeControlBlock rcb)
        throws SizeLimitException {

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("RESUME STARTS");
        }

        theInResume = true;

        ResumeInfo ri = rcb.getResumeInfo();

        int[] joinPathTables = ri.getJoinPathTables();
        byte[] joinPathKey = ri.getJoinPathKey();
        int joinPathLen;
        TableImpl nextTable = null;

        if (joinPathKey == null) {
            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("RESUME DONE with null joinPathKey");
            }

            theInResume = false;

            return theScanner.nextIndexRow(theTables[theTargetTablePos]);
        }

        AncestorScanner ancScanner =
            theFactory.getAncestorScanner(theScanner.getOp());

        joinPathLen = joinPathTables.length;
        joinPathKey = ri.getJoinPathKey();

        for (int i = 0; i < joinPathLen; ++i) {

            int tablePos = joinPathTables[i];
            TableImpl table = theTables[tablePos];
            IndexImpl index = null;
            byte[] primKey = null;
            byte[] secKey = null;

            primKey = Key.getPrefixKey(joinPathKey,
                                       table.getNumKeyComponents());

            if (i == 0 && theIndexName != null) {
                index = (IndexImpl)table.getIndex(theIndexName);
                secKey = ri.getJoinPathSecKey();
            }

            ancScanner.init(table, index, primKey, secKey);

            try {
                RecordValueImpl ancIndexRow = ancScanner.nextIndexRow(null);

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Got index row:\n" + ancIndexRow +
                              "\nat join path pos " + i);
                }

                if (ancIndexRow == null ||
                    !addRowToJoinPath(rcb, ancScanner, theJoinPath,
                                      i, tablePos, ancIndexRow)) {

                    if (rcb.getTraceLevel() >= 2) {
                        rcb.trace("Could not find ancestor row for table " +
                                  table.getFullName() + " at join path pos " +
                                  i);
                    }

                    if (tablePos == theTargetTablePos || theLinearJoin) {
                        nextTable = table;
                    }

                    break;
                }

                if (tablePos == theTargetTablePos) {
                    addAncestorValues(rcb, ancScanner.getPrimKeyBytes());
                }
            } finally {
                ancScanner.close();
            }

            theJoinPath[joinPathLen-1].matched = ri.getJoinPathMatched();
        }

        theInResume = false;

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("RESUME DONE");
        }

        return theScanner.nextIndexRow(nextTable);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        state.reset(this);

        if (theScanner != null) {
            theScanner.close();
            theScanner = null;
        }

        theJoinPathLength = 0;
        theNextTable = null;
        if (thePrimKeysSet != null) {
            thePrimKeysSet.clear();
        }

        for (int i = 0; i < thePredIters.length; ++ i) {
            if (thePredIters[i] != null) {
                thePredIters[i].reset(rcb);
            }
        }
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state == null) {
            return;
        }

        state.close();

        if (theScanner != null) {
            theScanner.close();
            theScanner = null;
        }

        for (int i = 0; i < thePredIters.length; ++ i) {
            if (thePredIters[i] != null) {
                thePredIters[i].close(rcb);
            }
        }
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        throwCannotCall("displayContent");
    }

    private void throwCannotCall(String method) {
        throw new QueryStateException(
            "ServerTableIter: " + method + " cannot be called");
    }
}

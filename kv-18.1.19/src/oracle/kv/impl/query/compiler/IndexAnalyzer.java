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

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.EmptyValueImpl;
import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.api.table.IndexKeyImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr.ExprIter;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.ExprSFW.FromClause;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldRange;

/**
 * An IndexAnalyzer is associated with a SFW expr and an index I on a table T
 * that appears in the FROM clause of the SFW. I may be the primary index of
 * T. The IndexAnalyzer tries to find the best way to replace a full table scan
 * on T with an index scan on I. It does so by examinig the exprs appearing in
 * SFW clauses to determine which ones can be evaluated by the info stored in
 * the index.
 *
 * Some terminology:
 * -----------------
 *
 * MapBoth index:
 * An index that indexes both the keys and the elements of a map, with the keys
 * indexed before all the element paths. for example, an index on
 * (keys(map), map[].mf1, col2, map[].mf2, map[].mf3) is a MapBoth index.
 *
 * Start/stop predicate:
 * A pred that can participate in determining a starting and/or ending point
 * for an index range scan. Currently, potential start/stop preds are the ones
 * that have one of the following forms:
 * - expr op const
 * - expr IS (NOT) NULL
 * - EXISTS expr
 * where expr is a path expr that "matches" one of the index paths, and
 * op is a comparison operator (value or "anu" comparison). Notice that
 * whether a pred having one of the above forms will actually be used as a
 * start/stop pred depends on what other preds exists in the query.
 *
 * Index-filtering predicate:
 * A pred that can be applied on each index entry during an index scan,
 * because its evaluation depends only on the field values of the current
 * index entry.
 *
 * Sargable predicate (a.k.a index predicate):
 * A pred that is either a start/stop or an index-filtering pred. 
 *
 * Top-level predicate:
 * If the root of WHERE-clause expression is an AND operator, the operands of
 * this AND are the top-level preds of theSFW. Otherwise, the whole WHERE-clause
 * expr is the single top-level pred of theSFW.
 *
 * Predicate factor:
 * A top-level pred may be a composite pred, consisting of a number of pred
 * factors. There are 2 cases of composite preds:
 *
 * 1. A pred that involves a path expr containing filtering steps.
 * For example, assume an index on (a.b.e, a.b.c[].d1, a.b.c[].d2).
 *
 * The pred: a.b[$elem.e = 5].c[$elem.d1 < 10].d2 =any 20 consists of 3 pred
 * factors: a.b.e = 5, a.b.c[$elem.d1 < 10], and a.b.c[$elem.d2 = 20].
 * All of these pred factors are sargable, and they can all be applied
 * together during the index scan.
 *
 * The pred a.b[$elem.e = 5 and $elem.c[].d1 <any 10].c[].d2 =any 20 consists
 * of 3 pred factors: a.b.e = 5, a.b.c[].d1 <any 20, and a.b.c[].d2 =any 20.
 * All of these pred factors are sargable, but the last 2 are mutually 
 * exclusive: they cannot be both used during the index scan, because they are
 * both existential preds that do not necessarily apply to the same array
 * element.
 *
 * 2. MapBoth preds:
 * This case may arise only if the index is a MapBoth index. Then, a "MapBoth"
 * pred is a value-comparison pred on one of the map-value index paths, for a
 * particular map key. For example: map.key1.mf1 = 3 is a MapBoth pred and 
 * consistes of 2 pred factors: map.keys($entry.key = "key1") and 
 * map.values($entry.value.mf1 = 3). Notice that a MapBoth pred is equivalent
 * to an existentially-quantified expr with 2 conditions; for example
 * map.key1.mf1 = 3 is equivalent to: [some $key in keys(map) satisfies
 * $key = "key1" and map.$key = 3]. A MapBoth pred may be pushed to a 
 * MapBoth index as 2 start/stop preds: an equality pred on the map-key 
 * field, and an equality or range pred on a map-value field. Here are some
 * example queries (only the WHERE clause shown):
 *
 * Q1. where map.key10.mf1 > 10
 * The pred can be pushed to the index as [IC1 = "key10" and IC2 > 10]
 *
 * Q2. where map.key5.mf1 = 3 and col2 = 20 and map.key5.mf2 = 5.
 * All preds can be pushed as [IC1 = "key5" and IC2 = 3 and IC3 = 20 and
 * IC4 = 5]
 *
 * Q3. where map.key5.mf1 = 3 and map.key5.mf2 = 5.
 * Both preds is pushable, the 2nd as a filtering pred:
 * [IC1 = "key5" and IC2 = 3 and (filtering) IC4 = 5]
 *
 * Q4. where map.key5.mf1 = 3 and (map.key5.mf2 < 5 or map.key5.mf2 > 15)
 * Both preds is pushable, the 2nd as a filtering pred:
 * [IC1 = "key5" and IC2 = 3 and (filtering) (IC4 < 5 or IC4 > 15)]
 *
 * Q5. where map.key5.mf1 = 3 and col2 = 20 and map.key6.mf2 = 5.
 * The 1st 2 preds can be pushed as [IC1 = "key5" and IC2 = 3 and
 * IC3 = 20]. Alternatively, the 3rd pred can be pushed as
 * [IC1 = "key6" and (filtering) IC4 = 5]. IndexAnalyzer has to choose one of
 * the map keys (key5 or key6). In this case it will to push on key5, as it
 * probably better than key6.
 *
 * Q6. where map.key5.mf1 = 3 and col2 = 20 and map.key6.mf1 = 5.
 * We have a choice whether to push the 1st or the 3rd pred. In this case
 * their "score" is the same, so it doesn't matter which. If we choose
 * the 1st, we push [IC1 = "key5" and IC2 = 3 and IC3 = 20].
 *
 * Q7. where map.key5.mf1 > 3 and col2 = 20 and map.key6.mf1 = 5.
 * We have a choice whether to push the 1st or the 3rd pred. But pushing
 * the 3rd pred is probably better, so we push [IC1 = "key6" and IC2 = 5
 * and IC3 = 20].
 *
 * Q8. where map.key5.mf1 = 3 and col2 = 20 and map.key6.mf1 = 5 and
 *           map.key6.mf2 < 30
 * We can push "key5" preds or "Key6" preds, but "key6" is probably better
 * (because it has more pushable preds), so we push [IC1 = "key6" and
 * IC2 = 5 and IC3 = 20 and IC4 < 30].
 *
 * Notice that if there are any MapBoth preds, the index should basically
 * be treated as a simple index, instead of a multi-key index.
 *
 * Data Members:
 * -------------
 *
 * theQCB:
 * The QueryControlBlock of the query containing the SFW expr.
 *
 * theSctx:
 * The StaticContext associated with the SFW expr.
 *
 * theSFW:
 * The SFW expr to analyze.
 *
 * theTable:
 * The table T referenced by the SFW and indexed by I.
 *
 * theIndex:
 * The index I to analyze. If I is the primary index, theIndex is null.
 *
 * theIsHintIndex:
 * Whether I is an index named in a PREFER_INDEXES hint.
 *
 * theIsPrimary:
 * Whether I is T's primary index.
 *
 * theIsMapBothIndex:
 * Whether I is a MapBoth index.
 *
 * theNumFields:
 * the number of fields in I, including the primary-key fields.
 *
 * theIndexPaths:
 * The paths defining the index fields, including the primary-key fields.
 *
 * theWherePreds:
 * A array storing info for each "top-level" predicate appearing in the WHERE
 * clause of theSFW. It includes, among other things, the pred factors of the
 * top-level pred. This is needed if the index associated with this
 * IndexAnalyzer is actually chosen to be used by the query. Then, we must
 * decide where the whole top-level pred or nay of its pred factors can
 * actually be removed from the WHERE clause. See comments in the apply()
 * method.
 *
 * thePredGroups:
 * We partition all the multikey/existential pred factors into groups that are
 * mutually exclussive with each other, that is, a pred factor from one group
 * cannot be pushed to the index together with a pred from another group,
 * because the 2 pred fsctors do not necessarily apply to the same array/map
 * element/field. An example of this was given above, in case 1 of the Predicate
 * factor definition. Here is another (simpler) example:
 *
 * Assume an index on a.b.c[] and the following WHERE clause:
 *
 * where exists a.b.c[10 <= $element and $element < 20] and a.b.c[] =any 30
 *
 * There are 3 pred factors grouped into 2 groups:
 * { a.b.c[10 <= $element] , a.b.c[$element < 20] } and
 * { a.b.c[] =any 30 }
 *
 * The 2 pred factors in the 1st group can both be pushed to the index because
 * they apply to the same array element. The 3rd pred factor can also be pushed,
 * but not together with any of the other 2 factors, because it does not apply
 * to the same array element. 
 *
 * When there is more than one pred group, the algorithm must decide which one
 * is better to push to the index. In the above example, the 2nd group is
 * better, because it includes an equality pred versus the 2 range preds of
 * the 1st group. This choice is done by the chooseMultiKeyPredGroup() method.
 *
 * theUnnestedGroup:
 * A single group for pred factors on unnseted arrays/maps. 
 *
 * theStartStopPreds:
 * Stores pred factors that are potential start/stop preds for the index
 * scan. It is implemented as a matrix that groups such pred factors by the
 * index field they apply to. Initially, all potential start/stop preds are
 * put in the matrix, but after we choose the best predicate group, preds
 * in the loosing groups are removed. Even within a single pred group, some
 * preds may be removed from theStartStopPreds for 2 reasons:
 * (a) A pred may turn out to be a filtering pred, because it applies to an
 * index field F, and there is another index field before F for which no
 * equality pred exists. 
 * (b) Only a single equality pred, or up to 2 range preds may be applied
 * on each index field. Although not expected, the query may contain more
 * preds that can be applied on a single index field. Here are some examples:
 * a = 10 and a = $var
 * a > 20 and a > 30
 * a = 10 and a < $var.
 * In this case, we must choose which preds to apply on an index field. This
 * is done by the skipExtraneousPreds() method.
 *
 * theBestPredGroup:
 * The pred group that is chosen by the chooseMultiKeyPredGroup() method as
 * the "best" to apply.
 *
 * theHaveMapKeyEqPred:
 * Set to true (a) if the index is a map index indexing the map keys (it may
 * also index the values), (b) the query contains a pred of the form
 * keys(map) =any "foo", and (c) the pred can be pushed to the index. In this
 * case there is no need to do duplicate elimination.
 *
 * theFilteringPreds:
 * Stores the pred factors that were determined to be sargable as 
 * index-filtering preds.
 *
 * thePrimaryKeys:
 * The primary key to be used for the scan of the primary index, if this is
 * the Indexanalyzer for the primary index. The primary key may be partial,
 * and it reflects the equality preds pushed on prim key columns. Although
 * thePrimaryKeys is a list, currently there will be only one PrimaryKey
 * in the list. In there future there may be more, if for example, we
 * implement a sargable IN operator.
 *
 * theSecondaryKeys:
 * The secondary keys to be used for the scan of the secondary index associated
 * with this Indexanalyzer. The secondary keys may be partial, and they reflect
 * the equality preds pushed on the index fields. Currently, the only case when
 * there may be more than 1 IndexKeys in this list is when an exists pred is
 * pushed to the index. An exists predicate translates to scaning two diffent
 * ranges in the index: one for entries less than EMPTY and another for entries
 * greater than EMPTY. 
 *
 * theRanges:
 * The FieldRanges used for the index scan. Each FieldRange reflects the
 * one or two range preds that a pushed on a single index field. Currently,
 * the only case when there may be more than 1 IndexKeys in this list is 
 * when an exists pred is pushed to the index.
 *
 * thePushedExternals:
 * This is used to handle the cases where a pushable pred contains external
 * variables, eg, foo = $x + $y, where foo is an indexed column of theTable.
 * If foo is an integer, we initially create a placeholder FieldValue with
 * value 0, and place it in the IndexKey or PrimaryKey or FieldRange (i.e.,
 * we push the pred foo = 0). thePushedExternals is then used to register
 * the $x + $y expr. thePushedExternals has one entry for each index field
 * on which an equality pred is pushed, and 2 entries for the single index
 * field on which a FieldRange is pushed. The ordering of the entries in
 * thePushedExternals is the same as the declaration order of the associated
 * index fields. If the predicate(s) pushed on an index field do not have
 * any external vars, the associated entry(s) in thePushedExternals is null.
 * If the current index is applied, thePushedExternals will be copied into
 * the associated ExprBaseTable, and during code generation, it will be
 * converted to an array of PlanIters and placed in the BaseTableIter.
 * During BaseTableIter.open(), the PlanIters stored in
 * BaseTableIter.thePushedExternals, will be evaluated and the resulting
 * values will be used to replace the associated placeholders.
 *
 * theHavePushedExternals:
 * Set to true if at least one entry in thePushedExternals is non-null.
 * 
 * theIsMultiKeyRange:
 * Whether the pred is a range pred for a multi key index. It is used in 
 * scoring the index.
 *
 * theIsCovering:
 * Set to true if the index is a covering one, i.e., the whole query can be
 * evaluated from the index columns, without any need to retrieve any table
 * rows.
 *
 * theEliminateDups:
 * Whether duplication elimination must be done on the prim keys returned by
 * an index scan on a multikey index.
 *
 * theScore:
 * A crude metric of how effective the index is going to be in optimizing
 * table access. See getScore() method.
 *
 * theScore2:
 * Same as theScore, but without any special treatment for the complete-key
 * case. See getScore() method.
 *
 * theNumEqPredsPushed:
 * The number of equality predicates pushed as start/stop conditions. It
 * includes partially pushed preds. Used to compute theScore and theScore2
 * for each each index in order to choose the "best" applicable index (see
 * getScore() and compareTo() methods).
 */
class IndexAnalyzer implements Comparable<IndexAnalyzer> {

    static int theTrace = 0;

    /*
     * The relative value of each kind of predicate. Used to compute a
     * score for each each index in order to choose the "best" applicable
     * index (see getScore() and compareTo() methods).
     */
    final static int eqValue = 32;
    final static int vrangeValue = 16; // value-range pred
    final static int arangeValue = 8;  // any-range pred
    final static int filterEqValue = 17;
    final static int filterOtherValue = 7;

    private final QueryControlBlock theQCB;

    private final StaticContext theSctx;

    ExprSFW theSFW;

    private final ExprBaseTable theTableExpr;

    private final TableImpl theTable;

    private final int theTablePos;

    private final int theTargetTablePos;

    private final IndexImpl theIndex;

    private final boolean theIsHintIndex;

    private final boolean theIsPrimary;

    private final boolean theIsMapBothIndex;

    private final int theNumFields;

    final List<IndexField> theIndexPaths;

    private final ArrayList<WherePredInfo> theWherePreds;

    private final ArrayList<PredGroup> thePredGroups;

    private PredGroup theUnnestedGroup;

    private final ArrayList<ArrayList<PredInfo>> theStartStopPreds;

    private PredGroup theBestPredGroup;

    private boolean theHaveMapKeyEqPred;

    private final ArrayList<PredInfo> theFilteringPreds;

    private final HashMap<Expr, ArrayList<ExprToReplace>> theExprRewriteMap;

    private ArrayList<PrimaryKeyImpl> thePrimaryKeys;

    private ArrayList<IndexKeyImpl> theSecondaryKeys;

    private ArrayList<FieldRange> theRanges;

    private final ArrayList<Expr> thePushedExternals;

    private boolean theHavePushedExternals;

    private boolean theIsMultiKeyRange;

    private boolean theIsCovering;

    private boolean theEliminateDups;

    private int theScore = -1;

    private int theScore2 = -1;

    private int theNumEqPredsPushed = 0;

    static private class ExprToReplace {
        Expr theExpr;
        int theIndexFieldPos;

        ExprToReplace(Expr expr, int pos) {
            theExpr = expr;
            theIndexFieldPos = pos;
        }
    }

    /**
     * Information for a top-level WHERE-clause predicate.
     *
     * thePred:
     * The full expr for this top-level pred.
     *
     * thePredInfos:
     * The pred factors of this top-level pred.
     *
     * theDoesSlicing:
     * Set to true if there is any path expr in this top-level pred that
     * contains a slicing array step. Such a step cannot be evaluated by
     * the index, and as a result, the top-level pred cannot be removed
     * from the WHERE clause.
     *
     * theLocalGroup:
     * A group of pred factors that belong to this top-level pred and whose
     * outer-most context var is positioned in the multikey step of the index.
     * The group also includes the "direct" pred factor (if any) of the top-
     * level pred, i.e. the pred factor that has no context var. All such 
     * preds can be applied together. Any other pred factor (whose outer-most
     * context var is positioned before the multikey step of the index) is 
     * placed in one PredGroup by themselves. This is too strict in that some
     * "non-local" pred factors may actually belong together in a pred group
     * (for example see query json_idx/q/filter13). However doing something
     * better would take a lot of effort, which is probably not worth. 
     */
    private class WherePredInfo {

        int theId;

        Expr thePred;

        final ArrayList<PredInfo> thePredInfos = new ArrayList<PredInfo>(8);

        boolean theDoesSlicing;

        PredGroup theLocalGroup;

        WherePredInfo(Expr pred) {
            theId = theWherePreds.size();
            thePred = pred;
        }

        boolean add(PredInfo pi) {

            if (pi.thePred == null) {

                if (theTrace >= 2) {
                    System.out.println(
                        "Collected keys() pred for MapBoth key " +
                        pi.mapBothKey());
                }

                thePredInfos.add(0, pi);
                return true;
            }

            boolean added = false;

            if (pi.isExists()) {
                Expr input = pi.thePred.getInput();

                if (input.getKind() == ExprKind.ARRAY_FILTER) {
                    ExprArrayFilter step = (ExprArrayFilter)input;
                    if (step.getPredExpr() == null) {
                        thePredInfos.add(pi);
                        added = true;
                    }
                } else if (input.getKind() == ExprKind.MAP_FILTER) {
                    ExprMapFilter step = (ExprMapFilter)input;
                    if (step.getPredExpr() == null) {
                        thePredInfos.add(pi);
                        added = true;
                    }
                } else {
                    thePredInfos.add(pi);
                    added = true;
                }
            } else {
                thePredInfos.add(pi);
                added = true;
            }

            if (added && theTrace >= 2) {
                System.out.println(
                    "WPI: " + theId + " Collected pred with status " +
                    pi.theStatus + "\nepath = " +
                    (pi.theEpath != null ? pi.theEpath.getPathName() : null) +
                    "\n" + pi.thePred.display() + "\n");
            }

            return added;
        }

        boolean doesFiltering() {
            return (thePredInfos.size() > 1 ||
                    thePredInfos.get(0).thePred != thePred);
        }

        boolean isFullyPushable() {

            if (theDoesSlicing) {
                return false;
            }

            for (PredInfo pi : thePredInfos) {

                if ((pi.theStatus != PredicateStatus.STARTSTOP &&
                     pi.theStatus != PredicateStatus.FILTERING &&
                     pi.theStatus != PredicateStatus.TRUE) ||
                    (pi.theEpath != null && pi.isUnnested())) {
                    return false;
                }
            }

            return true;
        }
    }

    /*
     * Information about a pred factor.
     */
    private class PredInfo {

        WherePredInfo theEnclosingPred;

        Expr thePred;

        FuncCode theOp;

        boolean theIsValueComp;

        boolean theIsExists;

        Expr theVarArg;

        Expr theConstArg;

        FieldValueImpl theConstVal;

        IndexExpr theEpath;

        int theIPathPos = -1;

        PredGroup thePredGroup;

        PredicateStatus theStatus;

        PredInfo(WherePredInfo enclosingPred, Expr pred) {
            theEnclosingPred = enclosingPred;
            thePred = pred;
            theStatus = PredicateStatus.UNKNOWN;
        }

        boolean isEq() {
            return theOp == FuncCode.OP_EQ;
        }

        boolean isMin() {
            return (theOp == FuncCode.OP_GT || theOp == FuncCode.OP_GE);
        }

        boolean isMax() {
            return (theOp == FuncCode.OP_LT || theOp == FuncCode.OP_LE);
        }

        boolean isInclusive() {
            return (theOp == FuncCode.OP_GE || theOp == FuncCode.OP_LE);
        }

        boolean isExists() {
            return theIsExists;
        }

        boolean isUnnested() {
            return theEpath.theIsUnnested;
        }

        String mapBothKey() {
            return theEpath.getMapBothKey();
        }

        boolean isMatched() {
            return theIPathPos >= 0;
        }

        IndexField getIndexPath() {
            return theIndexPaths.get(theIPathPos);
        }

        boolean isCompatible(PredInfo other) {

            assert(theIPathPos == other.theIPathPos);

            return (!theIndexPaths.get(theIPathPos).isMultiKey() ||
                    thePredGroup == other.thePredGroup);
        }

        boolean canBeRemoved() {
            return (isMatched() &&
                    thePred != theEnclosingPred.thePred &&
                    theEpath != null &&
                    (!getIndexPath().isMultiKey() ||
                     theEpath.getMapBothKey(theTable, theIndex) != null) &&
                    theEpath.getFilteringPreds() == null);
        }

        @Override
        public String toString() {
            if (thePred != null) {
                return thePred.display();
            }

            return ("Map Both key = " + mapBothKey());
        }
    }
    
    /*
     * Information about a group of compatible pred factors.
     *
     * theScore data field and the data fields after it are "transient" ones:
     * they are used only for scoring the group for the purpose of choosing the
     * "best" one.
     */
    private static class PredGroup {

        int theId;

        final ArrayList<PredInfo> thePredInfos = new ArrayList<PredInfo>(8);        

        ExprVar theCtxVar;

        String theMapBothKey;

        boolean theIsUnnested;

        int theScore;

        int theFieldScore;

        boolean theFoundRange;

        boolean theFilteringOnly;

        PredGroup(int id, PredInfo pi) {
            theId = id;
            thePredInfos.add(pi);
            pi.thePredGroup = this;
        }

        static void addUnnestedPred(IndexAnalyzer idx, PredInfo pi) {

            if (idx.theUnnestedGroup == null) {
                PredGroup pg = idx.addPredGroup(pi);
                pg.theIsUnnested = true;
                idx.theUnnestedGroup = pg;
                return;
            }

            idx.theUnnestedGroup.thePredInfos.add(pi);
            pi.thePredGroup = idx.theUnnestedGroup;
        }

        static boolean addMapBothPred(IndexAnalyzer idx, PredInfo pi) {

            boolean added = false;
            for (PredGroup pg : idx.thePredGroups) {
                if (pi.mapBothKey().equals(pg.theMapBothKey)) {
                    pg.thePredInfos.add(pi);
                    pi.thePredGroup = pg;
                    added = true;
                    break;
                }
            }

            if (!added) {
                PredGroup pg = idx.addPredGroup(pi);
                pg.theMapBothKey = pi.mapBothKey();
                return false;
            }

            return true;
        }
    }

    /**
     *
     */
    static enum PredicateStatus {

        UNKNOWN,

        /*
         * It's definitely not a start/stop pred. Such a pred may be later
         * determined to be an index-filtering one.
         */
        NOT_STARTSTOP,

        /*
         * It is a potentially start/stop pred
         */
        STARTSTOP,

        /*
         * It's an index filtering pred
         */
        FILTERING,

        /*
         * Definitely not sargable
         */
        SKIP,

        /*
         * The pred is always false. This makes the whole WHERE expr always
         * false.
         */
        FALSE,

        /*
         * The pred is always true, so it can be removed from the WHERE expr.
         */
        TRUE
    }

    IndexAnalyzer(
        ExprSFW sfw,
        ExprBaseTable tableExpr,
        int tablePos,
        IndexImpl index) {

        theQCB = sfw.getQCB();
        theSctx = sfw.getSctx();
        theSFW = sfw;
        theTableExpr = tableExpr;
        theTablePos = tablePos;
        theTable = tableExpr.getTable(tablePos);
        theTargetTablePos = tableExpr.getTargetTablePos();
        theIndex = index;
        theIsPrimary = (theIndex == null);

        int pkStartPos = 0;
        int pkSize = theTable.getPrimaryKeySize();

        if (!theIsPrimary) {
            theNumFields = theIndex.numFields() + pkSize;
            pkStartPos = theIndex.numFields();

            theIndexPaths = new ArrayList<IndexField>(theNumFields);
            theIndexPaths.addAll(theIndex.getIndexFields());
        } else {
            theNumFields = pkSize;
            theIndexPaths = new ArrayList<IndexField>(theNumFields);
        }
         
        List<String> pkColumnNames = theTable.getPrimaryKeyInternal();

        for (int i = 0; i < pkSize; ++i) {

            String name = pkColumnNames.get(i);

            IndexField ipath = new IndexField(theTable, name, null,
                                              i + pkStartPos);
            ipath.setType(theTable.getPrimKeyColumnDef(i));
            ipath.setNullable(false);
            theIndexPaths.add(ipath);
        }

        theIsHintIndex = theTableExpr.isIndexHint(theIndex);

        theWherePreds = new ArrayList<WherePredInfo>(32);
        thePredGroups = new ArrayList<PredGroup>(32);
        theStartStopPreds = new ArrayList<ArrayList<PredInfo>>(theNumFields);
        theFilteringPreds = new ArrayList<PredInfo>();
        theExprRewriteMap = new HashMap<Expr, ArrayList<ExprToReplace>>();

        for (int i = 0; i < theNumFields; ++i) {
            theStartStopPreds.add(null);
        }

        if (theIsPrimary) {
            thePrimaryKeys = new ArrayList<PrimaryKeyImpl>(1);
            thePrimaryKeys.add(theTable.createPrimaryKey());
            theIsMapBothIndex = false;
        } else {
            theSecondaryKeys = new ArrayList<IndexKeyImpl>(1);
            theSecondaryKeys.add(theIndex.createIndexKey());
            theIsMapBothIndex = theIndex.isMapBothIndex();
        }

        theRanges = new ArrayList<FieldRange>(1);
        theRanges.add(null);

        thePushedExternals = new ArrayList<Expr>();
    }

    //@SuppressWarnings("unused")
    private String getIndexName() {
        return (theIsPrimary ? "primary" : theIndex.getName());
    }

    ArrayList<PrimaryKeyImpl> getPrimaryKeys() {
        return thePrimaryKeys;
    }

    boolean hasShardKey() {
        return (theIsPrimary &&
                thePrimaryKeys.size() == 1 &&
                thePrimaryKeys.get(0).hasShardKey());
    }

    private PredGroup addPredGroup(PredInfo pi) {
        PredGroup pg = new PredGroup(thePredGroups.size(), pi);
        thePredGroups.add(pg);
        return pg;
    }

    private void addStartStopPred(PredInfo pi) {

        int ipos = pi.theIPathPos;
         ArrayList<PredInfo> startstopPIs = theStartStopPreds.get(ipos);

        if (startstopPIs == null) {
            startstopPIs = new ArrayList<PredInfo>();
            theStartStopPreds.set(ipos, startstopPIs);
        }

        if (theTrace >= 1) {
            System.out.println(
                "Added startstop pred at pos " + ipos + " pred = \n" + pi);
        }
        startstopPIs.add(pi);
    }

    /**
     * Remove a pred from the WHERE clause. The pred has either been
     * pushed in the index or is always true.
     */
    private void removePred(Expr pred) {

        if (pred == null) {
            return;
        }

        int numParents = pred.getNumParents();

        if (numParents == 0) {
            return;
        }

        Expr parent = pred.getParent(0);

        if (numParents > 1) {

            /*
             * It's possible for the pred to have a second parent, if it is
             * a non-matched index-filtering pred that has been added to the
             * base table expr.
             */
            if (numParents != 2 ||
                (pred.getParent(1).getKind() != ExprKind.BASE_TABLE &&
                 pred.getParent(1).getFunction(FuncCode.OP_AND) == null)) {
                throw new QueryStateException(
                    "Trying to remove a pred with more than one parents. pred:\n" +
                    pred.display() + "\nnum parents = " + numParents +
                    " 2nd parent:\n" + pred.getParent(1).display());
            }
        }

        Expr whereExpr = theSFW.getWhereExpr();

        if (pred == whereExpr) {
            theSFW.removeWhereExpr(true/*destroy*/);

        } else {
            parent.removeChild(pred, true/*destroy*/);

            if (parent == whereExpr && whereExpr.getNumChildren() == 0) {
                theSFW.removeWhereExpr(true /*destroy*/);
            }
        }
    }

    /*
     * The whole WHERE expr was found to be always false. Replace the
     * whole SFW expr with an empty expr.
     */
    private void processAlwaysFalse() {

        Function empty = Function.getFunction(FuncCode.FN_SEQ_CONCAT);
        Expr emptyExpr = ExprFuncCall.create(theQCB, theSctx,
                                             theSFW.getLocation(),
                                             empty,
                                             new ArrayList<Expr>());
        if (theQCB.getRootExpr() == theSFW) {
            theQCB.setRootExpr(emptyExpr);
        } else {
            theSFW.replace(emptyExpr, true);
        }

        theSFW = null;
    }

    /**
     * Used to sort the IndexAnalyzers in decreasing "value" order, where
     * "value" is a heuristic estimate of how effective the associated
     * index is going to be in optimizing the query.
     */
    @Override
    public int compareTo(IndexAnalyzer other) {

        int numFields1 = theNumFields;
        int numFields2 = other.theNumFields;

        boolean multiKey1 = (theIsPrimary ? false : theIndex.isMultiKey());

        boolean multiKey2 = (other.theIsPrimary ?
                             false :
                             other.theIndex.isMultiKey());

        /* Make sure the index scores are computed */
        getScore();
        other.getScore();

        /*
        System.out.println(
             "Comparing indexes " +  getIndexName() + " and " +
             other.getIndexName() + "\nscore1 = " + theScore +
             " score2 = " + other.theScore);
        */

        /*
         * If one of the indexes is covering, ....
         */
        if (theIsCovering != other.theIsCovering) {

            if (theIsCovering) {

                /*
                 * If the other is a preferred index, choose the covering
                 * index if it has at least one eq start/stop condition
                 * or 2 range start/stop conditions.
                 */
                if (!theIsHintIndex && other.theIsHintIndex) {
                    FieldRange range = theRanges.get(0);
                    return (theNumEqPredsPushed > 0 ||
                            (range != null &&
                             range.getStart() != null &&
                             range.getEnd() != null) ?
                            -1 : 1);
                }

                /*
                 * If the other index does not have a complete key, choose
                 * the covering index.
                 */
                if (other.theScore != Integer.MAX_VALUE) {
                    return -1;
                }

                /*
                 * The other index has a complete key. Choose the covering
                 * index if its score is >= to the score of the other index
                 * without taking into account the key completeness.
                 */
                return (theScore >= other.theScore2 ? -1 : 1);
            }

            if (other.theIsCovering) {

                if (!other.theIsHintIndex && theIsHintIndex) {
                    FieldRange range = theRanges.get(0);
                    return (other.theNumEqPredsPushed > 0 ||
                            (range != null &&
                             range.getStart() != null &&
                             range.getEnd() != null) ?
                            1 : -1);
                }

                if (theScore != Integer.MAX_VALUE) {
                    return 1;
                }

                return (other.theScore >= theScore2 ? 1 : -1);
            }
        }

        if (theScore == other.theScore) {

            /*
             * If none of the indexes has any predicates pushed and one of
             * them is the primary index, choose that one.
             */
            if (theScore == 0) {

                if (theIsPrimary || other.theIsPrimary) {
                    return (theIsPrimary ? -1 : 1);
                }

                if (multiKey1 != multiKey2) {
                    return (multiKey1 ? 1 : -1);
                }
            }

            /*
             * If one of the indexes is specified in a hint, choose that
             * one.
             */
            if (theIsHintIndex != other.theIsHintIndex) {
                return (theIsHintIndex ? -1 : 1);
            }

            /*
             * If one of the indexes is multi-key and other simple, choose
             * the simple one.
             */
            if (multiKey1 != multiKey2) {
                return (multiKey1 ? 1 : -1);
            }

            /*
             * If one of the indexes is the primary index, choose that one.
             */
            if (theIsPrimary || other.theIsPrimary) {
                return (theIsPrimary ? -1 : 1);
            }

            /*
             * Choose the index with the smaller number of fields. This is
             * based on the assumption that if the same number of preds are
             * pushed to both indexes, the more fields the index has the less
             * selective the pushed predicates are going to be.
             */
            if (numFields1 != numFields2) {
                return (numFields1 < numFields2 ? -1 : 1);
            }

            /*
             * TODO ???? Return the one with the smaller key size
             */

            return 0;
        }

        /*
         * If we have a complete key for one of the indexes, choose that
         * one.
         */
        if (theScore == Integer.MAX_VALUE ||
            other.theScore == Integer.MAX_VALUE) {
            return (theScore == Integer.MAX_VALUE ? -1 : 1);
        }

        /*
         * If one of the indexes is specified in a hint, choose that one.
         */
        if (theIsHintIndex != other.theIsHintIndex) {
            return (theIsHintIndex ? -1 : 1);
        }

        return (theScore > other.theScore ? -1 : 1);
    }

    /**
     * Computes the "score" of an index w.r.t. this query, if not done
     * already.
     *
     * Score is a crude estimate of how effective the index is going to
     * be in optimizing table access. Score is only a relative metric,
     * i.e., it doesn't estimate any real metric (e.g. selectivity), but
     * it is meant to be used only in comparing the relative value of two
     * indexes in order to choose the "best" among all applicable indexes.
     *
     * Score is an integer computed as a weighted sum of the predicates
     * that can be pushed into an index scan (as start/stop conditions or
     * filtering preds).  However, if there is a complete key for an index,
     * that index gets the highest score (Integer.MAX_VALUE).
     */
    private int getScore() {

        if (theScore >= 0) {
            return theScore;
        }

        theScore = 0;
        theScore2 = 0;

        theScore += theNumEqPredsPushed * eqValue;

        FieldRange range = theRanges.get(0);

        if (range != null) {

            if (range.getStart() != null) {
                theScore += (theIsMultiKeyRange ? arangeValue : vrangeValue);
            }

            if (range.getEnd() != null) {
                theScore += (theIsMultiKeyRange ? arangeValue : vrangeValue);
            }
        }

        for (PredInfo pi : theFilteringPreds) {
            if (pi.isEq()) {
                theScore += filterEqValue;
            } else {
                theScore += filterOtherValue;
            }
        }

        theScore2 = theScore;

        if (theTrace >= 2) {
            System.out.println(
                "Score for index " + getIndexName() + " = " + theScore +
                "\ntheNumEqPredsPushed = " + theNumEqPredsPushed);
        }

        int numPrimKeyCols = theTable.getPrimaryKeySize();
        int numFields = (theIsPrimary ?
                         numPrimKeyCols :
                         theNumFields - numPrimKeyCols);

        if (theNumEqPredsPushed == numFields) {
            theScore = Integer.MAX_VALUE;
            return theScore;
        }

        return theScore;
    }

    /**
     * The index has been chosen among the applicable indexes, so do the
     * actual pred pushdown and remove all the pushed preds from the
     * where clause.
     */
    void apply() {

        /* Collect info to be used later to eliminate unused FROM vars */
        int numFroms = theSFW.getNumFroms();
        int[] varRefsCounts = new int[numFroms];

        for (int i = 0; i < numFroms; ++i) {

            FromClause fc = theSFW.getFromClause(i);

            /* Table row variables can not be eliminated ever */
            if (fc.getTargetTable() != null) {
                varRefsCounts[i] = 0;
            } else {
                varRefsCounts[i] = fc.getVar().getNumParents();
            }
        }

        /* Add the scan boundaries to theTableExpr */
        if (theIsPrimary) {
            assert(theRanges.size() == thePrimaryKeys.size());
            theTableExpr.addPrimaryKeys(theTablePos,
                                        thePrimaryKeys,
                                        theRanges,
                                        theIsCovering);
            theQCB.setPushedPrimaryKey(thePrimaryKeys.get(0));
        } else {
            assert(theRanges.size() == theSecondaryKeys.size());
            theTableExpr.addSecondaryKeys(theTablePos,
                                          theSecondaryKeys,
                                          theRanges,
                                          theIsCovering);
        }

        if (theHavePushedExternals) {
            theTableExpr.setPushedExternals(thePushedExternals);
        }

        ExprVar idxVar = null;

        if (theIndex != null && 
            (!theFilteringPreds.isEmpty() || theIsCovering)) {
            idxVar = theSFW.addIndexVar(theTable, theIndex);
        }

        /*
         * Add the filtering preds to theTableExpr. If a secondary index is
         * used, rewrite the pred to reference the columns of the current
         * index row, instead of the row columns. No rewrite is necessary for
         * the primary index, because in this case the "index row" is actualy
         * a table row (RowImpl) that is populated with the prim key columns
         * only.
         */
        int numFilteringPreds = theFilteringPreds.size();
        ArrayList<Expr> args = new ArrayList<Expr>(numFilteringPreds);

        for (PredInfo pi : theFilteringPreds) {

            Expr pred = pi.thePred;
            if (theIndex != null) {
                pred = rewritePred(idxVar, pi);
            }
            args.add(pred);
        }

        if (args.size() > 1) {
            FunctionLib fnlib = CompilerAPI.getFuncLib();
            Function andFunc = fnlib.getFunc(FuncCode.OP_AND);

            Expr pred = ExprFuncCall.create(theQCB, theSctx,
                                            theTableExpr.getLocation(),
                                            andFunc,
                                            args);

            theTableExpr.setTablePred(theTablePos, pred, false);

        } else if (args.size() == 1) {
            theTableExpr.setTablePred(theTablePos, args.get(0), false);
        }

        if (theEliminateDups) {
            theTableExpr.setEliminateIndexDups();
        }

        /*
         * If possible, remove STARTSTOP, FILTERING and TRUE preds from the
         * WHERE clause.
         */
        for (WherePredInfo wpi : theWherePreds) {

            if (wpi.isFullyPushable()) {
                removePred(wpi.thePred);
                continue;
            }

            ArrayList<PredInfo> predinfos = wpi.thePredInfos;

            for (PredInfo pi : predinfos) {

                switch (pi.theStatus) {
                case STARTSTOP:
                case FILTERING:
                    /*
                     * If a multikey/existential pred P is pushed to the index,
                     * but there are other multikey preds that are not pushed,
                     * then P cannot be removed; it must be reapplied to the
                     * row selected by the index scan. For example, consider
                     * the following query, index, and document:
                     *
                     * create index idx_children_values on foo (
                     *    info.children.values().age as long,
                     *    info.children.values().school as string)
                     *
                     * select id
                     * from foo f
                     * where exists f.info.children.values($key = "Anna" and
                     *                                     $value.age <= 10)
                     *
                     * { 
                     *   "id":5,
                     *   "info":
                     *   {
                     *     "firstName":"first5", "lastName":"last5","age":11,
                     *     "children":
                     *     {
                     *       "Anna"  : { "age" : 29, "school" : "sch_1"},
                     *       "Mark"  : { "age" : 14, "school" : "sch_2"},
                     *       "Dave"  : { "age" : 16 },
                     *       "Tim"   : { "age" : 8,  "school" : "sch_2"},
                     *       "Julie" : { "age" : 12, "school" : "sch_2"}
                     *     }
                     *   }
                     * }
                     *
                     * The range pred on age is pushed to the index. The above
                     * doc will be selected by the index scan (because of
                     * "Tim"). However, the age and $key preds must both be
                     * applied together on each entry of the children map.
                     * If we remove the age pred from the WHERE clause, the
                     * above doc will be wrongly returned.
                     *
                     * It appears that a better rule would be to remove a 
                     * pred factor if it is in a pred group where all the
                     * pred factors are alos being pushed to the index. However,
                     * our pred factor grouping is not very accurate, so we
                     * may have 2 pred factors that belong to different groups
                     * but should be in the same group instead.
                     */
                    if (pi.canBeRemoved()) {
                        removePred(pi.thePred);
                    }
                    break;
                case TRUE:
                    removePred(pi.thePred);
                    break;
                case NOT_STARTSTOP:
                case SKIP:
                    break;
                default:
                    throw new QueryStateException(
                        "Unexpected state for predicate:\n" + pi);
                }
            }
        }

        /*
         * If a covering secondary index is used, rewrite the SELECT and
         * ORDER BY exprs to reference the columns of the current index row,
         * instead of the row columns. Futhermore, if theTableExpr has nested
         * tables, the WHERE clause may have preds that reference both target
         * and non-target table columns, and as a result, these preds have not
         * been pushed to the index. Therefore, we must rewrite the sub-exprs
         * that reference only target-table columns.
         */
        if (theIndex != null && theIsCovering) {

            if (theTableExpr.hasNestedTables()) {

                for (WherePredInfo wpi : theWherePreds) {

                    if (wpi.isFullyPushable()) {
                        continue;
                    }

                    rewriteExpr(idxVar, wpi.thePred);
                }
            }

            int numFields = theSFW.getNumFields();

            for (int i = 0; i < numFields; ++i) {

                Expr expr = theSFW.getFieldExpr(i);

                /*
                 * If the select expr is a row var, we must convert it to a
                 * number of step exprs that extract the column values from
                 * the index entry. If the row var is the only expr in the
                 * select, the step exprs become the new exprs of the select
                 * list. If not (for example select * from NESTED TABLES(...))
                 * we replace the row var with a record constructor whose field
                 * exprs are the step exprs.
                 */
                if (expr.getKind() == ExprKind.VAR &&
                    ((ExprVar)expr).getTable() != null &&
                    ((ExprVar)expr).getTable().getId() == theTable.getId()) {

                    RecordDefImpl rowDef = theTable.getRowDef();
                    int numCols = rowDef.getNumFields();
                    ArrayList<Expr> newFieldExprs = new ArrayList<Expr>(numCols);

                    for (int j = 0; j < numCols; ++j) {
                        String colName = rowDef.getFieldName(j);

                        int k = 0;
                        for (; k < theNumFields; ++k) {
                            IndexField ipath = theIndexPaths.get(k);
                            if (ipath.numSteps() == 1 &&
                                ipath.getStep(0).equalsIgnoreCase(colName)) {
                                break;
                            }
                        }

                        if (k == theNumFields) {
                            throw new QueryStateException(
                                "Column " + colName + " is not indexed by " +
                                "index " + theIndex.getName());
                        }

                        newFieldExprs.add(new ExprFieldStep(expr.getQCB(),
                                                            expr.getSctx(),
                                                            expr.getLocation(),
                                                            idxVar,
                                                            k));
                    }

                    if (numFields == 1) {
                        for (int j = 0; j < numCols; ++j) {
                            theSFW.addField(rowDef.getFieldName(j),
                                            newFieldExprs.get(j));
                        }
                        theSFW.removeField(i, true);
                    } else {
                        Expr newFieldExpr =
                            new ExprRecConstr(expr.getQCB(),
                                              expr.getSctx(),
                                              expr.getLocation(),
                                              rowDef,
                                              newFieldExprs);
                        theSFW.setFieldExpr(i, newFieldExpr, true);
                    }

                } else {
                    rewriteExpr(idxVar, expr);
                }
            }

            int numSortExprs = theSFW.getNumSortExprs();

            for (int i = 0; i < numSortExprs; ++i) {
                Expr expr = theSFW.getSortExpr(i);
                rewriteExpr(idxVar, expr);
            }

            int numTables = theTableExpr.getNumTables();

            for (int i = 0; i < numTables; ++i) {

                if (i == theTargetTablePos) {
                    continue;
                }

                Expr pred = theTableExpr.getTablePred(i);
                if (pred != null) {
                    rewriteExpr(idxVar, pred);
                }
            }
        }

        /*
         * Remove unused variables. If a var is not used anywhere, it can be
         * removed from the FROM clause if:
         * (a) Its domain expr is scalar, else
         * (b) It was used before applying this index, but is not used after.
         *     This means that all uses of the variable were pushed down to
         *     index. However, if the var ranges over a table, it should not 
         *     be removed.
         */
        for (int i = numFroms - 1; i >= 0; --i) {

            FromClause fc = theSFW.getFromClause(i);

            if (fc.getTargetTable() != null) {
                continue;
            }

            ExprVar var = fc.getVar();
            
            if (var.getNumParents() == 0) {
                
                if(theSFW.getDomainExpr(i).isScalar()) {
                    theSFW.removeFromClause(i, true);
                    
                } else if (varRefsCounts[i] != 0) {

                    if (theSFW.getDomainExpr(i).isMultiValued() &&
                        (theIndex == null || !theIndex.isMultiKey())) {
                        throw new QueryStateException(
                            "Attempt to remove a multi-valued variable when " +
                            "a non-multikey index is being applied.\n" +
                            "var name = " + var.getName() + " index name = " +
                            theIndex.getName());
                    }
                    assert(var.getTable() == null);

                    theSFW.removeFromClause(i, true);
                }
            }
        }

        /*
         * The non-target tables are always accessed via the primary index.
         * Check whether the primary index is a covering one for each of these
         * tables.
         */
        analyzeNonTargetTables();
    }

    Expr rewritePred(ExprVar idxVar, PredInfo pi) {

        if (!pi.isMatched()) {
            rewriteExpr(idxVar, pi.thePred);
            return pi.thePred;
        }

        assert(pi.theEpath != null);
        Location loc = pi.theEpath.theExpr.theLocation;
        int fieldPos;

        if (theIndex == null) {
            fieldPos = theTable.getPrimKeyPos(pi.theIPathPos);
        } else {
            fieldPos = pi.theIPathPos;
        }

        ExprFieldStep idxColRef = new ExprFieldStep(theQCB, theSctx, loc,
                                                    idxVar, fieldPos);
        
        ExprFuncCall pred = (ExprFuncCall)pi.thePred;

        if (pred == null) {
            assert(theIsMapBothIndex);
            assert(pi.theEpath.getMapBothKey(theTable, theIndex) != null);

            ExprConst keyvalExpr =
                new ExprConst(theQCB, theSctx, loc, pi.theConstVal);

            ArrayList<Expr> args = new ArrayList<Expr>(2);
            args.add(idxColRef);
            args.add(keyvalExpr);

            pred = new ExprFuncCall(theQCB, theSctx, loc,
                                    FuncCode.OP_EQ, args);
            return pred;
        }

        int numArgs = pred.getNumArgs();
        assert(numArgs <= 2);
        ArrayList<Expr> args = new ArrayList<Expr>(numArgs);

        for (int i = 0; i < numArgs; ++i) {
            Expr arg = pred.getArg(i);
            if (arg == pi.theVarArg) {
                args.add(idxColRef);
            } else {
                args.add(arg);
            }
        }

        pred = new ExprFuncCall(theQCB, theSctx, pred.theLocation,
                                pred.getFunction(null), args);
        return pred;
    }

    void rewriteExpr(ExprVar idxVar, Expr expr) {

        ArrayList<ExprToReplace> exprsToReplace =
            theExprRewriteMap.get(expr);

        if (exprsToReplace == null) {
            /* The expr does not reference any columns of theTable */
            return;
        }

        for (ExprToReplace etr : exprsToReplace) {

            int fieldPos;

            if (theIndex == null) {
                fieldPos = theTable.getPrimKeyPos(etr.theIndexFieldPos);
            } else {
                fieldPos = etr.theIndexFieldPos;
            }

            ExprFieldStep idxColRef = new ExprFieldStep(theQCB,
                                                        theSctx,
                                                        etr.theExpr.theLocation,
                                                        idxVar,
                                                        fieldPos);
        
            etr.theExpr.replace(idxColRef, true/*destroy*/);
        }
    }

    void analyzeNonTargetTables() {

        int numTables = theTableExpr.getNumTables();

        for (int i = 0; i < numTables; ++i) {

            if (i == theTargetTablePos) {
                continue;
            }

            IndexAnalyzer analyzer =
                new IndexAnalyzer(theSFW, theTableExpr, i, null/*index*/);

            analyzer.analyze();

            if (analyzer.theSFW == null) {
                return;
            }

            theTableExpr.addPrimaryKeys(i, null, null, analyzer.theIsCovering);
        }

        /*
         * TODO: Optimize the case where an ON pred is an index-only expr, but
         * the primary index is not covering. We should apply the pred using
         * the index row and retrieve the full table row only for the row that
         * survive the filter.
         */
    }

    /**
     * Do the work!
     *
     * Note: This method will set theSFW to null if it discovers that the whole
     * WHERE expr is always false. If so, it will also replace the whole SFW
     * expr with an empty expr. Callers of this method should check whether
     * theSFW has been set to null.
     */
    void analyze() {

        if (theTrace >= 1) {
            System.out.println(
                "\nAnalysing index " + getIndexName() + "\n");
        }

        /*
         * Analyze WHERE preds and collect start/stop and filtering preds for
         * the index. We do this even for non-target tables in a NESTED TABLES
         * clause, even though WHERE preds on non-target preds cannot be pushed
         * down to the index (it's not semantically correct). Nevertheless we
         * do it because we need the collected info to decide if the index used
         * to scan a non-target table is covering or not.
         */
        Expr predsExpr = theSFW.getWhereExpr();

        if (predsExpr != null) {

            ArrayList<Expr> preds;

            Function andOp = predsExpr.getFunction(FuncCode.OP_AND);

            if (andOp != null) {
                preds = ((ExprFuncCall)predsExpr).getArgs();
            } else {
                preds = new ArrayList<Expr>(1);
                preds.add(predsExpr);
            }

            for (Expr pred : preds) {

                WherePredInfo wpi = new WherePredInfo(pred);
                theWherePreds.add(wpi);

                collectPredInfo(wpi, pred);
                
                if (theSFW == null) {
                    return;
                }
            }

            /*
             * Look for conflicting preds within each "class" of preds.
             */
            for (int ipos = 0; ipos < theNumFields; ++ipos) {

                boolean alwaysFalse = skipExtraneousPreds(ipos);

                if (alwaysFalse) {
                    processAlwaysFalse();
                    return;
                }
            }

            /*
             * Only one of the multikey WHERE preds can be used for the index
             * scan. Choose the "best" one. Also convert STARTSTOP preds to
             * FILTERING ones, if needed.
             */
            chooseMultiKeyPredGroup();
        }

        if (theTablePos == theTargetTablePos) {
            /*
             * Go through the preds again to collect any filtering preds that
             * are not start/stop preds. We do this after choosing the best
             * MapBoth key, because only path exprs using the chosen map key
             * can be considered as index-only exprs.
             */
            for (WherePredInfo wpi : theWherePreds) {

                for (PredInfo pi : wpi.thePredInfos) {
                    // ????
                    boolean simplePathsOnly = (pi.thePredGroup != theBestPredGroup);
                    if (pi.theStatus == PredicateStatus.NOT_STARTSTOP &&
                        isIndexOnlyExpr(pi.thePred, true, simplePathsOnly)) {
                        pi.theStatus = PredicateStatus.FILTERING;

                        if (theTrace > 0) {
                            System.out.println(
                                "NOT_STARTSTOP pred converted to filtering " +
                                " pred:\n" +
                                pi.thePred.display());
                        }
                    }
                }
            }

            pushStartStopPreds();

            for (WherePredInfo wpi : theWherePreds) {

                for (PredInfo pi : wpi.thePredInfos) {

                    if (pi.theStatus == PredicateStatus.FILTERING) {
                        theFilteringPreds.add(pi);
                    }
                } 
            }
        }

        /* Check if the index is a covering one */
        checkIsCovering();
    }

    private void collectPredInfo(WherePredInfo wpi, Expr pred) {

        PredInfo pi = new PredInfo(wpi, pred);

        if (pred.getKind() == ExprKind.CONST) {
            ExprConst constExpr = (ExprConst)pred;

            if (constExpr.getValue() == BooleanValueImpl.falseValue) {
                pi.theStatus = PredicateStatus.FALSE;
                processAlwaysFalse();
                return;
            }

            if (constExpr.getValue() == BooleanValueImpl.trueValue) {
                pi.theStatus = PredicateStatus.TRUE;
                return;
            }
        }

        Function func = pred.getFunction(null);

        if (func == null ||
            (!func.isComparison() && func.getCode() != FuncCode.OP_EXISTS)) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            wpi.add(pi);
            return;
        }

        FuncCode op = func.getCode();
        ExprFuncCall compExpr = (ExprFuncCall)pred;
        Expr varArg;
        Expr constArg;
        FieldValueImpl constVal = null;
        boolean isExists = (op == FuncCode.OP_EXISTS);
        boolean isNullOp = (op == FuncCode.OP_IS_NULL ||
                            op == FuncCode.OP_IS_NOT_NULL);

        if (op == FuncCode.OP_IS_NULL) {
            op = FuncCode.OP_EQ;
        } else if (op == FuncCode.OP_IS_NOT_NULL) {
            op = FuncCode.OP_LT;
        } else if (op == FuncCode.OP_EXISTS) {
            op = FuncCode.OP_NEQ;
        } else if (func.isAnyComparison()) {
            op = FuncAnyOp.anyToComp(op);
        }

        if (isNullOp) {
            varArg = compExpr.getArg(0);
            constArg = null;
            constVal = NullValueImpl.getInstance();

        } else if (isExists) {
            varArg = compExpr.getArg(0);
            constArg = null;
            constVal = EmptyValueImpl.getInstance();

        } else {
            Expr arg0 = compExpr.getArg(0);
            Expr arg1 = compExpr.getArg(1);
            
            if (arg0.isConstant()) {
                constArg = arg0;
                varArg = arg1;
                op = FuncCompOp.swapCompOp(op);
            } else if (arg1.isConstant()) {
                constArg = arg1;
                varArg = arg0;
            } else {
                pi.theStatus = PredicateStatus.NOT_STARTSTOP;
                wpi.add(pi);
                // TODO: try to find path filtering preds in both args.
                return;
            }

            if (constArg.getKind() == ExprKind.CONST) {
                constVal = ((ExprConst)constArg).getValue();
            }
        }
            
        IndexExpr epath =  varArg.getIndexExpr();

        if (epath == null || epath.theTable.getId() != theTable.getId()) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            wpi.add(pi);
            return;
        }

        pi.theOp = op;
        pi.theIsValueComp = func.isValueComparison();
        pi.theIsExists = isExists;
        pi.theVarArg = varArg;
        pi.theConstArg = constArg;
        pi.theConstVal = constVal;
        pi.theEpath = epath;

        wpi.theDoesSlicing = (wpi.theDoesSlicing || epath.theDoesSlicing);

        if (func.isAnyComparison() &&
            (theIsPrimary || !theIndex.isMultiKey())) {
            pi.theStatus = PredicateStatus.SKIP;
        }

        if (op == FuncCode.OP_NEQ && !isExists) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
        }

        boolean added = wpi.add(pi);

        if (added) {
            matchPred(pi);
        }

        if (pi.theStatus == PredicateStatus.FALSE) {
            processAlwaysFalse();
            return;
        }

        if (pi.theStatus == PredicateStatus.STARTSTOP) {

            addStartStopPred(pi);

            if (epath.theIsUnnested && epath.getMapBothKey() != null) {
                throw new QueryStateException(
                    "Found a pred factor that is both unnested and has a " +
                    "MapBoth key. Predicate = \n" + pi.thePred.display());
            }

            if (epath.theIsUnnested) {
                PredGroup.addUnnestedPred(this, pi);

            } else if (epath.getMapBothKey() != null && theIsMapBothIndex) {

                boolean found = PredGroup.addMapBothPred(this, pi);

                if (!found) {

                    FieldValueImpl keyval = FieldDefImpl.stringDef.
                        createString(epath.getMapBothKey());

                    PredInfo keypi = new PredInfo(wpi, null);
                    keypi.theOp = FuncCode.OP_EQ;
                    keypi.theIsValueComp = true;
                    keypi.theConstVal = keyval;
                    keypi.theEpath = pi.theEpath;
                    keypi.theIPathPos = theIndex.getPosForKeysField();
                    keypi.theStatus = PredicateStatus.STARTSTOP;
                    
                    wpi.add(keypi);
                    PredGroup.addMapBothPred(this, keypi);
                    addStartStopPred(keypi);
                }

            } else if (epath.isMultiKey()) {

                PredGroup pg;

                if (epath.theIsDirect || epath.getRelativeCtxVarPos() > 0) {
                    if (wpi.theLocalGroup == null) {
                        pg = addPredGroup(pi);
                        wpi.theLocalGroup = pg;
                    } else {
                        pg = wpi.theLocalGroup;
                        wpi.theLocalGroup.thePredInfos.add(pi);
                        pi.thePredGroup = wpi.theLocalGroup;
                    }
                } else {
                    pg = addPredGroup(pi);
                }

                pg.theCtxVar = epath.theCtxVar;
            }
        }

        List<Expr> filteringPreds = epath.getFilteringPreds();

        if (filteringPreds != null) {

            for (Expr fpred : filteringPreds) {

                if (pi.isUnnested() && theUnnestedGroup != null) {
                    boolean foundDuplicate = false;
                    for (PredInfo pi2 : theUnnestedGroup.thePredInfos) {
                        if (pi2.thePred == fpred) {
                            foundDuplicate = true;
                            break;
                        }
                    }

                    if (foundDuplicate) {
                        continue;
                    }
                }

                collectPredInfo(wpi, fpred);
            }
        }
    }

    /**
     * Check if the given pred is a start/stop pred for a path of theIndex.
     * If not, return null. Otherwise, build a PredInfo for it, and check
     * the pred against any other start/stop preds on the same index path.
     * Return the PredInfo, which includes the result of this check.
     */
    private void matchPred(PredInfo pi) {

        if (pi.theStatus != PredicateStatus.UNKNOWN) {
            return;
        }

        IndexExpr epath = pi.theVarArg.getIndexExpr();

        boolean matched = matchPathExprToIndexPath(theIndex, epath, false);

        if (!matched) {
            pi.theStatus = PredicateStatus.SKIP;
            if (theTrace >= 2) {
                System.out.println(
                    "Match failure for epath " + epath.getPathName());
            }
            return;
        }

        if (epath.theIsDirect &&
            pi.theVarArg.isMultiValued() &&
            pi.theIsValueComp) {
            pi.theStatus = PredicateStatus.SKIP;
            return;
        }

        /*
         * A predicate on a prim-key column, which is not part of the index
         * key definition, cannot be used as a start/stop pred on a secondary
         * index. It can, however, be used as a filtering pred.
         */
        if (!theIsPrimary &&
            epath.thePrimKeyPos >= 0 &&
            epath.getPathPos() >= theIndex.numFields() ) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            return;
        }

        Expr constArg = pi.theConstArg;
        FieldValueImpl constVal = pi.theConstVal;
        
        if (constArg != null && constVal != null && !constVal.isNull()) {

            FieldDefImpl targetType = (epath.getJsonDeclaredType() != null ?
                                       epath.getJsonDeclaredType() :
                                       pi.theVarArg.getType().getDef());
            FieldValueImpl newConstVal;

            if (!TypeManager.areTypesComparable(targetType,
                                                constVal.getDefinition())) {

                if (theQCB.strictMode()) {
                    throw new QueryException(
                        "Incompatible types for comparison operator: \n" +
                        "Type1: " + pi.theVarArg.getType() +
                        "\nType2: " + pi.theConstArg.getType(),
                        pi.thePred.getLocation());
                }

                newConstVal = BooleanValueImpl.falseValue;

            } else {
                newConstVal = FuncCompOp.castConstInCompOp(
                    targetType,
                    epath.theIsJson, /*allowJsonNull*/
                    false, /* ignore nullability of varArg */
                    pi.theVarArg.isScalar(),
                    constVal,
                    pi.theOp,
                    theQCB.strictMode());
            }

            if (newConstVal != constVal) {

                if (newConstVal == BooleanValueImpl.falseValue) {
                    pi.theStatus = PredicateStatus.FALSE;
                    return;
                }

                if (newConstVal == BooleanValueImpl.trueValue) {
                    pi.theStatus = PredicateStatus.TRUE;
                    return;
                }

                constVal = newConstVal;
                pi.theConstVal = constVal;
            }
        }

        if (constArg != null && !checkTypes(pi.theVarArg, constArg, epath)) {
            if (theTrace >= 2) {
                System.out.println(
                    "Match failure due to type check for epath " +
                    epath.getPathName());
            }
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            return;
        }

        pi.theStatus = PredicateStatus.STARTSTOP;
        pi.theIPathPos = epath.getPathPos();

        if (pi.isUnnested() && !pi.getIndexPath().isMultiKey()) {
            throw new QueryStateException(
                "An unnested predicate matches with the non-multikey index " +
                "field at position pi.theIPathPos. predicate:\n" + pi);
        }
    }

    private boolean matchPathExprToIndexPath(
        IndexImpl index,
        IndexExpr epath,
        boolean matchSimplePathsOnly) {

        if (!epath.matchesIndex(theTable, index)) {
            return false;
        }

        if (matchSimplePathsOnly &&
            theBestPredGroup != null &&
            theBestPredGroup.theMapBothKey != null &&
            theBestPredGroup.theMapBothKey.equals(epath.getMapBothKey())) {
            matchSimplePathsOnly = false;
        }

        if (epath.isMultiKey() && matchSimplePathsOnly) {
            return false;
        }

        return true;
    }

    private boolean checkTypes(
        Expr varArg,
        Expr constArg,
        IndexExpr epath) {

        if (!constArg.isScalar()) {
            return false;
        }

        FieldDefImpl constType = constArg.getType().getDef();
        Type constTypeCode = constType.getType();
        FieldDefImpl varType;
        Type varTypeCode;
        boolean varIsScalar;

        if (epath.getJsonDeclaredType() != null) {
            varType = epath.getJsonDeclaredType();
            varIsScalar = (!epath.isMultiKey() || epath.getMapBothKey() != null);

            // TODO: change this when we have a specific type for json null
            if (constTypeCode == Type.JSON) {
                return true;
            }
        } else {
            varType = varArg.getType().getDef();
            varIsScalar = varArg.isScalar();
        }

        varTypeCode = varType.getType();

        switch (varTypeCode) {
        case INTEGER:
            return (constTypeCode == Type.INTEGER ||
                    (varIsScalar && constTypeCode == Type.LONG));
        case LONG:
            return (constTypeCode == Type.LONG ||
                    constTypeCode == Type.INTEGER);
        case FLOAT:
            return (constTypeCode == Type.FLOAT ||
                    (varIsScalar && constTypeCode == Type.DOUBLE) ||
                    constTypeCode == Type.INTEGER ||
                    constTypeCode == Type.LONG);

        case DOUBLE:
            return (constTypeCode == Type.DOUBLE ||
                    constTypeCode == Type.FLOAT ||
                    constTypeCode == Type.INTEGER ||
                    constTypeCode == Type.LONG);
        case NUMBER:
            return constType.isNumeric();
        case ENUM:
            return (constTypeCode == Type.STRING ||
                    constTypeCode == Type.ENUM);
        case STRING:
        case BOOLEAN:
            return varTypeCode == constTypeCode;
        case TIMESTAMP:
            return (varTypeCode == constTypeCode &&
                    ((TimestampDefImpl)varType).getPrecision() ==
                    ((TimestampDefImpl)constType).getPrecision());
        default:
            return false;
        }
    }

    boolean skipExtraneousPreds(int ipos) {

        ArrayList<PredInfo> predinfos = theStartStopPreds.get(ipos);

        if (predinfos == null) {
            return false;
        }

        for (int i = 0; i < predinfos.size(); ++i) {

            PredInfo pi1 = predinfos.get(i);

            for (int j = i + 1; j < predinfos.size(); ++j) {

                PredInfo pi2 = predinfos.get(j);
                    
                if (!pi1.isCompatible(pi2)) {
                    continue;
                }

                if (pi2.isExists()) {
                    pi2.theStatus = PredicateStatus.TRUE;

                } else if (pi2.isEq()) {

                    if (pi1.isExists()) {
                        pi1.theStatus = PredicateStatus.TRUE;
                    } else if (pi1.isEq()) {
                        checkEqEq(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkEqMin(pi2, pi1);
                    } else {
                        assert(pi1.isMax());
                        checkEqMax(pi2, pi1);
                    }

                } else if (pi2.isMin()) {

                    if (pi1.isExists()) {
                        pi1.theStatus = PredicateStatus.TRUE;
                    } else if (pi1.isEq()) {
                        checkEqMin(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkMinMin(pi2, pi1);
                    } else {
                        assert(pi1.isMax());
                        checkMinMax(pi2, pi1);
                    }

                } else {
                    assert(pi2.isMax());

                    if (pi1.isExists()) {
                        pi1.theStatus = PredicateStatus.TRUE;
                    } else if (pi1.isEq()) {
                        checkEqMax(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkMinMax(pi1, pi2);
                    } else {
                        assert(pi1.isMax());
                        checkMaxMax(pi2, pi1);
                    }
                }

                if (pi1.theStatus == PredicateStatus.FALSE ||
                    pi2.theStatus == PredicateStatus.FALSE) {
                    return true;
                }

                if (pi1.theStatus == PredicateStatus.TRUE ||
                    pi1.theStatus == PredicateStatus.FILTERING) {
                    predinfos.remove(i);
                    --i;
                    break;
                }

                if (pi2.theStatus == PredicateStatus.TRUE ||
                    pi2.theStatus == PredicateStatus.FILTERING) {
                    predinfos.remove(j);
                    --j;
                }
            }
        }

        return false;
    }

    private void checkEqEq(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp = FieldValueImpl.compareFieldValues(p1.theConstVal,
                                                        p2.theConstVal);

            if (cmp == 0) {
                p1.theStatus = PredicateStatus.TRUE;
            } else {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;
            }

        } else if (p1.theConstVal != null) {
            p2.theStatus = PredicateStatus.FILTERING;

        } else {
            p1.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkEqMin(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp = FieldValueImpl.compareFieldValues(p1.theConstVal,
                                                        p2.theConstVal);

            if (cmp < 0 || (cmp == 0 && !p2.isInclusive())) {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;
            } else {
                p2.theStatus = PredicateStatus.TRUE;
            }

        } else {
            p2.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkEqMax(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp = FieldValueImpl.compareFieldValues(p1.theConstVal,
                                                        p2.theConstVal);

            if (cmp > 0 || (cmp == 0 && !p2.isInclusive())) {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;
            } else {
                p2.theStatus = PredicateStatus.TRUE;
            }

        } else {
            p2.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkMinMin(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp =  FieldValueImpl.compareFieldValues(p1.theConstVal,
                                                         p2.theConstVal);

            if (cmp < 0 || (cmp == 0 && p1.isInclusive())) {
                p1.theStatus = PredicateStatus.TRUE;
            } else {
                p2.theStatus = PredicateStatus.TRUE;
            }

        } else if (p1.theConstVal != null) {
            p2.theStatus = PredicateStatus.FILTERING;

        } else {
            p1.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkMaxMax(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp =  FieldValueImpl.compareFieldValues(p1.theConstVal,
                                                         p2.theConstVal);

            if (cmp < 0 || (cmp == 0 && p2.isInclusive())) {
                p2.theStatus = PredicateStatus.TRUE;
            } else {
                p1.theStatus = PredicateStatus.TRUE;
            }

        } else if (p1.theConstVal != null) {
            p2.theStatus = PredicateStatus.FILTERING;

        } else {
            p1.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkMinMax(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp = p1.theConstVal.compareTo(p2.theConstVal);

            if (cmp > 0 ||
                (cmp == 0 && (!p2.isInclusive() || !p1.isInclusive()))) {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;

            } else if (cmp == 0) {
                p1.theOp = FuncCode.OP_EQ;
                p2.theStatus = PredicateStatus.TRUE;
            }
        }
    }

    private boolean checkAlwaysTrue(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal == null ||
            p2.theConstVal == null ||
            p2.theEnclosingPred.doesFiltering() ||
            p2.theEnclosingPred.theDoesSlicing) {
            return false;
        }

        int cmp = FieldValueImpl.compareFieldValues(p1.theConstVal,
                                                    p2.theConstVal);

        if (p1.isEq()) {
            if (p2.isEq()) {
                return (cmp == 0);
            }
            if (p2.isMin()) {
                if (cmp < 0 || (cmp == 0 && !p2.isInclusive())) {
                    return false;
                }
                return true;
            }
            assert(p2.isMax());
            if (cmp > 0 || (cmp == 0 && !p2.isInclusive())) {
                return false;
            }
            return true;
        }

        if (p1.isMin()) {
            if (p2.isEq()) {
                return false;
            }
            if (p2.isMin()) {
                if (cmp < 0 || (cmp == 0 &&
                                p1.isInclusive() && !p2.isInclusive())) {
                    return false;
                }
                return true;
            }
            assert(p2.isMax());
            return false;
        }

        if (p1.isMax()) {
            if (p2.isEq()) {
                return false;
            }
            if (p2.isMin()) {
                return false;
            }
            assert(p2.isMax());
            if (cmp < 0 || (cmp == 0 && 
                            (!p1.isInclusive() || p2.isInclusive()))) {
                return true;
            }
            return false;
        }

        return false;
    }

    private void chooseMultiKeyPredGroup() {

        boolean filteringOnly = false;

        if (theTrace >= 1) {
            System.out.println(
                "Choosing multikey predicate class");
        }

        for (int ipos = 0; ipos < theNumFields; ++ipos) {

            if (theTrace >= 1) {
                System.out.println("processing ifield at pos " + ipos);
            }

            ArrayList<PredInfo> predinfos = theStartStopPreds.get(ipos);

            if (predinfos == null) {
                if (theTrace >= 1) {
                    System.out.println("no preds at pos " + ipos);
                }
                filteringOnly = true;
                continue;
            }

            if (theIsPrimary || !theIndexPaths.get(ipos).isMultiKey()) {
                if (predinfos.size() > 2) {
                    throw new QueryStateException(
                        "More than two predicates for non-multikey index " +
                        "field at position " + ipos);
                }

                if (!predinfos.get(0).isEq()) {
                    if (theTrace >= 1) {
                        System.out.println("no EQ preds at pos " + ipos);
                    }
                    filteringOnly = true;
                }
                continue;
            }

            for (int i = 0; i < predinfos.size(); ++i) {

                PredInfo pi = predinfos.get(i);

                PredGroup pg = pi.thePredGroup;

                if (theTrace >= 1) {
                    System.out.println(
                        "processing pred at pos " + ipos + "\nPG: " +
                        pg.theId + " filtering = " + pg.theFilteringOnly +
                        "\nMapBothKey = " + pg.theMapBothKey + "\nPred = \n" +
                        pi);
                }

                if (pi.theStatus != PredicateStatus.STARTSTOP) {
                    throw new QueryStateException(
                        "Found a non STARTSTOP predicate in theStartStopPreds " +
                        " as position " + ipos);
                }

                if (filteringOnly || pg.theFilteringOnly) {
                    if (pi.isEq()) {
                        pg.theFieldScore += filterEqValue;
                    } else {
                        pg.theFieldScore += filterOtherValue;
                    }

                } else if (pi.isEq()) {
                    pg.theFieldScore += eqValue;
                } else {
                    pg.theFieldScore += vrangeValue;
                    pg.theFoundRange = true;
                }

                if (theTrace >= 1) {
                    System.out.println("Field score = " + pg.theFieldScore);
                }
            }

            for (PredGroup pg : thePredGroups) {
                pg.theScore += pg.theFieldScore;
                if (theTrace >= 1) {
                    System.out.println("Total score for PG " + pg.theId +
                                       " = " + pg.theScore);
                }
                if (pg.theFieldScore == 0 || pg.theFoundRange) {
                    pg.theFilteringOnly = true;
                }
                pg.theFieldScore = 0;
            }
        }

        /*
         * Now choose the "best" multikey WHERE pred
         */
        for (PredGroup pg : thePredGroups) {
            if (theBestPredGroup == null) {
                if (pg.theScore > 0) {
                    theBestPredGroup = pg;
                }
            } else if (pg.theScore > theBestPredGroup.theScore) {
                theBestPredGroup = pg;
            } else if (pg.theScore == theBestPredGroup.theScore) {
                if (!pg.theIsUnnested && theBestPredGroup.theIsUnnested ||
                    pg.theId < theBestPredGroup.theId) {
                    theBestPredGroup = pg;
                }
            }
        }

        if (theBestPredGroup == null) {
            return;
        }

        if (theIndex != null && theIndex.isMultiKeyMapIndex()) {
            for (PredInfo pi : theBestPredGroup.thePredInfos) {
                if (pi.theIPathPos >= 0) {
                    IndexField ipath = theIndexPaths.get(pi.theIPathPos);
                    if (pi.mapBothKey() != null ||
                        (pi.isEq() && ipath.isMapKeys())) {
                        theHaveMapKeyEqPred = true;
                        break;
                    }
                }
            }
        }

        if (theTrace >= 1) {
            System.out.println(
                "Best pred group = \n" + theBestPredGroup.theId);
        }

        /*
         * Remove from the multikey index paths in theStartStopPreds all pred
         * infos that do not belong to theBestMultiKeyPred, and mark them as SKIP.
         */
        for (int i = 0; i < theNumFields; ++i) {

            if (!theIndexPaths.get(i).isMultiKey()) {
                continue;
            }

            ArrayList<PredInfo> preds = theStartStopPreds.get(i);

            if (preds == null) {
                continue;
            }

            for (int j = 0; j < preds.size(); ++j) {

                PredInfo pi = preds.get(j);

                if (pi.theStatus == PredicateStatus.SKIP) {
                    preds.remove(j);
                    --j;
                    continue;
                }

                if (pi.thePredGroup == theBestPredGroup) {
                    continue;
                }

                preds.remove(j);
                --j;

                if (pi.mapBothKey() == null) {
                    for (PredInfo pi2 : preds) {
                        if (pi2.thePredGroup == theBestPredGroup &&
                            (pi2.isUnnested() == pi.isUnnested() ||
                             pi2.isUnnested()) &&
                            checkAlwaysTrue(pi2, pi)) {
                            pi.theStatus = PredicateStatus.TRUE;
                            break;
                        }
                    }
                }

                if (pi.theStatus != PredicateStatus.TRUE) {
                    pi.theStatus = PredicateStatus.SKIP;
                }
            }
        }
    }

    /**
     * Try to push start/stop preds on the given index path.
     */
    private void pushStartStopPreds() {

        boolean pushedMultiKeyPred = false;

        int lastStartStopPos = -2;
        int ipos = 0;
        for (; ipos < theNumFields; ++ipos) {

            ArrayList<PredInfo> predinfos = theStartStopPreds.get(ipos);

            if (predinfos == null || predinfos.isEmpty()) {
                if (lastStartStopPos == -2) {
                    lastStartStopPos = ipos-1;
                }
                continue;
            }

            if (predinfos.size() > 2) {
                throw new QueryStateException(
                    "More than 2 start/stop predicates for index field at " +
                    "position " + ipos);
            }

            PredInfo pi1 = predinfos.get(0);
            PredInfo pi2 = (predinfos.size() > 1 ? predinfos.get(1) : null);

            if (pi1.theStatus != PredicateStatus.STARTSTOP) {
                throw new QueryStateException(
                    "Pushing a predicate marked as " + pi1.theStatus + "\n" +
                    pi1);
            }

            if (pi2 != null && pi2.theStatus != PredicateStatus.STARTSTOP) {
                throw new QueryStateException(
                    "Pushing a predicate marked as " + pi2.theStatus + "\n" +
                    pi2);
            }

            if (lastStartStopPos >= -1) {
                pi1.theStatus = PredicateStatus.FILTERING;
                if (pi2 != null) {
                    pi2.theStatus = PredicateStatus.FILTERING;
                }

                continue;
            }

            IndexField ipath = theIndexPaths.get(ipos);
 
            pushedMultiKeyPred = (pushedMultiKeyPred || ipath.isMultiKey());

            if (pi1.isEq()) {
                assert(predinfos.size() == 1);

                assert(!ipath.isMapKeys() || theHaveMapKeyEqPred);

                FieldValueImpl constVal;

                if (pi1.theConstVal != null) {
                    thePushedExternals.add(null);
                    constVal = pi1.theConstVal;
                } else {
                    theHavePushedExternals = true;
                    thePushedExternals.add(pi1.theConstArg);
                    constVal = createPlaceHolderValue(ipath.getType());
                }

                if (theIsPrimary) {
                    thePrimaryKeys.get(0).put(ipath.getStep(0), constVal);
                } else {
                    theSecondaryKeys.get(0).put(ipos, constVal);
                }
                ++theNumEqPredsPushed;

                continue;
            }

            if (pi1.isExists()) {
                assert(predinfos.size() == 1);
                assert(!theIsPrimary);

                theSecondaryKeys.add(theSecondaryKeys.get(0));

                String pathName = theIndex.getFieldName(ipos);
                FieldDefImpl rangeDef = ipath.getType();

                FieldRange fr1 = new FieldRange(pathName, rangeDef, 0);
                FieldRange fr2 = new FieldRange(pathName, rangeDef, 0);

                fr1.setEnd(EmptyValueImpl.getInstance(), false, false);
                fr2.setStart(EmptyValueImpl.getInstance(), false, false);

                theRanges.set(0, fr1);
                theRanges.add(fr2);
                thePushedExternals.add(null);

                if (ipath.isMultiKey() && !theHaveMapKeyEqPred) {
                    theIsMultiKeyRange = true;
                    theEliminateDups = true;
                }
                
                lastStartStopPos = ipos;
                continue;
            }

            PredInfo minpi = null;
            PredInfo maxpi = null;

            if (pi1.isMin()) {
                minpi = pi1;

                if (pi2 != null) {
                    assert(pi2.isMax());
                    maxpi = pi2;
                }
            } else {
                assert(pi1.isMax());
                maxpi = pi1;

                if (pi2 != null) {
                    assert(pi2.isMin());
                    minpi = pi2;
                }
            }

            createRange(ipath, minpi, maxpi);

            if (theIsMultiKeyRange) {
                theEliminateDups = true;
            }

            lastStartStopPos = ipos;
        }

        if (theIndex != null &&
            theIndex.isMultiKey() &&
            !theHaveMapKeyEqPred &&
            !theEliminateDups) {

            if (!pushedMultiKeyPred) {
                theEliminateDups = true;
            } else {
                for (ipos = lastStartStopPos+1;
                     ipos < theIndexPaths.size();
                     ++ipos) {
                    IndexField ipath = theIndexPaths.get(ipos);
                    //System.out.println("Path at pos " + ipos +
                    //                   " isMultiKey = " + ipath.isMultiKey());
                    if (ipath.isMultiKey()) {
                        theEliminateDups = true;
                        break;
                    }
                }
            }
        }

        //System.out.println("theEliminateDups for index " + getIndexName() +
        //                   " = " + theEliminateDups);
    }

    /**
     *
     */
    private void createRange(IndexField ipath, PredInfo minpi, PredInfo maxpi) {

        int storageSize = (theIsPrimary ?
                           theTable.getPrimaryKeySize(ipath.getStep(0)) :
                           0);

        FieldDefImpl rangeDef = ipath.getType();

        String pathName = (theIsPrimary ? ipath.getStep(0) :
                           theIndex.getFieldName(ipath.getPosition()));

        FieldRange range = new FieldRange(pathName, rangeDef, storageSize);
        theRanges.set(0, range);

        if (minpi != null) {
            if (minpi.theConstVal == null) {
                theHavePushedExternals = true;
                thePushedExternals.add(minpi.theConstArg);
                FieldValueImpl val = createPlaceHolderValue(rangeDef);
                range.setStart(val, minpi.isInclusive(), false);
            } else {
                thePushedExternals.add(null);
                range.setStart(minpi.theConstVal, minpi.isInclusive());
            }

            if (ipath.isMultiKey() && !theHaveMapKeyEqPred) {
                theIsMultiKeyRange = true;
            }

        } else {
            thePushedExternals.add(null);
        }

        if (maxpi != null) {
            if (maxpi.theConstVal == null) {
                theHavePushedExternals = true;
                thePushedExternals.add(maxpi.theConstArg);
                FieldValueImpl val = createPlaceHolderValue(rangeDef);
                range.setEnd(val, maxpi.isInclusive(), false);
            } else {
                thePushedExternals.add(null);
                range.setEnd(maxpi.theConstVal, maxpi.isInclusive());
            }

            if (ipath.isMultiKey() && !theHaveMapKeyEqPred) {
                theIsMultiKeyRange = true;
            }

        } else {
            thePushedExternals.add(null);
        }
    }

    /*
     * Check if the index is a covering one. For this to be true, the index
     * must "cover" all the exprs in the query. We say that the index covers
     * an expr if the expr does not reference any non-indexed paths within
     * theTable. If the query does not have a NESTED TABLES, this means that
     * the whole expr can be evaluated using index fields only.
     */
    private void checkIsCovering() {

        int numPreds = getNumPreds();
        int numIndexPreds = 0;

        if (theSFW == null) {
            theIsCovering = false;
            return;
        }

        /*
         * Any index of key-only table is always covering. Nevertheless, we
         * must still go through all the exprs and call isIndexOnlyExpr on
         * them, because isIndexOnlyExpr() creates theExprRewriteMap.
         */
        boolean isKeyOnly = (theSFW != null && theTable.isKeyOnly());
 
        if (isKeyOnly) {
            assert(theIsPrimary || !theIndex.isMultiKey());
            assert(!theSFW.hasSort() ||
                   (theSFW.hasPrimaryIndexBasedSort() && theIsPrimary) ||
                   (theSFW.getSortingIndexes().contains(theIndex)));
        }

        boolean hasNestedTables = theTableExpr.hasNestedTables();

        /*
         * Check whether the index covers all the WHERE preds.
         */
        for (WherePredInfo wpi : theWherePreds) {

            if (wpi.isFullyPushable()) {
                ++numIndexPreds;
                continue;
            }

            if (hasNestedTables &&
                isIndexOnlyExpr(wpi.thePred, false, true)) {
                ++numIndexPreds;
            }
        }

        assert(numIndexPreds <= numPreds);

        theIsCovering = (numIndexPreds == numPreds);

        if (!theIsCovering) {
            assert(!isKeyOnly);
            return;
        }

        /*
         * Check whether the index covers all the exprs in the SELECT clause.
         */
        int numFieldExprs = theSFW.getNumFields();

        for (int i = 0; i < numFieldExprs; ++i) {

            Expr expr = theSFW.getFieldExpr(i);

            if (!isIndexOnlyExpr(expr, false, true)) {

                /*
                 * If the expr is a row var, see if every column of the table
                 * is contained in the index entry.
                 */
                if ((theIsPrimary || !theIndex.isMultiKey()) &&
                    expr.getKind() == ExprKind.VAR &&
                    ((ExprVar)expr).getTable() != null &&
                    ((ExprVar)expr).getTable().getId() == theTable.getId()) {

                    RecordDefImpl rowDef = theTable.getRowDef();
                    int numCols = rowDef.getNumFields();

                    int j = 0;
                    for (; j < numCols; ++j) {
                        String colName = rowDef.getFieldName(j);

                        int k = 0;
                        for (; k < theNumFields; ++k) {
                            IndexField ipath = theIndexPaths.get(k);
                            if (ipath.numSteps() == 1 &&
                                ipath.getStep(0).equalsIgnoreCase(colName)) {
                                break;
                            }
                        }

                        if (k == theNumFields) {
                            break;
                        }
                    }

                    if (j == numCols) {
                        continue;
                    }
                }
            } else {
                continue;
            }

            theIsCovering = false;
            return;
        }

        /*
         * The index must cover all the exprs in the ORDERBY clause. Normally,
         * this should be true already, but we must call isIndexOnlyExpr() on
         * each sort expr in order to rewrite it to access the index var. 
         * Furthermore, the primary index is always analyzed, and if the query
         * is not sorting by prim key columns, we must mark the prim index as
         * not covering.
         */
        if (theTablePos == theTargetTablePos) {
            int numSortExprs = theSFW.getNumSortExprs();

            for (int i = 0; i < numSortExprs; ++i) {
                Expr expr = theSFW.getSortExpr(i);
                if (!isIndexOnlyExpr(expr, true, true)) {
                    theIsCovering = false;
                    return;
                }
            }
        }

        int numFroms = theSFW.getNumFroms();

        for (int i = 0; i < numFroms; ++i) {

            FromClause fc = theSFW.getFromClause(i);
            Expr domExpr = fc.getDomainExpr();

            /*
             * Check whether the index covers all the ON preds in a NESTED
             * TABLES clause.
             */
            if (domExpr == theTableExpr) {

                if (!hasNestedTables) {
                    continue;
                }

                int numTables = theTableExpr.getNumTables();

                for (int j = 0; j < numTables; ++j) {
                    Expr pred = theTableExpr.getTablePred(j);
                    if (pred != null && 
                        !isIndexOnlyExpr(pred, false, true)) {
                        theIsCovering = false;
                        return;
                    }
                }

                continue;
            }

            ExprVar var = fc.getVar();

            /* 
             * If the var is used in any exprs, those exprs have been checked
             * above. Also, if the var is not used in any exprs, but its
             * domain expr is scalar, the var will be removed when we apply
             * the index. So, they only case we need to check here is that
             * the var is not used in any exprs and its domain is not scalar.
             */
            if (var.getNumParents() == 0 && !domExpr.isScalar()) {

                if (!isIndexOnlyExpr(domExpr, false, true)) {
                    theIsCovering = false;
                    assert(!isKeyOnly);
                    return;
                }
            }
        }
    }

    /**
     * If strict is true, this method checks whether the given expr is an expr
     * that can be evaluated using the columns of the current index only (which
     * may be the primary index, if theIndex is null). If strict is false, the
     * method allows the expr to reference columns from tables other than 
     * theTable. So it will return true if for each subexpr of expr that
     * references columns of theTable only, the subexpr can be evaluated using
     * the columns of the current index entry only.
     */
    private boolean isIndexOnlyExpr(
        Expr expr,
        boolean strict,
        boolean matchSimplePathsOnly) {

        return isIndexOnlyExpr(expr, expr, strict, matchSimplePathsOnly);
    }

    private boolean isIndexOnlyExpr(
        Expr initExpr,
        Expr expr,
        boolean strict,
        boolean matchSimplePathsOnly) {

        if (expr.isStepExpr() || expr.getKind() == ExprKind.VAR) {

            if (expr.getKind() == ExprKind.VAR) {
                ExprVar var = (ExprVar)expr;
                if (var.isExternal()) {
                    return true;
                }
            }

            IndexExpr epath = expr.getIndexExpr();

            if (epath == null ||
                epath.theDoesSlicing ||
                epath.theFilteringPreds != null ||
                (strict && epath.theTable.getId() != theTable.getId())) {
                return false;
            }

            if (!strict && epath.theTable.getId() != theTable.getId()) {
                return true;
            }

            if (matchSimplePathsOnly &&
                epath.getRelativeCtxVarPos(theTable, theIndex) > 0 &&
                epath.theCtxVar == theBestPredGroup.theCtxVar) {
                matchSimplePathsOnly = false;
            }

            if (!matchPathExprToIndexPath(theIndex,
                                          epath,
                                          matchSimplePathsOnly)) {
                return false;
            }
         
            ArrayList<ExprToReplace> exprsToReplace =
                theExprRewriteMap.get(initExpr);

            if (exprsToReplace == null) {
                exprsToReplace = new ArrayList<ExprToReplace>();
                theExprRewriteMap.put(initExpr, exprsToReplace);
            }

            exprsToReplace.add(new ExprToReplace(expr, epath.getPathPos()));

            return true;
        }

        if (expr.getKind() == ExprKind.BASE_TABLE) {
            return false;
        }

        if (expr.getKind() == ExprKind.CONST) {
            return true;
        }

        ExprIter children = expr.getChildren();

        while (children.hasNext()) {
            Expr child = children.next();
            if (!isIndexOnlyExpr(initExpr, child, strict,
                                 matchSimplePathsOnly)) {
                children.reset();
                return false;
            }
        }

        return true;
    }

    private static FieldValueImpl createPlaceHolderValue(FieldDefImpl type) {

        switch (type.getType()) {
        case INTEGER:
            return FieldDefImpl.integerDef.createInteger(0);
        case LONG:
            return FieldDefImpl.longDef.createLong(0);
        case FLOAT:
            return FieldDefImpl.floatDef.createFloat(0.0F);
        case DOUBLE:
            return FieldDefImpl.doubleDef.createDouble(0.0);
        case NUMBER:
            return FieldDefImpl.numberDef.createNumber(0);
        case STRING:
            return FieldDefImpl.stringDef.createString("");
        case ENUM:
            return ((EnumDefImpl)type).createEnum(1);
        default:
            throw new QueryStateException(
                "Unexpected type for index key: " + type);
        }
    }

    /**
     * Return the number of preds in the WHERE clause of the SFW expr
     */
    private int getNumPreds() {

        if (theSFW == null) {
            return 0;
        }

        Expr whereExpr = theSFW.getWhereExpr();

        if (whereExpr == null) {
            return 0;
        }

        Function andOp = whereExpr.getFunction(FuncCode.OP_AND);

        if (andOp != null) {
            return whereExpr.getNumChildren();
        }

        return 1;
    }
}

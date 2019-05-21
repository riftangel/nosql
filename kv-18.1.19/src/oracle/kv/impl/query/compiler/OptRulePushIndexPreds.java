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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.ExprBaseTable.IndexHint;
import oracle.kv.impl.query.compiler.ExprSFW.FromClause;
import oracle.kv.table.Index;

/**
 * The goal of this optimization rule is convert WHERE predicates into index
 * scan conditions in order to avoing a full table scan.
 *
 * The rule analyses the predicates in a WHERE clause to find, for each index
 * associated with a given table (including the table's primary index), (a) a
 * starting and/or ending key that could be applied to a scan over that index,
 * and (b) predicates that can be evaluated during the index scan from the
 * index columns only, thus filtering the retrieved index keys further.
 *
 * The rule assumes the WHERE-clause expr is in CNF. For each index, it first
 * collects all CNF factors that are "index predicates", i.e., they can be
 * evaluated fully from the index columns only. For example, if the current
 * index is the primary-key index and C1, C2, C3 are the primary-key columns,
 * "C1 > 10" and "C2 = 3 or C3 < 20" are primary index preds. Then, for each
 * index column, in the order that these columns appear in the index (or primary
 * key) declaration, the rule looks-for and processes index preds that are
 * comparison preds (eg, "C1 > 10" is a comparison pred, but "C2 = 3 or C3 < 20"
 * is not). The possible outomes of processing an index pred w.r.t. an index
 * column are listed in the PredicateStatus enum below. The rule stops
 * processing the current index as soon as it finds an index column for which
 * there is no equality pred to be pushed to the index.
 *
 * After the rule has analyzed all indexes, it chooses the "best" index to
 * use among the indexes that had something pushed down to them.
 *
 * TODO: need a "good" heuristic to choose the "best" index, as well as
 * a compiler hint or USE INDEX clause to let the user decide.
 */
class OptRulePushIndexPreds {

    // TODO move this to the Optimizer obj, when we have one
    private RuntimeException theException = null;

    private ArrayList<IndexAnalyzer> theAnalyzers;

    private boolean theCompletePrimaryKey;

    RuntimeException getException() {
        return theException;
    }

    void apply(Expr expr) {

        try {
            applyInternal(expr);
        } catch (RuntimeException e) {
            theException = e;
        }
    }

    void applyInternal(Expr expr) {

        if (expr.getKind() == ExprKind.SFW) {
            boolean applied = applyOnSFW((ExprSFW)expr);
            if (applied) {
                return;
            }
        }

        Iterator<Expr> children = expr.getChildren();

        while (children.hasNext()) {
            Expr child = children.next();
            applyInternal(child);
        }

        if (expr.getKind() == ExprKind.UPDATE_ROW) {

            if (!theCompletePrimaryKey) {
                throw new QueryException(
                    "Multi-row update is not supported. A complete and " +
                    "exact primary key must be specified in the WHERE clause.");
            }

            return;
        }
    }

    boolean applyOnSFW(ExprSFW sfw) {

        FromClause fc = sfw.getFirstFrom();
        ExprBaseTable tableExpr = fc.getTableExpr();

        if (tableExpr == null) {
            return false;
        }

        boolean hasSort = (sfw.hasSort() ||
                           (sfw.hasGroupBy() && sfw.getNumGroupByExprs() > 0));
        String sortingOp = (sfw.hasSort() ? "order-by" : "group-by");

        IndexHint forceIndexHint = tableExpr.getForceIndexHint();

        TableImpl table = tableExpr.getTargetTable();
        int tablePos = tableExpr.getTargetTablePos();
        Map<String, Index> indexes = table.getIndexes();

        theAnalyzers = new ArrayList<IndexAnalyzer>(1+indexes.size());
        IndexAnalyzer primaryAnalyzer = null;

        /*
         * Try to push predicates in the primary index. Do this even
         * if the query has order-by and the primary index is not the
         * sorting one, because we need to discover if the query has
         * a complete shard key.
         */
        primaryAnalyzer = new IndexAnalyzer(sfw, tableExpr, tablePos,
                                            null/*index*/);
        primaryAnalyzer.analyze();

        theCompletePrimaryKey =
            (primaryAnalyzer.getPrimaryKeys() != null &&
             primaryAnalyzer.getPrimaryKeys().get(0).isComplete());

        /* No reason to continue if the WHERE expr is always false */
        if (primaryAnalyzer.theSFW == null) {
            return true;
        }

        if (theCompletePrimaryKey) {
            sfw.removeSort();
        }

        if (!hasSort ||
            sfw.hasPrimaryIndexBasedSort()) {
            theAnalyzers.add(primaryAnalyzer);
        }

        if (forceIndexHint != null) {

            IndexImpl forcedIndex = forceIndexHint.theIndex;

            if (hasSort &&
                ((sfw.hasPrimaryIndexBasedSort() && forcedIndex != null) ||
                 (sfw.getSortingIndexes() != null &&
                  !sfw.getSortingIndexes().contains(forcedIndex)))) {

                String hintIndex = (forcedIndex == null ?
                                    "primary" :
                                    forcedIndex.getName());

                throw new QueryException(
                    "Cannot perform " + sortingOp + " because the index " +
                    "forced via a hint is not one of the sorting indexes.\n" +
                    "Hint index    : " + hintIndex + "\n",
                    sfw.getLocation());
            }

            IndexAnalyzer analyzer =
                (forcedIndex == null ?
                 primaryAnalyzer :
                 new IndexAnalyzer(sfw, tableExpr, tablePos, forcedIndex));

            if (analyzer != primaryAnalyzer) {
                analyzer.analyze();
            }

            if (analyzer.theSFW == null) {
                return true;
            }

            analyzer.apply();
            return true;
        }

        /*
         * If the query specifies a complete primary key, use the primary
         * index to execute it and remove any order-by.
         */
        if (theCompletePrimaryKey) {
            primaryAnalyzer.apply();
            return true;
        }

        /*
         * If the query specifies a complete shard key, use the primary
         * index to execute it. In this case, if the query has order-by
         * as well and the sorting index is not the primary one, an error
         * is raised.
         */
        if (primaryAnalyzer.hasShardKey()) {

            if (hasSort &&
                !sfw.hasPrimaryIndexBasedSort()) {
                throw new QueryException(
                    "Cannot perform " + sortingOp + " because the query " +
                    "specifies a complete shard key, but the sort cannot " +
                    "be done via the primary index",
                    sfw.getLocation());
            }

            primaryAnalyzer.apply();
            return true;
        }

        /*
         * If the SFW has sorting, scan the table using the index that
         * sorts the rows in the desired order.
         */
        if (sfw.hasPrimaryIndexBasedSort()) {
            primaryAnalyzer.apply();
            return true;
        }

        boolean alwaysFalse = false;

        if (sfw.hasSecondaryIndexBasedSort()) {

            for (IndexImpl index : sfw.getSortingIndexes()) {

                IndexAnalyzer analyzer =
                    new IndexAnalyzer(sfw, tableExpr, tablePos, index);

                theAnalyzers.add(analyzer);
                analyzer.analyze();

                if (analyzer.theSFW == null) {
                    alwaysFalse = true;
                    break;
                }
            }
        } else {
            for (Map.Entry<String, Index> entry : indexes.entrySet()) {

                IndexImpl index = (IndexImpl)entry.getValue();

                IndexAnalyzer analyzer =
                    new IndexAnalyzer(sfw, tableExpr, tablePos, index);

                theAnalyzers.add(analyzer);
                analyzer.analyze();

                if (analyzer.theSFW == null) {
                    alwaysFalse = true;
                    break;
                }
            }
        }

        /*
         * Choose the "best" of the applicable indexes.
         */
        if (!alwaysFalse) {
            chooseIndex();
        }

        return true;
    }

    /**
     * Choose and apply the "best" index among the applicable ones.
     */
    void chooseIndex() {
        IndexAnalyzer bestIndex = Collections.min(theAnalyzers);
        bestIndex.apply();
    }

}

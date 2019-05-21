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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.Direction;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprVar.VarKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.Index;

/**
 * Represents a SELECT-FROM-WHERE query block.
 *
 * Implementation of GroubBy:
 *
 * In general, a SFW that includes grouping is implemented with 2 SFW exprs.
 * The lower/inner SFW applies the WHERE clause and does the grouping. Its
 * SELECT lists consists of the grouping exprs, followed by the aggregate 
 * functions that appear in the original SELECT list (the SELECT list in the
 * user's query). The upper/outer SFW computes the original SELECT list.
 * For example the query:
 *
 * select a + b, sum(c+d) / count(*)
 * from foo
 * where e > 10
 * group by a, b
 *
 * is rewritten as:
 *
 * select gb-0 + gb-1, aggr-2 / aggr-3
 * from (select a as gb-0, b as gb-1, sum(c+d) as aggr-2, count(*) as aggr-3
 *       from foo
 *       where e > 10
 *       group by a, b)
 *
 * If the original SELECT list is identical to the SELECT list of the inner
 * SFW, then no outer SFW is added.
 *
 * The above rewrite is done during translation time. During distribution time
 * another SFW is added if the query is ALL_PARTITIONS or ALL_SHARDS. This SFW
 * regroups the partial groups that arrive from the RNs. So, the above example
 * will actually look like this:
 *
 * select gb-0 + gb-1, aggr-2 / aggr-3
 * from (select gb-0, gb-1, sum(aggr-2) as aggr-2, sum(aggr-3) as aggr-3
 *       from RCV(select a as gb-0,
 *                       b as gb-1,
 *                       sum(c+d) as aggr-2,
 *                       count(*) as aggr-3
 *                from foo
 *                where e > 10
 *                group by a, b))
 *
 * theFromClauses:
 * There is a FromClause for each table expression appearing in the FROM clause
 * of the actual query block. For now, the first of these table exprs is always
 * the name of a KVS table or a NESTED TABLES clause (see ExprBaseTable). Any
 * exprs following the first one cannot reference any table names (i.e., no 
 * joins among tables in different table hierarchies are supported yet)
 *
 * theWhereExpr:
 *
 * theFieldNames:
 *
 * theFieldExprs:
 *
 * theConstructsRecord
 * True if the SELECT clause constructs a record. Normally it is true, but
 * will be false if the SELECT clause contains a single expr with no associated
 * AS clause.
 *
 * theDoNullOnEmpty:
 * Normally, if a SELECT expr returns nothing, NULL is used as the result of
 * that expr. This creates a problem for order-by queries that are run on
 * multiple partitions/shards: Sort exprs are added to the SELECT list (if 
 * not there already) of the SFW that produces the query results at each RN.
 * What if a sort expr returns nothing? For sorting, EMPTY and NULL are
 * considered distinct values and EMPTY sorts before NULL. So, we should not
 * convert EMPTY to NULL in this case, because a merge-sort must be done at
 * the client side and this merge sort should see the EMPTY. To support this
 * case, theDoNullOnEmpty is normally true, but is set to false for the SFWs
 * under the RCV, if the query does sorting. The EMPTY values sent by such
 * SFWs are then converted back to NULL by the RCV iter.
 *
 * theNumGroupByExprs:
 * If theNumGroupByExprs is >= 0, the SFW is a grouping one. As mentioned above,
 * in this case the grouping exprs appear first in the SELECT list, followed by
 * the aggregate functions. theNumGroupByExprs is the number of the grouping
 * exprs. Because of this canonical form of the grouping SFW, there is no need
 * to implement a class to represent the group-by clause and store the grouping
 * exprs twice.
 *
 * theNeedOuterSFWForGroupBy:
 * A transient member used during translation to determine if an outer SFW is
 * needed. It acts as a global var for the rewriteSelectExprForGroupBy() method
 * and it is set to true by that method, if any of the exprs in the original
 * SELECT list does not match with the corresponding expr in the SELECT list of
 * the inner SFW.
 * 
 * theSortExprs:
 *
 * theSortSpecs:
 *
 * theUsePrimaryIndexForSort:
 *
 * theSortingIndexes:
 *
 * theOffsetExpr:
 *
 * theLimitExpr:
 */
class ExprSFW extends Expr {

    /**
     * FromClause does not represent the full FROM clause of an SFW expr.
     * Instead, it represents one of the top-level, comma-separated exprs
     * appearing in the actual FROM clause. A FromClause evaluates its
     * associated expr (called the "domain expr") and binds one or more
     * vars to the values produced by the domain expr.
     */
    class FromClause
    {
        /*
         * The "domain expr" of this FromClause. If there is one var associated
         * with the FromClause, the var iterates over the items generated by
         * this expr.
         */
        private Expr theDomainExpr;

        /*
         * The variables defined by this FromClause. Currently, the array
         * will contain more than one variables only if the domain expr is an
         * ExprBaseTable representing a NESTED TABLES clause. In this case, the
         * vars represent the table aliases used in the NESTED TABLES clause and
         * they are listed in the same order as their corresponding aliases in
         * the ExprBaseTable.
         */
        private final ArrayList<ExprVar> theVars = new ArrayList<ExprVar>();

        /*
         * If, for a table T, the query uses a secondary index I and I is
         * covering or any filtering predictes have been pushed to I, then
         * an "index variable" is created to range over the entries of index
         * I. The filtering preds, if any, are rewritten to access this index
         * var instead of the corresponding table variable. If I is covering,
         * every (sub)expression E which accesses T columns is also rewritten.
         *
         * Currently, only the target table of the query may use a secondary
         * index, so there can be at most one index var. Nevertheless,
         * theIndexVars array mirrors theVars array for convenience and future
         * extensions.
         */
        private final ArrayList<ExprVar> theIndexVars =
            new ArrayList<ExprVar>();

        FromClause(Expr domExpr, String varName, TableImpl table) {

            theDomainExpr = domExpr;
            theDomainExpr.addParent(ExprSFW.this);

            theVars.add(new ExprVar(theQCB, theSctx, domExpr.getLocation(),
                                    varName, table, this));
            theIndexVars.add(null);
        }

        /*
         * Creates and adds a var after theDomainExpr has already been set.
         * This is the case when theDomainExpr is a NESTED TABLES clause.
         */
        ExprVar addVar(String varName, TableImpl table) {

            assert(table != null);

            ExprVar var = new ExprVar(theQCB, theSctx,
                                      theDomainExpr.getLocation(),
                                      varName, table, this);
            theVars.add(var);
            theIndexVars.add(null);
            return var;
        }

        Expr getDomainExpr() {
            return theDomainExpr;
        }

        int getNumVars() {
            return theVars.size();
        }

        ArrayList<ExprVar> getVars() {
            return theVars;
        }

        ExprVar getVar(int i) {
            return theVars.get(i);
        }

        void setIndexVar(int i, ExprVar var) {
            theIndexVars.set(i, var);
        }

        ExprVar getIndexVar(int i) {
            return theIndexVars.get(i);
        }

        ExprVar getVar() {

            if (theVars.size() > 1) {
                throw new QueryStateException(
                    "Method called on FromClause with more than one " +
                    "variables. domain expression:\n" + theDomainExpr.display());
            }

            return theVars.get(0);
        }

        ArrayList<TableImpl> getTables() {

            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                return ((ExprBaseTable)theDomainExpr).getTables();
            }

            return null;
        }

        ExprBaseTable getTableExpr() {
            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                return (ExprBaseTable)theDomainExpr;
            }

            return null;
        }

        TableImpl getTargetTable() {

            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                return ((ExprBaseTable)theDomainExpr).getTargetTable();
            }

            return null;
        }

        ExprVar getTargetTableVar() {

            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                ExprBaseTable te = (ExprBaseTable)theDomainExpr;
                return theVars.get(te.getNumAncestors());
            }

            if (theDomainExpr.getKind() == ExprKind.UPDATE_ROW) {
                return theVars.get(0);
            }

            return null;
        }

        ExprVar getTargetTableIndexVar() {

            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                ExprBaseTable te = (ExprBaseTable)theDomainExpr;
                return theIndexVars.get(te.getNumAncestors());
            }

            return null;
        }
    }

    private int theNumChildren;

    private ArrayList<FromClause> theFromClauses;

    private Expr theWhereExpr;

    private ArrayList<String> theFieldNames;

    private ArrayList<Expr> theFieldExprs;

    private boolean theConstructsRecord = true;

    private boolean theDoNullOnEmpty = true;

    private int theNumGroupByExprs = -1;

    private boolean theNeedOuterSFWForGroupBy;

    private ArrayList<Expr> theSortExprs;

    private ArrayList<SortSpec> theSortSpecs;

    private boolean theUsePrimaryIndexForSort = false;

    private ArrayList<IndexImpl> theSortingIndexes = null;

    private Expr theOffsetExpr;

    private Expr theLimitExpr;

    ExprSFW(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location) {

        super(qcb, sctx, ExprKind.SFW, location);
        theFromClauses = new ArrayList<FromClause>(8);
    }

    ExprVar createFromVar(Expr domainExpr, String varName) {
        return createFrom(domainExpr, varName, null);
    }

    ExprVar createTableVar(
        Expr domExpr,
        TableImpl table,
        String varName) {

        assert(table != null &&
               (domExpr.getKind() == ExprKind.BASE_TABLE ||
                domExpr.getKind() == ExprKind.UPDATE_ROW));

        for (FromClause fc : theFromClauses) {
            if (fc.getDomainExpr() == domExpr) {
                return fc.addVar(varName, table);
            }
        }

        return createFrom(domExpr, varName, table);
    }

    private ExprVar createFrom(
        Expr domainExpr,
        String varName,
        TableImpl table) {

        FromClause fc = this.new FromClause(domainExpr, varName, table);
        theFromClauses.add(fc);
        ++theNumChildren;
        return fc.getVar(0);
    }

    void removeFromClause(int i, boolean destroy) {

        FromClause fc = theFromClauses.get(i);

        for (ExprVar var : fc.getVars()) {
            assert(!var.hasParents());
        }

        theFromClauses.remove(i);
        --theNumChildren;
        fc.getDomainExpr().removeParent(this, destroy);
    }

    FromClause getFromClause(int i) {
        return theFromClauses.get(i);
    }

    FromClause getFirstFrom() {
        return getFromClause(0);
    }

    int getNumFroms() {
        return theFromClauses.size();
    }

    Expr getDomainExpr(int i) {
        return theFromClauses.get(i).getDomainExpr();
    }

    void setDomainExpr(int i, Expr newExpr, boolean destroy) {
        FromClause fc = theFromClauses.get(i);
        fc.theDomainExpr.removeParent(this, destroy);
        fc.theDomainExpr = newExpr;
        newExpr.addParent(this);
    }

    /*
     * Check whether the given expr is the domain expr for a FromClause defined
     * by this SFW expr. If so, return the variables of that FromClause.
     */
    ArrayList<ExprVar> findVarsForExpr(Expr expr) {

        for (int i = 0; i < theFromClauses.size(); ++i) {
            FromClause fc = theFromClauses.get(i);
            if (fc.getDomainExpr() == expr) {
                return fc.getVars();
            }
        }

        return null;
    }

    void removeUnusedVars() {

        for (int i = theFromClauses.size() - 1; i >= 0; --i) {
            FromClause fc = theFromClauses.get(i);
            if (fc.getDomainExpr().isScalar() &&
                fc.getVar(0).getNumParents() == 0) {
                assert(fc.getNumVars() == 1);
                removeFromClause(i, true);
            }
        }
    }

    ExprVar addIndexVar(TableImpl table, IndexImpl index) {

        for (FromClause fc : theFromClauses) {

            ArrayList<TableImpl> tables = fc.getTables();

            if (tables == null) {
                continue;
            }

            int tablePos = -1;
            for (int i = 0; i < tables.size(); ++i) {
                if (tables.get(i).getId() == table.getId()) {
                    tablePos = i;
                    break;
                }
            }

            if (tablePos < 0) {
                continue;
            }

            ExprVar var = fc.getVar(tablePos);
            String idxVarName = var.createIndexVarName();

            ExprVar idxVar = new ExprVar(theQCB, theSctx, var.theLocation,
                                         idxVarName, table, fc);

            RecordDefImpl indexEntryDef;
            if (index != null) {
                indexEntryDef = index.getIndexEntryDef();
            } else {
                /*
                 * We use getRowDef() instead of getPrimKeyDef(), because during
                 * runtime, we always construct a full table row, even if we may
                 * fill just the prim-key columns in that row.
                 */
                indexEntryDef = table.getRowDef();
            }

            ExprType idxVarType = TypeManager.createType(indexEntryDef,
                                                           Quantifier.ONE);
            idxVar.setIndex(index, idxVarType);

            fc.setIndexVar(tablePos, idxVar);

            return idxVar;
        }

        return null;
    }

    void addWhereClause(Expr condExpr) {

        assert(theWhereExpr == null);

        theWhereExpr = ExprPromote.create(
            null, condExpr, TypeManager.BOOLEAN_QSTN());

        theWhereExpr.addParent(ExprSFW.this);
        ++theNumChildren;
    }

    Expr getWhereExpr() {
        return theWhereExpr;
    }

    void setWhereExpr(Expr newExpr, boolean destroy) {
        theWhereExpr.removeParent(this, destroy);
        theWhereExpr = null;
        addWhereClause(newExpr);
    }

    void removeWhereExpr(boolean destroy) {
        theWhereExpr.removeParent(this, destroy);
        theWhereExpr = null;
        --theNumChildren;
    }

    void addSelectClause(
        ArrayList<String> fieldNames,
        ArrayList<Expr> fieldExprs) {

        assert(fieldNames.size() == fieldExprs.size());
        theFieldNames = fieldNames;
        theFieldExprs = fieldExprs;

        for (int i = 0; i < fieldExprs.size(); ++i) {
            Expr expr = fieldExprs.get(i);

            if (expr.isMultiValued()) {
                ArrayList<Expr> args = new ArrayList<Expr>(1);
                args.add(expr);
                expr = new ExprArrayConstr(theQCB, theSctx, expr.getLocation(),
                                           args, true/*conditional*/);
            }

            expr.addParent(this);
            theFieldExprs.set(i, expr);
        }

        theNumChildren += fieldExprs.size();
    }

    boolean getConstructsRecord() {
        return theConstructsRecord;
    }

    void setConstructsRecord(boolean v) {
        theConstructsRecord = v;
    }

    Expr getFieldExpr(int i) {
        return theFieldExprs.get(i);
    }

    void setFieldExpr(int i, Expr newExpr, boolean destroy) {

        theFieldExprs.get(i).removeParent(this, destroy);

        if (newExpr.isMultiValued()) {
            ArrayList<Expr> args = new ArrayList<Expr>(1);
            args.add(newExpr);
            newExpr = new ExprArrayConstr(theQCB, theSctx, newExpr.theLocation,
                                          args, true/*conditional*/);
        }

        theFieldExprs.set(i, newExpr);
        newExpr.addParent(this);
    }

    void removeField(int i, boolean destroy) {
        theFieldExprs.get(i).removeParent(this, destroy);
        theFieldExprs.remove(i);
        theFieldNames.remove(i);
        theType = computeType();
        --theNumChildren;
    }

    void addField(String name, Expr expr) {

        if (theFieldExprs == null) {
            theFieldExprs = new ArrayList<Expr>();
            theFieldNames = new ArrayList<String>();
        }

        expr = ExprPromote.create(null, expr, TypeManager.ANY_QSTN());
        theFieldExprs.add(expr);
        theFieldNames.add(name);
        expr.addParent(this);

        if (theFieldExprs.size() > 1) {
            theConstructsRecord = true;
        }

        theType = computeType();
        ++theNumChildren;
    }

    String getFieldName(int i) {
        return theFieldNames.get(i);
    }

    int getNumFields() {
        return (theFieldExprs == null ? 0 : theFieldExprs.size());
    }

    ArrayList<String> getFieldNames() {
        return theFieldNames;
    }

    void setFieldNames(ArrayList<String> fieldNames) {
        theFieldNames = fieldNames;
        theType = computeType();
    }

    String[] getFieldNamesArray() {
        String[] arr = new String[theFieldNames.size()];
        return theFieldNames.toArray(arr);
    }

    boolean doNullOnEmpty() {
        return theDoNullOnEmpty;
    }

    void setDoNullOnEmpty(boolean v) {
        theDoNullOnEmpty = v;
    }

    int getNumGroupByExprs() {
        return theNumGroupByExprs;
    }

    void setNumGroupByExprs(int v) {
        if (theNumGroupByExprs < v) {
            theNumGroupByExprs = v;
        }
    }

    boolean hasGroupBy() {
        return theNumGroupByExprs >= 0;
    }

    boolean needOuterSFWForGroupBy() {
        return theNeedOuterSFWForGroupBy;
    }

    boolean isGroupingField(int i) {
        return (theNumGroupByExprs > 0 && i < theNumGroupByExprs);
    }

    void addGroupByClause(ArrayList<Expr> gbExprs) {

        theFieldNames = new ArrayList<String>(gbExprs.size());

        for (int i = 0; i < gbExprs.size(); ++i) {
            Expr expr = gbExprs.get(i);
            expr = ExprPromote.create(null, expr, TypeManager.ANY_ATOMIC_QSTN());
            expr.addParent(this);
            gbExprs.set(i, expr);
            theFieldNames.add("gb-" + i);
        }

        /*
         * The grouping exprs become the SELECT list of this inner SFW.
         * The grouping functions will be added later, when we poccess the
         * SELECT list of the overall SFW.
         */
        theFieldExprs = gbExprs;
        theNumGroupByExprs = gbExprs.size();

        theNumChildren += theNumGroupByExprs;
    }

    /*
     * Called by the Translator on the inner SFW to create, in the outer
     * SELECT list, the expr that corresponds to a given expr from the
     * original SELECT list. The expr to create will reference the grouping
     * exprs and/or the aggragate function that appear in the inner SELECT
     * list.
     *
     * - fieldPos : the position in the SELECT list of the expr to map in the
     *   the outer SELECT list
     * - fieldExpr : theExpr to map
     * - fieldName : the associate field name
     * - fieldSubExpr : On the initial invocation, it is the same as the
     *   fieldExpr. Method traverses the fieldExpr subtree looking for sub
     *   exprs that are either aggregate functions or match the grouping exprs.
     * - outerSFW : the outer SFW
     * - outerFromVar : from FROM var of the outer SFW.
     */
    Expr rewriteSelectExprForGroupBy(
        int fieldPos,
        Expr fieldExpr,
        String fieldName,
        Expr fieldSubExpr,
        ExprSFW outerSFW,
        ExprVar outerFromVar) {

        if (fieldSubExpr.isStepExpr()) {

            int i;
            for (i = 0; i < theNumGroupByExprs; ++i) {

                if (ExprUtils.matchExprs(fieldSubExpr, getFieldExpr(i))) {

                    String gbName = "gb-" + i;
                    Expr fieldRef = new ExprFieldStep(theQCB, theSctx,
                                                      theLocation,
                                                      outerFromVar,
                                                      gbName);

                    if (i != fieldPos || fieldSubExpr != fieldExpr) {
                        outerSFW.theNeedOuterSFWForGroupBy = true;
                    }

                    if (fieldSubExpr == fieldExpr) {
                        return fieldRef;
                    }

                    fieldSubExpr.replace(fieldRef, false/*destroy*/);
                    return fieldExpr;
                }
            }

        } else if (fieldSubExpr.getFunction(null) != null &&
                   fieldSubExpr.getFunction(null).isAggregate()) {

            int numFields = getNumFields();
            int i;
            for (i = theNumGroupByExprs; i < numFields; ++i) {

                if (ExprUtils.matchExprs(fieldSubExpr, getFieldExpr(i))) {
                    break;
                }
            }

            QueryException.Location loc = fieldSubExpr.getLocation();
            String aggrName = "aggr-" + i;
            Expr fieldRef;

            if (fieldSubExpr.getFunction(FuncCode.FN_AVG) != null) {

                /* convert avg to sum/count */
                assert(i == numFields);
                outerSFW.theNeedOuterSFWForGroupBy = true;

                Expr inExpr = fieldSubExpr.getInput();

                Expr sumExpr = ExprFuncCall.create(theQCB, theSctx, loc,
                                                   FuncCode.FN_SUM, inExpr);

                Expr cntExpr = ExprFuncCall.create(theQCB, theSctx, loc,
                                                   FuncCode.FN_COUNT_NUMBERS,
                                                   inExpr);

                addField(aggrName, sumExpr);
                String aggrName2 = "aggr-" + (i + 1);
                addField(aggrName2, cntExpr);
                numFields = getNumFields();
                outerFromVar.computeType(false);

                Expr sumRef = new ExprFieldStep(theQCB, theSctx, loc,
                                                outerFromVar, aggrName);

                Expr cntRef = new ExprFieldStep(theQCB, theSctx, loc,
                                                outerFromVar, aggrName2);

                cntRef = ExprCast.create(theQCB, theSctx, loc, cntRef,
                                          FieldDefImpl.doubleDef,
                                          Quantifier.ONE);


                ArrayList<Expr> args = new ArrayList<Expr>(3);
                args.add(sumRef);
                args.add(cntRef);
                FieldValueImpl ops = FieldDefImpl.stringDef.createString("*/");
                args.add(new ExprConst(theQCB, theSctx, loc, ops));

                fieldRef = ExprFuncCall.create(theQCB, theSctx, loc,
                                               FuncCode.OP_MULT_DIV, args);
            } else {

                if (i == numFields) {
                    addField(aggrName, fieldSubExpr);
                    outerFromVar.computeType(false);
                }

                fieldRef = new ExprFieldStep(theQCB, theSctx, loc,
                                             outerFromVar, aggrName);
            }


            if (i != fieldPos || fieldSubExpr != fieldExpr) {
                outerSFW.theNeedOuterSFWForGroupBy = true;
            }

            if (fieldSubExpr == fieldExpr) {
                return fieldRef;
            }

            fieldSubExpr.replace(fieldRef, false/*destroy*/);

            /*
             * we must reset the i-th field expr because the replace() call
             * above replaces with fieldRef.
             */
            setFieldExpr(i, fieldSubExpr, false);
            return fieldExpr;

        } else if (fieldSubExpr.getKind() == ExprKind.VAR) {

            ExprVar var = (ExprVar)fieldSubExpr;

            if (var.getVarKind() != VarKind.EXTERNAL) {
                throw new QueryException(
                    "Invalid expression in the SELECT clause. When a " +
                    "select-from-where expression includes grouping, " +
                    "its SELECT expressions must reference grouping " +
                    "columns and aggregate functions",
                    fieldSubExpr.getLocation());
            }

            return fieldExpr;
        }

        outerSFW.theNeedOuterSFWForGroupBy = true;

        Iterator<Expr> children = fieldSubExpr.getChildren();
        while (children.hasNext()) {
            Expr child = children.next();
            rewriteSelectExprForGroupBy(fieldPos,
                                        fieldExpr,
                                        fieldName,
                                        child,
                                        outerSFW,
                                        outerFromVar);
        }

        return fieldExpr;
    }

    void addSortClause(
        ArrayList<Expr> sortExprs,
        ArrayList<SortSpec> sortSpecs) {

        if (theNumGroupByExprs > 0) {
            throw new QueryException(
                "select-from-where expression cannot have both order by " +
                "and group by clauses");
        }

        for (int i = 0; i < sortExprs.size(); ++i) {
            // TODO: allow arrays as well
            Expr expr = sortExprs.get(i);
            expr = ExprPromote.create(null, expr, TypeManager.ANY_ATOMIC_QSTN());
            expr.addParent(this);
            sortExprs.set(i, expr);
        }

        theSortExprs = sortExprs;
        theSortSpecs = sortSpecs;

        theNumChildren += theSortExprs.size();
    }

    void removeSort() {

        if (!hasSort()) {
            return;
        }

        while (!theSortExprs.isEmpty()) {
            removeSortExpr(0, true);
        }

        theSortExprs = null;
        theSortSpecs = null;
        theSortingIndexes = null;
        theUsePrimaryIndexForSort = false;
    }

    boolean hasSort() {
        return (theSortExprs != null && !theSortExprs.isEmpty());
    }

    boolean hasPrimaryIndexBasedSort() {
        return theUsePrimaryIndexForSort;
    }

    boolean hasSecondaryIndexBasedSort() {
        return theSortingIndexes != null && !theSortingIndexes.isEmpty();
    }

    ArrayList<IndexImpl> getSortingIndexes() {
        return theSortingIndexes;
    }

    int getNumSortExprs() {
        return (theSortExprs == null ? 0 : theSortExprs.size());
    }

    Expr getSortExpr(int i) {
        return theSortExprs.get(i);
    }

    void setSortExpr(int i, Expr newExpr, boolean destroy) {
        theSortExprs.get(i).removeParent(this, destroy);
        theSortExprs.set(i, newExpr);
        newExpr.addParent(this);
    }

    void removeSortExpr(int i, boolean destroy) {
        Expr sortExpr = theSortExprs.remove(i);
        sortExpr.removeParent(this, destroy);
        theSortSpecs.remove(i);
        --theNumChildren;
    }

    SortSpec[] getSortSpecs() {
        SortSpec[] arr = new SortSpec[theSortSpecs.size()];
        return theSortSpecs.toArray(arr);
    }

    /*
     * Method to find the index to use for the sort or group-by and 
     * determine the direction, or throw error if no applicable index.
     */
    void analyseOrderOrGroupBy(boolean orderby) {

        String opKind = (orderby ? "order-by " : "group-by ");

        FromClause fc = getFirstFrom();
        TableImpl table = fc.getTargetTable();

        if (table == null) {
            throw new QueryException(
                opKind + "cannot be performed because the " + opKind +
                "expressions are not consecutive columns of any index",
                getLocation());
        }

        ExprBaseTable tableExpr = fc.getTableExpr();

        IndexField ipath = null;
        int i = 0;
        boolean desc = false;
        boolean nullsLast = false;
        Direction direction;

        /*
         * Determine the sort direction and store it in the BaseTableExpr
         */
        if (orderby) {
            SortSpec spec = theSortSpecs.get(0);
            desc = spec.theIsDesc;
            nullsLast = !spec.theNullsFirst;
            direction = (desc ? Direction.REVERSE : Direction.FORWARD);

            for (i = 1; i < theSortSpecs.size(); ++i) {
                spec = theSortSpecs.get(i);
                if (desc != spec.theIsDesc || nullsLast != (!spec.theNullsFirst)) {
                    throw new QueryException(
                        "In the current implementation, all order-by specs " +
                        "must have the same ordering direction and the same " +
                        "relative order for NULLs",
                        getSortExpr(i).getLocation());
                }
            }

            if ((desc && nullsLast) || (!desc && !nullsLast)) {
                throw new QueryException(
                    "NULLs ordering is not compatible with the way " +
                    "NULLs are ordered in indexes.");
            }

            tableExpr.setDirection(direction);
        }

        /*
         * Check whether the sort/group exprs are a prefix of the primary
         * key columns.
         */
        int numPkCols = table.getPrimaryKeySize();

        int numExprs = (orderby ? theSortExprs.size() : theNumGroupByExprs);

        for (i = 0; i < numPkCols && i < numExprs; ++i) {

            Expr expr = (orderby ? getSortExpr(i) : getFieldExpr(i));

            if (!ExprUtils.isPrimKeyColumnRef(table, i, expr)) {
                break;
            }
        }

        if (i == numExprs) {

            theUsePrimaryIndexForSort = true;

            if (orderby) {
                int numShardKeys = table.getShardKeySize();

                if (i > numShardKeys) {
                    while (numExprs > numShardKeys) {
                        removeSortExpr(numExprs - 1, true/*destroy*/);
                        --numExprs;
                    }
                }

                if (desc && tableExpr.getNumDescendants() > 0) {
                    throw new QueryException(
                        "In the current implementation, it is not possible " +
                        "to order by primary key columns in descending order " +
                        "when the NESTED TABLES clause contains descendants");
                }
            }

            return;
        }

        /*
         * Check whether the sort exprs are a prefix of the columns of some
         * secondary index.
         */
        theSortingIndexes = new ArrayList<IndexImpl>();
        Map<String, Index> indexes = table.getIndexes();

        for (Map.Entry<String, Index> entry : indexes.entrySet()) {

            IndexImpl index = (IndexImpl)entry.getValue();
            List<IndexField> indexPaths = index.getIndexFields();

            /*
             * Don't use multikey index for group-by because duplicate
             * elimination may be required. It's hard to implement duplicate
             * elimination together with group by, and it would result in no
             * actual grouping done at the RNs (because the prim key columns
             * would have to be added in the group-by).
             */
            if (hasGroupBy() && index.isMultiKey()) {
                continue;
            }

            for (i = 0;
                 i < indexPaths.size() && i < numExprs; ++i) {

                ipath = indexPaths.get(i);
                Expr expr = (orderby ? getSortExpr(i) : getFieldExpr(i));

                if (ipath.isMultiKey()) {
                    break;
                }

                IndexExpr iexpr = expr.getIndexExpr();

                if (iexpr == null ||
                    !iexpr.matchesIndex(index, ipath.getPosition())) {
                    break;
                }
            }

            if (i == numExprs) {
                theSortingIndexes.add(index);
                continue;
            }

            /*
             * Check if the remaining sort exprs are primary-key columns
             * (which exist in the index as well).
             */
            if (i == indexPaths.size()) {

                for (int j = 0;
                     j < numPkCols && i < numExprs;
                     ++i, ++j) {

                    Expr expr = (orderby ? getSortExpr(i) : getFieldExpr(i));

                    if (!ExprUtils.isPrimKeyColumnRef(table, j, expr)) {
                        break;
                    }
                }

                if (i == numExprs) {
                    theSortingIndexes.add(index);
                    continue;
                }
            }
        }

        if (theSortingIndexes.isEmpty()) {
            throw new QueryException(
                opKind + "cannot be performed because there is no index " +
                "that orders the table rows in the desired order (in the " +
                "current implementation sorting and grouping are possible " +
                "only if the ORDER/GROUP BY clause contains N expressions " +
                "that match the " +
                "first N fields on an index, and these fields are not " +
                "multi-key", getLocation());
        }
    }

    /*
     * Method to add the sort expr in the SELECT clause, if not there already.
     * The method is called from the Distributer, when a receive expr is pulled
     * above a SFW expr that has sort. The method returns the positions of the
     * sort exprs in the SELECT list. The positions are stored in the receive
     * expr.
     */
    int[] addSortExprsToSelect() {

        int numFieldExprs = theFieldExprs.size();
        int numSortExprs = theSortExprs.size();

        int[] sortPositions = new int[numSortExprs];

        for (int i = 0; i < numSortExprs; ++i) {

            Expr sortExpr = theSortExprs.get(i);

            int j;
            for (j = 0; j < numFieldExprs; ++j) {

                Expr fieldExpr = theFieldExprs.get(j);

                if (ExprUtils.matchExprs(sortExpr, fieldExpr)) {
                    break;
                }
            }

            if (j == numFieldExprs) {
                theFieldExprs.add(sortExpr);
                theFieldNames.add(theQCB.generateFieldName("sort"));
                sortExpr.addParent(this);
                theConstructsRecord = true;
                sortPositions[i] = theFieldExprs.size() - 1;
                ++theNumChildren;
            } else {
                sortPositions[i] = j;
                sortExpr.removeParent(this, true/*destroy*/);
            }
        }

        theNumChildren -= theSortExprs.size();
        theSortExprs = null;
        theType = computeType();

        return sortPositions;
    }

    void addOffsetLimit(Expr offset, Expr limit) {

        if (offset != null) {
            addOffset(offset);
        }

        if (limit != null) {
            addLimit(limit);
        }
    }

    Expr getOffset() {
        return theOffsetExpr;
    }

    void addOffset(Expr expr) {

        assert(theOffsetExpr == null);

        if (!expr.isConstant()) {
            throw new QueryException("Offset expression is not constant");
        }

        if (expr.getKind() == ExprKind.CONST) {
            FieldValueImpl val = ((ExprConst)expr).getValue();
            if (val.getLong() == 0) {
                return;
            }
        }

        theOffsetExpr = ExprPromote.create(null, expr, TypeManager.LONG_ONE());

        theOffsetExpr.addParent(this);
        ++theNumChildren;
    }

    void removeOffset(boolean destroy) {
        theOffsetExpr.removeParent(this, destroy);
        theOffsetExpr = null;
        --theNumChildren;
    }

    void setOffset(Expr newExpr, boolean destroy) {
        theOffsetExpr.removeParent(this, destroy);
        theOffsetExpr = null;
        --theNumChildren;
        addOffset(newExpr);
    }

    Expr getLimit() {
        return theLimitExpr;
    }

    void addLimit(Expr expr) {

        assert(theLimitExpr == null);

        if (!expr.isConstant()) {
            throw new QueryException("Limit expression is not constant");
        }

        theLimitExpr = ExprPromote.create(null, expr, TypeManager.LONG_ONE());
        theLimitExpr.addParent(this);
        ++theNumChildren;
    }

    void removeLimit(boolean destroy) {
        theLimitExpr.removeParent(this, destroy);
        theLimitExpr = null;
        --theNumChildren;
    }

    void setLimit(Expr newExpr, boolean destroy) {
        theLimitExpr.removeParent(this, destroy);
        theLimitExpr = null;
        --theNumChildren;
        addLimit(newExpr);
    }

    @Override
    int getNumChildren() {
        return theNumChildren;
    }

    int computeNumChildren() {
        return
            theFromClauses.size() +
            (theWhereExpr != null ? 1 : 0) +
            theFieldExprs.size() +
            (theSortExprs != null ? theSortExprs.size() : 0) +
            (theOffsetExpr != null ? 1 : 0) +
            (theLimitExpr != null ? 1 : 0);
    }

    /**
     * Add all prim-key columns to the SELECT list. A prim-key column is added
     * if not already there. This is needed when the use of a multi-key index
     * requires duplicate elimination to be performed, based on the prim-key of
     * the result rows.
     */
    int[] addPrimKeyToSelect() {

        FromClause fc = theFromClauses.get(0);

        TableImpl table = fc.getTargetTable();

        int numPrimKeyCols = table.getPrimaryKeySize();
        int numFieldExprs = theFieldExprs.size();
        int[] pkPositionsInSelect = new int[numPrimKeyCols];

        for (int i = 0; i < numPrimKeyCols; ++i) {

            Expr fieldExpr = null;
            int j;
            for (j = 0; j < numFieldExprs; ++j) {

                fieldExpr = theFieldExprs.get(j);

                if (ExprUtils.isPrimKeyColumnRef(table, i, fieldExpr)) {
                    break;
                }
            }

            if (j == numFieldExprs) {
                String pkColName = table.getPrimaryKeyColumnName(i);
                int pkColPos;
                ExprVar rowVar;
                ExprVar idxVar = fc.getTargetTableIndexVar();

                if (idxVar != null) {
                    rowVar = idxVar;
                    pkColPos = idxVar.getIndex().numFields() + i;
                } else {
                    rowVar = fc.getTargetTableVar();
                    pkColPos = table.getPrimKeyPos(i);
                }

                Expr primKeyExpr = new ExprFieldStep(getQCB(),
                                                     getSctx(),
                                                     getLocation(),
                                                     rowVar,
                                                     pkColPos);

                theFieldExprs.add(primKeyExpr);
                primKeyExpr.addParent(this);
                theFieldNames.add(theQCB.generateFieldName(pkColName));
                pkPositionsInSelect[i] = theFieldExprs.size() - 1;
                theConstructsRecord = true;
                ++theNumChildren;
            } else {
                pkPositionsInSelect[i] = j;
            }
        }

        theType = computeType();

        return pkPositionsInSelect;
    }

    @Override
    ExprType computeType() {

        Quantifier q = getDomainExpr(0).getType().getQuantifier();

        for (int i = 1; i < theFromClauses.size(); ++i) {

            Quantifier q1 = getDomainExpr(i).getType().getQuantifier();

            q = TypeManager.getUnionQuant(q, q1);

            if (q == Quantifier.STAR) {
                break;
            }
        }

        if (theWhereExpr != null) {
            q = TypeManager.getUnionQuant(q, Quantifier.QSTN);
        }

        if (!getConstructsRecord()) {
            ExprType type = TypeManager.createType(getFieldExpr(0).getType(), q);
            if (type.isAnyJson()) {
                theQCB.theHaveJsonConstructors = true;
            }

            return type;
        }

        FieldMap fieldMap = new FieldMap();

        for (int i = 0; i < theFieldNames.size(); ++i) {

            FieldDefImpl fieldDef = theFieldExprs.get(i).getType().getDef();

            if (fieldDef.isJson()) {
                theQCB.theHaveJsonConstructors = true;
            }
            /*
             * TODO: what about nullability? If a field is not nullable, it
             * must have a default value, so we need a method to create a 
             * default default value.
             */
            fieldMap.put(theFieldNames.get(i),
                         fieldDef,
                         true/*nullable*/,
                         null/*defaultValue*/);
        }

        RecordDefImpl recDef = FieldDefFactory.createRecordDef(fieldMap,
                                                               null/*descr*/);
        return TypeManager.createType(recDef, q);
    }

    @Override
    public boolean mayReturnNULL() {

        if (getConstructsRecord()) {
            return false;
        }

        return theFieldExprs.get(0).mayReturnNULL();
    }

    @Override
    void displayContent(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        for (int i = 0; i < theFromClauses.size(); ++i) {
            FromClause fc = theFromClauses.get(i);
            sb.append("FROM-" + i + " :\n");
            fc.getDomainExpr().display(sb, formatter);
            sb.append(" as ");
            List<ExprVar> vars = fc.getVars();
            for (ExprVar var : vars) {
                sb.append(var.getName() + "  ");
            }
            sb.append("\n\n");
        }

        if (theWhereExpr != null) {
            formatter.indent(sb);
            sb.append("WHERE:\n");
            theWhereExpr.display(sb, formatter);
            sb.append("\n\n");
        }

        formatter.indent(sb);
        sb.append("GROUP BY:").append(theNumGroupByExprs);
        sb.append("\n\n");

        formatter.indent(sb);
        sb.append("SELECT:\n");

        for (int i = 0; i < theFieldExprs.size(); ++i) {
            formatter.indent(sb);
            sb.append(theFieldNames.get(i)).append(": \n");
            theFieldExprs.get(i).display(sb, formatter);
            if (i < theFieldExprs.size() - 1) {
                sb.append(",\n");
            }
        }
    }
}

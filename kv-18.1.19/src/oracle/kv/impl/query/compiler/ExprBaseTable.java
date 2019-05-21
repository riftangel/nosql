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

import oracle.kv.Direction;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexKeyImpl;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.FieldRange;


/**
 * ExprBaseTable is an internal expression representing a single table or a
 * NESTED TABLES clause appearing in a FROM clause of a query. A NESTED TABLES
 * clause consists of a target table and a number of ancestor and/or descendant
 * tables of the target table.
 *
 * Evaluation of this expr returns a set of rows. In the single-table case, the 
 * rows are the rows of the referenced table and they are returned as tuples,
 * i.e., if the rows of the table consist of N columns, the expression returns
 * N values per row (stored in a TupleValue). In the NESTED TABLES case, the
 * returned rows are composite rows constructed by the left-outer-joins among
 * the participating tables. Each composite row has N columns, where N is the
 * number of tables in the NESTED TABLES. The value of each column is a row
 * from the associated table or NULL if there is no row from the associated
 * table that matches with the other table rows in the composite row. The
 * N values per composite row are stored in a TupleValue,
 *
 * theTargetTable:
 * The target table
 *
 * theTables:
 * Stores the TableImpl for each referenced table. In the NESTED TABLES case,
 * the tables are ordered in the order that they would be encountered during
 * a depth-first traversal of the table hierarchy tree (so, the ancestor 
 * tables are first followed by the target table, and then the descendandant
 * tables). The position of each table is this array serves as the id of the
 * table in the query context.
 *
 * theAliases:
 * Mirrors theTables and stores the alias used for each table.
 *
 * theNumAncestors:
 * The number of ancestor tables.
 *
 * theNumDescendants:
 * The number of descendant tables.
 *
 * theTablePreds:
 * Mirrors theTables and stores the condition expr, if any, that must be applied
 * on each table row during the evaluation of this ExprBaseTable. For the target
 * table, the condition is an index-filtering pred (i.e., can be evaluated by
 * index columns only) that has been pushed down from the WHERE clause. For each
 * non-target table, the condition is the ON predicate that appears in the
 * NESTED TABLES clause next to that table; it may be an index-filtering pred
 * or not.
 *
 * theUsesCoveringIndex:
 * For each table, it says whether the index used to access the table is
 * covering or not.
 */
class ExprBaseTable extends Expr {

    static class IndexHint {

        IndexImpl theIndex; // null means the primary index
        boolean theForce;

        IndexHint(IndexImpl index, boolean force) {
            theIndex = index;
            theForce = force;
        }

        @Override
        public String toString() {

            String name = (theIndex == null ? "primary" : theIndex.getName());

            if (theForce) {
                return "FORCE_INDEX(" + name + ")";
            }

            return "PREFER_INDEX(" + name + ")";
        }
    }

    private TableImpl theTargetTable;

    private ArrayList<TableImpl> theTables = new ArrayList<TableImpl>(1);

    private ArrayList<String> theAliases = new ArrayList<String>(1);

    private int theNumAncestors;

    private int theNumDescendants;
    
    private Expr[] theTablePreds;

    private Direction theDirection = Direction.FORWARD;

    private ArrayList<PrimaryKeyImpl> thePrimKeys;

    private ArrayList<IndexKeyImpl> theSecKeys;

    private ArrayList<FieldRange> theRanges;

    private ArrayList<Expr> thePushedExternals;

    private boolean[] theUsesCoveringIndex;

    private List<IndexHint> theIndexHints = null;

    private IndexHint theForceIndexHint = null;

    private boolean theEliminateIndexDups;

    private boolean theIsUpdate;

    ExprBaseTable(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location) {

        super(qcb, sctx, ExprKind.BASE_TABLE, location);
    }

    void addTable(
        TableImpl table,
        String alias,
        boolean isAncestor,
        boolean isDescendant,
        QueryException.Location loc) {

        theTables.add(table);
        theAliases.add(alias);

        if (isAncestor) {
            if (!theTargetTable.isAncestor(table)) {
                throw new QueryException(
                    "Table " + table.getFullName() + " is not an ancestor " +
                    "of target table " + theTargetTable.getFullName(), loc);
            }

            ++theNumAncestors;

        } else if (isDescendant) {

            if (!table.isAncestor(theTargetTable)) {
                throw new QueryException(
                    "Table " + table.getFullName() + " is not a descendant " +
                    "of target table " + theTargetTable.getFullName(), loc);
            }

            ++theNumDescendants;

        } else {
            theTargetTable = table;
        }
    }

    void setSortedTables(
        ArrayList<TableImpl> sortedTables,
        ArrayList<String> sortedAliases) {

        theTables = sortedTables;
        theAliases = sortedAliases;

        computeType();
    }

    void finalizeTables() {

        int numTables = theTables.size();

        theUsesCoveringIndex = new boolean[numTables];
        theTablePreds = new Expr[numTables];

        for (int i = 0; i < numTables; ++ i) {
            theUsesCoveringIndex[i] = false;
            theTablePreds[i] = null;
        }

        assert(theType == null);
        computeType();
    }

    int getNumTables() {
        return theTables.size();
    }

    int getNumAncestors() {
        return theNumAncestors;
    }

    int getNumDescendants() {
        return theNumDescendants;
    }

    boolean hasNestedTables() {
        return (theNumAncestors > 0 || theNumDescendants > 0);
    }

    boolean isDescendant(int tablePos) {
        return tablePos > theNumAncestors + 1;
    }

    TableImpl getTargetTable() {
        return theTargetTable;
    }

    int getTargetTablePos() {
        return theNumAncestors;
    }

    ArrayList<TableImpl> getTables() {
        return theTables;
    }

    TableImpl getTable(int pos) {
        return theTables.get(pos);
    }

    int getTablePos(TableImpl table) {

        for (int i = 0; i < theTables.size(); ++i) {
            if (theTables.get(i).getId() == table.getId()) {
                return i;
            }
        }

        return -1;
    }

    void setTablePred(int tablePos, Expr pred, boolean destroy) {

        if (theTablePreds[tablePos] != null) {
            theTablePreds[tablePos].removeParent(this, destroy);
        }

        pred = ExprPromote.create(null, pred, TypeManager.BOOLEAN_QSTN());
        theTablePreds[tablePos] = pred;
        pred.addParent(this);
    }

    Expr getTablePred(int tablePos) {
        return theTablePreds[tablePos];
    }

    void removeTablePred(int tablePos, boolean destroy) {
        theTablePreds[tablePos].removeParent(this, destroy);
        theTablePreds[tablePos] = null;
    }

    ArrayList<String> getAliases() {
        return theAliases;
    }

    TableImpl getTableForAlias(String alias) {

        for (int i = 0; i < theTables.size(); ++i) {
            if (theAliases.get(i).equals(alias))
                return theTables.get(i);
        }

        throw new QueryStateException(
            "Could not find table for alias " + alias);
    }

    IndexImpl getIndex() {
        if (theSecKeys == null) {
            return null;
        }

        return (IndexImpl)theSecKeys.get(0).getIndex();
    }

    /*
     * The children of an ExprbaseTable are the ON/filtering conditions
     * associated with each table. We assume that the number of children
     * is equal to the number of tables, even if some or all of the table
     * preds may be null. The ExprIterator skips over the null preds.
     */
    @Override
    int getNumChildren() {
        return (theNumAncestors + theNumDescendants + 1);
    }

    Direction getDirection() {
        return theDirection;
    }

    void setDirection(Direction dir) {
        theDirection = dir;
    }

    ArrayList<PrimaryKeyImpl> getPrimaryKeys() {
        return thePrimKeys;
    }

    void addPrimaryKeys(
        int tablePos,
        ArrayList<PrimaryKeyImpl> keys,
        ArrayList<FieldRange> ranges,
        boolean isCoveringIndex) {

        /* Set thePrimKeys and theRanges only if it is the target table */
        if (tablePos == theNumAncestors) {
            thePrimKeys = keys;
            theRanges = ranges;
        }
        theUsesCoveringIndex[tablePos] = isCoveringIndex;
    }

    ArrayList<IndexKeyImpl> getSecondaryKeys() {
        return theSecKeys;
    }

    void addSecondaryKeys(
        int tablePos,
        ArrayList<IndexKeyImpl> keys,
        ArrayList<FieldRange> ranges,
        boolean isCoveringIndex) {

        /* Set theSecKeys and theRanges only if it is the target table */
        if (tablePos == theNumAncestors) {
            theSecKeys = keys;
            theRanges = ranges;
        }
        theUsesCoveringIndex[tablePos] = isCoveringIndex;
    }

    ArrayList<FieldRange> getRanges() {
        return theRanges;
    }

    boolean[] getUsesCoveringIndex() {
        return theUsesCoveringIndex;
    }

    void setPushedExternals(ArrayList<Expr> v) {
        assert(thePushedExternals == null);
        thePushedExternals = v;
    }

    ArrayList<Expr> getPushedExternals() {
        return thePushedExternals;
    }

    /**
     * If index is null, we are checking whether the ptimary index
     * has been specified in a hint/
     */
    boolean isIndexHint(IndexImpl index) {

        if (theIndexHints == null) {
            return false;
        }

        for (IndexHint hint : theIndexHints) {
            if (hint.theIndex == index) {
                return true;
            }
        }

        return false;
    }

    IndexHint getForceIndexHint() {
        return theForceIndexHint;
    }

    /**
     * If index is null, it means the hint is about the primary index
     */
    void addIndexHint(IndexImpl index, boolean force, Location loc) {

        if (theIndexHints == null) {
            theIndexHints = new ArrayList<IndexHint>();
        }

        IndexHint hint = new IndexHint(index, force);

        if ( !containsHint(theIndexHints, hint) ) {
            theIndexHints.add(hint);
        }

        if (force) {
            if (theForceIndexHint != null) {
                throw new QueryException(
                    "Cannot have more than one FORCE_INDEX hints", loc);
            }

            theForceIndexHint = hint;
        }
    }

    private static boolean containsHint(
        List<IndexHint> indexHints,
        IndexHint hint) {
        for (IndexHint h : indexHints) {
            if (h.theIndex == null && hint.theIndex == null ||
                h.theIndex.getName().equals(hint.theIndex.getName())) {
                return true;
            }
        }
        return false;
    }

    void setEliminateIndexDups() {
        theEliminateIndexDups = true;
    }

    boolean getEliminateIndexDups() {
        return theEliminateIndexDups;
    }

    void setIsUpdate() {
        theIsUpdate = true;
    }

    boolean getIsUpdate() {
        return theIsUpdate;
    }

    @Override
    ExprType computeType() {

        if (theType != null) {
            return theType;
        }

        if (theTables.size() == 1) {
            theType = TypeManager.createTableRecordType(theTargetTable,
                                                        Quantifier.STAR);
            return theType;
        }

        FieldMap unionMap = new FieldMap();

        for (int i = 0; i < theTables.size(); ++i) {

            TableImpl table = theTables.get(i);
            RecordDefImpl rowDef = table.getRowDef();
            String fname = theAliases.get(i);

            unionMap.put(fname, rowDef, true, /*nullable*/
                         null/*defaultValue*/);
        }

        RecordDefImpl unionDef =
            FieldDefFactory.createRecordDef(unionMap, "fromDef");

        theType = TypeManager.createType(unionDef, Quantifier.STAR);

        return theType;
    }

    @Override
    public boolean mayReturnNULL() {
        return false;
    }

    @Override
    void display(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("TABLE");
        displayContent(sb, formatter);
    }

    @Override
    void displayContent(StringBuilder sb, QueryFormatter formatter) {

        int numRanges = (theRanges != null ? theRanges.size() : 0);

        sb.append("\n");
        formatter.indent(sb);
        sb.append("[\n");

        formatter.incIndent();
        formatter.indent(sb);
        sb.append(theTargetTable.getName());

        if (thePrimKeys != null) {

            if (theUsesCoveringIndex[theNumAncestors]) {
                sb.append(" via covering primary index");
            } else {
                sb.append(" via primary index");
            }

            for (int i = 0; i < numRanges; ++i) {
                sb.append("\n");
                formatter.indent(sb);
                sb.append("KEY: ");
                sb.append(thePrimKeys.get(i));
                sb.append("\n");
                formatter.indent(sb);
                sb.append("RANGE: ");
                sb.append(theRanges.get(i));
            }
        }

        if (theSecKeys != null) {

            if (theUsesCoveringIndex[theNumAncestors]) {
                sb.append(" via covering index ");
            } else {
                sb.append(" via index ");
            }
            sb.append(theSecKeys.get(0).getIndex().getName());
            if (theEliminateIndexDups) {
                sb.append(" with duplicate elimination");
            }

            for (int i = 0; i < numRanges; ++i) {
                sb.append("\n");
                formatter.indent(sb);
                sb.append("SEC KEY: ");
                sb.append(theSecKeys.get(i));
                sb.append("\n");
                formatter.indent(sb);
                sb.append("RANGE: ");
                sb.append(theRanges.get(i));
            }
        }

        if (theNumAncestors > 0) {
            sb.append("\n");
            formatter.indent(sb);
            sb.append("Ancestors :\n");
            for (int i = 0; i < theNumAncestors; ++i) {
                formatter.indent(sb);
                sb.append(theTables.get(i).getFullName());
                if (theUsesCoveringIndex[i]) {
                    sb.append(" via covering primary index");
                } else {
                    sb.append(" via primary index");
                }
                sb.append("\n");
            }
        }

        if (theNumDescendants > 0) {
            sb.append("\n");
            formatter.indent(sb);
            sb.append("Descendants :\n");
            for (int i = theNumAncestors + 1; i < theTables.size(); ++i) {
                formatter.indent(sb);
                sb.append(theTables.get(i).getFullName());
                if (theUsesCoveringIndex[i]) {
                    sb.append(" via covering primary index");
                } else {
                    sb.append(" via primary index");
                }
                sb.append("\n");
            }
        }

        if (thePushedExternals != null) {
            sb.append("\n");
            formatter.indent(sb);
            sb.append("PUSHED EXTERNAL EXPRS: ");
            for (Expr expr : thePushedExternals) {
                sb.append("\n");
                if (expr == null) {
                    formatter.indent(sb);
                    sb.append("null");
                } else {
                    expr.display(sb, formatter);
                }
            }
        }

        if (theTablePreds != null) {

            if (theTablePreds[theNumAncestors] != null) {
                sb.append("\n\n");
                formatter.indent(sb);
                sb.append("Filtering Predicate:\n");
                theTablePreds[theNumAncestors].display(sb, formatter);
            }

            for (int i = 0; i < theTables.size(); ++i) {

                if (i == theNumAncestors || theTablePreds[i] == null) {
                    continue;
                }

                sb.append("\n\n");
                formatter.indent(sb);
                sb.append("ON Predicate for table ").
                   append(theTables.get(i).getFullName()).
                   append(":\n");
                theTablePreds[i].display(sb, formatter);
            }
        }

        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("]");
    }
}

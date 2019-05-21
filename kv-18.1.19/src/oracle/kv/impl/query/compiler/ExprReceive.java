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

import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.IndexKeyImpl;

import oracle.kv.impl.query.types.ExprType;


/**
 * This expr is used to mark the boundaries between parts of the query that
 * execute on different "machines". The receive expr itself executes at a
 * "client machine" and its child subplan executes at a "server machine".
 * The child subplan may actually be replicated on several server machines,
 * in which case the receive expr acts as a UNION ALL expr, collecting and
 * propagating the results it receives from its children. Furthermore, the
 * receive expr may perform a merge-sort over its inputs (if the inputs
 * return sorted results).
 *
 * Receive exprs are always created as parents of the BaseTable exprs in the
 * exprs graph, After their creation, Receive exprs are pulled-up as far as
 * they can go. All this is done by the Distributer class.
 *
 * theInput:
 * The expr producing the input to this receive expr.
 *
 * theEliminateIndexDups:
 * Whether or not to eliminate index dups. These are duplicate results that
 * may be generated during the scan of a multikey (array/map) index.
 *
 * thePrimKeyPositions:
 * The positions of the primary key columns in the RecordValues received
 * from the servers. This is non-null only if the ReceiveIter must do
 * elimination of index duplicates.
 */
class ExprReceive extends Expr {

    private Expr theInput;

    private int[] theSortFieldPositions;

    private SortSpec[] theSortSpecs;

    private DistributionKind theDistributionKind;

    private PrimaryKeyImpl thePrimaryKey;

    private ArrayList<Expr> thePushedExternals;

    private boolean theEliminateIndexDups;

    private int[] thePrimKeyPositions;

     private boolean theIsUpdate;

    ExprReceive(QueryControlBlock qcb, StaticContext sctx) {
        super(qcb, sctx, ExprKind.RECEIVE, null);
    }

    @Override
    int getNumChildren() {
        return 1;
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    void setInput(Expr newExpr, boolean destroy) {
        if (theInput != null) {
            theInput.removeParent(this, destroy);
        }
        theInput = newExpr;
        theInput.addParent(this);
        theType = computeType();
        computeDistributionKind();
        setLocation(newExpr.getLocation());
    }

    void computeDistributionKind() {

        if (theInput.getKind() != ExprKind.BASE_TABLE) {
            return;
        }

        ExprBaseTable tableExpr = (ExprBaseTable)theInput;

        ArrayList<PrimaryKeyImpl> primKeys = tableExpr.getPrimaryKeys();
        ArrayList<IndexKeyImpl> secKeys = tableExpr.getSecondaryKeys();

        if (secKeys != null) {
           theDistributionKind = DistributionKind.ALL_SHARDS;

        } else if (primKeys != null &&
                   primKeys.size() == 1 && // this is always true, for now
                   primKeys.get(0).hasShardKey()) {
            theDistributionKind = DistributionKind.SINGLE_PARTITION;
            thePrimaryKey = primKeys.get(0);

            if (tableExpr.getPushedExternals() != null) {
                thePushedExternals =
                    new ArrayList<Expr>(tableExpr.getPushedExternals());

                /* Remove entries associated with a range, if any */
                if (tableExpr.getRanges().get(0) != null) {
                    thePushedExternals.remove(thePushedExternals.size() - 1);
                    thePushedExternals.remove(thePushedExternals.size() - 1);
                }
            }
        } else {
            // TODO: handle the case of multiple prim keys, when it arises
            // TODO : MULTI_PARTITTIONS distribution kind
            theDistributionKind = DistributionKind.ALL_PARTITIONS;
        }
    }

    DistributionKind getDistributionKind() {
        return theDistributionKind;
    }

    PrimaryKeyImpl getPrimaryKey() {
        return thePrimaryKey;
    }

    ArrayList<Expr> getPushedExternals() {
        return thePushedExternals;
    }

    void addSort(int[] sortExprPositions, SortSpec[] specs) {
        theSortFieldPositions = sortExprPositions;
        theSortSpecs = specs;
        theType = computeType();
    }

    int[] getSortFieldPositions() {
        return theSortFieldPositions;
    }

    SortSpec[] getSortSpecs() {
        return theSortSpecs;
    }

    void setEliminateIndexDups(boolean v) {
        theEliminateIndexDups = v;
    }

    boolean getEliminateIndexDups() {
        return theEliminateIndexDups;
    }

    void setIsUpdate(boolean v) {
        theIsUpdate = v;
    }

    boolean getIsUpdate() {
        return theIsUpdate;
    }

    void addPrimKeyPositions(int[] positions) {
        thePrimKeyPositions = positions;
        theType = computeType();
    }

    int[] getPrimKeyPositions() {
        return thePrimKeyPositions;
    }

    @Override
    ExprType computeType() {
        return theInput.getType();
    }

    @Override
    public boolean mayReturnNULL() {
        return theInput.mayReturnNULL();
    }

    @Override
    void displayContent(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("DistributionKind : ").append(theDistributionKind);
        sb.append(",\n");
        if (thePrimaryKey != null) {
            formatter.indent(sb);
            sb.append("PrimaryKey :").append(thePrimaryKey);
            sb.append(",\n");
        }
        if (theSortFieldPositions != null) {
            formatter.indent(sb);
            sb.append("Sort Field Positions : ").append(theSortFieldPositions);
            sb.append(",\n");
        }
        if (thePrimKeyPositions != null) {
            formatter.indent(sb);
            sb.append("Primary Key Positions : ");
            for (int i = 0; i < thePrimKeyPositions.length; ++i) {
                sb.append(thePrimKeyPositions[i]);
                if (i < thePrimKeyPositions.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(",\n");
        }
        formatter.indent(sb);
        theInput.display(sb, formatter);
    }
}

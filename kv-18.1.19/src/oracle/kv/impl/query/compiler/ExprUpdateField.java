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

import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * See javadoc for ExprUpdateRow and UpdateFieldIter.
 */
class ExprUpdateField extends Expr {

    private UpdateKind theUpdateKind;

    private Expr theInput;

    private Expr thePosExpr;

    private ExprVar theTargetItemVar;

    private Expr theNewValueExpr;

    private boolean theCloneNewValues = true;

    ExprUpdateField(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Expr input) {

        super(qcb, sctx, ExprKind.UPDATE_FIELD, location);

        if (input != null) {
            theInput = input;
            theInput.addParent(this);
        }

        theType = TypeManager.EMPTY();
    }

    void setUpdateKind(UpdateKind k) {

        theUpdateKind = k;

        if (theUpdateKind == UpdateKind.REMOVE) {

            theCloneNewValues = false;

            if(theInput.getType().isRecord()) {
                throw new QueryException(
                    "Cannot remove fields from records.",
                    theLocation);
            }
        }
    }

    UpdateKind getUpdateKind() {
        return theUpdateKind;
    }

    boolean isTTLUpdate() {
        return (theUpdateKind == UpdateKind.TTL_HOURS ||
                theUpdateKind == UpdateKind.TTL_DAYS ||
                theUpdateKind == UpdateKind.TTL_TABLE);
    }

    void addTargetItemVar(ExprVar v) {
        theTargetItemVar = v;
    }

    void addNewValueExpr(Expr e) {

        if (theUpdateKind == UpdateKind.SET && e.isMultiValued()) {
            ArrayList<Expr> args = new ArrayList<Expr>(1);
            args.add(e);
            e = new ExprArrayConstr(theQCB, theSctx, e.getLocation(),
                                    args, true/*conditional*/);
        }

        theNewValueExpr = e;
        theNewValueExpr.addParent(this);

        theCloneNewValues = mustCloneNewValues(e, theTargetItemVar, false);
    }

    void addPosExpr(Expr e) {
        thePosExpr = e;
        thePosExpr.addParent(this);
    }

    void removeTargetItemVar() {
        if (theTargetItemVar.getNumParents() == 0) {
            theTargetItemVar = null;
        }
    }

    void setPosExpr(Expr newExpr, boolean destroy) {
        thePosExpr.removeParent(this, destroy);
        thePosExpr = newExpr;
        newExpr.addParent(this);
        removeTargetItemVar();
    }

    void setNewValueExpr(Expr newExpr, boolean destroy) {

        theNewValueExpr.removeParent(this, destroy);
        theNewValueExpr = newExpr;
        newExpr.addParent(this);
        removeTargetItemVar();

        theCloneNewValues = mustCloneNewValues(newExpr, theTargetItemVar, false);
    }
 
    @Override
    int getNumChildren() {
        return (theInput == null ? 0 :
                (theNewValueExpr != null ?
                 (thePosExpr != null ? 3 : 2) :
                 1));
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    Expr getPosExpr() {
        return thePosExpr;
    }

    Expr getNewValueExpr() {
        return theNewValueExpr;
    }

    ExprVar getTargetItemVar() {
        return theTargetItemVar;
    }

    boolean cloneNewValues() {
        return theCloneNewValues;
    }

    /*
     * This method decides whether complex values returned by a new-valuse expr 
     * (in SET, ADD, or PUT clauses) nedd to be cloned before they are used to
     * update the the target item(s). Cloning may be needed because it is 
     * possible to create cycles among items, resulting in stack overflows when
     * such a circular data structure is serialized.
     *
     * However, to produce a cycle, the new-value expr must return items that
     * are not proper descendants of the target item. This is possible only if
     * the new-value expr references the row variable (the table alias) or the
     * target-item variable ($). If the row variable is referenced, we clone
     * (because it is hard or impossible to deduce at compile time whether a
     * cycle will be formed). If $ is referenced, but in a way that guaranties
     * that only proper descendants of the target item will be returned (eg
     * $.a.b) then we don't clone; otherwise we do.
     */
    private static boolean mustCloneNewValues(
        Expr expr,
        Expr targetItemVar,
        boolean inPathExpr) {

        /* No need to clone atomic values */
        if (expr.getType().getDef().isAtomic()) {
            return false;
        }

        switch (expr.getKind()) {

        case CONST:
            return false;

        case VAR:
            ExprVar var = (ExprVar)expr;

            /*
             * We clone if the var is a row var or it is the $ var and it's
             * not referenced in a path expr. 
             */
            if (var.getTable() != null ||
                (var == targetItemVar && !inPathExpr)) {
                return true;
            }

            return false;

        case FUNC_CALL:
            if (expr.getFunction(FuncCode.FN_SEQ_CONCAT) != null) {

                ExprFuncCall fncall = (ExprFuncCall)expr;

                for (int i = 0; i < fncall.getNumArgs(); ++i) {
                    if (mustCloneNewValues(fncall.getArg(i),
                                           targetItemVar,
                                           inPathExpr)) {
                        return true;
                    }
                }

                return false;
            }

            /*
             * For now, all other functions return atomic values, so we should
             * not be here.
             */
            Function func = expr.getFunction(null);
            throw new QueryStateException("Unexpected function call: " +
                                          func.getCode());

        case PROMOTE:
        case CAST:
        case SEQ_MAP:
            return mustCloneNewValues(expr.getInput(),
                                      targetItemVar,
                                      inPathExpr);

        case CASE:
            ExprCase caseExpr = (ExprCase)expr;

            for (int i = 0; i < caseExpr.getNumWhenClauses(); ++i) {
                if (mustCloneNewValues(caseExpr.getThenExpr(i),
                                       targetItemVar,
                                       inPathExpr)) {
                    return true;
                }
            }

            if (caseExpr.getElseExpr() != null &&
                mustCloneNewValues(caseExpr.getElseExpr(),
                                   targetItemVar,
                                   inPathExpr)) {
                return true;
            }

            return false;

        case ARRAY_CONSTR:
            ExprArrayConstr arr = (ExprArrayConstr)expr;

            for (int i = 0; i < arr.getNumArgs(); ++i) {

                if (mustCloneNewValues(arr.getArg(i),
                                       targetItemVar,
                                       inPathExpr)) {
                    return true;
                }
            }

            return false;

        case MAP_CONSTR:
            ExprMapConstr map = (ExprMapConstr)expr;

            for (int i = 0; i < map.getNumArgs(); ++i) {

                if (i % 2 == 0) {
                    continue;
                }

                if (mustCloneNewValues(map.getArg(i),
                                       targetItemVar,
                                       inPathExpr)) {
                    return true;
                }
            }

            return false;

        case FIELD_STEP:
        case MAP_FILTER:
        case ARRAY_SLICE:
        case ARRAY_FILTER:
            return mustCloneNewValues(expr.getInput(), targetItemVar, true);

        case BASE_TABLE:
        case IS_OF_TYPE:
        case SFW:
        case REC_CONSTR:
        case UPDATE_ROW:
        case UPDATE_FIELD:
        case RECEIVE:
            throw new QueryStateException("Unexpected expression kind: " +
                                          expr.getKind());
        }

        return true;
    }

    @Override
    public ExprType computeType() {
        return theType;
    }

    @Override
    public boolean mayReturnNULL() {
        return false;
    }

    @Override
    void displayContent(StringBuilder sb, QueryFormatter formatter) {

        if (theInput != null) {
            theInput.display(sb, formatter);
        }

        if (theTargetItemVar != null) {
            sb.append("\n");
            theTargetItemVar.display(sb, formatter);
        }

        if (theNewValueExpr != null) {
            sb.append("\n");
            theNewValueExpr.display(sb, formatter);
        }
    }
}

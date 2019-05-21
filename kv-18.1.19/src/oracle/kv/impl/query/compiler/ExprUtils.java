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
import java.util.Iterator;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr.ExprIter;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.ExprVar.VarKind;
import oracle.kv.impl.query.runtime.CastIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.table.FieldDef;

/**
 * Various utility methods used during optimizations
 */
class ExprUtils {

    /*
     * Method called just before codegen to adjust the expr type of constructor
     * expressions, based on where the constructed arrays/maps are going to be
     * used. Specifically, if a constructed array/map C is going to be inserted
     * into another constructed array/map P, and the element type of P is JSON,
     * the element type of C must also be JSON. We do this to guarantee that 
     * strongly type data does not get inserted into JSON data.
     *
     * The method traverses the expr graph looking for array/map constructors.
     * When it finds such a constructor P, it calls constructJsonArrayMap() on
     * its child exprs to propagate the JSON-ness down to any descendant
     * constructors. Descendant constructors will have their element type
     * changed to JSON, but only if the array/map they construct may indeed be
     * consumed by the P constructor.
     */
    static void adjustConstructorTypes(Expr expr) {

        switch (expr.getKind()) {
        case ARRAY_CONSTR: {
            ExprArrayConstr arrExpr = (ExprArrayConstr)expr;
            ArrayDefImpl arrDef = arrExpr.getArrayType();

            if (arrDef.getElement().equals(FieldDefImpl.jsonDef)) {

                int numArgs = arrExpr.getNumArgs();
                for (int i = 0; i < numArgs; ++i) {
                    constructJsonArrayMap(arrExpr.getArg(i));
                }
            }
            break;
        }
        case MAP_CONSTR: {
            ExprMapConstr mapExpr = (ExprMapConstr)expr;
            MapDefImpl mapDef = mapExpr.getMapType();

            if (mapDef.getElement().equals(FieldDefImpl.jsonDef)) {

                int numArgs = mapExpr.getNumArgs();
                for (int i = 1; i < numArgs; i += 2) {
                    constructJsonArrayMap(mapExpr.getArg(i));
                }
            }
            break;
        }
        case REC_CONSTR: {
            ExprRecConstr recExpr = (ExprRecConstr)expr;
            RecordDefImpl recDef = recExpr.getDef();
            int numArgs = recExpr.getNumArgs();

            for (int i = 0; i < numArgs; ++i) {
                if (recDef.getFieldDef(i).equals(FieldDefImpl.jsonDef)) {
                    constructJsonArrayMap(recExpr.getArg(i));
                }
            }
            break;
        }
        case UPDATE_FIELD: {
            ExprUpdateField upd = (ExprUpdateField)expr;
            Expr path = expr.getInput();
            if (path.getType().getDef().equals(FieldDefImpl.jsonDef) &&
                upd.getNewValueExpr() != null) {
                constructJsonArrayMap(upd.getNewValueExpr());
            }
            break;
        }
        case SFW: {
            /*
             * SFW expr does implicit record construction, so it should also
             * be handled here.
             */
            ExprSFW sfw = (ExprSFW)expr;
            int numFields = sfw.getNumFields();

            for (int i = 0; i < numFields; ++i) {
                Expr fieldExpr = sfw.getFieldExpr(i);
                FieldDefImpl fieldDef = fieldExpr.getType().getDef();
                if (fieldDef.equals(FieldDefImpl.jsonDef)) {
                    constructJsonArrayMap(fieldExpr);
                }
            }
            break;
        }
        default:
            break;
        }

        Iterator<Expr> children = expr.getChildren();
        while (children.hasNext()) {
            Expr child = children.next();
            adjustConstructorTypes(child);
        }
    }

    static private void constructJsonArrayMap(Expr expr) {

        FieldDefImpl exprDef = expr.getType().getDef();

        if (exprDef.isAtomic() ||
            exprDef.isRecord() ||
            exprDef.isAnyRecord()) {
            return;
        }

        switch (expr.getKind()) {

        case ARRAY_CONSTR:
            ExprArrayConstr arrExpr = (ExprArrayConstr)expr;
            ArrayDefImpl arrayDef = arrExpr.getArrayType();
            if (arrayDef.getElement().isSubtype(FieldDefImpl.jsonDef)) {
                arrExpr.setJsonArrayType();
                if (arrExpr.computeType(false)) {
                    propagateTypeChange(arrExpr);
                }
            }
            break;

        case MAP_CONSTR:
            ExprMapConstr mapExpr = (ExprMapConstr)expr;
            MapDefImpl mapDef = mapExpr.getMapType();
            if (mapDef.getElement().isSubtype(FieldDefImpl.jsonDef)) {
                mapExpr.setJsonMapType();
                if (mapExpr.computeType(false)) {
                    propagateTypeChange(mapExpr);
                }
            }
            break;

        case REC_CONSTR:
            ExprRecConstr recExpr = (ExprRecConstr)expr;
            int numArgs = recExpr.getNumArgs();

            for (int i = 0; i < numArgs; ++i) {
                constructJsonArrayMap(recExpr.getArg(i));
            }
            break;

        case VAR:
            ExprVar var = (ExprVar)expr;
            if (var.isFor()) {
                constructJsonArrayMap(var.getDomainExpr());
            }
            return;

        case SFW:
            ExprSFW sfw = (ExprSFW)expr;
            int numFieldExprs = sfw.getNumFields();
            for (int i = 0; i < numFieldExprs; ++i) {
                constructJsonArrayMap(sfw.getFieldExpr(i));
            }
            return;

        case ARRAY_SLICE:
        case ARRAY_FILTER:
        case MAP_FILTER:
        case FIELD_STEP:
            expr = expr.getInput();
            break;

        case FUNC_CALL:
            ExprFuncCall funcExpr = (ExprFuncCall)expr;
            switch (funcExpr.getFunction().getCode()) {
            case FN_SEQ_CONCAT:
                break;
            default:
                return;
            }
            break;

        case SEQ_MAP:
            ExprSeqMap seqMapExpr = (ExprSeqMap)expr;
            expr = seqMapExpr.getMapExpr();
            break;

        case PROMOTE:
        case RECEIVE:
        case CASE:
            break;

        case CONST:
            ExprConst constExpr = (ExprConst)expr;
            FieldValueImpl val = constExpr.getValue();
            FieldDefImpl valDef = val.getDefinition();
            FieldValueImpl newVal = null;
            QueryException.Location loc = expr.getLocation();

            if (valDef.isArray() &&
                !valDef.equals(FieldDefImpl.arrayJsonDef)) {

                newVal = CastIter.castValue(val, FieldDefImpl.arrayJsonDef, loc);

            } else if (valDef.isMap() &&
                       !valDef.equals(FieldDefImpl.mapJsonDef)) {

                newVal = CastIter.castValue(val, FieldDefImpl.mapJsonDef, loc);
            }

            if (newVal != null) {
                ExprConst newExpr = new ExprConst(expr.getQCB(),
                                                  expr.getSctx(),
                                                  loc,
                                                  newVal);

                expr.replace(newExpr, true/*destroy*/);
            }

            return;

        case CAST:
        case IS_OF_TYPE:
        case BASE_TABLE:
            return;
        default:
            throw new QueryStateException(
                "Unexpected expression kind: " + expr.getKind());
        }

        Iterator<Expr> children = expr.getChildren();
        while (children.hasNext()) {
            Expr child = children.next();
            constructJsonArrayMap(child);
        }
    }

    /*
     * The type of the given expr was presumably just changed. This method
     * propagates the type change to the ancestors of the given expr.
     */
    static private void propagateTypeChange(Expr expr) {

        int numParents = expr.getNumParents();

        for (int i = 0; i < numParents; ++i) {

            Expr parent = expr.getParent(i);

            switch (parent.getKind()) {

            case SFW: {
                ExprSFW sfw = (ExprSFW)parent;
                ArrayList<ExprVar> vars = sfw.findVarsForExpr(expr);
                if (vars != null) {
                    for (ExprVar var : vars) {
                        boolean modified = var.computeType(false);
                        if (modified) {
                            propagateTypeChange(var);
                        }
                    }
                }
                break;
            }
            case MAP_FILTER: {
                ExprMapFilter mapFilter = (ExprMapFilter)parent;
                ExprVar ctxVar = mapFilter.getCtxItemVar();
                ExprVar elemVar = mapFilter.getCtxElemVar();

                if (ctxVar != null && ctxVar.computeType(false)) {
                    propagateTypeChange(ctxVar);
                }
                if (elemVar != null && elemVar.computeType(false)) {
                    propagateTypeChange(elemVar);
                }
                break;
            }
            case ARRAY_FILTER: {
                ExprArrayFilter arrFilter = (ExprArrayFilter)parent;
                ExprVar ctxVar = arrFilter.getCtxItemVar();
                ExprVar elemVar = arrFilter.getCtxElemVar();

                if (ctxVar != null && ctxVar.computeType(false)) {
                    propagateTypeChange(ctxVar);
                }
                if (elemVar != null && elemVar.computeType(false)) {
                    propagateTypeChange(elemVar);
                }
                break;
            }
            case ARRAY_SLICE: {
                ExprArraySlice arrSlice = (ExprArraySlice)parent;
                ExprVar ctxVar = arrSlice.getCtxItemVar();

                if (ctxVar != null && ctxVar.computeType(false)) {
                    propagateTypeChange(ctxVar);
                }
                break;
            }
            case FIELD_STEP: {
                ExprFieldStep fieldStep = (ExprFieldStep)parent;
                ExprVar ctxVar = fieldStep.getCtxItemVar();

                if (ctxVar != null && ctxVar.computeType(false)) {
                    propagateTypeChange(ctxVar);
                }
                break;
            }
            case SEQ_MAP: {
                ExprSeqMap seqmap = (ExprSeqMap)parent;
                ExprVar ctxVar = seqmap.getCtxVar();

                if (ctxVar != null && ctxVar.computeType(false)) {
                    propagateTypeChange(ctxVar);
                }
                break;
            }
            case CASE:
            case FUNC_CALL:
            case IS_OF_TYPE:
            case PROMOTE:
            case CAST:
            case ARRAY_CONSTR:
            case MAP_CONSTR:
            case REC_CONSTR:
            case UPDATE_ROW:
            case UPDATE_FIELD:
            case RECEIVE:
                break;

            case BASE_TABLE:
            case CONST:
            case VAR:
                throw new QueryStateException(
                    "A " + parent.getKind() + " expression cannot be the " +
                    "parent of any expression");
            }

            boolean modified = parent.computeType(false);
            if (modified) {
                propagateTypeChange(parent);
            }
        }
    }

    /**
     * return true if two expressions are identical; otherwise return false
     */
    static boolean matchExprs(Expr expr1, Expr expr2) {
        return matchExprsInternal(expr1, expr2, ++Expr.theVisitCounter);
    }

    private static boolean matchExprsInternal(Expr expr1, Expr expr2, int vid) {

        expr1.theVisitId = vid;
        expr2.theVisitId = vid;

        if (expr1.getKind() != expr2.getKind()) {
            return false;
        }

        if (expr1.getNumChildren() != expr2.getNumChildren()) {
            return false;
        }

        switch (expr1.getKind()) {
        case CONST: {
            ExprConst e1 = (ExprConst)expr1;
            ExprConst e2 = (ExprConst)expr2;
            return e1.getValue().equals(e2.getValue());
        }
        case BASE_TABLE: {
            ExprBaseTable e1 = (ExprBaseTable)expr1;
            ExprBaseTable e2 = (ExprBaseTable)expr2;

            /*
             * For now, there can be only one ExprBaseTable in the query, so
             * just return true. Otherwise, uncomment and finish up the code
             * below (TODO).
             */
            assert(e1 == e2);
            return true;
            /*
            if (e1.getTable() != e2.getTable()) {
                return false;
            }
            if (e1.getPrimaryKey() != null) {
                if (e2.getPrimaryKey() == null) {
                    return false;
                }
                if (!e1.getPrimaryKey().equals(e2.getPrimaryKey())) {
                    return false;
                }
            } else if (e2.getPrimaryKey() != null) {
                return false;
            }
            if (e2.getSecondaryKey() != null) {
                if (e2.getSecondaryKey() == null) {
                    return false;
                }
                if (!e1.getSecondaryKey().equals(e2.getSecondaryKey())) {
                    return false;
                }
            } else if (e2.getSecondaryKey() != null) {
                return false;
            }

            compare range the filtering preds as well.....

            break;
            */
        }
        case VAR: {
            ExprVar e1 = (ExprVar)expr1;
            ExprVar e2 = (ExprVar)expr2;

            if (e1.getVarKind() != e2.getVarKind()) {
                return false;
            }

            if (e1.getVarKind() == VarKind.EXTERNAL) {
                return e1.getId() == e2.getId();
            }

            if (e1.isContext()) {
                Expr ctxExpr1 = e1.getCtxExpr();
                Expr ctxExpr2 = e2.getCtxExpr();

                /*
                 * The context expr will typically reference the contex
                 * vars, so if the context exprs have been visited already
                 * during this traversal, don't try to match them again
                 * as this will lead to infinite recursive calls.
                 */
                if (ctxExpr1.theVisitId == vid && ctxExpr2.theVisitId == vid) {
                    return e1.getName().equals(e2.getName());
                }

                return matchExprsInternal(ctxExpr1, ctxExpr2, vid);
            }

            /*
             * If they are both table vars, compare the associated tables.
             * Notice that in the case of nested tables queries, both vars
             * may have the same ExprBaseTable as their domain expr, even
             * though they range over different tables, so matching their
             * domain exprs would not work. 
             */
            if (e1.getTable() != null && e2.getTable() != null) {
                return e1.getTable().getId() == e2.getTable().getId();
            }

            return matchExprsInternal(e1.getDomainExpr(), e2.getDomainExpr(),
                                      vid);
        }
        case FUNC_CALL: {
            ExprFuncCall e1 = (ExprFuncCall)expr1;
            ExprFuncCall e2 = (ExprFuncCall)expr2;

            if (e1.getFunction() != e2.getFunction()) {
                return false;
            }

            return matchChildren(e1, e2, vid);
        }
        case PROMOTE: {
            ExprPromote e1 = (ExprPromote)expr1;
            ExprPromote e2 = (ExprPromote)expr2;

            return (e1.getTargetType().equals(e2.getTargetType()) &&
                    matchExprsInternal(e1.getInput(), e2.getInput(), vid));
        }
        case IS_OF_TYPE: {
            ExprIsOfType e1 = (ExprIsOfType)expr1;
            ExprIsOfType e2 = (ExprIsOfType)expr2;

            if (e1.isNot() != e2.isNot()) {
                return false;
            }

            List<FieldDef> types1 = e1.getTargetTypes();
            List<FieldDef> types2 = e2.getTargetTypes();
            List<ExprType.Quantifier> quants1 = e1.getTargetQuantifiers();
            List<ExprType.Quantifier> quants2 = e2.getTargetQuantifiers();
            List<Boolean> onlyflags1 = e1.getOnlyTargetFlags();
            List<Boolean> onlyflags2 = e2.getOnlyTargetFlags();

            if (types1.size() != types2.size()) {
                return false;
            }

            for (int i = 0; i < types1.size(); ++i) {

                if (quants1.get(i) != quants2.get(i) ||
                    onlyflags1.get(i) != onlyflags2.get(i) ||
                    !types1.get(i).equals(types2.get(i))) {
                    return false;
                }
            }

            return matchChildren(e1, e2, vid);
        }
        case CAST: {
            ExprCast e1 = (ExprCast)expr1;
            ExprCast e2 = (ExprCast)expr2;

            if (e1.getTargetQuantifier() != e2.getTargetQuantifier() ||
                !e1.getTargetType().equals(e2.getTargetType())) {
                return false;
            }

            return matchChildren(e1, e2, vid);
        }
        case FIELD_STEP: {
            ExprFieldStep e1 = (ExprFieldStep)expr1;
            ExprFieldStep e2 = (ExprFieldStep)expr2;

            if (e1.isConst() != e2.isConst()) {
                return false;
            }

            if (e1.isConst()) {

                if (e1.getFieldPos() >= 0 && e2.getFieldPos() >= 0) {
                    if (e1.getFieldPos() != e2.getFieldPos()) {
                        return false;
                    }
                } else if (!e1.getFieldName().equals(e2.getFieldName())) {
                    return false;
                }
            }

            return matchChildren(e1, e2, vid);
        }
        case MAP_FILTER: {
            ExprMapFilter e1 = (ExprMapFilter)expr1;
            ExprMapFilter e2 = (ExprMapFilter)expr2;

            if (e1.isConst() != e2.isConst()) {
                return false;
            }

            if (e1.isConst()) {
                if (e1.getConstValue() != e2.getConstValue()) {
                    return false;
                }
            }

            return matchChildren(e1, e2, vid);
        }
        case ARRAY_FILTER: {
            ExprArrayFilter e1 = (ExprArrayFilter)expr1;
            ExprArrayFilter e2 = (ExprArrayFilter)expr2;

            if (e1.isConst() != e2.isConst()) {
                return false;
            }

            if (e1.isConst()) {
                if (!e1.getConstValue().equals(e2.getConstValue())) {
                    return false;
                }
            }

            return matchChildren(e1, e2, vid);
        }
        case ARRAY_SLICE: {
            ExprArraySlice e1 = (ExprArraySlice)expr1;
            ExprArraySlice e2 = (ExprArraySlice)expr2;

            if (e1.getLowValue() != null) {
                if (!e1.getLowValue().equals(e2.getLowValue())) {
                    return false;
                }
            } else if (e2.getLowValue() != null) {
                return false;
            }

            if (e1.getHighValue() != null) {
                if (!e1.getHighValue().equals(e2.getHighValue())) {
                    return false;
                }
            } else if (e2.getHighValue() != null) {
                return false;
            }

            return matchChildren(e1, e2, vid);
        }
        case SEQ_MAP: {
             return matchChildren(expr1, expr2, vid);
        }
        case CASE: {
            return matchChildren(expr1, expr2, vid);
        }
        case ARRAY_CONSTR: {
            return matchChildren(expr1, expr2, vid);
        }
        case MAP_CONSTR: {
            ExprMapConstr map1 = (ExprMapConstr)expr1;
            ExprMapConstr map2 = (ExprMapConstr)expr2;

            if (map1.theArgs.size() != map2.theArgs.size()) {
                return false;
            }

            int numArgs = map1.theArgs.size();
            boolean[] matched = new boolean[numArgs];

            for (int i = 0; i < numArgs; ++i) {

                int j = 0;
                for (; j < numArgs; ++j) {
                    if ((i % 2 != j % 2) || matched[j]) {
                        continue;
                    }
                    if (matchExprsInternal(map1.theArgs.get(i),
                                           map2.theArgs.get(j),
                                           vid)) {
                        matched[j] = true;
                        break;
                    }
                }

                if (j == numArgs) {
                    return false;
                }
            }

            return true;
        }
        case REC_CONSTR: {
            ExprRecConstr rec1 = (ExprRecConstr)expr1;
            ExprRecConstr rec2 = (ExprRecConstr)expr2;

            if (!rec1.getDef().equals(rec2.getDef())) {
                return false;
            }

            return matchChildren(expr1, expr2, vid);
        }
        case SFW: {
            return matchChildren(expr1, expr2, vid);
        }
        case RECEIVE:
        case UPDATE_FIELD:
        case UPDATE_ROW: {
            throw new QueryStateException(
                "Unexprected expression kind : " + expr1.getKind());
        }
        }

        throw new QueryStateException("Should not be here!!");
    }

    private static boolean matchChildren(Expr expr1, Expr expr2, int vid) {

        ExprIter children1 = expr1.getChildren();
        ExprIter children2 = expr2.getChildren();

        while (children1.hasNext()) {
            assert(children2.hasNext());
            Expr child1 = children1.next();
            Expr child2 = children2.next();

            if (!matchExprsInternal(child1, child2, vid)) {
                return false;
            }
        }

        return true;
    }

    /**
     * This method checks whether the given expr is a reference to a
     * a given primary key column of a given table. The primary key column
     * is specified by its ordinal number within the prim key (e.g. the 2nd
     * column of the primary key). Notice that the reference to the prim-key
     * column may be over the table row var or the index var (and its position
     * within the table rows is not the same as its position within the index
     * entries).
     */
    static boolean isPrimKeyColumnRef(
        TableImpl table,
        int pkCol,
        Expr expr) {

        if (expr.getKind() != ExprKind.FIELD_STEP) {
            return false;
        }

        ExprFieldStep stepExpr = (ExprFieldStep)expr;
        int fieldPos = stepExpr.getFieldPos();

        if (fieldPos < 0) {
            return false;
        }

        if (stepExpr.getInput().getKind() != ExprKind.VAR) {
            return false;
        }

        ExprVar var = (ExprVar)stepExpr.getInput();
        TableImpl table2 = var.getTable();
        IndexImpl index = var.getIndex();

        if (table2 == null || table2.getId() != table.getId()) {
            return false;
        }

        if (index == null) {
            int[] pkPositions = table.getPrimKeyPositions();
            return (fieldPos == pkPositions[pkCol]);
        }

        pkCol += index.numFields();
        return (fieldPos == pkCol);
    }
}

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
import java.util.List;
import java.util.Map;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TablePath.StepKind;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.ExprMapFilter.FilterKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.Index;

/**
 * A class that represents a query expr in a form that is used to match the
 * expr with the definition of an index field.
 *
 * An instance of IndexExpr is created only for query exprs that may be
 * matchable with an index field expr. Today this includes path exprs, FOR
 * variables whose domain expr is a path expr, and the CTX_ELEM and CTX_KEY
 * variables. The IndexExpr instance is referenced from the associated Expr
 * instance. For path exprs, an IndexExpr is created only for the last step.
 * However, such an instance will not be created if the path expr is for 
 * sure not matchable with any index path (for example, if it contains more 
 * than 1 multikey steps).
 *
 * Assuming a query path expr QP that does not contain any filtering/slicing
 * steps, a "match" is established between QP and an index path IP in the
 * following cases.
 *
 * 1. QP and IP match if their steps are identical.
 *
 * 2. [] steps that are not the last step of a path expr are noop steps. So,
 * QP and IP match if after removing any non-last [] steps, their remaining
 * steps are identical. For example if QP = a.b.c and IP = a.b[].c, QP and
 * IP match.
 *
 * 3. A "MapBoth" match. In this case, the index is a "MapBoth" index.
 * QP selects the value associated with a specific map key, and IP selects
 * all the values of the same map. For example QP = foo.map.someKey.bar and
 * IP = foo.map.values().bar. A MapBoth match results to two preds being
 * pushed down to the map index, as described in the header javadoc of the
 * IndexAnalyzer class.
 *
 * Handling slicing and filtering steps:
 *
 * QP may contain a slice or a filtering step that may project-out
 * elements from the input arrays/maps. Such a slice/filter step is
 * "partially" matched with a [] step in IP, at the same position. For
 * example, the path expr foo.array[1:5].f matches partially with index
 * path foo.array[].f. A partially matched pred is treated by leaving the
 * original pred in the query and pushing to the index a pred that is the
 * same as the original but with the expr(s) inside the [] removed.
 *
 *
 * theExpr:
 * 
 * theTable:
 *
 * theSteps:
 * A list of StepInfos, reflecting the steps of the path without any of the
 * conditions and/or the boundary expressions that may exist inside filtering
 * of slicing steps. This representation is the same as the one used by
 * IndexImpl.IndexField instances (see TablePath.steps), and as a result, it
 * is used for matching IndexExpr instances with IndexField instances.
 *
 * theFilteringPreds:
 * The condition and/or the boundary expressions that may exist inside
 * filtering or slicing steps of this path expr.
 *
 * theDoesSliciing:
 * Set to true if QP contains an array slicing step. In this case, the match
 * between QP and IP is partial, and as a result, the pred where QP appears
 * in must be retained.
 *
 * theIsJson:
 * Whether the path crosses into json data
 *
 * theIsDirect:
 * True if the path expr goes all the way down to the table, without 
 * crossing any FOR or CTX variables.
 *
 * theIsUnnested:
 * Whether the input to QP is a FROM var, whose domain expr is multi-valued,
 * in which case it is matched partially with IP. Such a pred does not apply
 * to a table directly, but to an unnested version of the table.
 *
 * theCtxVar:
 *
 * theCtxVarPos:
 *
 * theIsMultiKey:
 * Set to true when the expr is matched with a multikey index field. It is also
 * set to true during creation, if it is a path expr containing a mutlikey step
 * over input that is known to be array or map. However, before a match occurs,
 * the path expr may be multikey even if it does not contain a multikey step 
 * (this is the case when the path is over json data). Because theIsMultiKey may
 * not be accurate before a match occurs, during that period it is used only as
 * an optimization in matchToIndexPath().
 *
 * theIndexMatches:
 *
 * thePrimKeyPos:
 *
 * theCurrentMatch:
 *
 *
 * Data Members of IndexMatch
 * --------------------------
 *
 * theIndex:
 *
 * theFieldPos:
 * The ordinal number of the index path that matches with "this" query
 * path. -1 if no match actually exists.
 *
 * theMapBothKey:
 * If a "MapBoth" match is made between this path expr and an ipath, 
 * theMapBothKey is the specific map key that gets matched with the values()
 * step in the ipath.
 *
 * theRelativeCtxVarPos:
 *
 * theJsonDeclaredType:
 * If the query path matches with a type-constrained json index path,
 * theJsonDeclaredType is set to the type of that index path.
 */
class IndexExpr {

    static class StepInfo {

        String theName;

        StepKind theKind;

        Expr theExpr;

        StepInfo(String name, StepKind kind, Expr expr) {
            theName = name;
            theKind = kind;
            theExpr = expr;
        }
    }

    static class IndexMatch {

        TableImpl theTable;

        IndexImpl theIndex;

        int theFieldPos;

        String theMapBothKey;

        int theRelativeCtxVarPos;

        FieldDefImpl theJsonDeclaredType;

        IndexMatch(
            TableImpl table,
            IndexImpl index,
            int ipos,
            String mapKey,
            int relCtxVarPos,
            FieldDefImpl jsonDeclaredType) {
            theTable = table;
            theIndex = index;
            theFieldPos = ipos;
            theMapBothKey = mapKey;
            theRelativeCtxVarPos = relCtxVarPos;
            theJsonDeclaredType = jsonDeclaredType;
        }
    }

    final Expr theExpr;

    TableImpl theTable;

    final List<StepInfo> theSteps;

    List<Expr> theFilteringPreds;

    boolean theDoesSlicing;

    boolean theIsJson;

    boolean theIsDirect = true;

    boolean theIsUnnested;

    ExprVar theCtxVar;

    int theCtxVarPos = -1;

    private boolean theIsMultiKey;

    List<IndexMatch> theIndexMatches;

    int thePrimKeyPos = -1;

    private int theCurrentMatch = -1;

    IndexExpr(Expr expr) {
        theExpr = expr;
        theSteps = new ArrayList<StepInfo>();
    }

    int numSteps() {
        return theSteps.size();
    }

    StepKind getStepKind(int i) {
        return theSteps.get(i).theKind;
    }

    String getStepName(int i) {
        return theSteps.get(i).theName;
    }
    
    String getLastStepName() {
        return theSteps.get(theSteps.size() - 1).theName;
    }

    List<Expr> getFilteringPreds() {
        return theFilteringPreds;
    }

    boolean matchesIndex(TableImpl table, IndexImpl index) {

        theCurrentMatch = -1;

        if (theIndexMatches == null) {
            return false;
        }

        for (int i = 0; i < theIndexMatches.size(); ++i) {
            IndexMatch m = theIndexMatches.get(i);
            if (m.theTable.getId() == table.getId() && m.theIndex == index) {
                theCurrentMatch = i;
                return true;
            }
        }

        return false;
    }

    boolean matchesIndex(IndexImpl index, int ipos) {

        theCurrentMatch = -1;

        if (theIndexMatches == null) {
            return false;
        }

        for (int i = 0; i < theIndexMatches.size(); ++i) {
            IndexMatch m = theIndexMatches.get(i);
            if (m.theIndex == index && m.theFieldPos == ipos) {
                theCurrentMatch = i;
                return true;
            }
        }

        return false;
    }

    int getPathPos() {
        return theIndexMatches.get(theCurrentMatch).theFieldPos;
    }

    String getMapBothKey() {
        return theIndexMatches.get(theCurrentMatch).theMapBothKey;
    }

    /*
     * This method is called during IndexAnalyzer.apply(), in which case
     * theCurrentMatch may not be the match for the index being applied. So
     * we pass the index as a param and call matchesIndex again to find the
     * correct match.
     */
    String getMapBothKey(TableImpl table, IndexImpl index) {

        if (index == null) {
            return null;
        }

        if (matchesIndex(table, index)) {
            return theIndexMatches.get(theCurrentMatch).theMapBothKey;
        }

        throw new QueryStateException(
            "No match found for index " + index.getName());
    }

    int getRelativeCtxVarPos() {
        return theIndexMatches.get(theCurrentMatch).theRelativeCtxVarPos;
    }

    int getRelativeCtxVarPos(TableImpl table, IndexImpl index) {

        if (matchesIndex(table, index)) {
            return theIndexMatches.get(theCurrentMatch).theRelativeCtxVarPos;
        }

        return 0;
    }

    FieldDefImpl getJsonDeclaredType() {
        return theIndexMatches.get(theCurrentMatch).theJsonDeclaredType;
    }

    boolean isMultiKey() {
        IndexMatch m = theIndexMatches.get(theCurrentMatch);

        if (m.theIndex == null) {
            return false;
        }

        if (m.theFieldPos >= m.theIndex.numFields()) {
            // it's a prik-key column ref
            return false;
        }

        return m.theIndex.getIndexPath(m.theFieldPos).isMultiKey();
    }

    private void reverseSteps() {
        Collections.reverse(theSteps);
        if (theCtxVarPos >= 0) {
            theCtxVarPos = theSteps.size() - theCtxVarPos - 1;
        }
    }

    private void add(String name, StepKind kind, Expr expr) {
        theSteps.add(new StepInfo(name, kind, expr));
    }

    private void addFilteringPred(Expr cond) {

        if (cond == null || theCtxVarPos >= 0) {
            return;
        }

        if (theFilteringPreds == null) {
            theFilteringPreds = new ArrayList<Expr>();
        }

        Function andOp = cond.getFunction(FuncCode.OP_AND);
                    
        if (andOp != null) {
            theFilteringPreds.addAll(((ExprFuncCall)cond).getArgs());
        } else {
            theFilteringPreds.add(cond);
        }
    }

    private void addIndexMatch(
        IndexImpl index,
        int ipos,
        String mapKey,
        int relCtxVarPos,
        FieldDefImpl jsonDeclaredType) {

        if (theIndexMatches == null) {
            theIndexMatches = new ArrayList<IndexMatch>(8);
        }
        
        theIndexMatches.add(new IndexMatch(theTable,
                                           index,
                                           ipos,
                                           mapKey,
                                           relCtxVarPos,
                                           jsonDeclaredType));
        theCurrentMatch = theIndexMatches.size() - 1;
    }

    static IndexExpr create(Expr expr) {

        IndexExpr epath = new IndexExpr(expr);

        while (expr != null) {

            if (!epath.theIsJson &&
                (expr.getType().isAnyJson() ||
                 expr.getType().isAnyJsonAtomic())) {
                epath.theIsJson = true;
            }

            switch (expr.getKind()) {

            case FIELD_STEP: {
                ExprFieldStep stepExpr = (ExprFieldStep)expr;
                String fieldName = stepExpr.getFieldName();
                ExprType inType = stepExpr.getInput().getType();

                if (fieldName == null || inType.isAtomic()) {
                    return null;
                }

                if (inType.isArray()) {

                    if (epath.theIsMultiKey) {
                        return null;
                    }
                    epath.theIsMultiKey = true;

                    FieldDefImpl elemDef = 
                        ((ArrayDefImpl)inType.getDef()).getElement();

                    if (elemDef.isArray() || elemDef.isAtomic()) {
                        return null;
                    }

                    if (elemDef.isRecord()) {
                        epath.add(fieldName, StepKind.REC_FIELD, expr);
                    } else {
                        epath.add(fieldName, StepKind.MAP_FIELD, expr);
                    }

                } else if (inType.isRecord()) {
                    epath.add(fieldName, StepKind.REC_FIELD, expr);
                } else {
                    epath.add(fieldName, StepKind.MAP_FIELD, expr);
                }

                expr = expr.getInput();
                break;
            }
            case MAP_FILTER: {
                ExprMapFilter stepExpr = (ExprMapFilter)expr;
                ExprType inType = expr.getInput().getType();

                if (!inType.isMap() && !inType.isAnyJson()) {
                    return null;
                }

                if (inType.isMap()) {
                    if (epath.theIsMultiKey) {
                        return null;
                    }
                    epath.theIsMultiKey = true;
                }

                if (stepExpr.getFilterKind() == FilterKind.KEYS) {
                    epath.add(TableImpl.KEYS, StepKind.KEYS, expr);
                } else {
                    epath.add(TableImpl.VALUES, StepKind.VALUES, expr);
                }

                epath.addFilteringPred(stepExpr.getPredExpr());

                expr = expr.getInput();
                break;
            }
            case ARRAY_SLICE:
            case ARRAY_FILTER: {
                ExprType inType = expr.getInput().getType();

                if (inType.isArray()) {
                    if (epath.theIsMultiKey) {
                        return null;
                    }
                    epath.theIsMultiKey = true;
                }

                epath.add(TableImpl.BRACKETS, StepKind.BRACKETS, expr);

                if (expr.getKind() == ExprKind.ARRAY_SLICE) {
                    ExprArraySlice step = (ExprArraySlice)expr;
                    if (step.hasBounds()) {
                        epath.theDoesSlicing = true;
                    }
                } else {
                    ExprArrayFilter step = (ExprArrayFilter)expr;
                    Expr pred = step.getPredExpr();
                    if (pred != null) {
                        if (pred.getType().getDef().isBoolean()) {
                            epath.addFilteringPred(pred);
                        } else {
                            /*
                             * We conservatively assume that the pred expr may
                             * return numeric results, in which case it is
                             * actually a slicing step.
                             */
                            epath.theDoesSlicing = true;
                        }
                    }
                }

                expr = expr.getInput();
                break;
            }
            case VAR: {
                ExprVar varExpr = (ExprVar)expr;

                switch (varExpr.getVarKind()) {
                case FOR: {
                    expr = varExpr.getDomainExpr();

                    if (expr.getKind() != ExprKind.BASE_TABLE) {
                        epath.theIsDirect = false;
                        if (expr.isMultiValued()) {
                            epath.theIsUnnested = true;
                        }
                    } else {
                        ExprBaseTable tableExpr = (ExprBaseTable)expr;
                        epath.reverseSteps();
                        epath.theTable =
                            tableExpr.getTableForAlias(varExpr.getTableAlias());
                        expr = null; // terminate the while loop
                        break;
                    }

                    break;
                }
                case CTX_ELEM: {
                    expr = varExpr.getCtxExpr();
                    epath.theIsDirect = false;
                    epath.theCtxVar = varExpr;
                    epath.theCtxVarPos = epath.theSteps.size();

                    if (expr.getKind() == ExprKind.ARRAY_FILTER) {
                        epath.add(TableImpl.BRACKETS, StepKind.BRACKETS, expr);
                        expr = expr.getInput();
                        break;
                    } else if (expr.getKind() == ExprKind.MAP_FILTER) {
                        epath.add(TableImpl.VALUES, StepKind.VALUES, expr);
                        expr = expr.getInput();
                        break;
                    }

                    return null;
                }
                case CTX_KEY: {
                    expr = varExpr.getCtxExpr();
                    epath.theIsDirect = false;
                    epath.theCtxVar = varExpr;
                    epath.theCtxVarPos = epath.theSteps.size();

                    assert(expr.getKind() == ExprKind.MAP_FILTER);
                    epath.add(TableImpl.KEYS, StepKind.KEYS, expr);
                    expr = expr.getInput();
                    break;
                }
                default: {
                    return null;
                }
                }

                break;
            }
            case BASE_TABLE: {
                throw new QueryStateException(
                   "Reached base table expression for path " +
                   epath.getPathName());
            }
            default:
                return null;
            }
        }

        // Check whether it is a ref to a prim-key column
        int pkPos = -1;
        if (epath.numSteps() == 1) {

            pkPos = epath.theTable.findKeyComponent(epath.getLastStepName());

            if (pkPos >= 0) {
                epath.thePrimKeyPos = pkPos;
                epath.addIndexMatch(null, pkPos, null, 0, null);
            }
        }

        Map<String, Index> indexes = epath.theTable.getIndexes();
        
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {

            boolean foundMatch = false;
            IndexImpl index = (IndexImpl)entry.getValue();
            List<IndexField> indexPaths = index.getIndexFields();
            int numFields = indexPaths.size();

            for (IndexField ipath : indexPaths) {

                if (!epath.matchToIndexPath(index, ipath)) {
                    continue;
                }

                if (ipath.getDeclaredType() == null) {
                    foundMatch = true;
                    break;
                }

                Quantifier quant =
                    (ipath.isMultiKey() && epath.getMapBothKey() == null ?
                     Quantifier.STAR :
                     Quantifier.QSTN);

                ExprType t = TypeManager.createType(
                    TypeManager.ANY_JATOMIC_ONE(), quant);

                epath.theExpr.setType(t);
                foundMatch = true;
                break;
            }

            if (!foundMatch && pkPos >= 0) {
                epath.addIndexMatch(index, numFields + pkPos, null, 0, null);
                continue;
            }

        }

        return epath;
    }

    /*
     *
     */
    private boolean matchToIndexPath(IndexImpl index, IndexField ipath) {

        if (theIsMultiKey && !ipath.isMultiKey()) {
            return false;
        }

        IndexExpr epath = this;
        boolean mapBothIndex = index.isMapBothIndex();

        String mapKey = null;
        int inumSteps = ipath.numSteps();
        int enumSteps = numSteps();

        //System.out.println("Matching epath " + getPathName() +
        //                   "\nwith     ipath " + ipath);

        int relativeCtxVarPos = -2;
        int ii = 0;
        int ie = 0;
        for (; ii < inumSteps && ie < enumSteps;) {

            String istep = ipath.getStep(ii);
            String estep = epath.getStepName(ie);
            StepKind ikind = ipath.getStepKind(ii);
            StepKind ekind = epath.getStepKind(ie);

            if (ie == theCtxVarPos) {
                if (ii >= ipath.getMultiKeyStepPos()) {
                    relativeCtxVarPos = 1;
                } else {
                    relativeCtxVarPos = -1;
                }
            }

            boolean eq = (ikind == ekind &&
                          (ipath.isMapKeyStep(ii) ?
                           istep.equals(estep) :
                           istep.equalsIgnoreCase(estep)));

            if (eq) {
                ++ii;
                ++ie;
                continue;
            }

            if (ikind == StepKind.BRACKETS) {

                if (ii == inumSteps -1) {
                    return false;
                }

                ++ii;
                continue;
            }

            if (ekind == StepKind.BRACKETS) {

                if (ie == enumSteps - 1) {
                    return false;
                }

                ++ie;
                continue;
            }

            /*
             * We have a map-both index and a values() ipath, the matching
             * between epath and ipath has to consider the case where the
             * query pred is a MapBoth pred. i.e., we have to consider the
             * case where the epath is a.b.c.d and the ipath is a.b.values().d
             */
            if (ipath.isValuesStep(ii) && mapBothIndex) {
                mapKey = estep;
                ++ii;
                ++ie;
                continue;
            }

            return false;
        }

        if (ii == inumSteps && ie == enumSteps) {

            addIndexMatch(index,
                          ipath.getPosition(),
                          mapKey,
                          relativeCtxVarPos,
                          ipath.getDeclaredType());

            //System.out.println("Paths matched!!!");
            return true;
        }

        return false;
    }

    String getPathName() {

        StringBuilder sb = new StringBuilder();
            
        int numSteps = theSteps.size();
            
        for (int i = 0; i < numSteps; ++i) {

            String step = theSteps.get(i).theName;
                
            /* Delete the dot that was added after the previous step */
            if (TableImpl.BRACKETS.equals(step)) {
                sb.delete(sb.length() - 1, sb.length());
            }

            sb.append(step);

            if (i < numSteps - 1) {
                sb.append(TableImpl.SEPARATOR);
            }
        }
            
        return sb.toString();
    }
}

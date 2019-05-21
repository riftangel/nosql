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

import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;


/**
 * Base class for all kinds of expressions.
 *
 * theQCB:
 * The query control block.
 *
 * theSctx:
 * The static context within which this expression is to be evaluated.
 *
 * theKind:
 * The kind of the expression (see ExprKind enum).
 *
 * theParents:
 * The parent exprs of this expr (i.e., exprs that directly reference "this").
 * Currently, only var exprs may have more than one parent, but any expr may
 * get a 2nd parent temporariliy. This happens when a new expr is injected
 *  between a parent-child pair.
 *
 * theChildren:
 * An iterator over the child exprs of "this". Because iteration over the
 * children is a very common operation, each expr pre-allocates a children's
 * iterator (an instance of the ExprIter nested class) and assigns it to
 * theChildren. Then, the getChildren() method simply returns theChildren.
 * This works because we don't normally expect to have any nested loops over
 * the children of the same expr. For exceptions to this rule, a
 * getChildrenIter() method is also provided that allocates and returns a new
 * ExprIter.
 *
 * theType:
 * The type of the expression result. It is computed dynamically (see getType()
 * method), and may change as a result of optimizations.
 *
 * theIndexExpr :
 * see class IndexExpr.
 *
 * theVisitId
 * A "transient" field that serves as a tag on the expr when it is visited
 * during a traversal of the exprs graph. All exprs during the same traversal
 * are taged with the same visit id. So the visit id is used to detect and
 * break cycles during the traversal. Before a traversal starts, the visit
 * id to use for the traversal is computed by incrementing the theVisitCounter
 * static int.
 */
public abstract class Expr {

    /**
     * Enumeration of the different kinds of expressions
     */
    static enum ExprKind {
        CONST,        /* see class ExprConst */
        BASE_TABLE,   /* see class ExprBaseTable */
        FUNC_CALL,    /* see class ExprFuncCall */
        PROMOTE,      /* see class ExprPromote */
        REC_CONSTR,
        ARRAY_CONSTR, /* see class ExprArrayConstr */
        MAP_CONSTR,   /* see class ExprMapConstr */
        FIELD_STEP,   /* see class ExprFieldStep */
        MAP_FILTER,
        ARRAY_SLICE,   /* see class ExprArraySlice */
        ARRAY_FILTER,  /* see class ExprArrayFilter */
        VAR,           /* see class ExprVar */
        SFW,           /* see class ExprSFW */
        IS_OF_TYPE,   /* see class ExprIsOfType */
        CAST,         /* see class ExprCast */
        RECEIVE,
        CASE,
        UPDATE_ROW,
        UPDATE_FIELD,
        SEQ_MAP
    }

    public static enum UpdateKind {
        SET,
        ADD,
        PUT,
        REMOVE,
        TTL_HOURS,
        TTL_DAYS,
        TTL_TABLE
    }

    private static int FILTERING_PRED = 0x1;

    static int theVisitCounter = 0;

    final QueryControlBlock theQCB;

    final StaticContext theSctx;

    final ExprKind theKind;

    final ArrayList<Expr> theParents = new ArrayList<Expr>(1);

    private final ExprIter theChildren;

    Location theLocation;

    ExprType theType = null;

    int theFlags;

    IndexExpr theIndexExpr = null;

    boolean theIsIndexExprComputed;

    int theVisitId;

    public Expr(
        QueryControlBlock qcb,
        StaticContext sctx,
        ExprKind kind,
        Location location) {

        theQCB = qcb;
        theSctx = sctx;
        theKind = kind;
        theChildren = new ExprIter(this, false);
        theLocation = location;
    }

    public final QueryControlBlock getQCB() {
        return theQCB;
    }

    public final StaticContext getSctx() {
        return theSctx;
    }

    public final Location getLocation() {
        return theLocation;
    }

    final void setLocation(Location loc) {
        theLocation = loc;
    }

    final ExprKind getKind() {
        return theKind;
    }

    IndexExpr getIndexExpr() {
        if (!theIsIndexExprComputed) {
            theIsIndexExprComputed = true;
            theIndexExpr = IndexExpr.create(this);
        }
        return theIndexExpr;
    }

    final void setType(ExprType t) {
        theType = t;
    }

    final ExprType getTypeInternal() {
        return theType;
    }

    public final ExprType getType() {

        if (theType != null) {
            return theType;
        }

        computeType(false/*deep*/);
        return theType;
    }

    final boolean isScalar() {
        return getType().getQuantifier() == Quantifier.ONE;
    }

    final boolean isMultiValued() {
        Quantifier q = getType().getQuantifier();
        return (q == Quantifier.STAR || q == Quantifier.PLUS);
    }

    final boolean computeType(boolean deep) {

        boolean modified = false;

        if (deep) {
            ExprIter iter = new ExprIter(this);

            while (iter.hasNext()) {
                modified |= iter.next().computeType(deep);
            }
        }

        ExprType newType = computeType();

        if (!newType.equals(theType)) {
            modified = true;
            theType = newType;
        }

        return modified;
    }

    abstract ExprType computeType();

    /*
     * True if the exor may return SQL or JSON NULL
     */
    abstract boolean mayReturnNULL();

    /*
     * --------------------------------------------------------------------
     * Various methods that manipulate the graph of expressions (navigating
     * it, adding and removing exprs, etc).
     * -------------------------------------------------------------------
     */

    final boolean hasParents() {
        return !theParents.isEmpty();
    }

    final int getNumParents() {
        return theParents.size();
    }

    /**
     * Get the idx-th parent of this expr. "this" must have at least
     * idx+1 parents.
     */
    final Expr getParent(int idx) {
        return theParents.get(idx);
    }

    /**
     * Add a parent to this expr.
     */
    final void addParent(Expr parent) {

        assert(parent != null);
        //assert(theParents.isEmpty() || getKind() == ExprKind.VAR);
        //assert(!theParents.contains(parent));

        theParents.add(parent);
    }

    /**
     * Remove the given parent from the parents of this expr. The parent is
     * supposed to be indeed a parent of "this".
     *
     * If destroy is true the intent of the caller is to disconnect and
     * permanently remove "this" and its descendants from the expr graph. To
     * achieve this, the method calls itself recursivelly on the children of
     * "this". However, the recursion should stop if a visited node D has more
     * than one parents. In this case, a D parent may be a still-active node
     * (a node that is not a descendant of the original node on which this
     * method was called) that references D, and as a result, D and its own
     * descendants must remain in the Expr graph.
     *
     * destroy is false if the removal is supposed to be temporary, e.g. to
     * inject another expr between the parent and the child.
     *
     * This method should be called by a parent P on a child C when P wants to
     * remove or replace C. The method is "uni-directional": on return, C will
     * no longer have a ref to P, but P still has a ref to C. P should then
     * proceed to either completely remove his ref to C (removal) or set that
     * ref to another child (replacement). So, this is really a utility method
     * that does half the job of a complete removal or replacement.
     */
    final void removeParent(Expr parent, boolean destroy) {

        if (parent != null) {
            boolean found = theParents.remove(parent);
            assert(found);
        }

       if (destroy && !hasParents()) {
           Iterator<Expr> children = getChildren();
           while (children.hasNext()) {
               Expr child = children.next();
               child.removeParent(this, destroy);
           }
       }
    }

    /**
     * Remove the given child from the children of this expr, if the child
     * does indeed appear among the children of "this".
     *
     * If destroy is true, the child and its descendants should be destroyed
     * (i.e. made gc-able) if the child has no more parents.
     *
     * This method is bi-directional: the child is removed from the parent and
     * the parent is removed from the child.
     */
    final void removeChild(Expr child, boolean destroy) {

        ExprIter children = getChildren();

        while (children.hasNext()) {
            Expr currChild = children.next();
            if (currChild == child) {
                children.remove(destroy);
                children.reset();
                break;
            }
        }
    }

    /**
     * Replace the given child of this expr with another given child, if the
     * child does indeed appear among the children of "this".
     *
     * If destroy is true, the child and its descendants should be destroyed
     * (i.e. made gc-able) if the child has no more parents.
     *
     * This method is bi-directional: the child is removed from the parent and
     * the parent is removed from the child.
     */
    final void replaceChild(Expr child, Expr newChild, boolean destroy) {

        ExprIter children = getChildren();

        while (children.hasNext()) {
            Expr currChild = children.next();
            if (currChild == child) {
                children.replace(newChild, destroy);
                children.reset();
                break;
            }
        }
    }

    /**
     * Replace this expr in the exprs graph with another given expr.
     */
    final void replace(Expr other, boolean destroy) {

        if (this == theQCB.getRootExpr()) {
            theQCB.setRootExpr(other);
            assert(!hasParents());
            if (destroy) {
                Iterator<Expr> children = getChildren();
                while (children.hasNext()) {
                    Expr child = children.next();
                    child.removeParent(this, true /*destroy*/);
                }
            }
        } else {
            while (getNumParents() > 0) {
                assert(getParent(0) != other);
                getParent(0).replaceChild(this, other, destroy);
            }
        }
    }

    /**
     * Get the number of children of this expr.
     */
    abstract int getNumChildren();

    /**
     * Returns theChildren iterator.
     */
    public final ExprIter getChildren() {

        if (theChildren.isOpen()) {
            return new ExprIter(this);
        }

        theChildren.reset();
        return theChildren;
    }

    /**
     * Returns a new ExprIter over the children of "this"
     */
    public final ExprIter getChildrenIter() {
        ExprIter iter = new ExprIter(this);
        return iter;
    }

    /**
     * This method is redefined by exprs that have a single input.
     */
    Expr getInput() {
        throw new ClassCastException(
            "Expression does not have a single input: " + getClass());
    }

    /**
     * An Iterator<> class to iterate over the children of a given expr.
     *
     * The iterator also provides a non-destructive method to remove the
     * current child.
     *
     * The rest() method should be called every time we do an early-out from
     * an iteration loop. It is needed to make sure (in getChildren()) that
     * an expr does not ever try to reuse the pre-allocated ExprITer when
     * it's already in use (e.g., in a nested loop).
     */
    static class ExprIter implements Iterator<Expr> {

        Expr theExpr;

        int theNumChildren = 0;

        int theCurrentChild = -1;

        boolean theHasNext = false;

        ExprIter(Expr e) {
            this(e, true);
        }

        ExprIter(Expr e, boolean reset) {
            theExpr = e;
            if (reset) {
                reset();
            }
        }

        public void reset() {
            theNumChildren = theExpr.getNumChildren();
            theCurrentChild = -1;
            theHasNext = (theNumChildren > 0);
        }

        public boolean isOpen() {
            return theCurrentChild != -1 && theHasNext;
        }

        @Override
        public boolean hasNext() {

            if (theExpr.getKind() == ExprKind.BASE_TABLE) {
                ExprBaseTable tableExpr = (ExprBaseTable)theExpr;
                Expr pred = tableExpr.getTablePred(theCurrentChild + 1);
                while (pred == null && theCurrentChild + 2 < theNumChildren) {
                    ++theCurrentChild;
                    pred = tableExpr.getTablePred(theCurrentChild + 1);
                }

                if (theCurrentChild + 2 < theNumChildren) {
                    theHasNext = true;
                    return true;
                }

                theHasNext = false;
            }

            return theHasNext;
        }

        @Override
        public Expr next() {

            if (!theHasNext) {
                return null;
            }

            ++theCurrentChild;

            if (theCurrentChild == theNumChildren - 1) {
                theHasNext = false;
            }

            switch (theExpr.getKind()) {
            case UPDATE_ROW:
                return ((ExprUpdateRow)theExpr).getArg(theCurrentChild);

            case UPDATE_FIELD: {
                ExprUpdateField upd = (ExprUpdateField)theExpr;
                if (theCurrentChild == 0) {
                    return upd.getInput();
                }
                if (theCurrentChild == 1) {
                    if (upd.getPosExpr() != null) {
                        return upd.getPosExpr();
                    }
                    assert(upd.getNewValueExpr() != null);
                    return upd.getNewValueExpr();
                }
                assert(theCurrentChild == 2);
                assert(upd.getNewValueExpr() != null);
                return upd.getNewValueExpr();
            }
            case SFW:
                ExprSFW sfw = (ExprSFW)theExpr;

                assert(theNumChildren == sfw.computeNumChildren());

                int currentChild = theCurrentChild;

                if (currentChild < sfw.getNumFroms()) {
                    return sfw.getDomainExpr(currentChild);
                }

                currentChild -= sfw.getNumFroms();

                if (sfw.getWhereExpr() != null) {
                    if (currentChild == 0) {
                        return sfw.getWhereExpr();
                    }
                    --currentChild;
                }

                if (currentChild < sfw.getNumSortExprs()) {
                    return sfw.getSortExpr(currentChild);
                }

                currentChild -= sfw.getNumSortExprs();

                if (currentChild < sfw.getNumFields()) {
                    return sfw.getFieldExpr(currentChild);
                }

                currentChild -= sfw.getNumFields();

                if (sfw.getOffset() != null) {
                    if (currentChild == 0) {
                        return sfw.getOffset();
                    }
                    --currentChild;
                }

                assert(currentChild == 0);
                assert(sfw.getLimit() != null);
                assert(theHasNext == false);

                return sfw.getLimit();

            case FIELD_STEP: {
                ExprFieldStep step = (ExprFieldStep)theExpr;
                if (theCurrentChild == 0) {
                    return step.getInput();
                }
                assert(theCurrentChild == 1);
                assert(step.getFieldNameExpr() != null);
                return step.getFieldNameExpr();
            }
            case MAP_FILTER: {
                ExprMapFilter step = (ExprMapFilter)theExpr;
                if (theCurrentChild == 0) {
                    return step.getInput();
                }
                assert(theCurrentChild == 1);
                assert(step.getPredExpr() != null);
                return step.getPredExpr();
            }
            case ARRAY_FILTER: {
                ExprArrayFilter step = (ExprArrayFilter)theExpr;
                if (theCurrentChild == 0) {
                    return step.getInput();
                }
                assert(theCurrentChild == 1);
                assert(step.getPredExpr() != null);
                return step.getPredExpr();
            }
            case ARRAY_SLICE: {
                ExprArraySlice step = (ExprArraySlice)theExpr;
                if (theCurrentChild == 0) {
                    return step.getInput();
                }
                if (theCurrentChild == 1) {
                    if (step.getLowExpr() != null) {
                        return step.getLowExpr();
                    }
                    assert(step.getHighExpr() != null);
                    return step.getHighExpr();
                }
                assert(theCurrentChild == 2);
                assert(step.getHighExpr() != null);
                return step.getHighExpr();
            }
            case SEQ_MAP: {
                ExprSeqMap seqmap = (ExprSeqMap)theExpr;
                if (theCurrentChild == 0) {
                    return seqmap.getInput();
                }
                assert(theCurrentChild == 1);
                assert(seqmap.getMapExpr() != null);
                return seqmap.getMapExpr();
            }
            case CASE: {
                ExprCase caseExpr = (ExprCase)theExpr;
                return caseExpr.getExpr(theCurrentChild);
            }
            case FUNC_CALL:
                return ((ExprFuncCall)theExpr).getArg(theCurrentChild);

            case ARRAY_CONSTR:
                return ((ExprArrayConstr)theExpr).getArg(theCurrentChild);

           case MAP_CONSTR:
                return ((ExprMapConstr)theExpr).getArg(theCurrentChild);

           case REC_CONSTR:
                return ((ExprRecConstr)theExpr).getArg(theCurrentChild);

            case PROMOTE:
            case RECEIVE:
            case IS_OF_TYPE:
            case CAST:
                assert(theCurrentChild == 0);
                return theExpr.getInput();

            case BASE_TABLE:
                ExprBaseTable tableExpr = (ExprBaseTable)theExpr;
                Expr pred  = tableExpr.getTablePred(theCurrentChild);
                while (pred == null && theCurrentChild + 1 < theNumChildren) {
                    ++theCurrentChild;
                    pred = tableExpr.getTablePred(theCurrentChild);
                }

                if (theCurrentChild < theNumChildren) {
                    return tableExpr.getTablePred(theCurrentChild);
                }

                theHasNext = false;
                return null;

            case VAR:
            case CONST:
            default:
                throw new QueryStateException(
                    "Unexpected expression kind: " + theExpr.getKind());
            }
        }

        @Override
        public void remove() {
            remove(true);
        }

        /*
         * Removes the current child from theExpr. Not only is the child expr
         * detached from the parent, but the "slot" that stores the child ref
         * is also removed from the parent.
         *
         * In several cases, theExpr MUST have a non-null child at the current
         * position. For example, it doesn't make sense to have a SFW without
         * a FROM expr, or a field-step without an input. In such cases, the
         * method throws an exception.
         *
         * This method is bi-directional: the child is removed from the parent
         * and the parent is removed from the child.
         */
        void remove(boolean destroy) {

            switch (theExpr.getKind()) {
            case UPDATE_ROW: {
                throw new QueryStateException(
                        "Cannot remove input expr from update-row expr");
            }
            case UPDATE_FIELD: {
                ExprUpdateField upd = (ExprUpdateField)theExpr;
                if (theCurrentChild == 0) {
                    throw new QueryStateException(
                        "Cannot remove input expr from update-field expr");
                }
                if (theCurrentChild == 1) {
                    if (upd.getPosExpr() != null) {
                        throw new QueryStateException(
                            "Cannot remove position expr from update-field expr");
                    }
                    throw new QueryStateException(
                        "Cannot remove new value expr from update-field expr");
                }
                assert(theCurrentChild == 2);
                assert(upd.getNewValueExpr() != null);
                throw new QueryStateException(
                    "Cannot remove new value expr from update-field expr");
            }
            case SFW:
                ExprSFW sfw = (ExprSFW)theExpr;

                int currentChild = theCurrentChild;

                if (currentChild == 0) {
                    throw new QueryStateException(
                        "Cannot remove table from FROM clause");
                }

                if (currentChild < sfw.getNumFroms()) {
                    sfw.removeFromClause(currentChild, destroy);
                    --theNumChildren;
                    --theCurrentChild;
                    break;
                }

                currentChild -= sfw.getNumFroms();

                if (sfw.getWhereExpr() != null) {

                    if (currentChild == 0) {
                        sfw.removeWhereExpr(destroy);
                        --theNumChildren;
                        --theCurrentChild;
                        break;
                    }

                   --currentChild;
                }

                if (currentChild < sfw.getNumSortExprs()) {
                    sfw.removeSortExpr(currentChild, destroy);
                    --theNumChildren;
                    --theCurrentChild;
                    break;
                }

                currentChild -= sfw.getNumSortExprs();

                if (currentChild < sfw.getNumFields()) {
                    sfw.removeField(currentChild, destroy);
                    --theNumChildren;
                    --theCurrentChild;
                    break;
                }

                currentChild -= sfw.getNumFields();

                if (sfw.getOffset() != null) {

                    if (currentChild == 0) {
                        sfw.removeOffset(destroy);
                        --theNumChildren;
                        --theCurrentChild;
                        break;
                    }

                    --currentChild;
                }

                assert(currentChild == 0);

                if (sfw.getLimit() != null) {
                    sfw.removeLimit(destroy);
                    --theNumChildren;
                    --theCurrentChild;
                }

                break;

            case FIELD_STEP: {
                ExprFieldStep step = (ExprFieldStep)theExpr;

                if (theCurrentChild == 0) {
                    throw new QueryStateException(
                        "Cannot remove input expr from field step");
                }

                assert(theCurrentChild == 1);
                assert(step.getFieldNameExpr() != null);
                throw new QueryStateException(
                    "Cannot remove name expr from field step");
            }
            case MAP_FILTER: {
                ExprMapFilter step = (ExprMapFilter)theExpr;
                if (theCurrentChild == 0) {
                    throw new QueryStateException(
                        "Cannot remove input expr from map filter step");
                }

                assert(theCurrentChild == 1);
                assert(step.getPredExpr() != null);
                throw new QueryStateException(
                    "Cannot remove predicate expr from map filter step");
            }
            case ARRAY_FILTER: {
                ExprArrayFilter step = (ExprArrayFilter)theExpr;
                if (theCurrentChild == 0) {
                    throw new QueryStateException(
                        "Cannot remove input expr from array filter step");
                }

                assert(theCurrentChild == 1);
                assert(step.getPredExpr() != null);
                step.removePredExpr(destroy);
                --theNumChildren;
                --theCurrentChild;
                break;
            }
            case ARRAY_SLICE: {
                ExprArraySlice step = (ExprArraySlice)theExpr;

                if (theCurrentChild == 0) {
                    throw new QueryStateException(
                        "Cannot remove input expr from slice step");

                } else if (theCurrentChild == 1) {
                    if (step.getLowExpr() != null) {
                        step.removeLowExpr(destroy);
                        --theNumChildren;
                        --theCurrentChild;
                    } else {
                        assert(step.getHighExpr() != null);
                        step.removeHighExpr(destroy);
                        --theNumChildren;
                        --theCurrentChild;
                    }
                } else {
                    assert(theCurrentChild == 2);
                    assert(step.getHighExpr() != null);
                    step.removeHighExpr(destroy);
                    --theNumChildren;
                    --theCurrentChild;
                }
                break;
            }
            case SEQ_MAP: {
                if (theCurrentChild == 0) {
                    throw new QueryStateException(
                        "Cannot remove input expr from sequence map expression");
                }
                assert(theCurrentChild == 1);
                throw new QueryStateException(
                    "Cannot remove map expr from sequence map expression");
            }
            case CASE: {
                ExprCase caseExpr = (ExprCase)theExpr;
                if (theCurrentChild % 2 != 0) {
                    throw new QueryStateException(
                        "Cannot remove a THEN expr from a CASE expr");
                }
                if (theCurrentChild == caseExpr.getNumChildren() - 1) {
                    assert(caseExpr.hasElseClause());
                    caseExpr.removeElseClause(destroy);
                } else {
                    caseExpr.removeWhenClause(theCurrentChild / 2, destroy);
                }
                break;
            }
            case FUNC_CALL: {
                /*
                 * Removing an arg of a function call makes sense only if the
                 * function is variadic. The check is done in removeArg().
                 */
                ((ExprFuncCall)theExpr).removeArg(theCurrentChild, destroy);
                --theNumChildren;
               --theCurrentChild;
                break;
            }
            case ARRAY_CONSTR: {
                /*
                 * Removing an arg of an array ctor call makes sense only if the
                 * type of the arg is EMPTY. The check is done in removeArg().
                 */
               ((ExprArrayConstr)theExpr).removeArg(theCurrentChild, destroy);
               --theNumChildren;
               --theCurrentChild;
                break;
            }
            case MAP_CONSTR: {
                /*
                 * Removing an arg of an array ctor call makes sense only if the
                 * type of the arg is EMPTY. The check is done in removeArg().
                 */
               ((ExprMapConstr)theExpr).removeArg(theCurrentChild, destroy);
               --theNumChildren;
               --theCurrentChild;
                break;
            }
            case REC_CONSTR: {
                throw new QueryStateException(
                    "Cannot remove input expr from record constructor expr");
            }
            case PROMOTE: {
                assert(theCurrentChild == 0);
                throw new QueryStateException(
                    "Cannot remove input expr from promote expr");
            }
            case IS_OF_TYPE: {
                assert(theCurrentChild == 0);
                throw new QueryStateException(
                    "Cannot remove input expr from is_of_type expr");
            }
            case CAST: {
                assert(theCurrentChild == 0);
                throw new QueryStateException(
                    "Cannot remove input expr from cast expr");
            }
            case RECEIVE: {
                assert(theCurrentChild == 0);
                throw new QueryStateException(
                        "Cannot remove input expr from receive expr");
            }
            case BASE_TABLE: {
                ExprBaseTable tableExpr = (ExprBaseTable)theExpr;
                Expr pred  = tableExpr.getTablePred(theCurrentChild);
                while (pred == null && theCurrentChild < theNumChildren - 1) {
                    ++theCurrentChild;
                    pred = tableExpr.getTablePred(theCurrentChild);
                }
                tableExpr.removeTablePred(theCurrentChild, destroy);
                --theNumChildren;
                --theCurrentChild;
                break;
            }
            case VAR:
            case CONST:
            default:
                throw new QueryStateException(
                    "Unexpected expression kind: " + theExpr.getKind());
            }

            if (theCurrentChild >= theNumChildren) {
                theHasNext = false;
            }
        }

        void replace(Expr newChild, boolean destroy) {

            switch (theExpr.getKind()) {
            case UPDATE_ROW: {
                if (theCurrentChild == 0) {
                    ((ExprUpdateRow)theExpr).setArg(0, newChild, destroy);
                    return;
                }
                throw new QueryStateException(
                        "Cannot replace update clause of update-row expr");
            }
            case UPDATE_FIELD: {
                ExprUpdateField upd = (ExprUpdateField)theExpr;
                if (theCurrentChild == 0) {
                    throw new QueryStateException(
                        "Cannot replace input expr of update-field expr");
                }
                if (theCurrentChild == 1) {
                    if (upd.getPosExpr() != null) {
                        upd.setPosExpr(newChild, destroy);
                        break;
                    }
                    upd.setNewValueExpr(newChild, destroy);
                    break;
                }
                assert(theCurrentChild == 2);
                upd.setNewValueExpr(newChild, destroy);
                break;
            }
            case SFW:
                ExprSFW sfw = (ExprSFW)theExpr;

                int currentChild = theCurrentChild;

                if (currentChild < sfw.getNumFroms()) {
                    sfw.setDomainExpr(currentChild, newChild, destroy);
                    break;
                }

                currentChild -= sfw.getNumFroms();

                if (sfw.getWhereExpr() != null) {

                    if (currentChild == 0) {
                        sfw.setWhereExpr(newChild, destroy);
                        break;
                    }

                    --currentChild;
                }

                if (currentChild < sfw.getNumSortExprs()) {
                    sfw.setSortExpr(currentChild, newChild, destroy);
                    break;
                }

                currentChild -= sfw.getNumSortExprs();

                if (currentChild < sfw.getNumFields()) {
                    sfw.setFieldExpr(currentChild, newChild, destroy);
                    break;
                }

                currentChild -= sfw.getNumFields();

                if (sfw.getOffset() != null) {
                    if (currentChild == 0) {
                        sfw.setOffset(newChild, destroy);
                        break;
                    }
                    --currentChild;
                }

                assert(currentChild == 0);

                if (sfw.getLimit() != null) {
                    sfw.setLimit(newChild, destroy);
                }

                break;

            case FIELD_STEP: {
                ExprFieldStep step = (ExprFieldStep)theExpr;
                if (theCurrentChild == 0) {
                    step.setInput(newChild, destroy);
                }

                assert(theCurrentChild == 1);
                assert(step.getFieldNameExpr() != null);
                step.setFieldNameExpr(newChild, destroy);

                break;
            }
            case MAP_FILTER: {
                ExprMapFilter step = (ExprMapFilter)theExpr;
                if (theCurrentChild == 0) {
                    step.setInput(newChild, destroy);
                }

                assert(theCurrentChild == 1);
                assert(step.getPredExpr() != null);
                step.setPredExpr(newChild, destroy);

                break;
            }
            case ARRAY_FILTER: {
                ExprArrayFilter step = (ExprArrayFilter)theExpr;
                if (theCurrentChild == 0) {
                    step.setInput(newChild, destroy);
                }

                assert(theCurrentChild == 1);
                assert(step.getPredExpr() != null);
                step.setPredExpr(newChild, destroy);

                break;
            }
            case ARRAY_SLICE: {
                ExprArraySlice step = (ExprArraySlice)theExpr;
                if (theCurrentChild == 0) {
                    step.setInput(newChild, destroy);
                } else if (theCurrentChild == 1) {
                    if (step.getLowExpr() != null) {
                        step.setLowExpr(newChild, destroy);
                    } else {
                        assert(step.getHighExpr() != null);
                        step.setHighExpr(newChild, destroy);
                    }
                } else {
                    assert(theCurrentChild == 2);
                    assert(step.getHighExpr() != null);
                    step.setHighExpr(newChild, destroy);
                }
                break;
            }
            case SEQ_MAP: {
                ExprSeqMap seqmap = (ExprSeqMap)theExpr;
                if (theCurrentChild == 0) {
                    seqmap.setInput(newChild, destroy);
                }
                assert(theCurrentChild == 1);
                seqmap.setMapExpr(newChild, destroy);
                break;
            }
            case CASE: {
                ExprCase caseExpr = (ExprCase)theExpr;
                caseExpr.setExpr(theCurrentChild, newChild, destroy);
                break;
            }
            case FUNC_CALL: {
                ((ExprFuncCall)theExpr).setArg(
                    theCurrentChild, newChild, destroy);
                break;
            }
            case ARRAY_CONSTR: {
               ((ExprArrayConstr)theExpr).setArg(
                   theCurrentChild, newChild, destroy);
                break;
            }
            case MAP_CONSTR: {
               ((ExprMapConstr)theExpr).setArg(
                   theCurrentChild, newChild, destroy);
                break;
            }
            case REC_CONSTR: {
               ((ExprRecConstr)theExpr).setArg(
                   theCurrentChild, newChild, destroy);
                break;
            }
            case PROMOTE: {
                assert(theCurrentChild == 0);
                ((ExprPromote)theExpr).setInput(newChild, destroy);
                break;
            }
            case IS_OF_TYPE: {
                assert(theCurrentChild == 0);
                ((ExprIsOfType)theExpr).setInput(newChild, destroy);
                break;
            }
            case CAST: {
                assert(theCurrentChild == 0);
                ((ExprCast)theExpr).setInput(newChild, destroy);
                break;
            }
            case RECEIVE: {
                assert(theCurrentChild == 0);
                ((ExprReceive)theExpr).setInput(newChild, destroy);
                break;
            }
            case BASE_TABLE: {
                ExprBaseTable tableExpr = (ExprBaseTable)theExpr;
                Expr pred  = tableExpr.getTablePred(theCurrentChild);
                while (pred == null && theCurrentChild < theNumChildren - 1) {
                    ++theCurrentChild;
                    pred = tableExpr.getTablePred(theCurrentChild);
                }
                tableExpr.setTablePred(theCurrentChild, newChild, destroy);
                break;
            }
            case VAR:
            case CONST:
            default:
                throw new QueryStateException(
                    "Unexpected expression kind: " + theExpr.getKind());
            }
        }
    }

    /*
     * -------------------------------------------------
     * Various utility methods used during optimizations
     * -------------------------------------------------
     */

    void setFilteringPredFlag() {
        theFlags |= FILTERING_PRED;
    }

    void clearFilteringPredFlag() {
        theFlags &= ~FILTERING_PRED;
    }

    boolean getFilteringPredFlag() {
        return (theFlags & FILTERING_PRED) != 0;
    }

    /*
     * If this expr is a call of the specified function, return the underlying
     * Function obj. Otherwise return null. A null value for fcode is
     * interpreted as "any function".
     */
    final Function getFunction(FuncCode fcode) {

        if (getKind() != ExprKind.FUNC_CALL) {
            return null;
        }

        Function func = ((ExprFuncCall)this).getFunction();

        if (fcode == null || fcode.equals(func.getCode())) {
            return func;
        }

        return null;
    }

    final boolean isStepExpr() {

        if (theKind == ExprKind.ARRAY_FILTER ||
            theKind == ExprKind.ARRAY_SLICE ||
            theKind == ExprKind.MAP_FILTER ||
            theKind == ExprKind.FIELD_STEP) {
            return true;
        }

        return false;
    }

    boolean isConstant() {

        if (getKind() == ExprKind.CONST) {
            return true;
        }

        if (getKind() == ExprKind.VAR) {

            if (((ExprVar)this).isExternal()) {
                return true;
            }

            return false;
        }

        if (getKind() == ExprKind.BASE_TABLE) {
            return false;
        }

        ExprIter children = getChildren();

        while (children.hasNext()) {
            Expr child = children.next();
            if (!child.isConstant()) {
                children.reset();
                return false;
            }
        }

        return true;
    }

    public final String display() {
        StringBuilder sb = new StringBuilder();
        display(sb, new QueryFormatter());
        return sb.toString();
    }

    void display(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append(theKind).append("\n");

        formatter.indent(sb);
        sb.append("[\n");

        formatter.incIndent();
        displayContent(sb, formatter);
        formatter.decIndent();
        sb.append("\n");

        formatter.indent(sb);
        sb.append("]");
    }

    abstract void displayContent(StringBuilder sb, QueryFormatter formatter);
}

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

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Constructs an array containing the values returned by the input exprs.
 * Initially an empty array is created. Then each input expr is computed
 * and its result is appended in the array. The input exprs are computed
 * in the order they appear in the query.
 *
 * The element type of the array to be constructed during runtime is determined
 * during compilation time. Initially, this is done based only on the types of
 * the input exprs to this ExprArrayConstr. However, just before codegen,
 * theMustBeJson will be set to true if the array is going to be inserted into
 * another constructed array or map whose element type is JSON (see 
 * ExprUtils.adjustConstructorTypes()). In this case, the element type of the
 * constructed array will also be JSON. We do this to guarantee that strongly
 * type data does not get inserted into JSON data.
 *
 * If there are no input exprs, the type is ARRAY(JSON).
 *
 * theIsConditional:
 * A "conditional" array constructor is created to wrap an expr that appears
 * in the SELECT list and which may return more than one item. Such an array
 * constructor will actually create an array only if the input expr does 
 * actually return more than one item during runtime. Otherwise, it will just
 * return the single input item or the empty set, if the input expr does not
 * produce any items.
 */
public class ExprArrayConstr extends Expr {

    private final ArrayList<Expr> theArgs;

    private boolean theMustBeJson;

    private final boolean theIsConditional;

    ExprArrayConstr(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        ArrayList<Expr> args,
        boolean conditional) {

        super(qcb, sctx, ExprKind.ARRAY_CONSTR, location);
        theArgs = args;

        for (Expr arg : args) {
            arg.addParent(this);
        }

        theIsConditional = conditional;

        /*
         * Must compute the type here to guarantee that 
         * theQCB.theHaveJsonConstructors will be set, if necessary, BEFORE
         * ExprUtils.adjustConstructorTypes() is called.
         */
        theType = computeType();
    }

    @Override
    int getNumChildren() {
        return theArgs.size();
    }

    public boolean isConditional() {
        return theIsConditional;
    }

    int getNumArgs() {
        return theArgs.size();
    }

    Expr getArg(int i) {
        return theArgs.get(i);
    }

    void setArg(int i, Expr newExpr, boolean destroy) {
        theArgs.get(i).removeParent(this, destroy);
        theArgs.set(i, newExpr);
        newExpr.addParent(this);
    }

    void removeArg(int i, boolean destroy) {
        if (!theArgs.get(i).getType().isEmpty()) {
            throw new QueryStateException(
                "Cannot remove non-empty input expr from array " +
                "constructor expr");
        }
        theArgs.get(i).removeParent(this, destroy);
        theArgs.remove(i);
    }

    void setJsonArrayType() {
        theMustBeJson = true;
    }

    public ArrayDefImpl getArrayType() {

        if (!theIsConditional) {
            return (ArrayDefImpl)getType().getDef();
        }

        FieldDefImpl elemDef = (theMustBeJson ? 
                                FieldDefImpl.jsonDef :
                                theArgs.get(0).getType().getDef());
        return FieldDefFactory.createArrayDef(elemDef);
    }

    @Override
    ExprType computeType() {

        int numArgs = theArgs.size();

        if (numArgs == 0) {
            return TypeManager.createArrayType(TypeManager.JSON_ONE(),
                                               Quantifier.ONE); 
        }

        ExprType elemType = theArgs.get(0).getType();

        if (theIsConditional) {

            assert(numArgs == 1);

            if (elemType.isSubType(TypeManager.JSON_STAR())) {
                return TypeManager.JSON_QSTN();
            }

            return TypeManager.ANY_QSTN();
        }

        for (int i = 1; i < numArgs; ++i) {
            ExprType type = theArgs.get(i).getType();
            elemType = TypeManager.getConcatType(elemType, type);

            if (elemType.isAny()) {
                break;
            }
        }

        if (theMustBeJson &&
            elemType.getDef().isSubtype(FieldDefImpl.jsonDef)) {
            elemType = TypeManager.JSON_ONE();
        }

        if (elemType.isAnyJson()) {
            theQCB.theHaveJsonConstructors = true;
        }

        return TypeManager.createArrayType(elemType, Quantifier.ONE);
    }

    @Override
    public boolean mayReturnNULL() {

        if (theIsConditional) {
            return theArgs.get(0).mayReturnNULL();
        }

        return false;
    }

    @Override
    void displayContent(StringBuilder sb, QueryFormatter formatter) {
        for (int i = 0; i < theArgs.size(); ++i) {
            theArgs.get(i).display(sb, formatter);
            if (i < theArgs.size() - 1) {
                sb.append(",\n");
            }
        }
    }
}

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


/*
 * ExprFuncCall represents a function call to a first-order function, i.e., a
 * function that does not take as input another function and does not return
 * another function as a result.
 */
class ExprFuncCall extends Expr {

    private final ArrayList<Expr> theArgs;

    private final Function theFunction;

    static Expr create(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        Function func) {

        ArrayList<Expr> args = new ArrayList<Expr>(0);
        return create(qcb, sctx, loc, func, args);
    }

    static Expr create(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        Function func,
        Expr arg) {

        ArrayList<Expr> args = new ArrayList<Expr>(1);
        args.add(arg);
        return create(qcb, sctx, loc, func, args);
    }

    static Expr create(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        FuncCode fcode,
        Expr arg) {

        ArrayList<Expr> args = new ArrayList<Expr>(1);
        args.add(arg);
        Function func = CompilerAPI.theFunctionLib.getFunc(fcode);
        return create(qcb, sctx, loc, func, args);
    }

    static Expr create(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        FuncCode fcode,
        Expr arg1,
        Expr arg2) {

        ArrayList<Expr> args = new ArrayList<Expr>(2);
        args.add(arg1);
        args.add(arg2);
        Function func = CompilerAPI.theFunctionLib.getFunc(fcode);
        return create(qcb, sctx, loc, func, args);
    }


    static Expr create(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        FuncCode fcode,
        ArrayList<Expr> args) {

        Function func = CompilerAPI.theFunctionLib.getFunc(fcode);
        return create(qcb, sctx, loc, func, args);
    }

    static Expr create(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        Function func,
        ArrayList<Expr> args) {

        int numArgs = args.size();

        /* Wrap the args in promote exprs, if needed */
        for (int i = 0; i < numArgs; ++i) {
            Expr arg = args.get(i);
            ExprType paramType = func.getParamType(i);
            arg = ExprPromote.create(null, arg, paramType);
            args.set(i, arg);
        }

        ExprFuncCall fncall = new ExprFuncCall(qcb, sctx, loc, func, args);

        /*
         * Do function-specific normalization, if any applies. 
         * Based on the args, normalizeCall() may actually create and return a
         * call to a different function, or even an expr that is not a function
         * call. 
         */
        Expr res = func.normalizeCall(fncall);

        if (res != fncall) {
            fncall.removeParent(null, true/*destroy*/);
        }

        return res;
    }

    ExprFuncCall(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Function f,
        ArrayList<Expr> args) {

        super(qcb, sctx, ExprKind.FUNC_CALL, location);
        theFunction = f;
        theArgs = args;

        for (Expr arg : args) {
            arg.addParent(this);
        }
    }

    ExprFuncCall(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        FuncCode f,
        ArrayList<Expr> args) {

        this(qcb, sctx, location, CompilerAPI.theFunctionLib.getFunc(f), args);
    }

    @Override
    int getNumChildren() {
        return theArgs.size();
    }

    int getNumArgs() {
        return theArgs.size();
    }

    ArrayList<Expr> getArgs() {
        return theArgs;
    }

    @Override
    Expr getInput() {
        if (getNumArgs() != 1) {
            throw new ClassCastException(
                "Expression does not have a single input: " + getClass());
        }
        return getArg(0);
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
        if (!theFunction.isVariadic()) {
            throw new QueryStateException(
                "Cannot remove argument from a non-variadic function");
        }
        theArgs.get(i).removeParent(this, destroy);
        theArgs.remove(i);
    }

    void setArgInternal(int i, Expr arg) {
        theArgs.set(i, arg);
    }

    Function getFunction() {
        return theFunction;
    }

    @Override
    ExprType computeType() {
        return theFunction.getRetType(this);
    }

    @Override
    public boolean mayReturnNULL() {
        return theFunction.mayReturnNULL(this);
    }

    @Override
    void display(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append(theFunction.getName());

        sb.append("\n");
        formatter.indent(sb);
        sb.append("[\n");
        formatter.incIndent();
        displayContent(sb, formatter);
        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("]");
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

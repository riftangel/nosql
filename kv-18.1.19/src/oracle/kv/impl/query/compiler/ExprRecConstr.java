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

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;


/**
 * Internal expr to construct a record.
 */
public class ExprRecConstr extends Expr {

    final RecordDefImpl theDef;

    final ArrayList<Expr> theArgs;

    ExprRecConstr(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        RecordDefImpl def,
        ArrayList<Expr> args) {

        super(qcb, sctx, ExprKind.REC_CONSTR, location);
        theDef = def;
        theArgs = args;
    }

    @Override
    int getNumChildren() {
        return theArgs.size();
    }

    public RecordDefImpl getDef() {
        return theDef;
    }

    int getNumArgs() {
        return theArgs.size();
    }

    Expr getArg(int i) {
        return theArgs.get(i);
    }

    void setArg(int i, Expr newExpr, boolean destroy) {

        theArgs.get(i).removeParent(this, destroy);

        FieldDefImpl argType = newExpr.getType().getDef();

        if (!argType.isSubtype(theDef)) {
            throw new QueryException(
                "Type:\n" + argType.getDDLString() + "\nis not a subtype of\n" +
                theDef.getDDLString(), theLocation);
        }

        theArgs.set(i, newExpr);
        newExpr.addParent(this);
    }

    @Override
    public boolean mayReturnNULL() {
        return false;
    }

    @Override
    ExprType computeType() {
        return TypeManager.createType(theDef, Quantifier.ONE);
    }

    @Override
    void displayContent(StringBuilder sb, QueryFormatter formatter) {

        sb.append("type = \n");
        theDef.display(sb, formatter);

        for (int i = 0; i < theArgs.size(); ++i) {
            theArgs.get(i).display(sb, formatter);
            if (i < theArgs.size() - 1) {
                sb.append(",\n");
            }
        }
    }
}

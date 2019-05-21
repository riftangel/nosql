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

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;


/**
 * Represents a literal in the query text.
 *
 * For now, literals can be strings and numbers (long or double).
 */
class ExprConst extends Expr {

    private FieldValueImpl theValue;

    ExprConst(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        FieldValueImpl value) {

        super(qcb, sctx, ExprKind.CONST, location);
        setValue(value);
    }

    ExprConst(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        boolean value) {

        super(qcb, sctx, ExprKind.CONST, location);
        setValue(FieldDefImpl.booleanDef.createBoolean(value));
    }

    @Override
    int getNumChildren() {
        return 0;
    }

    FieldValueImpl getValue() {
        return theValue;
    }

    void setValue(FieldValueImpl val) {
        theValue = val;
        setType(TypeManager.createValueType(val));
    }

    @Override
    ExprType computeType() {
        return getTypeInternal();
    }

    @Override
    public boolean mayReturnNULL() {
        return theValue.isNull();
    }

    @Override
    void display(StringBuilder sb, QueryFormatter formatter) {
        formatter.indent(sb);
        sb.append(theKind);
        sb.append("[");
        displayContent(sb, formatter);
        sb.append("]");
    }

    @Override
    void displayContent(StringBuilder sb, QueryFormatter formatter) {
        theValue.toStringBuilder(sb);
    }
}

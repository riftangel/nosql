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

package oracle.kv.impl.query.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.table.FieldDef;


/**
 * Iterator to implement the arithmetic unary negate operator '-'.
 *
 * any_atomic? ArithOp(any_atomic?)
 *
 * ArithUnaryOpIter.next() must check that the input is a numeric value.
 *
 * Result:
 *     if arg is int result is an int
 *     if arg is long result is a long
 *     if arg is float result is a float
 *     if arg is double result is a double
 */
public class ArithUnaryOpIter extends PlanIter {

    private final FunctionLib.FuncCode theCode;
    private final PlanIter theArg;

    public ArithUnaryOpIter(
        Expr e,
        int resultReg,
        FunctionLib.FuncCode code,
        PlanIter argIter) {

        super(e, resultReg);
        theCode = code;

        /* ArithUnaryOpIter works only with
           FunctionLib.FuncCode.OP_ARITH_UNARY */
        assert (theCode == FunctionLib.FuncCode.OP_ARITH_UNARY );

        theArg = argIter;
    }

    /**
     * FastExternalizable constructor.
     */
    public ArithUnaryOpIter(DataInput in, short serialVersion)
            throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FunctionLib.FuncCode.values().length);
        theCode = FunctionLib.FuncCode.values()[ordinal];
        theArg = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theCode.ordinal());
        serializeIter(theArg, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARITH_UNARY_OP;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theArg.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        PlanIter argIter = theArg;
        boolean opNext = argIter.next(rcb);

        if (!opNext) {
            state.done();
            return false;
        }

        FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());
        assert (argValue != null);

        FieldValueImpl res;

        if (argValue.isNull()) {
            res = NullValueImpl.getInstance();
        } else {
            res = getNegativeOfValue(argValue, argIter.getLocation());
        }

        rcb.setRegVal(theResultReg, res);
        state.done();
        return true;
    }

    public static FieldValueImpl getNegativeOfValue(
        FieldValueImpl argValue,
        Location location) {

        FieldDef.Type argType = argValue.getType();
        FieldValueImpl res;

        switch (argType) {
        case INTEGER:
            int iRes = -argValue.getInt();
            res = FieldDefImpl.integerDef.createInteger(iRes);
            break;
        case LONG:
            long lRes = -argValue.getLong();
            res = FieldDefImpl.longDef.createLong(lRes);
            break;
        case FLOAT:
            float fRes = -argValue.getFloat();
            res = FieldDefImpl.floatDef.createFloat(fRes);
            break;
        case DOUBLE:
            double dRes = -argValue.getDouble();
            res = FieldDefImpl.doubleDef.createDouble(dRes);
            break;
        case NUMBER:
            BigDecimal nRes = argValue.getDecimal().negate();
            res = FieldDefImpl.numberDef.createNumber(nRes);
            break;
        default:
            throw new QueryException(
                "Operand in unary arithmetic operation has illegal type\n" +
                "Operand type : " + argValue.getDefinition().getDDLString(),
                location);
        }

        return res;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theArg.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theArg.close(rcb);

        state.close();
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        formatter.indent(sb);
        sb.append('-');
        sb.append(",\n");
        theArg.display(sb, formatter);
    }
}

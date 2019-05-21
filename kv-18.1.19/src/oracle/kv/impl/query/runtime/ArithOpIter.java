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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_5;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;

/**
 * Iterator to implement the arithmetic operators
 *
 * any_atomic? ArithOp(any?, ....)
 *
 * ArithOpIter.next() must check that all inputs are numeric values.
 *
 * Result:
 *     if all args are int result is an int
 *     if all args are int or long result is a long
 *     if all args are int, long or float result is a float
 *     if all args are int, long, float or double result is a double
 */
public class ArithOpIter extends PlanIter {

    private final FuncCode theCode;

    private final PlanIter[] theArgs;

    /**
     * For theCode == FuncCode.OP_ADD_SUB
     *   contains order of + or - ops
     * For theCode == FuncCode.OP_MULT_DIV
     *   contains order of * or / ops
     */
    private final String theOps;

    private final transient int theInitResult;

    public ArithOpIter(
        Expr e,
        int resultReg,
        FunctionLib.FuncCode code,
        PlanIter[] argIters,
        String ops) {

        super(e, resultReg);
        theCode = code;

        /* ArithOpIter works only with FunctionLib.FuncCode.OP_ADD_SUB and
         FunctionLib.FuncCode.OP_MULT_DIV
         It must have at least 2 args */
        assert ((theCode == FunctionLib.FuncCode.OP_ADD_SUB ||
                theCode == FunctionLib.FuncCode.OP_MULT_DIV) &&
            argIters.length >= 2 );

        theArgs = argIters;
        theOps = ops;

        /*
         * Don't initialize nRes because BigDecimal is expensive. Do it on
         * demand.
         */
        switch (theCode) {
        case OP_ADD_SUB:
            theInitResult = 0;
            break;
        case OP_MULT_DIV:
            theInitResult = 1;
            break;
        default:
            throw new QueryStateException(
                "Invalid operation code: " + theCode);
        }
    }

    /**
     * FastExternalizable constructor.
     */
    public ArithOpIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FunctionLib.FuncCode.values().length);
        theCode = FunctionLib.FuncCode.valueOf(ordinal);
        theArgs = deserializeIters(in, serialVersion);
        theOps = (serialVersion >= QUERY_VERSION_5) ?
            readString(in, serialVersion) :
            in.readUTF();

        switch (theCode) {
        case OP_ADD_SUB:
            theInitResult = 0;
            break;
        case OP_MULT_DIV:
            theInitResult = 1;
            break;
        default:
            throw new QueryStateException(
                "Invalid operation code: " + theCode);
        }
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
        serializeIters(theArgs, out, serialVersion);
        if (serialVersion >= QUERY_VERSION_5) {
            writeString(out, serialVersion, theOps);
        } else {
            out.writeUTF(theOps);
        }
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARITH_OP;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        for (PlanIter argIter : theArgs) {
            argIter.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        /*
         * Determine the type of the result for the expression by iterating
         * its components, enforcing the promotion rules for numeric types.
         *
         * Start with INTEGER.
         */
        FieldDef.Type resultType = Type.INTEGER;

        for (int i = 0; i < theArgs.length; i++) {

            PlanIter argIter = theArgs[i];
            boolean opNext = argIter.next(rcb);

            if (!opNext) {
                state.done();
                return false;
            }

            FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());

            if (argValue.isNull()) {
                FieldValueImpl res = NullValueImpl.getInstance();
                rcb.setRegVal(theResultReg, res);
                state.done();
                return true;
            }

            FieldDef.Type argType = argValue.getType();

            switch (argType) {
            case INTEGER:
                break;
            case LONG:
                if (resultType == Type.INTEGER) {
                    resultType = Type.LONG;
                }
                break;
            case FLOAT:
                if (resultType == Type.INTEGER || resultType == Type.LONG) {
                    resultType = Type.FLOAT;
                }
                break;
            case DOUBLE:
                if (resultType == Type.INTEGER || resultType == Type.LONG ||
                    resultType == Type.FLOAT) {
                    resultType = Type.DOUBLE;
                }
                break;
            case NUMBER:
                resultType = Type.NUMBER;
                break;
            default:
                throw new QueryException(
                    "Operand in arithmetic operation has illegal type\n" +
                    "Operand : " + i + " type :\n" +
                    argValue.getDefinition().getDDLString(),
                    getLocation());
            }
        }

        assert theOps.length() == theArgs.length :
            "Not enough operations: ops:" + (theOps.length() - 1) + " args:" +
                theArgs.length;

        int iRes = theInitResult;
        long lRes = theInitResult;
        float fRes = theInitResult;
        double dRes = theInitResult;
        BigDecimal nRes = null;

        try {
            for (int i = 0 ; i < theArgs.length; i++) {

                PlanIter argIter = theArgs[i];
                FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());
                assert (argValue != null);

                if (theCode == FuncCode.OP_ADD_SUB) {
                    if (theOps.charAt(i) == '+') {
                        switch (resultType) {
                        case INTEGER:
                            iRes += argValue.castAsInt();
                            break;
                        case LONG:
                            lRes += argValue.castAsLong();
                            break;
                        case FLOAT:
                            fRes += argValue.castAsFloat();
                            break;
                        case DOUBLE:
                            dRes += argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = argValue.castAsDecimal();
                            } else {
                                nRes = nRes.add(argValue.castAsDecimal(),
                                                rcb.getMathContext());
                            }
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    } else {
                        switch (resultType) {
                        case INTEGER:
                            iRes -= argValue.castAsInt();
                            break;
                        case LONG:
                            lRes -= argValue.castAsLong();
                            break;
                        case FLOAT:
                            fRes -= argValue.castAsFloat();
                            break;
                        case DOUBLE:
                            dRes -= argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = argValue.castAsDecimal().negate();
                            } else {
                                nRes = nRes.subtract(argValue.castAsDecimal(),
                                                     rcb.getMathContext());
                            }
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    }
                } else {
                    if (theOps.charAt(i) == '*') {
                        switch (resultType) {
                        case INTEGER:
                            iRes *= argValue.castAsInt();
                            break;
                        case LONG:
                            lRes *= argValue.castAsLong();
                            break;
                        case FLOAT:
                            fRes *= argValue.castAsFloat();
                            break;
                        case DOUBLE:
                            dRes *= argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = argValue.castAsDecimal();
                            } else {
                                nRes = nRes.multiply(argValue.castAsDecimal(),
                                                     rcb.getMathContext());
                            }
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    } else {
                        switch (resultType) {
                        case INTEGER:
                            iRes /= argValue.castAsInt();
                            break;
                        case LONG:
                            lRes /= argValue.castAsLong();
                            break;
                        case FLOAT:
                            fRes /= argValue.castAsFloat();
                            break;
                        case DOUBLE:
                            dRes /= argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = new BigDecimal(1);
                            }

                            nRes = nRes.divide(argValue.castAsDecimal(),
                                               rcb.getMathContext());
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    }
                }
            }
        } catch (ArithmeticException ae) {
            throw new QueryException(
                "Arithmetic exception in query: " + ae.getMessage(),
                ae, getLocation());
        }

        FieldValueImpl res = null;
        switch (resultType) {
        case INTEGER:
            res = FieldDefImpl.integerDef.createInteger(iRes);
            break;
        case LONG:
            res = FieldDefImpl.longDef.createLong(lRes);
            break;
        case FLOAT:
            res = FieldDefImpl.floatDef.createFloat(fRes);
            break;
        case DOUBLE:
            res = FieldDefImpl.doubleDef.createDouble(dRes);
            break;
        case NUMBER:
            res = FieldDefImpl.numberDef.createNumber(nRes);
            break;
        default:
            throw new QueryStateException(
                "Invalid result type: " + resultType);
        }

        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        for (PlanIter argIter : theArgs) {
            argIter.reset(rcb);
        }

        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (PlanIter argIter : theArgs) {
            argIter.close(rcb);
        }

        state.close();
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        int i = 0;
        for (PlanIter argIter : theArgs) {

            formatter.indent(sb);
            if (theCode == FuncCode.OP_ADD_SUB) {
                if (theOps.charAt(i) == '+') {
                    sb.append('+');
                } else {
                    sb.append('-');
                }
            }
            else {
                if (theOps.charAt(i) == '*') {
                    sb.append('*');
                } else {
                    sb.append('/');
                }
            }
            sb.append(",\n");
            argIter.display(sb, formatter);
            if (i < theArgs.length - 1) {
                sb.append(",\n");
            }
            ++i;
        }
    }
}

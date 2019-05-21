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

import oracle.kv.impl.api.table.DoubleValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FloatValueImpl;
import oracle.kv.impl.api.table.IntegerValueImpl;
import oracle.kv.impl.api.table.LongValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.NumberValueImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.table.FieldDef.Type;

/**
 *  any_atomic sum(any*)
 *
 * Note: The next() method does not actually return a value; it just adds a new
 * value (if it is of a numeric type) to the running sum kept in the state. Also
 * the reset() method resets the input iter (so that the next input value can be
 * computed), but does not reset the FuncSumState. The state is reset, and the
 * current sum value is returned, by the getAggrValue() method.
 */
public class FuncSumIter extends PlanIter {

    static class FuncSumState extends PlanIterState {

        long theLongSum;

        double theDoubleSum;

        BigDecimal theNumberSum = null;

        Type theSumType = Type.LONG;

        boolean theNullInputOnly = true;

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theLongSum = 0;
            theDoubleSum = 0; 
            theNumberSum = null;
            theSumType = Type.LONG;
            theNullInputOnly = true;
        }
    }

    private final PlanIter theInput;

    public FuncSumIter(Expr e, int resultReg, PlanIter input) {
        super(e, resultReg);
        theInput = input;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncSumIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theInput, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_SUM;
    }

    @Override
    PlanIter getInputIter() {
        return theInput;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new FuncSumState());
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        /*
         * Don't reset the state of "this". Resetting the state is done in
         * method getAggrValue below.
         */
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInput.close(rcb);
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        FuncSumState state = (FuncSumState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {

            boolean more = theInput.next(rcb);

            if (!more) {
                return true;
            }

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());
            BigDecimal bd;

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Summing up value " + val);
            }

            if (val.isNull()) {
                continue;
            }

            state.theNullInputOnly = false;

            switch (val.getType()) {
            case INTEGER: {
                switch (state.theSumType) {
                case LONG:
                    state.theLongSum += ((IntegerValueImpl)val).get();
                    break;
                case DOUBLE:
                    state.theDoubleSum += ((IntegerValueImpl)val).get();
                    break;
                case NUMBER:
                    bd = new BigDecimal(((IntegerValueImpl)val).get());
                    state.theNumberSum = state.theNumberSum.add(bd);
                    break;
                default:
                    assert(false);
                }
                break;
            }
            case LONG: {
                switch (state.theSumType) {
                case LONG:
                    state.theLongSum += ((LongValueImpl)val).get();
                    break;
                case DOUBLE:
                    state.theDoubleSum += ((LongValueImpl)val).get();
                    break;
                case NUMBER:
                    bd = new BigDecimal(((LongValueImpl)val).get());
                    state.theNumberSum = state.theNumberSum.add(bd);
                    break;
                default:
                    assert(false);
                }
                break;
            }
            case FLOAT: {
                switch (state.theSumType) {
                case LONG:
                    state.theDoubleSum = state.theLongSum;
                    state.theDoubleSum += ((FloatValueImpl)val).get();
                    state.theSumType = Type.DOUBLE;
                    break;
                case DOUBLE:
                    state.theDoubleSum += ((FloatValueImpl)val).get();
                    break;
                case NUMBER:
                    bd = new BigDecimal(((FloatValueImpl)val).get());
                    state.theNumberSum = state.theNumberSum.add(bd);
                    break;
                default:
                    assert(false);
                }
                break;
            }
            case DOUBLE: {
                switch (state.theSumType) {
                case LONG:
                    state.theDoubleSum = state.theLongSum;
                    state.theDoubleSum += ((DoubleValueImpl)val).get();
                    state.theSumType = Type.DOUBLE;
                    break;
                case DOUBLE:
                    state.theDoubleSum += ((DoubleValueImpl)val).get();
                    break;
                case NUMBER:
                    bd = new BigDecimal(((DoubleValueImpl)val).get());
                    state.theNumberSum = state.theNumberSum.add(bd);
                    break;
                default:
                    assert(false);
                }
                break;
            }
            case NUMBER: {
                if (state.theNumberSum == null) {
                    state.theNumberSum = new BigDecimal(0);
                }
                
                switch (state.theSumType) {
                case LONG:
                    state.theNumberSum =  new BigDecimal(state.theLongSum);
                    state.theNumberSum =
                        state.theNumberSum.add(((NumberValueImpl)val).get());
                    state.theSumType = Type.NUMBER;
                    break;
                case DOUBLE:
                    state.theNumberSum =  new BigDecimal(state.theDoubleSum);
                    state.theNumberSum =
                        state.theNumberSum.add(((NumberValueImpl)val).get());
                    state.theSumType = Type.NUMBER;
                    break;
                case NUMBER:
                    state.theNumberSum =
                        state.theNumberSum.add(((NumberValueImpl)val).get());
                    break;
                default:
                    assert(false);
                }
                break;
            }
            default:
                break;
            }
        }
    }

    /*
     * Called during SFWIter.open(), when the open() is actually a resume
     * operation, in which case a partially computed GB tuple is sent back
     * to the RNs from the client. 
     */
    @Override
    void initAggrValue(RuntimeControlBlock rcb, FieldValueImpl val) {

        FuncSumState state = (FuncSumState)rcb.getState(theStatePos);

        if (val.isNull()) {
            return;
        }

        state.theNullInputOnly = false;

        switch (val.getType()) {
        case LONG:
            state.theLongSum = ((LongValueImpl)val).get();
            state.theSumType = Type.LONG;
            break;
        case DOUBLE:
            state.theDoubleSum = ((DoubleValueImpl)val).get();
            state.theSumType = Type.DOUBLE;
            break;
        case NUMBER:
            state.theNumberSum  = ((NumberValueImpl)val).get();
            state.theSumType = Type.NUMBER;
            break;
        default:
            throw new QueryStateException(
                "Unexpected result type for SUM function: " + val.getType());
        }
    }

    /*
     * This method is called twice when a group completes and a new group
     * starts. In both cases it return the current value of the SUM that is
     * stored in the FuncSumState. The 1st time, the SUM value is the final
     * SUM value for the just completed group. In this case the "reset" param
     * is true in order to reset the running sum in the state. The 2nd time
     * the SUM value is the inital SUM value computed from the 1st tuple of
     * the new group.
     */
    @Override
    FieldValueImpl getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        FuncSumState state = (FuncSumState)rcb.getState(theStatePos);
        FieldValueImpl res = null;

        if (state.theNullInputOnly) {
            return NullValueImpl.getInstance();
        }

        switch (state.theSumType) {
        case LONG:
            res = FieldDefImpl.longDef.createLong(state.theLongSum);
            break;
        case DOUBLE:
            res = FieldDefImpl.doubleDef.createDouble(state.theDoubleSum);
            break;
        case NUMBER:
            res = FieldDefImpl.numberDef.createNumber(state.theNumberSum);
            break;
        default:
            throw new QueryStateException(
                "Unexpected result type for SUM function: " + state.theSumType);
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Computed sum = " + res);
        }

        if (reset) {
            state.reset(this);
        }
        return res;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        theInput.display(sb, formatter);
    }
}

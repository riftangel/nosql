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

import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.impl.query.runtime.CompOpIter;
import oracle.kv.impl.query.runtime.CompOpIter.CompResult;


/*
 * any_atomic min(any*)
 *
 * any_atomic max(any*)
 */
public class FuncMinMaxIter extends PlanIter {

    static class FuncMinMaxState extends PlanIterState {

        FieldValueImpl theMinMax = NullValueImpl.getInstance();

        final CompResult theCompRes = new CompResult();

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theMinMax = NullValueImpl.getInstance();
            theCompRes.clear();
        }
    }

    private final FuncCode theFuncCode;

    private final PlanIter theInput;

    public FuncMinMaxIter(
        Expr e,
        int resultReg,
        FuncCode code,
        PlanIter input) {
        super(e, resultReg);
        theFuncCode = code;
        theInput = input;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncMinMaxIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = in.readShort();
        theFuncCode = FuncCode.values()[ordinal];
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
        out.writeShort(theFuncCode.ordinal());
        serializeIter(theInput, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_MIN_MAX;
    }

    @Override
    FuncCode getFuncCode() {
        return theFuncCode;
    }

    @Override
    PlanIter getInputIter() {
        return theInput;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new FuncMinMaxState());
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

        FuncMinMaxState state = (FuncMinMaxState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {

            boolean more = theInput.next(rcb);

            if (!more) {
                return true;
            }

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNull() || val.isJsonNull()) {
                continue;
            }

            if (state.theMinMax.isNull()) {

                switch (val.getType()) {
                case BINARY:
                case FIXED_BINARY:
                case RECORD:
                case MAP:
                    continue;
                default:
                    break;
                }

                state.theMinMax = val;
                continue;
            }

            CompOpIter.compare(rcb, state.theMinMax, val, FuncCode.OP_LT,
                               state.theCompRes, theLocation);

            if (theFuncCode == FuncCode.FN_MIN) {
                if (state.theCompRes.incompatible ||
                    state.theCompRes.comp < 0) {
                    continue;
                }
            } else {
                if (state.theCompRes.incompatible ||
                    state.theCompRes.comp > 0) {
                    continue;
                }
            }

            state.theMinMax = val;
        }
    }

    @Override
    void initAggrValue(RuntimeControlBlock rcb, FieldValueImpl val) {

        FuncMinMaxState state = (FuncMinMaxState)rcb.getState(theStatePos);
        state.theMinMax = val;
    }

    @Override
    FieldValueImpl getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        FuncMinMaxState state = (FuncMinMaxState)rcb.getState(theStatePos);

        FieldValueImpl res = state.theMinMax;

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

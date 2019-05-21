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

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.QueryFormatter;


/**
 * boolean IsNull(any?)
 */
public class IsNullIter extends PlanIter {

    private final FunctionLib.FuncCode theCode;

    private final PlanIter theArg;

    public IsNullIter(
        Expr e,
        int resultReg,
        FunctionLib.FuncCode code,
        PlanIter argIter) {

        super(e, resultReg);

        theCode = code;
        theArg = argIter;
    }

    /**
     * FastExternalizable constructor.
     */
    public IsNullIter(DataInput in, short serialVersion) throws IOException {
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
        return PlanIterKind.IS_NULL;
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

        boolean more = theArg.next(rcb);

        if (!more) {
            if (theCode == FuncCode.OP_IS_NULL) {
                rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            } else {
                rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
            }

            state.done();
            return true;
        }

        FieldValueImpl val = rcb.getRegVal(theArg.getResultReg());

        if (theCode == FuncCode.OP_IS_NULL) {
            if (val.isNull()) {
                rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
            } else {
                rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            }
        } else {
            if (val.isNull()) {
                rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            } else {
                rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
            }
        }

        state.done();
        return true;
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
        theArg.display(sb, formatter);
    }
}

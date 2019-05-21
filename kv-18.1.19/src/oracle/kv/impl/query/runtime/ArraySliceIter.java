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

import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.impl.query.runtime.PlanIterState.StateEnum;

/**
 *
 */
public class ArraySliceIter extends PlanIter {

    static private class ArraySliceState extends PlanIterState {

        long theLow;

        long theHigh;

        boolean theHaveNullOrEmptyBound;

        FieldValueImpl theCtxItem;

        int theCtxItemSize;

        ArrayValueImpl theSingletonArray;

        int theElemPos;

        ArraySliceState(ArraySliceIter iter) {
            init(iter);
            theSingletonArray = FieldDefImpl.arrayAnyDef.createArray();
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            init((ArraySliceIter)iter);
            theHaveNullOrEmptyBound = false;
        }

        @Override
        public void close() {
            super.close();
            theCtxItem = null;
            theSingletonArray = null;
        }

        private void init(ArraySliceIter iter) {

            theLow = iter.theLowValue;
            theHigh = iter.theHighValue;

            theCtxItem = null;
            theElemPos = 0;
        }
    }

    private final PlanIter theInputIter;

    private final PlanIter theLowIter;

    private final PlanIter theHighIter;

    private final Long theLowValue;

    private final Long theHighValue;

    private final int theCtxItemReg;

    public ArraySliceIter(
        Expr e,
        int resultReg,
        PlanIter inputIter,
        PlanIter lowIter,
        PlanIter highIter,
        Long lowValue,
        Long highValue,
        int ctxItemReg) {

        super(e, resultReg);
        theInputIter = inputIter;
        theLowIter = lowIter;
        theHighIter = highIter;

        theLowValue = (lowValue != null ? lowValue : 0);
        theHighValue = (highValue != null ? highValue : Integer.MAX_VALUE);
        theCtxItemReg = ctxItemReg;
        assert(theLowValue >= 0);
    }

    /**
     * FastExternalizable constructor.
     */
    ArraySliceIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theCtxItemReg = readPositiveInt(in, true);
        theLowValue = in.readLong();
        theHighValue = in.readLong();
        theInputIter = deserializeIter(in, serialVersion);
        theLowIter = deserializeIter(in, serialVersion);
        theHighIter = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(theCtxItemReg);
        out.writeLong(theLowValue);
        out.writeLong(theHighValue);
        serializeIter(theInputIter, out, serialVersion);
        serializeIter(theLowIter, out, serialVersion);
        serializeIter(theHighIter, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARRAY_SLICE;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new ArraySliceState(this));
        theInputIter.open(rcb);

        if (theLowIter != null) {
            theLowIter.open(rcb);
        }
        if (theHighIter != null) {
            theHighIter.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        ArraySliceState state = (ArraySliceState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        /*
         * Compute the boundary exprs once here, if they do not depend on the
         * ctx item and they have not been computed already.
         */
        if (theCtxItemReg < 0 && state.isOpen()) {

            state.setState(StateEnum.RUNNING);

            computeBoundaryExprs(rcb, state, false);

            if (state.theHaveNullOrEmptyBound || state.theLow > state.theHigh) {
                state.done();
                return false;
            }
        }

        while (true) {
            /*
             * Get the next context item. It's either the array cached in the
             * state, or if no such array, compute it from the input iter.
             */
            if (state.theCtxItem == null || state.theCtxItem.isNull()) {

                boolean more = theInputIter.next(rcb);

                if (!more) {
                    state.done();
                    return false;
                }

                int inputReg = theInputIter.getResultReg();
                FieldValueImpl val = rcb.getRegVal(inputReg);

                if (val.isNull()) {
                    state.theCtxItem = val;
                    rcb.setRegVal(theResultReg, val);
                    return true;
                }

                if (val.isArray()) {
                    state.theCtxItem = val;
                    state.theCtxItemSize = ((ArrayValueImpl)val).size();

                } else {
                    state.theSingletonArray.clear();
                    state.theSingletonArray.addInternal(val);
                    state.theCtxItem = state.theSingletonArray;
                    state.theCtxItemSize = 1;
                }

                /*
                 * We have a new ctx item now. If the boundary expr depend on
                 * the ctx item, bind the $$ var and compute the exprs again.
                 */
                if (theCtxItemReg >= 0) {
                    computeBoundaryExprs(rcb, state, true);
                }

                state.theElemPos = (int)state.theLow;
            }

            if (state.theHaveNullOrEmptyBound ||
                state.theElemPos > state.theHigh ||
                state.theElemPos >= state.theCtxItemSize) {
                state.theCtxItem = null;
                continue;
            }

            FieldValueImpl res = state.theCtxItem.getElement(state.theElemPos);
            rcb.setRegVal(theResultReg, res);
            ++state.theElemPos;
            return true;
        }
    }

    private void computeBoundaryExprs(
        RuntimeControlBlock rcb,
        ArraySliceState state,
        boolean reset) {

        state.theHaveNullOrEmptyBound = false;

        if (theCtxItemReg > 0) {
            rcb.setRegVal(theCtxItemReg, state.theCtxItem);
        }

        if (theLowIter != null) {

            if (reset) {
                theLowIter.reset(rcb);
            }

            boolean more = theLowIter.next(rcb);

            if (!more) {
                state.theHaveNullOrEmptyBound = true;
            } else {
                FieldValueImpl val = rcb.getRegVal(theLowIter.getResultReg());

                if (val.isNull()) {
                    state.theHaveNullOrEmptyBound = true;
                } else {
                    state.theLow = val.getLong();
                    if (state.theLow < 0) {
                        state.theLow = 0;
                    }
                }
            }
        }

        if (theHighIter != null) {

            if (theHighIter == theLowIter) {
                state.theHigh = state.theLow;
                return;
            }

            if (reset) {
                theHighIter.reset(rcb);
            }

            boolean more = theHighIter.next(rcb);

            if (!more) {
                state.theHaveNullOrEmptyBound = true;
            } else {
                FieldValueImpl val = rcb.getRegVal(theHighIter.getResultReg());

                if (val.isNull()) {
                    state.theHaveNullOrEmptyBound = true;
                } else {
                    state.theHigh = val.getLong();
                }
            }
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
        if (theLowIter != null) {
            theLowIter.reset(rcb);
        }
        if (theHighIter != null) {
            theHighIter.reset(rcb);
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

        theInputIter.close(rcb);
        if (theLowIter != null) {
            theLowIter.close(rcb);
        }
        if (theHighIter != null) {
            theHighIter.close(rcb);
        }

        state.close();
    }

    @Override
    void getParentItemContext(RuntimeControlBlock rcb, ParentItemContext ctx) {

        ArraySliceState state = (ArraySliceState)rcb.getState(theStatePos);

        if (state.theCtxItem == state.theSingletonArray) {
            theInputIter.getParentItemContext(rcb, ctx);
        } else {
            ctx.theParentItem = state.theCtxItem;
            ctx.theTargetPos = state.theElemPos - 1;
            ctx.theTargetKey = null;
        }
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        theInputIter.display(sb, formatter);

        sb.append(",\n");
        if (theLowIter != null) {
            theLowIter.display(sb, formatter);
        } else {
            formatter.indent(sb);
            sb.append("low bound: ").append(theLowValue);
        }

        sb.append(",\n");
        if (theHighIter != null) {
            theHighIter.display(sb, formatter);
        } else {
            formatter.indent(sb);
            sb.append("high bound: ").append(theHighValue);
        }

        if (theCtxItemReg >= 0) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("theCtxItemReg : ").append(theCtxItemReg);
        }
    }
}

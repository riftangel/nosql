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

import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.QueryFormatter;

/**
 * Iterator to concatenate a number of input sequences
 *
 * Inputs:
 *   0 or more operand iterators
 *
 * Result:
 *   ANY*
 */
public class ConcatIter extends PlanIter {

    private static class ConcatIterState extends PlanIterState {

        int theCurrentInput = 0;

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCurrentInput = 0;
        }

        @Override
        public void close() {
            super.close();
            theCurrentInput = 0;
        }
    }

    private final PlanIter[] theArgs;

    public ConcatIter(Expr e, int resultReg, PlanIter[] argIters) {
        super(e, resultReg);
        theArgs = argIters;
    }

    /**
     * FastExternalizable constructor.
     */
    public ConcatIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theArgs = deserializeIters(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIters(theArgs, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.CONCAT;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new ConcatIterState());
        for (PlanIter arg : theArgs) {
            arg.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        ConcatIterState state = (ConcatIterState)rcb.getState(theStatePos);

        if (theArgs.length == 0 || state.isDone()) {
            return false;
        }

        do {
            PlanIter input = theArgs[state.theCurrentInput];
            if (input.next(rcb)) {
                rcb.setRegVal(theResultReg,
                              rcb.getRegVal(input.getResultReg()));
                return true;
            }

            ++state.theCurrentInput;

        } while (state.theCurrentInput < theArgs.length);

        state.done();
        return false;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        for (PlanIter arg : theArgs) {
            arg.reset(rcb);
        }
        ConcatIterState state = (ConcatIterState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        ConcatIterState state = (ConcatIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (PlanIter arg : theArgs) {
            arg.close(rcb);
        }
        state.close();
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        for (int i = 0; i < theArgs.length; ++i) {
            theArgs[i].display(sb, formatter);
            if (i < theArgs.length - 1) {
                sb.append(",\n");
            }
        }
    }
}

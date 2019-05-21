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
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.QueryFormatter;

/**
 *
 */
public class ConstIter extends PlanIter {

    final FieldValueImpl theValue;

    public ConstIter(Expr e, int resultReg, FieldValueImpl value) {
        super(e, resultReg);
        theValue = value;
        assert(theValue != null);
    }

    /**
     * FastExternalizable constructor.
     */
    ConstIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theValue = deserializeFieldValue(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeFieldValue(theValue, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.CONST;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        rcb.setRegVal(theResultReg, theValue);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }
        state.close();
    }

    public FieldValueImpl getValue() {
        return theValue;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        formatter.indent(sb);
        theValue.toStringBuilder(sb);
    }
}

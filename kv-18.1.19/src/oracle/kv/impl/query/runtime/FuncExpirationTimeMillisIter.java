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

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.QueryFormatter;

/**
 *
 */
public class FuncExpirationTimeMillisIter extends PlanIter {

    private final PlanIter theInput;

    public FuncExpirationTimeMillisIter(Expr e, int resultReg, PlanIter input) {
        super(e, resultReg);
        theInput = input;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncExpirationTimeMillisIter(DataInput in, short serialVersion)
        throws IOException {
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
        return PlanIterKind.FUNC_EXPIRATION_TIME_MILLIS;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theInput.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theInput.next(rcb);
        assert(more);

        FieldValueImpl row = rcb.getRegVal(theInput.getResultReg());
        long expTime;

        if (row == NullValueImpl.getInstance()) {
             rcb.setRegVal(theResultReg, row);
             state.done();
             return true;
        }

        if (row.isTuple()) {
            expTime = ((TupleValue)row).getExpirationTime();
        } else if (row.isRecord()) {
            expTime = ((RowImpl)row).getExpirationTime();
        } else {
            throw new QueryException(
                "Input to the expiration_time() function is not a row",
                getLocation());
        }

        rcb.setRegVal(theResultReg, FieldDefImpl.longDef.createLong(expTime));
        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
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
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        theInput.display(sb, formatter);
    }
}

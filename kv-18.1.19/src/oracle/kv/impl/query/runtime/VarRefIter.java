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

import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.QueryFormatter;

/**
 * VarRefIter represents a reference to a non-external variable in the query.
 * It simply returns the value that the variable is currently bound to. This
 * value is computed by the variable's "domain iterator" (the iterator that
 * evaluates the domain expression of the variable).
 *
 * For now, the only kind of variables we
 * have are implicitly created variables ranging over the table references
 * in the FROM clause. Such variables are always bound to the tuples produced
 * by their associated tables.
 *
 * theName:
 * The name of the variable. Used only when displaying the execution plan.
 *
 * theTupleRegs:
 * Not null if the domain expr of this var produces records that are stored
 * as a TupleValue. For example, this is the case if the domain expr is an
 * ExprBaseTable with a single table (no NESTED TABLES clause).
 */
public class VarRefIter extends PlanIter {

    private final String theName;

    private final int[] theTupleRegs;

    public VarRefIter(Expr e, int resultReg, int[] tupleRegs, String name) {
        super(e, resultReg);
        theName = name;
        theTupleRegs = tupleRegs;
    }

    /**
     * FastExternalizable constructor.
     */
    VarRefIter(DataInput in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theName = (serialVersion >= QUERY_VERSION_5) ?
            readString(in, serialVersion) :
            in.readUTF();
        theTupleRegs = deserializeIntArray(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        if (serialVersion >= QUERY_VERSION_5) {
            writeString(out, serialVersion, theName);
        } else {
            out.writeUTF(theName);
        }
        serializeIntArray(theTupleRegs, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.VAR_REF;
    }

    @Override
    public int[] getTupleRegs() {
        return theTupleRegs;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            if (rcb.getTraceLevel() >= 4) {
                rcb.trace("No Value for variable " + theName + " in register " +
                          theResultReg);
            }
            return false;
        }

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("Value for variable " + theName + " in register " +
                      theResultReg + ":\n" + rcb.getRegVal(theResultReg));
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

    @Override
    protected void display(StringBuilder sb, QueryFormatter formatter) {
        formatter.indent(sb);
        displayContent(sb, formatter);
        displayRegs(sb);
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        sb.append("VAR_REF(");
        sb.append(theName);
        sb.append(")");
    }
}

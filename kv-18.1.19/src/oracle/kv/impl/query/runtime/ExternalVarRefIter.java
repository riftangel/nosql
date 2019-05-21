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

import oracle.kv.impl.api.table.FieldValueImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.QueryFormatter;

/**
 * VarRefIter represents a reference to an external variable in the query.
 * It simply returns the value that the variable is currently bound to. This
 * value is set by the app via the methods of BoundStatement.
 *
 * theName:
 * The name of the variable. Used only when displaying the execution plan
 * and in error messages.
 *
 * theId:
 * The valriable id. IT is used as an index into an array of FieldValues
 * in the RCB that stores the values of the external vars.
 */
public class ExternalVarRefIter extends PlanIter {

    private final String theName;

    private final int theId;

    public ExternalVarRefIter(Expr e, int resultReg, int id, String name) {
        super(e, resultReg);
        theName = name;
        theId = id;
    }

    /**
     * FastExternalizable constructor.
     */
    ExternalVarRefIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theName = (serialVersion >= QUERY_VERSION_5) ?
            readString(in, serialVersion) :
            in.readUTF();
        theId = readPositiveInt(in);
    }

    /**
     * FastExternalizable writer. Must call superclass method first to
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
        out.writeInt(theId);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.EXTERNAL_VAR_REF;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        FieldValueImpl val = rcb.getExternalVar(theId);

        /*
         * val should not be null, because we check before starting query
         * execution that all the external vars have been bound. So this is
         * a sanity check.
         */
        if (val == null) {
            throw new QueryStateException(
                "Variable " + theName + " has not been set");
        }

        rcb.setRegVal(theResultReg, val);
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
        sb.append("EXTENAL_VAR_REF(");
        sb.append(theName);
        sb.append(", ").append(theId);
        sb.append(")");
    }
}

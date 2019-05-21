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

import java.util.concurrent.TimeUnit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.TimeToLive;

/**
 *
 */
public class UpdateRowIter extends PlanIter {

    public static class UpdateRowState extends PlanIterState {

        PlanIter theWorkerIter;

        public UpdateRowState(PlanIter worker) {
            theWorkerIter = worker;
        }
    }

    protected final String theNamespace;

    protected final String theTableName;

    protected PlanIter theInputIter;

    protected PlanIter[] theUpdateOps;

    protected boolean theUpdateTTL;

    protected PlanIter theTTLIter;

    protected TimeUnit theTTLUnit;

    protected boolean theHasReturningClause;

    public UpdateRowIter(
        Expr e,
        int resultReg,
        TableImpl table,
        PlanIter inputIter,
        PlanIter[] ops,
        boolean updateTTL,
        PlanIter ttlIter,
        TimeUnit ttlUnit,
        boolean hasReturningClause) {

        super(e, resultReg);
        theNamespace = table.getNamespace();
        theTableName = table.getFullName();
        theInputIter = inputIter;
        theUpdateOps = ops;
        theUpdateTTL = updateTTL;
        theTTLIter = ttlIter;
        theTTLUnit = ttlUnit;
        assert(theTTLIter == null || theUpdateTTL);
        theHasReturningClause = hasReturningClause;
    }

    /*
     * This constructor is called from the ServerUpdateRowIter constructor.
     * So, we are actually creating a ServerUpdateRowIter to be the worker
     * iter for a "parent" UpdateRowIter.
     */
    public UpdateRowIter(UpdateRowIter parent) {
        super(parent.theStatePos, parent.theResultReg, parent.getLocation());
        theNamespace = parent.theNamespace;
        theTableName = parent.theTableName;
        theInputIter = parent.theInputIter;
        theUpdateOps = parent.theUpdateOps;
        theUpdateTTL = parent.theUpdateTTL;
        theTTLIter = parent.theTTLIter;
        theTTLUnit = parent.theTTLUnit;
        theHasReturningClause = parent.theHasReturningClause;
    }

    public UpdateRowIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theNamespace = SerializationUtil.readString(in, serialVersion);
        theTableName = SerializationUtil.readString(in, serialVersion);
        theInputIter = deserializeIter(in, serialVersion);
        theUpdateOps = deserializeIters(in, serialVersion);
        theUpdateTTL = in.readBoolean();
        if (theUpdateTTL) {
            theTTLIter = deserializeIter(in, serialVersion);
            if (theTTLIter != null) {
                theTTLUnit = TimeToLive.readTTLUnit(in, 1);
            }
        }
        theHasReturningClause = in.readBoolean();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        SerializationUtil.writeString(out, serialVersion, theNamespace);
        SerializationUtil.writeString(out, serialVersion, theTableName);
        serializeIter(theInputIter, out, serialVersion);
        serializeIters(theUpdateOps, out, serialVersion);
        out.writeBoolean(theUpdateTTL);
        if (theUpdateTTL) {
            serializeIter(theTTLIter, out, serialVersion);
            if (theTTLIter != null) {
                out.writeByte((byte) theTTLUnit.ordinal());
            }
        }
        out.writeBoolean(theHasReturningClause);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.UPDATE_ROW;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        ServerIterFactory serverIterFactory = rcb.getServerIterFactory();
        PlanIter worker = serverIterFactory.createUpdateRowIter(this);
        worker.open(rcb);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        UpdateRowState state = (UpdateRowState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        state.theWorkerIter.close(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        UpdateRowState state = (UpdateRowState)rcb.getState(theStatePos);
        state.theWorkerIter.reset(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        UpdateRowState state = (UpdateRowState)rcb.getState(theStatePos);
        return state.theWorkerIter.next(rcb);
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        theInputIter.display(sb, formatter);
        sb.append("\n");

        for (int i = 0; i < theUpdateOps.length; ++i) {
            theUpdateOps[i].display(sb, formatter);
            sb.append(",\n");
        }

        formatter.indent(sb);
        sb.append("UpdateTTL = ").append(theUpdateTTL);
        if (theTTLIter != null) {
            sb.append("\n");
            theTTLIter.display(sb, formatter);
            sb.append("\n");
            formatter.indent(sb);
            sb.append("TimeUnit = " + theTTLUnit);
        }
    }
}


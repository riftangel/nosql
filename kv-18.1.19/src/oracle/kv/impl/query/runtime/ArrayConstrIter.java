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

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.ExprArrayConstr;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.impl.util.SerialVersion;

/**
 *
 */
public class ArrayConstrIter extends PlanIter {

    private final PlanIter[] theArgs;

    private final ArrayDefImpl theDef;

    private boolean theIsConditional;

    public ArrayConstrIter(
        ExprArrayConstr e,
        int resultReg,
        PlanIter[] args) {

        super(e, resultReg);
        theArgs = args;
        theDef = e.getArrayType();
        theIsConditional = e.isConditional();
    }

    /**
     * FastExternalizable constructor.
     */
    ArrayConstrIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theArgs = deserializeIters(in, serialVersion);
        theDef = (ArrayDefImpl)deserializeFieldDef(in, serialVersion);
        if (serialVersion >= SerialVersion.QUERY_VERSION_3) {
            theIsConditional = in.readBoolean();
        } else {
            theIsConditional = false;
        }
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
        serializeFieldDef(theDef, out, serialVersion);
        if (serialVersion >= SerialVersion.QUERY_VERSION_3) {
            out.writeBoolean(theIsConditional);
        } 
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARRAY_CONSTR;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        for (PlanIter arg : theArgs) {
            arg.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        ArrayValueImpl array;

        if (theIsConditional) {

            boolean more = theArgs[0].next(rcb);

            if (!more) {
                state.done();
                return false;
            }

            FieldValueImpl val = rcb.getRegVal(theArgs[0].getResultReg());

            if (val.isTuple()) {
                val = ((TupleValue)val).toRecord();
            }

            more = theArgs[0].next(rcb);

            if (!more) {
                rcb.setRegVal(theResultReg, val);
                state.done();
                return true;
            }

            array = theDef.createArray();

            try {
                if (!val.isNull()) {
                    array.add(val);
                }

                val = rcb.getRegVal(theArgs[0].getResultReg());

                if (val.isTuple()) {
                    val = ((TupleValue)val).toRecord();
                }

                if (!val.isNull()) {
                    array.add(val);
                }
            } catch (IllegalArgumentException e) {
                handleIAE(rcb, val, e);
            }

        } else {
            array = theDef.createArray();
        }

        for (int currArg = 0; currArg < theArgs.length; ++currArg) {

            while (true) {
                boolean more = theArgs[currArg].next(rcb);

                if (!more) {
                    break;
                }

                FieldValueImpl val =
                    rcb.getRegVal(theArgs[currArg].getResultReg());

                if (val.isNull()) {
                    continue;
                }

                try {
                    if (val.isTuple()) {
                        array.add(((TupleValue)val).toRecord());
                    } else {
                        array.add(val);
                    }
                } catch (IllegalArgumentException e) {
                    handleIAE(rcb, val, e);
                }
            }
        }

        rcb.setRegVal(theResultReg, array);
        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        for (PlanIter arg : theArgs) {
            arg.reset(rcb);
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

        for (PlanIter arg : theArgs) {
            arg.close(rcb);
        }

        state.close();
    }

    private void handleIAE(
        RuntimeControlBlock rcb,
        FieldValueImpl val,
        IllegalArgumentException e) {

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("Query Plan:\n" + rcb.getRootIter().display() +
                      "\nValue:\n" + val);
        }
        throw new QueryException(e, theLocation);
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        if (theIsConditional) {
            formatter.indent(sb);
            sb.append("conditional");
            sb.append(",\n");
        }

        formatter.indent(sb);
        sb.append("Type:\n");
        formatter.indent(sb);
        theDef.display(sb, formatter);
        sb.append("\n\n");

        for (int i = 0; i < theArgs.length; ++i) {
            theArgs[i].display(sb, formatter);
            if (i < theArgs.length - 1) {
                sb.append(",\n");
            }
        }
    }
}

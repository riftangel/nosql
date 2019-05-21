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

import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.StringValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.ExprMapConstr;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.impl.util.SerialVersion;

/**
 * Note: 
 * - field-value exprs that may return more than one item are wrapped in
 *   conditional array constructors.
 * - field name exprs are wrapped with promote exprs whose type is STRING?
 */
public class MapConstrIter extends PlanIter {

    private final PlanIter[] theArgs;

    private final MapDefImpl theDef;

    public MapConstrIter(
        ExprMapConstr e,
        int resultReg,
        PlanIter[] args) {

        super(e, resultReg);
        theArgs = args;
        theDef = e.getMapType();
    }

    /**
     * FastExternalizable constructor.
     */
    MapConstrIter(DataInput in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theArgs = deserializeIters(in, serialVersion);
        theDef = (MapDefImpl)deserializeFieldDef(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        if (serialVersion < SerialVersion.QUERY_VERSION_3) {

            String QV3String =
                SerialVersion.getKVVersion(SerialVersion.QUERY_VERSION_3).
                getNumericVersionString();

            throw new QueryException(
                "Cannot execute a query with a map constructor at a server " +
                "whose version is less than " + QV3String +
                "\nserialVersion = " + serialVersion +
                " expected version = " + SerialVersion.QUERY_VERSION_3);
        }

        super.writeFastExternal(out, serialVersion);
        serializeIters(theArgs, out, serialVersion);
        serializeFieldDef(theDef, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.MAP_CONSTR;
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

        int numArgs = theArgs.length;
        MapValueImpl map = theDef.createMap();

        for (int i = 0; i < numArgs; i += 2) {

            boolean more = theArgs[i].next(rcb);

            if (!more) {
                continue;
            }

            more = theArgs[i+1].next(rcb);

            if (!more) {
                continue;
            }

            FieldValueImpl nameValue = 
                rcb.getRegVal(theArgs[i].getResultReg());

            if (nameValue.isNull()) {
                continue;
            }

            String name = ((StringValueImpl)nameValue).get();

            FieldValueImpl elemValue =
                rcb.getRegVal(theArgs[i+1].getResultReg());

            if (elemValue.isNull()) {
                continue;
            }

            if (elemValue.isTuple()) {
                elemValue = ((TupleValue)elemValue).toRecord();
            }

            try {
                map.put(name, elemValue);
            } catch (IllegalArgumentException e) {
                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("Query Plan:\n" + rcb.getRootIter().display() +
                              "\nValue:\n" + elemValue);
                }
                throw new QueryException(e, theLocation);
            }
        }

        rcb.setRegVal(theResultReg, map);
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


    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("Type:\n");
        formatter.indent(sb);
        theDef.display(sb, formatter);
        sb.append("\n");

        for (int i = 0; i < theArgs.length; ++i) {
            theArgs[i].display(sb, formatter);
            if (i < theArgs.length - 1) {
                sb.append(",\n");
            }
        }
    }
}

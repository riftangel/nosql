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

package oracle.kv.impl.query.runtime.server;

import java.util.concurrent.TimeUnit;

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ReturnValueVersion;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.ops.PutHandler;
import oracle.kv.impl.api.table.IntegerValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprUpdateRow;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.impl.query.runtime.CastIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.PlanIterState;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.query.runtime.UpdateRowIter;
import oracle.kv.table.TimeToLive;

/**
 *
 */
public class ServerUpdateRowIter extends UpdateRowIter {

    ServerIterFactoryImpl theOpContext;

    PutHandler thePutHandler;

    static IntegerValueImpl one = FieldDefImpl.integerDef.createInteger(1);

    static IntegerValueImpl zero = FieldDefImpl.integerDef.createInteger(0);

    ServerUpdateRowIter(UpdateRowIter parent) {
        super(parent);
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new UpdateRowState(this));

        theInputIter.open(rcb);
        for (PlanIter updIter : theUpdateOps) {
            updIter.open(rcb);
        }

        if (theTTLIter != null) {
            theTTLIter.open(rcb);
        }

        theOpContext = (ServerIterFactoryImpl)rcb.getServerIterFactory();
        thePutHandler = new PutHandler(theOpContext.getOperationHandler());
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        theInputIter.close(rcb);
        for (PlanIter updIter : theUpdateOps) {
            updIter.close(rcb);
        }

        if (theTTLIter != null) {
            theTTLIter.close(rcb);
        }

        state.close();
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
        for (PlanIter updIter : theUpdateOps) {
            updIter.reset(rcb);
        }

        if (theTTLIter != null) {
            theTTLIter.reset(rcb);
        }

        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theInputIter.next(rcb);

        if (!more) {
            if (theHasReturningClause) {
                state.done();
                return false;
            }
            rcb.setRegVal(theResultReg, zero);
            state.done();
            return true;
        }

        int inputReg = theInputIter.getResultReg();
        FieldValueImpl inVal = rcb.getRegVal(inputReg);

        if (!(inVal instanceof RowImpl)) {
            throw new QueryStateException(
                "Update statement expected a row, but got this field value:\n" +
                inVal);
        }

        RowImpl row = (RowImpl)inVal;
        boolean updated = false;

        for (PlanIter updFieldIter : theUpdateOps) {
            if (updFieldIter.next(rcb)) {
                updated = true;
            }
            updFieldIter.reset(rcb);
        }

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("Row after update =\n" + row);
        }

        TimeToLive ttlObj = null;
        boolean updateTTL = theUpdateTTL;

        if (theUpdateTTL) {

            if (theTTLIter != null) {

                if (theTTLIter.next(rcb)) {
                    FieldValueImpl ttlVal =
                        rcb.getRegVal(theTTLIter.getResultReg());

                    ttlVal = CastIter.castValue(ttlVal,
                                                FieldDefImpl.integerDef,
                                                theLocation);
                    int ttl = ((IntegerValueImpl)ttlVal).get();
                    if (ttl < 0) {
                        ttl = 0;
                    }
                    if (theTTLUnit == TimeUnit.HOURS) {
                        ttlObj = TimeToLive.ofHours(ttl);
                    } else {
                        ttlObj = TimeToLive.ofDays(ttl);
                    }

                    updated = true;
                } else {
                   updateTTL = false;
                }
            } else {
                ttlObj = row.getTable().getDefaultTTL();
                updated = true;
            }

            if (ttlObj != null) {
                TimeUnit unit = ttlObj.getUnit();
                long ttl = ttlObj.getValue();
                long expTime;

                if (ttl == 0) {
                    expTime = 0;
                } else if (unit == TimeUnit.DAYS) {
                    expTime = ((System.currentTimeMillis() +
                                TimestampDefImpl.MILLIS_PER_DAY - 1) /
                               TimestampDefImpl.MILLIS_PER_DAY);

                    expTime = (expTime + ttl) * TimestampDefImpl.MILLIS_PER_DAY;
                } else {
                    expTime = ((System.currentTimeMillis() +
                                TimestampDefImpl.MILLIS_PER_HOUR - 1) /
                               TimestampDefImpl.MILLIS_PER_HOUR);

                    expTime = (expTime + ttl) * TimestampDefImpl.MILLIS_PER_HOUR;
                }

                if (rcb.getTraceLevel() > 3) {
                    rcb.trace("ttl = " + ttl + " expiration time = " + expTime);
                }

                row.setExpirationTime(expTime);
            } else if (updateTTL) {
                row.setExpirationTime(0);
            }
        }

        if (updated) {
            Key rowkey = row.getPrimaryKey(false/*allowPartial*/);
            Value rowval = row.createValue();

            KeySerializer keySerializer =
                KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
            byte[] keybytes = keySerializer.toByteArray(rowkey);

            Put put = new Put(keybytes,
                              rowval,
                              ReturnValueVersion.Choice.NONE,
                              row.getTableImpl().getId(),
                              ttlObj,
                              updateTTL);

            /*
             * Configures the ThroughputTracker of Put op with the
             * ThroughputTracker of TableQuery.
             */
            put.setThroughputTracker(rcb.getQueryOp());

            thePutHandler.execute(put,
                                  theOpContext.getTxn(),
                                  theOpContext.getPartitionId());
            /* Tally write KB to RuntimeControlBlock.writeKB */
            rcb.tallyWriteKB(put.getWriteKB());
        }

        if (theHasReturningClause) {
            rcb.setRegVal(theResultReg, row);
        } else {
            RecordValueImpl res = 
                ExprUpdateRow.theNumRowsUpdatedType.createRecord();
            res.put(0, (updated ? one : zero));
            rcb.setRegVal(theResultReg, res);
        }

        state.done();
        return true;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        assert(false);
    }
}

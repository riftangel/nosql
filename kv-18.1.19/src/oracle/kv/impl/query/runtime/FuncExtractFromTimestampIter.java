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

import static oracle.kv.impl.api.table.TimestampDefImpl.MAX_PRECISION;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FuncExtractFromTimestamp.Unit;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.table.TimestampValue;

/**
 * The FuncTimestampElementIter is to extract the element of TIMESTAMP value:
 *
 *  int year(timestamp)
 *      Returns the year for the timestamp, in the range -6383 ~ 9999.
 *
 *  int month(timestamp)
 *      Returns the month for the timestamp, in the range 1 ~ 12.
 *
 *  int day(timestamp)
 *      Returns the day of month for the timestamp, in the range 1 ~ 31.
 *
 *  int hour(timestamp)
 *      Returns the hour of day for the  timestamp, in the range 0 ~ 23.
 *
 *  int minute(timestamp)
 *      Returns the minute for the timestamp, in the range 0 ~ 59.
 *
 *  int second(timestamp)
 *      Returns the second for the timestamp, in the range 0 ~ 59.
 *
 *  int milisecond(timestamp)
 *      Returns the fractional second in millisecond for the timestamp, in the
 *      range 0 ~ 999.
 *
 *  int microsecond(timestamp)
 *      Returns the fractional second in microsecond for the timestamp, in the
 *      range 0 ~ 999999.
 *
 *  int nanosecond(timestamp)
 *      Returns the fractional second in nanosecond for the timestamp, in the
 *      range 0 ~ 999999999.
 *
 *  int week(timestamp)
 *      Returns the week number within the year where a week starts on Sunday
 *      and the first week has a minimum of 1 day in this year, in the range
 *      1 ~ 54.
 *
 *  int isoweek(timestamp)
 *      Returns the week number within the year based on IS0-8601 where a week
 *      starts on Monday and the first week has a minimum of 4 days in this
 *      year, in range 0 ~ 53.
 */
public class FuncExtractFromTimestampIter extends PlanIter {

    private final Unit unit;
    private final PlanIter theInput;

    public FuncExtractFromTimestampIter(Expr e,
                                        int resultReg,
                                        Unit unit,
                                        PlanIter theInput) {
        super(e, resultReg);
        this.unit = unit;
        this.theInput = theInput;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncExtractFromTimestampIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        short ordinal = readOrdinal(in, Unit.values().length);
        unit = Unit.values()[ordinal];
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
        out.writeShort(unit.ordinal());
        serializeIter(theInput, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_EXTRACT_FROM_TIMESTAMP;
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

        if (!more) {
            state.done();
            return false;
        }

        FieldValueImpl item = rcb.getRegVal(theInput.getResultReg());

        if (item.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        assert(item.isTimestamp());
        int result = extractFromTimestamp(item.asTimestamp());

        FieldValueImpl res = FieldDefImpl.integerDef.createInteger(result);
        rcb.setRegVal(theResultReg, res);
        state.done();
        return true;
    }

     private int extractFromTimestamp(TimestampValue ts) {
        int result = 0;

        switch (unit) {
        case YEAR:
            result = ts.getYear();
            break;
        case MONTH:
            result = ts.getMonth();
            break;
        case DAY:
            result = ts.getDay();
            break;
        case HOUR:
            result = ts.getHour();
            break;
        case MINUTE:
            result = ts.getMinute();
            break;
        case SECOND:
            result = ts.getSecond();
            break;
        case MILLISECOND:
            result = formatFracSecond(ts, 3);
            break;
        case MICROSECOND:
            result = formatFracSecond(ts, 6);
            break;
        case NANOSECOND:
            result = formatFracSecond(ts, 9);
            break;
        case WEEK:
            result = TimestampUtils.getWeekOfYear(ts.get());
            break;
        case ISOWEEK:
            result = TimestampUtils.getISOWeekOfYear(ts.get());
            break;
        default:
            throw new QueryStateException("Unexpected unit: " + unit);
        }

        return result;
    }

    /**
     * Returns the fractional second as a number in the given precision.
     */
    private int formatFracSecond(TimestampValue ts, int toPrecision) {

        int tsPrecision = ts.getDefinition().asTimestamp().getPrecision();
        if (tsPrecision == 0 || toPrecision == 0) {
            return 0;
        }

        if (tsPrecision == toPrecision) {
            return ts.getFracSecond();
        }

        int nanos = ts.getNano();
        if (nanos == 0 || toPrecision == MAX_PRECISION) {
            return nanos;
        }
        return nanos /= Math.pow(10, MAX_PRECISION - toPrecision);
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

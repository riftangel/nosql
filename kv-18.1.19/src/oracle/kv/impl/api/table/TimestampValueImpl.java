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

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TimestampUtils.fromBytes;
import static oracle.kv.impl.api.table.TimestampUtils.formatString;
import static oracle.kv.impl.api.table.TimestampUtils.roundToPrecision;
import static oracle.kv.impl.api.table.TimestampUtils.toMilliseconds;

import java.sql.Timestamp;
import java.util.Arrays;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;
import org.codehaus.jackson.util.CharTypes;

import oracle.kv.impl.util.SortableString;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.TimestampDef;
import oracle.kv.table.TimestampValue;

/**
 * This class represents a Timestamp value with precision in range of 0 ~ 9.
 */
public class TimestampValueImpl extends FieldValueImpl
    implements TimestampValue {

    private static final long serialVersionUID = 1L;

    static final byte[] DEFAULT_VALUE =
        TimestampUtils.toBytes(new Timestamp(0), 0);

    private final TimestampDefImpl def;

    /* The number of microseconds from the epoch of 1970-01-01T00:00:00Z */
    private byte[] value;

    TimestampValueImpl(TimestampDef def, Timestamp timestamp) {
        validate(timestamp);
        this.def = (TimestampDefImpl)def;
        value = TimestampUtils.toBytes
                    (roundToPrecision(timestamp, def.getPrecision()),
                     def.getPrecision());
    }

    /**
     * This constructor creates TimestampValueImpl from the String format used
     * for sorted keys.
     */
    TimestampValueImpl(TimestampDef def, String keyValue) {
        this(def, SortableString.bytesFromSortable(keyValue));
    }

    TimestampValueImpl(TimestampDef def, byte[] value) {
        this.def = (TimestampDefImpl)def;
        this.value = value;
    }

    TimestampValueImpl(TimestampDef def, long milliseconds) {
        this(def, new Timestamp(milliseconds));
    }

    /*
     * Public api methods from Object and FieldValue
     */
    @Override
    public TimestampValueImpl clone() {
        return new TimestampValueImpl(def, value);
    }

    /**
     * Cast "this" to another timestamp with the given precision.
     */
    public TimestampValueImpl castToPrecision(int targetPrec) {

        int myPrec = def.getPrecision();
        assert(myPrec != targetPrec);

        /*
         * Casting to a lower precision, may require rounding up the
         * frac second in the new timestamp value. So we do the cast
         * the "hard way".
         */
        if (targetPrec < myPrec) {
            return FieldDefImpl.getTimeDef(targetPrec).createTimestamp(get());
        }

        int myFracSec = getFracSecond();
        int targetFracSec;

        if (myPrec == 0 || targetPrec == 0) {
            targetFracSec = 0;
        } else if (myPrec < targetPrec) {
            targetFracSec = myFracSec * (int)Math.pow(10, targetPrec - myPrec);
        } else {
            targetFracSec = myFracSec / (int)Math.pow(10, myPrec - targetPrec);
        }

        return
            (TimestampValueImpl)
            FieldValueFactory.createTimestamp(getYear(),
                                              getMonth(),
                                              getDay(),
                                              getHour(),
                                              getMinute(),
                                              getSecond(),
                                              targetFracSec,
                                              targetPrec);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof TimestampValueImpl) {
            return (compareTo((FieldValue)other) == 0);
        }
        return false;
    }

    /**
     * Allow comparisons against TimestampValue to succeed.
     */
    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof TimestampValueImpl) {
            TimestampValueImpl otherVal = ((TimestampValueImpl)other);
            return TimestampUtils.compareBytes(
                value,
                def.getPrecision(),
                otherVal.value,
                otherVal.getDefinition().getPrecision());
        }
        throw new ClassCastException("Object is not a TimestampValue");
    }

    @Override
    public String toString() {
        return toString(null, true);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.TIMESTAMP;
    }

    @Override
    public TimestampDefImpl getDefinition() {
        return def;
    }

    @Override
    public TimestampValue asTimestamp() {
        return this;
    }

    @Override
    public boolean isTimestamp() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    /*
     * Public api methods from TimestampValue
     */
    @Override
    public Timestamp get() {
        return fromBytes(value, def.getPrecision());
    }

    @Override
    public int getYear() {
        return TimestampUtils.getYear(value);
    }

    @Override
    public int getMonth() {
        return TimestampUtils.getMonth(value);
    }

    @Override
    public int getDay() {
        return TimestampUtils.getDay(value);
    }

    @Override
    public int getHour() {
        return TimestampUtils.getHour(value);
    }

    @Override
    public int getMinute() {
        return TimestampUtils.getMinute(value);
    }

    @Override
    public int getSecond() {
        return TimestampUtils.getSecond(value);
    }

    @Override
    public int getNano() {
        return TimestampUtils.getNano(value, def.getPrecision());
    }

    @Override
    public int getFracSecond() {
        return TimestampUtils.getFracSecond(value, def.getPrecision());
    }

    /*
     * FieldValueImpl internal api methods
     */
    @Override
    public void setTimestamp(Timestamp ts) {
        if (ts == null) {
            throw new IllegalArgumentException("timestamp should not be null");
        }
        value = TimestampUtils.toBytes(roundToPrecision(ts, def.getPrecision()),
                                       def.getPrecision());
    }

    @Override
    public Timestamp getTimestamp() {
        return get();
    }

    /* Returns the milliseconds since Java epoch */
    @Override
    public long castAsLong() {
        return toMilliseconds(getTimestamp());
    }

    @Override
    public String castAsString() {
        return toString();
    }

    @Override
    public String formatForKey(FieldDef field1, int storageSize) {
        return toKeyString(value);
    }

    @Override
    FieldValueImpl getNextValue() {
        Timestamp ts = get();
        if (ts.compareTo(def.getMaxValue()) == 0) {
            return null;
        }
        final int precision = def.getPrecision();
        Timestamp nextVal;
        if (def.getPrecision() <= 3) {
            nextVal = TimestampUtils.plusMillis
                        (ts, (long)Math.pow(10, 3 - precision));
        } else {
            nextVal = TimestampUtils.plusNanos
                        (ts, (long)Math.pow(10, TimestampDefImpl.MAX_PRECISION -
                                                precision));
        }
        return def.createTimestamp(nextVal);
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return def.createTimestamp(def.getMinValue());
    }

    @Override
    public JsonNode toJsonNode() {
        return new TextNode(toString());
    }

    @Override
    public void toStringBuilder(StringBuilder sb) {
        sb.append('\"');
        CharTypes.appendQuoted(sb, toString());
        sb.append('\"');
    }

    @Override
    public String toString(String pattern, boolean withZoneUTC) {
        return formatString(this, pattern, withZoneUTC);
    }

    @Override
    public byte[] getBytes() {
        return getBytes(false);
    }

    /**
     * Returns the byte array that represents the timestamp value.
     *
     * @param fullSize indicates if returning byte array is a full size or
     * compact. If true, then pad with zeros to obtain the max length.
     *
     * @return the byte array.
     */
    byte[] getBytes(boolean fullSize) {
        return fullSize ? Arrays.copyOf(value, def.getNumBytes()) : value;
    }

    void validate(Timestamp timestamp) {
        if (timestamp.compareTo(TimestampDefImpl.MAX_VALUE) > 0 ||
            timestamp.compareTo(TimestampDefImpl.MIN_VALUE) < 0) {

            throw new IllegalArgumentException("Timestamp should be " +
                "in range from " + formatString(TimestampDefImpl.MIN_VALUE) +
                " to " + formatString(TimestampDefImpl.MAX_VALUE) + ": " +
                formatString(timestamp));
        }
    }

    static String toKeyString(byte[] bytes) {
        return SortableString.toSortable(bytes);
    }

    @Override
    public byte[] getTimestampBytes() {
        return getBytes();
    }
}


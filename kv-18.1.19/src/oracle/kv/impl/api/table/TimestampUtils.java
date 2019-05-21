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

import java.sql.Timestamp;
import java.util.Arrays;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;

import oracle.kv.table.TimestampDef;

/**
 * This class provides utility methods for Timestamp value:
 *  - Serialization and deserialization.
 *  - Convert to/from a string
 *  - Other facility methods.
 */
public class TimestampUtils {

    /* The max precision of Timestamp type. */
    private final static int MAX_PRECISION = TimestampDefImpl.MAX_PRECISION;

    /* The UTC zone */
    private final static ZoneId UTCZone = ZoneId.of(ZoneOffset.UTC.getId());

    /*
     * The position and the number of bits for each component of Timestamp for
     * serialization
     */
    private final static int YEAR_POS = 0;
    private final static int YEAR_BITS = 14;

    private final static int MONTH_POS = YEAR_POS + YEAR_BITS;
    private final static int MONTH_BITS = 4;

    private final static int DAY_POS = MONTH_POS + MONTH_BITS;
    private final static int DAY_BITS = 5;

    private final static int HOUR_POS = DAY_POS + DAY_BITS;
    private final static int HOUR_BITS = 5;

    private final static int MINUTE_POS = HOUR_POS + HOUR_BITS;
    private final static int MINUTE_BITS = 6;

    private final static int SECOND_POS = MINUTE_POS + MINUTE_BITS;
    private final static int SECOND_BITS = 6;

    private final static int NANO_POS = SECOND_POS + SECOND_BITS;

    private final static int YEAR_EXCESS = 6384;

    private final static int NO_FRACSECOND_BYTES = 5;

    private final static char compSeparators[] = { '-', '-', 'T', ':', ':', '.'};

    private final static String compNames[] = {
        "year", "month", "day", "hour", "minute", "second", "fractional second"
    };


    /**
     * Methods to serialize/deserialize Timestamp to/from a byte array.
     */

     /**
     * Serialize a Timestamp value to a byte array:
     *
     * Variable-length bytes from 3 to 9 bytes:
     *  bit[0~13]  year - 14 bits
     *  bit[14~17] month - 4 bits
     *  bit[18~22] day - 5 bits
     *  bit[23~27] hour - 5 bits        [optional]
     *  bit[28~33] minute - 6 bits      [optional]
     *  bit[34~39] second - 6 bits      [optional]
     *  bit[40~71] fractional second    [optional with variable length]
     */
    static byte[] toBytes(Timestamp value, int precision) {

        final ZonedDateTime zdt = toUTCDateTime(value);
        int fracSeconds = fracSecondToPrecision(zdt.getNano(), precision);
        return toBytes(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(),
                       zdt.getHour(), zdt.getMinute(), zdt.getSecond(),
                       fracSeconds, precision);
    }

    public static byte[] toBytes(int year, int month, int day,
                                 int hour, int minute, int second,
                                 int fracSeconds, int precision) {

        byte[] bytes = new byte[getNumBytes(precision)];
        /* Year */
        writeBits(bytes, year + YEAR_EXCESS, YEAR_POS, YEAR_BITS);
        /* Month */
        writeBits(bytes, month, MONTH_POS, MONTH_BITS);
        /* Day */
        int pos = writeBits(bytes, day, DAY_POS, DAY_BITS);
        /* Hour */
        if (hour > 0) {
            pos = writeBits(bytes, hour, HOUR_POS, HOUR_BITS);
        }
        /* Minute */
        if (minute > 0) {
            pos = writeBits(bytes, minute, MINUTE_POS, MINUTE_BITS);
        }
        /* Second */
        if (second> 0) {
            pos = writeBits(bytes, second, SECOND_POS, SECOND_BITS);
        }
        /* Fractional second */
        if (fracSeconds > 0) {
            pos = writeBits(bytes, fracSeconds, NANO_POS,
                            getFracSecondBits(precision));
        }

        int nbytes = pos / 8 + 1;
        return (nbytes < bytes.length) ? Arrays.copyOf(bytes, nbytes): bytes;
    }

    static int fracSecondToPrecision(int nanos, int precision) {
        if (precision == 0) {
            return 0;
        }
        if (precision == MAX_PRECISION) {
            return nanos;
        }
        return nanos / (int)Math.pow(10, MAX_PRECISION - precision);
    }

    /**
     * Deserialize a Timestamp value from a byte array.
     */
    static Timestamp fromBytes(byte[] bytes, int precision) {
        int[] comps = extractFromBytes(bytes, precision);
        return createTimestamp(comps);
    }

    /**
     * Extracts the components of Timestamp from a byte array including year,
     * month, day, hour, minute, second and fractional second.
     */
    private static int[] extractFromBytes(byte[] bytes, int precision) {
        int[] comps = new int[7];
        /* Year */
        comps[0] = getYear(bytes);
        /* Month */
        comps[1] = getMonth(bytes);
        /* Day */
        comps[2] = getDay(bytes);
        /* Hour */
        comps[3] = getHour(bytes);
        /* Minute */
        comps[4] = getMinute(bytes);
        /* Second */
        comps[5] = getSecond(bytes);
        /* Nano */
        comps[6] = getNano(bytes, precision);
        return comps;
    }

    /**
     * Reads the year value from byte array.
     */
    static int getYear(byte[] bytes) {
        return readBits(bytes, YEAR_POS, YEAR_BITS) - YEAR_EXCESS;
    }

    /**
     * Reads the month value from byte array.
     */
    static int getMonth(byte[] bytes) {
        return readBits(bytes, MONTH_POS, MONTH_BITS);
    }

    /**
     * Reads the day of month value from byte array.
     */
    static int getDay(byte[] bytes) {
        return readBits(bytes, DAY_POS, DAY_BITS);
    }

    /**
     * Reads the hour value from byte array.
     */
    static int getHour(byte[] bytes) {
        if (HOUR_POS < bytes.length * 8) {
            return readBits(bytes, HOUR_POS, HOUR_BITS);
        }
        return 0;
    }

    /**
     * Reads the minute value from byte array.
     */
    static int getMinute(byte[] bytes) {
        if (MINUTE_POS < bytes.length * 8) {
            return readBits(bytes, MINUTE_POS, MINUTE_BITS);
        }
        return 0;
    }

    /**
     * Reads the second value from byte array.
     */
    static int getSecond(byte[] bytes) {
        if (SECOND_POS < bytes.length * 8) {
            return readBits(bytes, SECOND_POS, SECOND_BITS);
        }
        return 0;
    }

    /**
     * Reads the nanoseconds value from byte array.
     */
    static int getNano(byte[] bytes, int precision) {
        int fracSecond = getFracSecond(bytes, precision);
        if (fracSecond > 0 && precision < MAX_PRECISION) {
            fracSecond *= (int)Math.pow(10, MAX_PRECISION - precision);
        }
        return fracSecond;
    }

    /**
     * Get the fractional seconds from byte array
     */
    static int getFracSecond(byte[] bytes, int precision) {
        if (NANO_POS < bytes.length * 8) {
            int num = getFracSecondBits(precision);
            return readBits(bytes, NANO_POS, num);
        }
        return 0;
    }

    /**
     * Returns the week number within the year where a week starts on Sunday and
     * the first week has a minimum of 1 day in this year, in the range 1 ~ 54.
     */
    public static int getWeekOfYear(Timestamp ts) {
        return toUTCDateTime(ts).get(WeekFields.SUNDAY_START.weekOfYear());
    }

    /**
     * Returns the week number within the year based on IS0-8601 where a week
     * starts on Monday and the first week has a minimum of 4 days in this year,
     * in the range 0 ~ 53.
     */
    public static int getISOWeekOfYear(Timestamp ts) {
        return toUTCDateTime(ts).get(WeekFields.ISO.weekOfYear());
    }

    /**
     * Compares 2 byte arrays ignoring the trailing bytes with zero.
     */
    static int compareBytes(byte[] bs1, byte[] bs2) {
        int size = Math.min(bs1.length, bs2.length);
        int ret = compare(bs1, bs2, size);
        if (ret != 0) {
            return ret;
        }

        ret = bs1.length - bs2.length;
        if (ret != 0) {
            byte[] bs = (ret > 0) ? bs1 : bs2;
            for (int i = size; i < bs.length; i++) {
                 if (bs[i] != (byte)0x0) {
                    return ret;
                 }
            }
        }
        return 0;
    }

    /**
     * Compares 2 byte arrays, they can be with different precision.
     */
    static int compareBytes(byte[] bs1, int precision1,
                            byte[] bs2, int precision2) {

        if (precision1 == precision2 ||
            bs1.length <= NO_FRACSECOND_BYTES ||
            bs2.length <= NO_FRACSECOND_BYTES) {

            return compareBytes(bs1, bs2);
        }

        /* Compares the date and time without fractional second part */
        assert (bs1.length > NO_FRACSECOND_BYTES);
        assert (bs2.length > NO_FRACSECOND_BYTES);
        int ret = compare(bs1, bs2, NO_FRACSECOND_BYTES);
        if (ret != 0) {
            return ret;
        }

        /* Compares the fractional seconds */
        int fs1 = getFracSecond(bs1, precision1);
        int fs2 = getFracSecond(bs2, precision2);
        int base = (int)Math.pow(10, Math.abs(precision1 - precision2));
        return (precision1 < precision2) ?
                (fs1 * base - fs2) : -1 * (fs2 * base - fs1);
    }

    private static int compare(byte[] bs1, byte[] bs2, int len) {
        assert(bs1.length >= len && bs2.length >= len);
        for (int i = 0; i < len; i++) {
            byte b1 = bs1[i];
            byte b2 = bs2[i];
            if (b1 == b2) {
                continue;
            }
            return (b1 & 0xff) - (b2 & 0xff);
        }
        return 0;
    }

    /**
     * Returns the max number of byte that represents a timestamp with given
     * precision.
     */
    static int getNumBytes(int precision) {
        return NO_FRACSECOND_BYTES + (getFracSecondBits(precision) + 7) / 8;
    }

    /**
     * Returns the max number of bits for fractional second based on the
     * specified precision.
     *    precision    max         hex           bit #    byte #
     *        1         9           0x9             4       1
     *        2         99          0x63            7       1
     *        3         999         0x3E7           10      2
     *        4         9999        0x270F          14      2
     *        5         99999       0x1869F         17      3
     *        6         999999      0xF423F         20      3
     *        7         9999999     0x98967F        24      3
     *        8         99999999    0x5F5E0FF       27      4
     *        9         999999999   0x3B9AC9FF      30      4
     */
    private static final int[] nFracSecbits = new int[] {
        0, 4, 7, 10, 14, 17, 20, 24, 27, 30
    };

    private static int getFracSecondBits(int precision) {
       return nFracSecbits[precision];
    }

    /**
     * Methods to parse Timestamp from a string and convert Timestamp to
     * a string.
     */

    /**
     * Parses the timestamp string with the specified pattern to a Date.
     *
     * @param timestampString the sting to parse
     * @param pattern the pattern of timestampString
     * @param withZoneUTC true if UTC time zone is used as default zone
     * when parse the timestamp string, otherwise local time zone is used as
     * default zone.  If the timestampString contains time zone, then the zone
     * in timestampString will take precedence over the default zone.
     *
     * @return a String representation of the value
     *
     */
    static Timestamp parseString(String timestampString,
                                 String pattern,
                                 boolean withZoneUTC) {
        return parseString(timestampString, pattern, withZoneUTC,
                           (pattern == null));
    }

    /**
     * Parses the timestamp string with default pattern and UTC zone.
     */
    public static Timestamp parseString(String timestampString) {
        return parseString(timestampString, null, true);
    }

    private static Timestamp parseString(String timestampString,
                                         String pattern,
                                         boolean withZoneUTC,
                                         boolean optionalFracSecond) {

        /*
         * If the specified pattern is the default pattern and with UTC zone,
         * then call parseWithDefaultPattern(String) to parse the timestamp
         * string in a more efficient way.
         */
        boolean optionalZoneOffset = false;
        if ((pattern == null ||
             pattern.equals(TimestampDef.DEFAULT_PATTERN)) &&
            withZoneUTC) {

            String tsStr = trimUTCZoneOffset(timestampString);
            /*
             * If no zone offset or UTC zone in timestamp string, then parse it
             * using parseWithDefaultPattern(). Otherwise, parse it with
             * DateTimeFormatter.
             */
            if (tsStr != null) {
                return parseWithDefaultPattern(tsStr);
            }
            optionalZoneOffset = true;
        }

        String fmt = (pattern == null) ? TimestampDef.DEFAULT_PATTERN : pattern;
        try {
            DateTimeFormatter dtf = getDateTimeFormatter(fmt, withZoneUTC,
                                                         optionalFracSecond,
                                                         optionalZoneOffset,
                                                         0);
            TemporalAccessor ta = dtf.parse(timestampString);
            if (!ta.isSupported(ChronoField.YEAR) ||
                !ta.isSupported(ChronoField.MONTH_OF_YEAR) ||
                !ta.isSupported(ChronoField.DAY_OF_MONTH)) {

                throw new IllegalArgumentException("The timestamp string " +
                    "must contain year, month and day");
            }

            Instant instant;
            boolean hasOffset = (ta.isSupported(ChronoField.OFFSET_SECONDS) &&
                ta.get(ChronoField.OFFSET_SECONDS) != 0);
            if (ta.isSupported(ChronoField.HOUR_OF_DAY)) {
                instant = hasOffset ? OffsetDateTime.from(ta).toInstant() :
                    Instant.from(ta);
            } else {
                instant = LocalDate.from(ta).atStartOfDay
                    ((hasOffset ? ZoneOffset.from(ta) : UTCZone)).toInstant();
            }
            return toTimestamp(instant);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + timestampString + "' with the pattern: " +
                fmt + ": " + iae.getMessage(), iae);
        } catch (DateTimeParseException dtpe) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + timestampString + "' with the pattern: " +
                fmt + ": " + dtpe.getMessage(), dtpe);
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Failed to parse the date " +
                "string '" + timestampString + "' with the pattern: " +
                fmt + ": " + dte.getMessage(), dte);
        }
    }

    /**
     * Trims the designator 'Z' or "+00:00" that represents UTC zone from the
     * Timestamp string if exists, return null if Timestamp string contains
     * non-zero offset.
     */
    private static String trimUTCZoneOffset(String ts) {
        if (ts.endsWith("Z")) {
            return ts.substring(0, ts.length() - 1);
        }
        if (ts.endsWith("+00:00")) {
            return ts.substring(0, ts.length() - 6);
        }

        if (!hasSignOfZoneOffset(ts)) {
            return ts;
        }
        return null;
    }

    /**
     * Returns true if the Timestamp string in default pattern contain the
     * sign of ZoneOffset: plus(+) or hyphen(-).
     *
     * If timestamp string in default pattern contains negative zone offset, it
     * must contain 3 hyphen(-), e.g. 2017-12-05T10:20:01-03:00.
     *
     * If timestamp  string contains positive zone offset, it must contain
     * plus(+) sign.
     */
    private static boolean hasSignOfZoneOffset(String ts) {
        if (ts.indexOf('+') > 0) {
            return true;
        }
        int pos = 0;
        for (int i = 0; i < 3; i++) {
            pos = ts.indexOf('-', pos + 1);
            if (pos < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Parses the timestamp string in format of default pattern
     * "uuuu-MM-dd[THH:mm:ss[.S..S]]" with UTC zone.
     */
    private static Timestamp parseWithDefaultPattern(String ts){

        final int[] comps = new int[7];

        /*
         * The component that is currently being parsed, starting with 0
         * for the year, and up to 6 for the fractional seconds
         */
        int comp = 0;

        int val = 0;
        int ndigits = 0;

        int len = ts.length();
        boolean isBC = (ts.charAt(0) == '-');

        for (int i = (isBC ? 1 : 0); i < len; ++i) {

            char ch = ts.charAt(i);

            if (comp < 6) {

                switch (ch) {
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    val = val * 10 + (ch - '0');
                    ++ndigits;
                    break;
                default:
                    if (ch == compSeparators[comp]) {
                        checkAndSetValue(comps, comp, val, ndigits, ts);
                        ++comp;
                        val = 0;
                        ndigits = 0;

                    } else {
                        raiseParseError(
                            ts, "invalid character '" + ch +
                            "' while parsing component " + compNames[comp]);
                    }
                }
            } else {
                assert(comp == 6);

                switch (ch) {
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    val = val * 10 + (ch - '0');
                    ndigits++;
                    break;
                default:
                    raiseParseError(
                        ts, "invalid character '" + ch +
                        "' while parsing component " + compNames[comp]);
                }
            }
        }

        /* Set the last component */
        checkAndSetValue(comps, comp, val, ndigits, ts);

        if (comp < 2) {
            raiseParseError(
                ts, "the timestamp string must have at least the 3 " +
                "date components");
        }

        if (comp == 6 && comps[6] > 0) {

            if (ndigits > MAX_PRECISION) {
                raiseParseError(
                    ts, "the fractional-seconds part contains more than " +
                    MAX_PRECISION + " digits");
            } else if (ndigits < MAX_PRECISION) {
                /* Nanosecond *= 10 ^ (MAX_PRECISION - s.length()) */
                comps[6] *= (int)Math.pow(10, MAX_PRECISION - ndigits);
            }
        }

        if (isBC) {
            comps[0] = -comps[0];
        }

        return createTimestamp(comps);
    }

    private static void checkAndSetValue(
        int[] comps,
        int comp,
        int value,
        int ndigits,
        String ts) {

        if (ndigits == 0) {
            raiseParseError(
                ts, "component " + compNames[comp] + "has 0 digits");
        }

        comps[comp] = value;
    }

    private static void raiseParseError(String ts, String err) {

        String errMsg =
            ("Failed to parse the timestamp string '" + ts +
             "' with the pattern: " + TimestampDef.DEFAULT_PATTERN + ": ");

        throw new IllegalArgumentException(errMsg + err);
    }

    /**
     * Formats the Timestamp value to a string with the specified pattern.
     *
     * @param value the Timestamp value object
     * @param pattern the pattern string
     * @param withZoneUTC true if use UTC zone to format the Timestamp value,
     * otherwise local time zone is used.
     *
     * @return the formatted string.
     */
    static String formatString(TimestampValueImpl value,
                               String pattern,
                               boolean withZoneUTC) {

        if ((pattern == null ||
             pattern.equals(TimestampDef.DEFAULT_PATTERN)) &&
            withZoneUTC == true) {

            return stringFromBytes(value.getBytes(),
                                   value.getDefinition().getPrecision());
        }
        return formatString(value.get(), pattern, withZoneUTC,
                            false /* optionalFracSecond */,
                            ((pattern == null) ?
                             value.getDefinition().getPrecision() : 0));
    }

    /**
     * Extracts timestamp information from specified byte array and build a
     * string with default pattern format: "uuuu-MM-ddTHH:mm:ss[.S..S]"
     */
    private static String stringFromBytes(byte[] bytes, int precision) {
        int[] comps = extractFromBytes(bytes, precision);
        StringBuilder sb = new StringBuilder(TimestampDefImpl.DEF_STRING_FORMAT);
        if (precision > 0) {
            sb.append(".");
            sb.append("%0");
            sb.append(precision);
            sb.append("d");
            int fs = comps[6] / (int)Math.pow(10, (MAX_PRECISION - precision));
            return String.format(sb.toString(), comps[0], comps[1], comps[2],
                                 comps[3], comps[4], comps[5], fs);
        }
        return String.format(sb.toString(), comps[0], comps[1], comps[2],
                             comps[3], comps[4], comps[5]);
    }

    /**
     * Formats the Timestamp value to a string with the specified pattern.
     *
     * @param timestamp the Timestamp object
     * @param pattern the pattern string
     * @param withZoneUTC true if use UTC zone to format the Timestamp value,
     * otherwise local time zone is used.
     *
     * @return the formatted string.
     */
    public static String formatString(Timestamp timestamp,
                                      String pattern,
                                      boolean withZoneUTC) {
        return formatString(timestamp, pattern, withZoneUTC, false, 0);
    }

    /**
     * For unit test only.
     *
     * Formats the Timestamp value to a string with the specified pattern.
     *
     * @param timestamp the Timestamp object
     * @param pattern the pattern string
     * @param withZoneUTC true if use UTC zone to format the Timestamp value,
     * otherwise local time zone is used.
     * @param optionalFracSecond true if the fractional second are optional
     * with varied length from 1 to 9.
     * @param nFracSecond the number of digits of fractional second, if
     * optionalFracSecond is true, this argument is ignored.
     *
     * @return the formatted string.
     */
    static String formatString(Timestamp timestamp,
                               String pattern,
                               boolean withZoneUTC,
                               boolean optionalFracSecond,
                               int nFracSecond) {

        String fmt = (pattern == null) ? TimestampDef.DEFAULT_PATTERN : pattern;
        try {
            ZonedDateTime zdt = toUTCDateTime(timestamp);
            return zdt.format(getDateTimeFormatter(fmt, withZoneUTC,
                                                   optionalFracSecond,
                                                   false, nFracSecond));
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Failed to format the " +
                "timestamp with pattern '" + fmt + "': " + iae.getMessage(),
                iae);
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Failed to format the " +
                "timestamp with pattern '" + fmt + "': " + dte.getMessage(),
                dte);
        }
    }

    /**
     * For unit test only.
     *
     * Formats the Timestamp value to a string using the default pattern with
     * fractional second.
     */
    static String formatString(Timestamp value) {
        return formatString(value, null, true, true, 0);
    }

    /**
     * Timestamp value related methods.
     */

    /**
     * Rounds the fractional second of Timestamp according to the specified
     * precision.
     */
    static Timestamp roundToPrecision(Timestamp timestamp, int precision) {
        if (precision == MAX_PRECISION || timestamp.getNanos() == 0) {
            return timestamp;
        }

        long seconds = getSeconds(timestamp);
        int nanos = getNanosOfSecond(timestamp);
        double base = Math.pow(10, (MAX_PRECISION - precision));
        nanos = (int)(Math.round(nanos / base) * base);
        if (nanos == (int)Math.pow(10, MAX_PRECISION)) {
            seconds++;
            nanos = 0;
        }
        Timestamp ts = createTimestamp(seconds, nanos);
        if (ts.compareTo(TimestampDefImpl.MAX_VALUE) > 0 ) {
            ts = (Timestamp)TimestampDefImpl.MAX_VALUE.clone();
            nanos = (int)((int)(ts.getNanos() / base) * base);
            ts.setNanos((int)((int)(ts.getNanos() / base) * base));
        }
        return ts;
    }

    /**
     * Returns the number of milliseconds from the Java epoch of
     * 1970-01-01T00:00:00Z.
     */
    static long toMilliseconds(Timestamp timestamp) {
        return timestamp.getTime();
    }

    /**
     * Gets the number of seconds from the Java epoch of 1970-01-01T00:00:00Z.
     */
    public static long getSeconds(Timestamp timestamp) {
        long ms = timestamp.getTime();
        return ms > 0 ? (ms / 1000) : (ms - 999)/1000;
    }

    /**
     * Gets the nanoseconds of the Timestamp value.
     */
    public static int getNanosOfSecond(Timestamp timestamp) {
        return timestamp.getNanos();
    }

    /**
     * Returns a copy of this Timestamp with the specified duration in
     * nanoseconds subtracted.
     */
    static Timestamp minusNanos(Timestamp base, long nanosToSubtract) {
        return toTimestamp(toInstant(base).minusNanos(nanosToSubtract));
    }

    /**
     * Returns a copy of this Timestamp with the specified duration in
     * nanoseconds added.
     */
    static Timestamp plusNanos(Timestamp base, long nanosToAdd) {
        return toTimestamp(toInstant(base).plusNanos(nanosToAdd));
    }

    /**
     * Returns a copy of this Timestamp with the specified duration in
     * milliseconds subtracted.
     */
    static Timestamp minusMillis(Timestamp base, long millisToSubtract) {
        return toTimestamp(toInstant(base).minusMillis(millisToSubtract));
    }

    /**
     * Returns a copy of this Timestamp with the specified duration in
     * milliseconds added.
     */
    static Timestamp plusMillis(Timestamp base, long millisToAdd) {
        return toTimestamp(toInstant(base).plusMillis(millisToAdd));
    }

    /**
     * Creates a Timestamp with given seconds since Java epoch and nanosOfSecond.
     */
    public static Timestamp createTimestamp(long seconds, int nanosOfSecond) {
        Timestamp ts = new Timestamp(seconds * 1000);
        ts.setNanos(nanosOfSecond);
        return ts;
    }

    /**
     * Creates a Timestamp from components: year, month, day, hour, minute,
     * second and nanosecond.
     */
    public static Timestamp createTimestamp(int[] comps) {

        if (comps.length < 3) {
            throw new IllegalArgumentException("Invalid timestamp " +
                "components, it should contain at least 3 components: year, " +
                "month and day, but only " + comps.length);
        } else if (comps.length > 7) {
            throw new IllegalArgumentException("Invalid timestamp " +
                "components, it should contain at most 7 components: year, " +
                "month, day, hour, minute, second and nanosecond, but has " +
                comps.length + " components");
        }

        int num = comps.length;
        for (int i = 0; i < num; i++) {
            validateComponent(i, comps[i]);
        }
        try {
            ZonedDateTime zdt = ZonedDateTime.of(comps[0],
                                                 comps[1],
                                                 comps[2],
                                                 ((num > 3) ? comps[3] : 0),
                                                 ((num > 4) ? comps[4] : 0),
                                                 ((num > 5) ? comps[5] : 0),
                                                 ((num > 6) ? comps[6] : 0),
                                                 UTCZone);
             return toTimestamp(zdt.toInstant());
        } catch (DateTimeException dte) {
            throw new IllegalArgumentException("Invalid timestamp " +
                "components: " + dte.getMessage());
        }
    }

    /**
     * Validates the component of Timestamp, the component is indexed from 0 to
     * 6 that maps to year, month, day, hour, minute, second and nanosecond.
     */
    public static void validateComponent(int index, int value) {
        switch(index) {
            case 0: /* Year */
                if (value < TimestampDefImpl.MIN_YEAR ||
                    value > TimestampDefImpl.MAX_YEAR) {
                    throw new IllegalArgumentException("Invalid year, it " +
                            "should be in range from " +
                            TimestampDefImpl.MIN_YEAR + " to "+
                            TimestampDefImpl.MAX_YEAR + ": " + value);
                }
                break;
            case 1: /* Month */
                if (value < 1 || value > 12) {
                    throw new IllegalArgumentException("Invalid month, it " +
                            "should be in range from 1 to 12: " + value);
                }
                break;
            case 2: /* Day */
                if (value < 1 || value > 31) {
                    throw new IllegalArgumentException("Invalid day, it " +
                            "should be in range from 1 to 31: " + value);
                }
                break;
            case 3: /* Hour */
                if (value < 0 || value > 23) {
                    throw new IllegalArgumentException("Invalid hour, it " +
                            "should be in range from 0 to 23: " + value);
                }
                break;
            case 4: /* Minute */
                if (value < 0 || value > 59) {
                    throw new IllegalArgumentException("Invalid minute, it " +
                            "should be in range from 0 to 59: " + value);
                }
                break;
            case 5: /* Second */
                if (value < 0 || value > 59) {
                    throw new IllegalArgumentException("Invalid second, it " +
                            "should be in range from 0 to 59: " + value);
                }
                break;
            case 6: /* Nanosecond */
                if (value < 0 || value > 999999999) {
                    throw new IllegalArgumentException("Invalid second, it " +
                            "should be in range from 0 to 999999999: " + value);
                }
                break;
        }
    }

    /**
     * Converts Timestamp to Instant
     */
    private static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(getSeconds(timestamp),
                                     getNanosOfSecond(timestamp));
    }

    /**
     * Converts Timestamp to ZonedDataTime at UTC zone.
     */
    private static ZonedDateTime toUTCDateTime(Timestamp timestamp) {
        return toInstant(timestamp).atZone(UTCZone);
    }

    /**
     * Converts a Instant object to Timestamp
     */
    private static Timestamp toTimestamp(Instant instant) {
        return createTimestamp(instant.getEpochSecond(), instant.getNano());
    }

    /**
     * Stores a int value to the byte buffer from the given position, returns
     * the the last position written.
     */
    private static int writeBits(byte[] bytes, int value, int pos, int len) {
        assert(value > 0 && pos + len <= bytes.length * 8);
        int ind = pos / 8;
        int lastPos = 0;
        for (int i = 0; i < len; i++) {
            int bi = (pos + i) % 8;
            if ((value & (1 << (len - i - 1))) != 0) {
                bytes[ind] |= 1 << (7 - bi);
                lastPos = pos + i;
            }
            if (bi == 7) {
                ind++;
            }
        }
        return lastPos;
    }

    /**
     * Reads the number of bits from byte array starting from the given
     * position, store them to a int and return.
     */
    private static int readBits(byte[] bytes, int pos, int len) {
        int value = 0;
        int ind = pos / 8;
        for (int i = 0; i < len && ind < bytes.length; i++) {
            int bi = (i + pos) % 8;
            if ((bytes[ind] & (1 << (7 - bi))) != 0) {
               value |= 1 << (len - 1 - i);
            }
            if (bi == 7) {
                ind++;
            }
        }
        return value;
    }

    /**
     * Returns the DateTimeFormatter with the given pattern.
     */
    private static
        DateTimeFormatter getDateTimeFormatter(String pattern,
                                               boolean withZoneUTC,
                                               boolean optionalFracSecond,
                                               boolean optionalOffsetId,
                                               int nFracSecond) {

        DateTimeFormatterBuilder dtfb = new DateTimeFormatterBuilder();
        dtfb.appendPattern(pattern);
        if (optionalFracSecond) {
            dtfb.optionalStart();
            dtfb.appendFraction(ChronoField.NANO_OF_SECOND,
                                0, MAX_PRECISION, true);
            dtfb.optionalEnd();
        } else {
            if (nFracSecond > 0) {
                dtfb.appendFraction(ChronoField.NANO_OF_SECOND,
                                    nFracSecond, nFracSecond, true);
            }
        }
        if (optionalOffsetId) {
            dtfb.optionalStart();
            dtfb.appendOffset("+HH:MM","Z");
            dtfb.optionalEnd();
        }
        return dtfb.toFormatter().withZone
                (withZoneUTC ? UTCZone : ZoneId.systemDefault());
    }
}

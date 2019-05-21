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

package oracle.kv.impl.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Centralize formatting, to make it as uniform as possible.
 * TODO: make this configurable, based on a Locale
 */
public class FormatUtils {

    public static String defaultTimeZone = "UTC";
    private static final TimeZone tz = TimeZone.getTimeZone(defaultTimeZone);

    public static TimeZone getTimeZone() {
        return tz;
    }

    /**
     * This is the format used by the NoSQL DB logger utilities
     */
    public static DateFormat getDateTimeAndTimeZoneFormatter() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
        df.setTimeZone(tz);
        return df;
    }

    /**
     * This is used by the NoSQL DB stats
     */
    public static String formatTime(long time) {
        DateFormat formatter =  new SimpleDateFormat("HH:mm:ss.SSS z");
        formatter.setTimeZone(tz);
        return formatter.format(new Date(time));
    }

    /** 
     * Used to produce a UTC time stamp in the format of yy-MM-dd HH:mm:ss for
     * the .perf files.
     */
    public static String formatPerfTime(long time) {
        DateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
        df.setTimeZone(tz);
        return df.format(new Date(time));
    }

    /**
     * This is used by the admin and planner
     */
    public static String formatDateAndTime(long time) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        formatter.setTimeZone(tz);
        return formatter.format(new Date(time));
    }
}

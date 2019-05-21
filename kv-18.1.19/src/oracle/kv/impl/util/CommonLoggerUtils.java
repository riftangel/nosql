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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.LogManager;

/**
 * Logger utilities common to both client and server side.
 */
public class CommonLoggerUtils {

    /**
     * Get the value of a specified Logger property.
     */
    public static String getLoggerProperty(String property) {
        LogManager mgr = LogManager.getLogManager();
        return mgr.getProperty(property);
    }

    /**
     * Utility method to return a String version of a stack trace
     */
    public static String getStackTrace(Throwable t) {
        if (t == null) {
            return "";
        }

        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        stackTrace = stackTrace.replaceAll("&lt", "<");
        stackTrace = stackTrace.replaceAll("&gt", ">");

        return stackTrace;
    }

    /**
     * Modify the stack trace of the specified exception to append the current
     * call stack.  Call this method to include information about the current
     * stack when rethrowing an exception that was originally thrown and caught
     * in another thread.  Note that this method will have no effect on
     * exceptions that are not writable.
     *
     * @param exception the exception
     */
    public static void appendCurrentStack(Throwable exception) {
        checkNull("exception", exception);
        final StackTraceElement[] existing = exception.getStackTrace();
        final StackTraceElement[] current = new Throwable().getStackTrace();
        final StackTraceElement[] updated =
            new StackTraceElement[existing.length + current.length];
        System.arraycopy(existing, 0, updated, 0, existing.length);
        System.arraycopy(current, 0, updated, existing.length, current.length);
        exception.setStackTrace(updated);
    }
}

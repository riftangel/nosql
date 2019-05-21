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

package oracle.kv.impl.async.perf;

/**
 * Collects and shows the string representation of the performance of recently
 * sampled dialogs
 */
public class DialogDetail {

    private final StringBuilder sb = new StringBuilder();

    /**
     * Updates the string with the dialog perf if the dialog is sampled.
     */
    public void update(DialogPerf dperf) {
        if (dperf.isSampled()) {
            synchronized(this) {
                sb.append("\t\t").
                    append(dperf.getDetailedTimeString(true)).
                    append("\n");
            }
        }
    }

    /**
     * Gets the string representation of the performance details of sampled dialogs.
     */
    public synchronized String get() {
        String result = (sb.length() == 0) ? "" :
            String.format("[sampled detail]\n%s", sb.toString());
        reset();
        return result;
    }

    private synchronized void reset() {
        sb.delete(0, sb.length());
    }

}

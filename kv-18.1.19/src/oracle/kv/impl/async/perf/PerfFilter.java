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
 * Filters for perf.
 */
public class PerfFilter {

    /**
     * A sample rate determining how often we should profile a dialog.
     *
     * The system property sets the sample rate. The rate is the count of
     * dialogs (inclusive for the profiled dialogs) before we do profile again.
     * The rate should be set as an integer and is normalized to the next power
     * of 2 of the set value. If the rate is set to negative, it is normalized
     * to zero. A "zero" rate means sampling is disabled. A "one" rate means
     * sampling every dialog.
     */
    public static final String SAMPLE_RATE_PROPERTY =
        "oracle.kv.async.perf.samplerate";
    public static final int DEFAULT_SAMPLE_RATE = 1024;

    /**
     * A filter for long values.
     */
    public interface LongFilt {

        /**
         * Checks if the value should be included.
         *
         * @param value the value
         * @return whether to include the value
         */
        boolean accept(long value);
    }

    /**
     * A filter that accepts if the argument is a multiple of the sample rate.
     *
     * Sample rate is ensure to be a power of 2 for optimization. The check is
     * then simply comparing the number of trailing zeros.
     */
    public static final LongFilt SYSTEM_SAMPLING = new LongFilt() {
        @Override
        public boolean accept(long count) {
            if (!PerfCondition.SYSTEM_PERF_ENABLED.holds()) {
                return false;
            }
            if (sampleRateNextPow2NumTrailingZeros == -1) {
                return false;
            }
            return (Long.numberOfTrailingZeros(count)
                    >= sampleRateNextPow2NumTrailingZeros);
        }
    };

    /**
     * Sample rate set through system property SAMPLE_RATE_PROPERTY represented
     * by the number of trailing zeros of the next power of 2 of the set value.
     */
    private static final int sampleRateNextPow2NumTrailingZeros =
        getSampleRateNextPow2NumTrailingZeros();

    private static int getSampleRateNextPow2NumTrailingZeros() {
        int v = Math.max(0, Integer.getInteger(
                    SAMPLE_RATE_PROPERTY, DEFAULT_SAMPLE_RATE));
        return (v == 0) ?
            -1 : 32 - Integer.numberOfLeadingZeros(v - 1);
    }
}


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

package oracle.kv.impl.util.sklogger;

import java.util.regex.Pattern;

/**
 * StatsData is the root of all monitor stats class.
 */
public abstract class StatsData {
    // convenience constants
    /**
     * Number of nanoseconds in a second.
     */
    public static final double NANOSECONDS_PER_SECOND = 1E9;
    /**
     * Number of milliseconds in a second.
     */
    public static final double MILLISECONDS_PER_SECOND = 1E3;

    /*
     * Stats common properties and field keys that will be used when process
     * and/or log stats.
     */
    // common properties
    public static final String SHARD = "shard";
    public static final String RESOURCE = "resource";
    public static final String COMPONENT_ID = "componentId";
    // RateMetric fields
    public static final String DURATION_NAME = "duration";
    // Counter fields
    public static final String COUNT_NAME = "count";
    public static final String THROUGHPUT_NAME = "throughputPerSecond";
    // Gauge fields
    public static final String GAUGE_NAME = "value";
    // Quantile fields
    public static final String QUANTILE_NAME = "quantile";
    // SizeQuantile fields
    public static final String SUM_NAME = "sum";
    // PerfQuantile fields
    public static final String OPERATION_COUNT_NAME = "operationCount";
    public static final String REQUEST_COUNT_NAME = "requestCount";
    public static final String OVERFLOW_COUNT_NAME = "overflowCount";
    public static final String OPERATION_MIN_NAME = "operationMin";
    public static final String OPERATION_MAX_NAME = "operationMax";
    public static final String OPERATION_AVG_NAME = "operationAvg";
    // Histogram fields
    public static final String HISTOGRAM_NAME = "histogram";

    public static final String DELIMITER = "_";

    protected final String statsName;

    private static final Pattern METRIC_NAME_RE =
        Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    /**
     * @param statsName is stats name that will be used when stats is
     * registered and/or processed.
     */
    StatsData(String statsName) {
        checkStatsName(statsName);
        this.statsName = statsName;
    }

    /**
     * @return The name of the stats.
     */
    public String getStatsName() {
        return statsName;
    }

    /**
     * Throw an exception if the stats name is invalid.
     */
    private void checkStatsName(String name) {
        if (name == null || !METRIC_NAME_RE.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid stats name: " + name);
        }
    }

    /**
     * StatsData types.
     */
    public enum Type {
        COUNTER, LONG_GAUGE, CUSTOM_GAUGE, SIZE_QUANTILE, PERF_QUANTILE,
        HISTOGRAM, EVENT, UNTYPED,
    }
}

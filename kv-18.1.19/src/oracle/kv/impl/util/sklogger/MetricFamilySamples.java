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

import java.io.Serializable;
import java.util.List;

import oracle.kv.impl.util.sklogger.StatsData.Type;

/**
 * A metric, and all of its samples.
 */
public class MetricFamilySamples<T extends Metric.Result>
    implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long reportTimeMs;
    private final String name;
    private final Type type;
    private final List<String> labelNames;
    private final List<Sample<T>> samples;

    public MetricFamilySamples(String name,
                               Type type,
                               List<String> labelNames,
                               List<Sample<T>> samples) {
        this.name = name;
        this.type = type;
        this.labelNames = labelNames;
        this.samples = samples;
        this.reportTimeMs = System.currentTimeMillis();
    }

    public long getReportTimeMs() {
        return reportTimeMs;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public List<String> getLabelNames() {
        return labelNames;
    }

    public List<Sample<T>> getSamples() {
        return samples;
    }

    /**
     * A single Sample.
     */
    public static class Sample<T extends Metric.Result>
        implements Serializable {

        private static final long serialVersionUID = 1L;

        /* Must have same length as labelNames. */
        public final List<String> labelValues;
        public final T dataValue;

        public Sample(List<String> labelValues, T dataValues) {
            this.labelValues = labelValues;
            this.dataValue = dataValues;
        }
    }
}

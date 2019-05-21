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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A special {@link Metric} data that has rate of change.
 */
public abstract class RateMetric<T extends RateMetric.RateResult,
                                     E extends RateMetric.Element<T>>
    extends Metric<E> {

    /**
     * A default watcher name.
     */
    public static final String DEFAULT_WATCHER = "defaultWatcher";

    /**
     * {@link Metric#Metric(String, String...)}.
     */
    public RateMetric(final String name, String... labelNames) {
        super(name, labelNames);
    }

    /**
     * {@link Metric.Element} for {@link RateMetric}.
     */
    public abstract static class Element<T> implements Metric.Element {
        protected long initialTime;

        protected Element() {
            initialTime = System.nanoTime();
        }

        /**
         * Calculate this element rate of change of the whole lifetime.<p>
         * Implementing this method must consider that rate and set element
         * value might be involved with different threads simultaneously and
         * synchronize accordingly.
         */
        public abstract T rate();

        /**
         * Calculate this element rate of change for the specified watcherName
         * after last call. If it is the first time for this watcherName, it
         * will return rate change of the whole lifetime.<p>
         * Implementing this method must consider that rate and set element
         * value might be involved with different threads simultaneously and
         * synchronize accordingly.
         * But we don't expect the same watcherName will execute
         * collectSinceLastTime at the same time.<p>
         * TODO do we need a way to release watcher or set max watcher?
         */
        public abstract T rateSinceLastTime(String watcherName);
    }

    /**
     * Parent class for saving collected {@link RateMetric} rate change
     * result.
     */
    public abstract static class RateResult extends Metric.Result {

        private static final long serialVersionUID = 1L;

        protected final long duration;

        public RateResult(long duration) {
            this.duration = duration;
        }

        public long getDuration() {
            return duration;
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put(DURATION_NAME, duration);
            return map;
        }
    }

    /**
     * {@link Element#rate()}
     * Note: as a convenience method, it only works for no label metric.
     */
    public T rate() {
        return noLabelsElement.rate();
    }

    /**
     * {@link Element#rateSinceLastTime(String)}
     * Note: as a convenience method, it only works for no label metric.
     */
    public T rateSinceLastTime(String watcherName) {
        return noLabelsElement.rateSinceLastTime(watcherName);
    }

    /**
     * Collect a samples family for the specified watcherName after last call,
     * that including all label values and its relevant samples value. If it is
     * the first time for this watcherName, it will collect for the whole
     * lifetime.
     */
    public abstract MetricFamilySamples<T>
        collectSinceLastTime(String watcherName);

    /**
     * A common implement of {@link Metric#collect()}.
     */
    protected MetricFamilySamples<T> collect(Type type) {

        final List<MetricFamilySamples.Sample<T>> samples =
            new ArrayList<MetricFamilySamples.Sample<T>>(elements.size());
        for (Map.Entry<String[], E> e : elements.entrySet()) {
            samples.add(
                new MetricFamilySamples.Sample<T>(Arrays.asList(e.getKey()),
                                                  e.getValue().rate()));
        }
        return new MetricFamilySamples<T>(statsName, type,
                                          labelNames, samples);
    }

    /**
     * A common implement of {@link RateMetric#collectSinceLastTime(String)}.
     */
    protected MetricFamilySamples<T>
        collectSinceLastTime(Type type, String watcherName) {

        final List<MetricFamilySamples.Sample<T>> samples =
            new ArrayList<MetricFamilySamples.Sample<T>>(elements.size());
        for (Map.Entry<String[], E> e : elements.entrySet()) {
            samples.add(
                new MetricFamilySamples.Sample<T>(
                    Arrays.asList(e.getKey()),
                    e.getValue().rateSinceLastTime(watcherName)));
        }
        return new MetricFamilySamples<T>(statsName, type,
                                          labelNames, samples);
    }
}

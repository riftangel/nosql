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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metric is one kind of monitor stats data. Metric can be labeled with multiple
 * names and support collecting samples of this metric data.
 * Implementing classes must consider that they might be involved with
 * different threads simultaneously and synchronize accordingly. One use case
 * with multiple threads has a timer that calls collect() periodically,
 * while other threads are setting the values of the metrics asynchronously.
 */
public abstract class Metric<E extends Metric.Element> extends StatsData {

    /**
     * Metric tagged label names.
     */
    protected final List<String> labelNames;
    /**
     * The map for different label values and its relevant
     * {@link Metric.Element}.
     */
    protected TreeMap<String[], E> elements;
    /**
     * Most case, metrics won't be tagged with labels. So here is a convenience
     * variable for referencing no label metric's {@link Metric.Element}
     * directly.
     */
    protected E noLabelsElement;

    /**
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param labelNames to set metric tag names that is optional for Metric
     */
    public Metric(String name, String... labelNames) {
        super(name);
        for (String labelName: labelNames) {
            checkMetricLabelName(labelName);
        }
        this.labelNames = Arrays.asList(labelNames);
        elements = new TreeMap<String[], E>(new StringArrayComparator());
    }

    /**
     * Return the {@link Metric.Element} with the given label values,
     * creating a new {@link Metric.Element} it if needed.
     * <p>
     * Must be passed the same number of labels are were passed to #labelNames.
     * For better performance, it will use and save the labelValues array
     * without deep copy, so NEVERE change any element in the labelValues array
     * later.
     */
    public E labels(String... labelValues) {
        //TODO do we support other type of label values, such as boolean?
        if (labelValues.length != labelNames.size()) {
            throw new IllegalArgumentException("Incorrect number of labels.");
        }
        for (String label : labelValues) {
            if (label == null) {
                throw new IllegalArgumentException("Label cannot be null.");
            }
        }
        E e = elements.get(labelValues);
        if (e != null) {
            return e;
        }
        /*
         * It is fine to lock, there are very limited range of labelValues to
         * insert in each Metric.
         */
        synchronized (this) {
            e = elements.get(labelValues);
            if (e != null) {
                return e;
            }
            e = newElement();
            final TreeMap<String[], E> newElements =
                new TreeMap<String[], E>(elements);
            newElements.put(labelValues, e);
            elements = newElements;
            return e;
        }
    }

    /**
     * Initialize the element with no labels.
     */
    protected void initializeNoLabelsElement() {
        // Initialize metric if it has no labels.
        if (labelNames.size() == 0) {
            noLabelsElement = labels();
        }
    }

    /**
     * Throw an exception if the metric label name is invalid.
     */
    private void checkMetricLabelName(String name) {
        if (name == null || name.contains(".")) {
            throw new IllegalArgumentException(
                "Invalid metric label name: " + name);
        }
    }

    /**
     * It is called when generating new Element for new Metric label values.
     */
    protected abstract E newElement();

    /**
     * Metric maintain one Element for each different label values.<p>
     * Implementing this interface must consider that get and set value might
     * be involved with different threads simultaneously and synchronize
     * accordingly.
     */
    public interface Element { }

    /**
     * Collect a samples family of the whole lifetime that including all label
     * values and its relevant samples value.
     */
    public abstract MetricFamilySamples<?> collect();

    /**
     * Register the Collector with the given registry.
     */
     public Metric<?> registerTo(MetricRegistry registry) {
         registry.addMetricData(this);
         return this;
     }

    /**
     * Parent class for saving collected {@link Metric} result.
     */
    public abstract static class Result implements Serializable {

        public static final long serialVersionUID = 1L;

        /**
         * Produce a key/value pairs map for Result.
         */
        public abstract Map<String, Object> toMap();
    }

    private static class StringArrayComparator implements Comparator<String[]> {

        @Override
        public int compare(String[] s1, String[] s2) {
            /*
             * We already know both of the two arrays are not null and they are
             * the same length.
             */

            if (s1.equals(s2)) {
                return 0;
            }
            for (int i = 0; i < s1.length; ++i) {
                final int result = s1[i].compareTo(s2[i]);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }
}

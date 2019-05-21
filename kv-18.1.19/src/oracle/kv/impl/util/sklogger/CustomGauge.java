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
import java.util.List;
import java.util.Map;

/**
 * A CustomGauge as one kind of {@link Metric} is useful when Gauge value is
 * not possible to directly instrument code, as it is not in your control, such
 * as gauge System memory value.
 * <pre>
 * Usage example:
 * <code>
 * private static class TestCalculator implements GaugeCalculator {
 *     &#64;Override
 *     public List<GaugeResult> getValuesList() {
 *         Map<String, Object> metrics = new HashMap<String, Object>(2);
 *         metrics.put("free", Runtime.getRuntime().freeMemory());
 *         metrics.put("total", Runtime.getRuntime().maxMemory());
 *         return Arrays.asList(new GaugeResult(metrics));
 *     }
 * }
 * CustomGauge memory = new CustomGauge("memory", new TestCalculator());
 * memory.getValuesList();
 * </code>
 * </pre>
 */
public class CustomGauge extends Metric<CustomGauge.GaugeCalculator> {

    private final static List<String> EMPTY_LABEL_VALUES = Arrays.asList();
    private final GaugeCalculator gaugeCalculator;

    /**
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param gaugeCalculator is used to get the current gauge value map.
     */
    public CustomGauge(final String name,
                       GaugeCalculator gaugeCalculator) {

        super(name);
        this.gaugeCalculator = gaugeCalculator;
    }

    /**
     * Implement the interface to get current GaugeResult list.
     */
    public interface GaugeCalculator extends Metric.Element {

        /**
         * Get current GaugeResult list.
         */
        List<GaugeResult> getValuesList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected GaugeCalculator newElement() {
        return gaugeCalculator;
    }

    // Convenience methods.

    /**
     * {@link GaugeCalculator#getValuesList}.
     */
    public List<GaugeResult> getValuesList() {
        try {
            return gaugeCalculator.getValuesList();
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetricFamilySamples<GaugeResult> collect() {
        List<GaugeResult> gaugeResultList = null;
        try {
            gaugeResultList = gaugeCalculator.getValuesList();
        } catch (Exception e) {
        }
        if (gaugeResultList == null) {
            return null;
        }
        final List<MetricFamilySamples.Sample<GaugeResult>> samples =
            new ArrayList<MetricFamilySamples.Sample<GaugeResult>>(
                gaugeResultList.size());
        for (GaugeResult e : gaugeResultList) {
            samples.add(
                new MetricFamilySamples.Sample<GaugeResult>(
                    EMPTY_LABEL_VALUES, e));
        }
        return new MetricFamilySamples<GaugeResult>(statsName,
                                                    Type.CUSTOM_GAUGE,
                                                    labelNames, samples);
    }

    /**
     * Saving collected {@link CustomGauge} result.
     */
    public static class GaugeResult extends Metric.Result {

        private static final long serialVersionUID = 1L;

        private Map<String, Object> map;

        public GaugeResult(Map<String, Object> map) {
            this.map = map;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Map<String, Object> toMap() {
            return map;
        }
    }
}

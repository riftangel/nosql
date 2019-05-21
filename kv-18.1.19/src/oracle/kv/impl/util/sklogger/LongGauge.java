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
import java.util.concurrent.atomic.AtomicLong;

/**
 * A LongGauge as one kind of {@link Metric} is a long value that fluctuates
 * over time, whose value is unrelated to a previously reported value.
 * <pre>
 * Usage example:
 * {@code
 * LongGauge activeRequest = new LongGauge("activeRequest", "opType");
 * add one read request.
 * activeRequest.label("read").incrValue();
 *
 * Decrease one read request.
 * activeRequest.label("read").decrValue();
 *
 * add one write request.
 * activeRequest.label("write").incrValue();
 *
 * Decrease one write request.
 * activeRequest.label("write").decrValue();
 *
 * get current active read request.
 * activeRequest.label("read").getValue();
 * }
 * </pre>
 */
public class LongGauge extends Metric<LongGauge.Element> {

    /**
     * {@link Metric#Metric(String, String...)}.
     */
    public LongGauge(final String name, String... labelNames) {
        super(name, labelNames);
        initializeNoLabelsElement();
    }

    /**
     * {@link Metric.Element} for {@link LongGauge}.
     */
    public static final class Element implements Metric.Element {

        //TODO change to LongAdder if this package move to JAVA 8.
        private final AtomicLong value;

        private Element() {
            value = new AtomicLong(0);
        }

        /**
         * Return the current value of LongGauge Element.
         */
        public long getValue() {
            return value.get();
        }

        /**
         * Add one to the LongGauge Element.
         */
        public void incrValue() {
            value.incrementAndGet();
        }

        /**
         * Add delta value to LongGauge Element.
         */
        public void incrValue(long delta) {
            value.addAndGet(delta);
        }

        /**
         * Minus one to LongGauge Element.
         */
        public void decrValue() {
            value.decrementAndGet();
        }

        /**
         * Minus delta value to LongGauge Element.
         */
        public void decrValue(long delta) {
            value.addAndGet(-delta);
        }

        /**
         * Set the newValue to LongGauge Element.
         */
        public void setValue(long newValue) {
            value.set(newValue);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Element newElement() {
        return new Element();
    }

    // Convenience methods.

    /**
     * {@link Element#setValue(long)}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void setValue(long newValue) {
        noLabelsElement.setValue(newValue);
    }

    /**
     * {@link Element#getValue}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public long getValue() {
        return noLabelsElement.getValue();
    }

    /**
     * {@link Element#incrValue}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void incrValue() {
        noLabelsElement.incrValue();
    }

    /**
     * {@link Element#incrValue(long)}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void incrValue(long delta) {
        noLabelsElement.incrValue(delta);
    }

    /**
     * {@link Element#decrValue}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void decrValue() {
        noLabelsElement.decrValue();
    }

    /**
     * {@link Element#decrValue(long)}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void decrValue(long delta) {
        noLabelsElement.decrValue(delta);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetricFamilySamples<GaugeResult> collect() {

        final List<MetricFamilySamples.Sample<GaugeResult>> samples =
            new ArrayList<MetricFamilySamples.Sample<GaugeResult>>(
                elements.size());
        for (Map.Entry<String[], Element> e : elements.entrySet()) {
            samples.add(
                new MetricFamilySamples.Sample<GaugeResult>(
                    Arrays.asList(e.getKey()),
                    new GaugeResult(e.getValue().getValue())));
        }
        return new MetricFamilySamples<GaugeResult>(statsName, Type.LONG_GAUGE,
                                                    labelNames, samples);
    }

    /**
     * Saving collected {@link LongGauge} result.
     */
    public static class GaugeResult extends Metric.Result {

        private static final long serialVersionUID = 1L;

        private long gaugeVal;

        public GaugeResult(long gaugeVal) {
            this.gaugeVal = gaugeVal;
        }

        public long getGaugeVal() {
            return gaugeVal;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put(GAUGE_NAME, gaugeVal);
            return map;
        }
    }
}

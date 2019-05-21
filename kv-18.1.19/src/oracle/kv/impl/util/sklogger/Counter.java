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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Counter as one kind of {@link RateMetric} is an Long value that
 * typically increases over time.
 * <pre>
 * Usage example:
 * {@code
 * Counter requestCount = new Counter("requestCount", "opType");
 * add one read request.
 * requestCount.label("read").incrValue();
 *
 * add one write request.
 * requestCount.label("write").incrValue();
 *
 * get total read request count.
 * requestCount.label("read").getValue();
 *
 * get delta read request count change since last call for watcherName.
 * requestCount.label("read").rateSinceLastTime("watcherName")
 * }
 * </pre>
 */
public class Counter extends
    RateMetric<Counter.RateResult, Counter.Element> {

    /**
     * {@link RateMetric#RateMetric(String, String...)}.
     */
    public Counter(final String name, String... labelNames) {
        super(name, labelNames);
        initializeNoLabelsElement();
    }

    /**
     * {@link Metric.Element} for {@link Counter}.
     */
    public static final class Element extends RateMetric.Element<RateResult> {
        private final ConcurrentHashMap<String, HistoryItem> watchers;
        //TODO change to LongAdder if this package move to JAVA 8.
        private final AtomicLong value;

        private Element() {
            value = new AtomicLong(0);
            watchers = new ConcurrentHashMap<String, HistoryItem>();
        }

        /**
         * Return the Counter Element current value.
         */
        public long getValue() {
            return value.get();
        }

        /**
         * Add one to the Counter Element.
         */
        public void incrValue() {
            value.incrementAndGet();
        }

        /**
         * Add delta value to Counter Element.
         */
        public void incrValue(long delta) {
            value.addAndGet(delta);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RateResult rate() {
            final long currentTime = System.nanoTime();
            final long currentValue = getValue();
            final long lastTime = initialTime;
            final long lastValue = 0;
            return new RateResult(currentTime - lastTime,
                                  currentValue - lastValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RateResult rateSinceLastTime(String watcherName) {
            final long currentTime = System.nanoTime();
            final long currentValue = getValue();
            long lastTime = initialTime;
            long lastValue = 0;
            final HistoryItem historyItem = watchers.get(watcherName);
            if (historyItem != null) {
                lastTime = historyItem.lastTime;
                lastValue = historyItem.lastValue;
                historyItem.lastTime = currentTime;
                historyItem.lastValue = currentValue;
            } else {
                watchers.put(watcherName,
                             new HistoryItem(currentTime, currentValue));
            }
            return new RateResult(currentTime - lastTime,
                                  currentValue - lastValue);
        }

        /* A structure to record Counter history item for each watcher */
        private static final class HistoryItem {
            private long lastTime;
            private long lastValue;

            private HistoryItem(long time, long value) {
                lastTime = time;
                lastValue = value;
            }
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
     * {@link Element#getValue()}
     * Note: as a convenience method, it only works for no label Counter.
     */
    public long getValue() {
        return noLabelsElement.getValue();
    }

    /**
     * {@link Element#incrValue()}
     * Note: as a convenience method, it only works for no label Counter.
     */
    public void incrValue() {
        noLabelsElement.incrValue();
    }

    /**
     * {@link Element#incrValue(long)}
     * Note: as a convenience method, it only works for no label Counter.
     */
    public void incrValue(long delta) {
        noLabelsElement.incrValue(delta);
    }

    /**
     * Saving collected {@link Counter} rate change result.
     */
    public static class RateResult extends RateMetric.RateResult {

        private static final long serialVersionUID = 1L;

        private final long count;

        public RateResult(long duration, long count) {
            super(duration);
            this.count = count;
        }

        public long getCount() {
            return count;
        }

        public double getThroughputPerSecond() {
            final double seconds = duration / NANOSECONDS_PER_SECOND;
            if (seconds == 0) {
                return 0.0;
            }
            return count / seconds;
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = super.toMap();
            map.put(COUNT_NAME, count);
            map.put(THROUGHPUT_NAME, getThroughputPerSecond());
            return map;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetricFamilySamples<RateResult> collect() {
        return collect(Type.COUNTER);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetricFamilySamples<RateResult>
        collectSinceLastTime(String watcherName) {

        return collectSinceLastTime(Type.COUNTER, watcherName);
    }
}

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
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * <pre>
 * A Histogram as one kind of {@link RateMetric} is Long array buckets that
 * record counts in each buckets. If observed value is between
 * ( bucket[N-1], bucket[N] ] and then the observed value fall into N bucket
 * and add one count in bucket N. <p>
 * If your service runs replicated with a number of instances, and you want to
 * get an overview 95th/99th of some metrics, then you might want to use this
 * Histogram metric to help estimate 95th/99th value.
 * You collect Histogram metric from every single one of them, and then you
 * want to aggregate everything into an overall 95th percentile.
 *
 * For example:
 * we create exponential 12 buckets: 1, 2, 4, 8, 16, 32, ... 1024, 2048, 4096
 * for each proxy instance. And we query each proxy instance at the same time
 * to get the count values for these 12 buckets.
 * Let's say:
 * Instance 1:
 * 0-1    1000
 * 1-2    2000
 * 2-4    3000
 * 4-8    500
 * 8-16   100
 * 16-
 * ...    50
 * 4096
 *
 * Total: 6650
 *
 * Instance 2:
 * 0-1    3000
 * 1-2    1000
 * 2-4    500
 * 4-8    100
 * 8-16   100
 * 16-
 * ...    30
 * 4096
 *
 * Total: 4730
 *
 * The total is 6650 + 4730 = 11380, 95th count is 11380 * 0.95 = 10811,
 * 0-1 count is 3000+1000=4000, 1-2 count is 1000+2000=3000,
 * 2-4 count is 3000+ 500=3500, so 0-4 count is 4000+3000+3500 = 10500;
 * Still less than target 95th count 10811. 4-8 count is 500 + 100 = 600;
 * so 0-8 count is 10500 + 600 = 11000 that is bigger than
 * 10811(target 95th count). Now we know 95th latency is between 4-8, and if we
 * assume the latency values are even, then we can estimate 95th/99th latency.
 * (10811 - 10500) / 600 * (8-4) + 4  that approximate to 6. So we can say the
 * two proxy instances 95th latency is 6 ms.
 *
 * </pre>
 * TODO implement class to estimate 95th/99th from multiple Histogram.
 * <pre>
 * Usage example:
 * {@code
 * Histogram requestLatency = new Histogram("requestLatency", "opType");
 * add one observed read request latency.
 * requestLatency.label("read").observe(latency);
 *
 * add one observed write request latency.
 * requestLatency.label("write").observe(latency);
 *
 * get read request latency each buckets values since last call for watcherName.
 * requestCount.label("read").rateSinceLastTime("watcherName")
 * }
 * </pre>
*/
public class Histogram extends
    RateMetric<Histogram.RateResult, Histogram.Element> {

    private final long[] upperBounds;

    /**
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param upperBounds set the upper bounds for each buckets. If observed
     * value is between ( bucket[N-1], bucket[N] ] and then the observed fall
     * into N bucket and increase one in bucket N.
     * @param labelNames to set metric tag names that is optional for Metric
     *
     * {@link RateMetric#RateMetric(String, String...)}
     */
    public Histogram(final String name,
                     long[] upperBounds,
                     String... labelNames) {

        super(name, labelNames);
        if (upperBounds == null || upperBounds.length == 0) {
            throw new IllegalArgumentException("upperBounds cannot be empty");
        }
        // Append a max bucket if it's not already there.
        if (upperBounds[upperBounds.length - 1] != Long.MAX_VALUE) {
            final long[] tmp = new long[upperBounds.length + 1];
            System.arraycopy(upperBounds, 0, tmp, 0, upperBounds.length);
            tmp[upperBounds.length] = Long.MAX_VALUE;
            this.upperBounds = tmp;
        } else {
            this.upperBounds = upperBounds;
        }
        initializeNoLabelsElement();
    }

    /**
     * {@link Metric.Element} for {@link Histogram}.
     */
    public static final class Element extends RateMetric.Element<RateResult> {
        private final long[] upperBounds;
        private final AtomicLongArray cumulativeCounts;
        private final ConcurrentHashMap<String, HistoryItem> watchers;

        private Element(long... bounds) {
            upperBounds = bounds;
            cumulativeCounts = new AtomicLongArray(upperBounds.length);
            watchers = new ConcurrentHashMap<String, HistoryItem>();
        }

        /**
         * Observe the given value.
         */
        public void observe(long v) {
            observe(v, 1);
        }

        /**
         * Observe the given value with N times.
         */
        public void observe(long v, int times) {
            for (int i = 0; i < upperBounds.length; ++i) {
                // The last bucket is max value, so we always increment.
                if (v <= upperBounds[i]) {
                    cumulativeCounts.addAndGet(i, times);
                    break;
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RateResult rate() {
            final long[] intervalCounts = new long[upperBounds.length];
            long intervalTotalCount = 0;
            for (int i = 0; i < upperBounds.length; i++) {
                intervalCounts[i] = cumulativeCounts.get(i);
                intervalTotalCount += intervalCounts[i];
            }
            return new RateResult(System.nanoTime() - initialTime,
                                  intervalTotalCount, upperBounds,
                                  intervalCounts);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RateResult rateSinceLastTime(String watcherName) {
            final long currentTime = System.nanoTime();
            long lastTime = initialTime;
            long[] lastHistogramCounts;
            final HistoryItem historyItem = watchers.get(watcherName);
            if (historyItem != null) {
                lastTime = historyItem.lastTime;
                lastHistogramCounts = historyItem.lastHistogramCounts;
                historyItem.lastTime = currentTime;
            } else {
                lastHistogramCounts = new long[upperBounds.length];
                watchers.put(watcherName,
                             new HistoryItem(currentTime, lastHistogramCounts));
            }
            final long[] intervalCounts = new long[upperBounds.length];
            long cumulativeICount = 0;
            long intervalTotalCount = 0;
            for (int i = 0; i < upperBounds.length; i++) {
                cumulativeICount = cumulativeCounts.get(i);
                intervalCounts[i] = cumulativeICount - lastHistogramCounts[i];
                lastHistogramCounts[i] = cumulativeICount;
                intervalTotalCount += intervalCounts[i];
            }

            return new RateResult(currentTime - lastTime, intervalTotalCount,
                                  upperBounds, intervalCounts);
        }

        /* A structure to record Histogram history item for each watcher */
        private static final class HistoryItem {
            private long lastTime;
            private long[] lastHistogramCounts;

            private HistoryItem(long time,
                                long[] histogramCounts) {
                lastTime = time;
                lastHistogramCounts = histogramCounts;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Element newElement() {
        return new Element(upperBounds);
    }

    /**
     * Saving collected {@link Histogram} rate change result.
     */
    public static class RateResult extends RateMetric.RateResult {

        private static final long serialVersionUID = 1L;

        private final long totalCount;
        private final long[] upperBounds;
        private final long[] histogramCounts;

        public RateResult(long duration,
                          long totalCount,
                          long[] upperBounds,
                          long[] histogramCounts) {
            super(duration);
            this.totalCount = totalCount;
            this.upperBounds = upperBounds;
            this.histogramCounts = histogramCounts;
        }

        public long[] getHistogramCounts() {
            return histogramCounts;
        }

        public long getTotalCount() {
            return totalCount;
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = super.toMap();
            for (int i = 0; i < upperBounds.length; i++) {
                final String key = HISTOGRAM_NAME + DELIMITER + upperBounds[i];
                map.put(key, histogramCounts[i]);
            }
            return map;
        }
    }

    // Convenience methods.

    /**
     * {@link Element#observe(long)}
     * Note: as a convenience method, it only works for no label Counter.
     */
    public void observe(long v) {
        noLabelsElement.observe(v);
    }

    /**
     * {@link Element#observe(long, int)}
     * Note: as a convenience method, it only works for no label Counter.
     */
    public void observe(long v, int times) {
        noLabelsElement.observe(v, times);
    }

    /**
     * Set the upper bounds of buckets for the histogram with a linear sequence.
     */
    public static long[] linearBuckets(long start,
                                       long width,
                                       int count) {
        final long[] buckets = new long[count];
        for (int i = 0; i < count; i++) {
            buckets[i] = start + i * width;
        }
        return buckets;
    }

    /**
     * Set the upper bounds of buckets for the histogram with an exponential
     * sequence.
     */
    public static long[] exponentialBuckets(long start,
                                            long factor,
                                            int count) {
        final long[] buckets = new long[count];
        for (int i = 0; i < count; i++) {
            buckets[i] = start * (long) Math.pow(factor, i);
        }
        return buckets;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetricFamilySamples<RateResult> collect() {
        return collect(Type.HISTOGRAM);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetricFamilySamples<RateResult>
        collectSinceLastTime(String watcherName) {

        return collectSinceLastTime(Type.HISTOGRAM, watcherName);
    }
}

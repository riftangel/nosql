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
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A SizeQuantile as one kind of {@link RateMetric} is to estimate quantiles
 * values of observed values. It doesn't have limitation to observed value
 * range. But it only get estimated quantiles. The default quantiles are
 * median/95th/99th.
 * <pre>
 * Usage example:
 * {@code
 * SizeQuantile requestSize = new SizeQuantile("requestSize", "opType");
 * add one observed read request size.
 * requestSize.label("read").observe(size);
 *
 * add one observed write request size.
 * requestSize.label("write").observe(size);
 *
 * get read request avg/95th/99th size since last call for watcherName.
 * requestSize.label("read").rateSinceLastTime("watcherName")
 * }
 * </pre>
 */
public class SizeQuantile
    extends RateMetric<SizeQuantile.RateResult, SizeQuantile.Element> {

    private static final double[] DEFAULT_QUANTILES =
        new double[] { 0.50, 0.95, 0.99 };

    /*
     * The quantile values will be 0 if there is no observed value.
     */
    private static final int DEFAULT_VAL = 0;

    private final double[] quantiles;

    /**
     * Create a SizeQuantile to estimate default quantiles: 0.50, 0.95, 0.99.
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param labelNames to set metric tag names that is optional for Metric
     */
    public SizeQuantile(final String name, String... labelNames) {
        this(name, DEFAULT_QUANTILES, labelNames);
    }

    /**
     * Create a SizeQuantile to estimate specified quantiles.
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param quantiles that SizeQuantile to calculate
     * @param labelNames to set metric tag names that is optional for Metric
     */
    public SizeQuantile(final String name,
                        double[] quantiles,
                        String... labelNames) {
        super(name, labelNames);
        if (quantiles == null || quantiles.length == 0) {
            throw new IllegalArgumentException("quantiles cannot be empty");
        }
        this.quantiles = quantiles;
        initializeNoLabelsElement();
    }

    /**
     * {@link Metric.Element} for {@link SizeQuantile}.
     */
    public static final class Element extends RateMetric.Element<RateResult> {
        //TODO change to LongAdder if this package move to JAVA 8.
        private AtomicLong sum;
        private final WatchersQuantiles watchersQuantiles;
        private final ConcurrentHashMap<String, HistoryItem> watchers;
        private final double[] quantiles;

        private Element(double[] quantiles) {
            this.quantiles = quantiles;
            sum = new AtomicLong();
            watchersQuantiles = new WatchersQuantiles(quantiles);
            watchers = new ConcurrentHashMap<String, HistoryItem>();
            initialTime = System.nanoTime();
        }

        /**
         * Observe the given value.
         */
        public void observe(int v) {
            observe(v, 1);
        }

        /**
         * Observe the given value with N times.
         */
        public void observe(int v, int times) {
            sum.addAndGet(v * times);
            for (int i = 0; i < times; i++) {
                watchersQuantiles.observe(v);
            }
        }

        /**
         * {@inheritDoc}
         * Updating sum and quantiles is not an atomic operation, so they will
         * be a bit inconsistent when calculating rate and observing new value
         * concurrently. This tradeoff is made in order to avoid the cost of
         * synchronization.
         */
        @Override
        public RateResult rate() {
            final int[] quantileValues = watchersQuantiles.getQuantileValues();
            return new RateResult(System.nanoTime() - initialTime,
                                  sum.get(), quantiles, quantileValues);
        }

        /**
         * {@inheritDoc}
         * Updating sum and quantiles are not an atomic operation, so they will
         * be a bit inconsistent when calculating rate and observing new value
         * concurrently. This tradeoff is made in order to avoid the cost of
         * synchronization.
         */
        @Override
        public RateResult rateSinceLastTime(String watcherName) {
            final long currentTime = System.nanoTime();
            final long currentSum = sum.get();
            long lastSum = 0;
            long lastTime = initialTime;

            final HistoryItem historyItem = watchers.get(watcherName);
            if (historyItem != null) {
                lastTime = historyItem.lastTime;
                lastSum = historyItem.lastSum;
                historyItem.lastTime = currentTime;
                historyItem.lastSum = currentSum;
            } else {
                watchers.put(watcherName,
                             new HistoryItem(currentTime, currentSum));
            }
            final int[] quantileValues =
                watchersQuantiles.getQuantileValues(watcherName);
            return new RateResult(currentTime - lastTime, currentSum - lastSum,
                                  quantiles, quantileValues);
        }

        /* A structure to record SizeQuantile history item for each watcher */
        private static final class HistoryItem {
            private long lastTime;
            private long lastSum;

            private HistoryItem(long time, long sum) {
                lastTime = time;
                lastSum = sum;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Element newElement() {
        return new Element(quantiles);
    }

    /**
     * Saving collected {@link SizeQuantile} rate change result.
     */
    public static class RateResult extends RateMetric.RateResult {

        private static final long serialVersionUID = 1L;

        private final long sum;
        private final double[] quantiles;
        private final int[] quantileValues;

        public RateResult(long duration,
                          long sum,
                          double[] quantiles,
                          int[] quantileValues) {

            super(duration);
            this.sum = sum;
            this.quantiles = quantiles;
            this.quantileValues = quantileValues;
        }

        public long getSum() {
            return sum;
        }

        public double[] getQuantile() {
            return quantiles;
        }

        public int[] getQuantileValues() {
            return quantileValues;
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = super.toMap();
            map.put(SUM_NAME, sum);
            for (int i = 0; i < quantiles.length; i++) {
                final String key = QUANTILE_NAME + DELIMITER +
                                   quantiles[i];
                map.put(key, quantileValues[i]);
            }
            return map;
        }
    }

    // Convenience methods.

    /**
     * {@link Element#observe(int)}
     * Note: as a convenience method, it only works for no label SizeQuantile.
     */
    public void observe(int v) {
        noLabelsElement.observe(v);
    }

    /**
     * {@link Element#observe(int, int)}
     * Note: as a convenience method, it only works for no label SizeQuantile.
     */
    public void observe(int v, int times) {
        noLabelsElement.observe(v, times);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetricFamilySamples<RateResult> collect() {
        return collect(Type.SIZE_QUANTILE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetricFamilySamples<RateResult>
        collectSinceLastTime(String watcherName) {

        return collectSinceLastTime(Type.SIZE_QUANTILE, watcherName);
    }

    /**
     * Wrapper around Frugal2UQuantiles.
     *
     * Maintains watchers buffer of Frugal2UQuantiles to provide quantiles for
     * different watchers.
     */
    private static class WatchersQuantiles {

        private final double[] quantiles;
        private final Map<String, Frugal2UQuantiles> watchersBuffer;
        private final Frugal2UQuantiles wholeStream;

        public WatchersQuantiles(double[] quantiles) {
            this.quantiles = quantiles;
            this.watchersBuffer =
                new ConcurrentHashMap<String, Frugal2UQuantiles>();
            this.wholeStream = new Frugal2UQuantiles(quantiles);
        }

        public int[] getQuantileValues() {
            return wholeStream.getQuantileValues();
        }

        public int[] getQuantileValues(String watcherName) {
            final Frugal2UQuantiles watcherStream =
                watchersBuffer.put(watcherName,
                                   new Frugal2UQuantiles(quantiles));
            if (watcherStream == null) {
                return wholeStream.getQuantileValues();
            }
            return watcherStream.getQuantileValues();
        }

        public void observe(int value) {
            wholeStream.observe(value);
            for (Frugal2UQuantiles fQuantiles : watchersBuffer.values()) {
                fQuantiles.observe(value);
            }
        }
    }

    /*
     * Implementation of the Frugal2U Algorithm.
     * More info: http://blog.aggregateknowledge.com/2013/09/16/sketch-of-the-day-frugal-streaming/
     */
    private static final class Frugal2UQuantiles {

        private final Quantile[] quantiles;

        private Frugal2UQuantiles(Quantile[] quantiles) {
            this.quantiles = quantiles;
        }

        private Frugal2UQuantiles(double[] quantiles) {
            this.quantiles = new Quantile[quantiles.length];
            for (int i = 0; i < quantiles.length; i++) {
                this.quantiles[i] = new Quantile(quantiles[i]);
            }
        }

        private synchronized void observe(int value) {
            for (Quantile q : quantiles) {
                q.observe(value);
            }
        }

        private int[] getQuantileValues() {
            final int[] quantileValues = new int[quantiles.length];
            for (int i = 0; i < quantileValues.length; i++) {
                quantileValues[i] = quantiles[i].estimatedVal;
            }
            return quantileValues;
        }

        private static class Quantile {

            /* estimated quantile value */
            private int estimatedVal = DEFAULT_VAL;
            /* target quantile */
            private final double q;
            private int step = 1;
            private int sign = 0;
            private Random r = new Random();

            Quantile(double quantile) {
                q = quantile;
            }

            private void observe(int item) {
                /* first item */
                if (sign == 0) {
                    estimatedVal = item;
                    sign = 1;
                    return;
                }

                if (item > estimatedVal && r.nextDouble() > 1 - q) {
                    // Increase estimatedVal direction

                    /*
                     * Increment the step size if and only if the estimate keeps
                     * moving in the same direction. Step size is incremented by
                     * the result of applying the specified step function to the
                     * previous step size.
                     */
                    step += sign * f(step);
                    /*
                     * Increment the estimate by step size if step is positive.
                     * Otherwise, increment the step size by one.
                     */
                    if (step > 0) {
                        estimatedVal += step;
                    } else {
                        estimatedVal += 1;
                    }
                    /*
                     * If the estimate overshot the item in the stream, pull the
                     * estimate back and re-adjust the step size.
                     */
                    if (estimatedVal > item) {
                        step += (item - estimatedVal);
                        estimatedVal = item;
                    }
                    /*
                     * Reset the step if direction is changed.
                     */
                    if (sign < 0) {
                        step = 1;
                    }
                    /*
                     * Mark that the estimate as increased direction.
                     */
                    sign = 1;
                } else if (item < estimatedVal && r.nextDouble() > q) {
                    /*
                     * Opposite to above increase estimatedVal direction, it is
                     * decreasing estimatedVal direction.
                     */

                    step += -sign * f(step);

                    if (step > 0) {
                        estimatedVal -= step;
                    } else {
                        estimatedVal--;
                    }

                    if (estimatedVal < item) {
                        step += (estimatedVal - item);
                        estimatedVal = item;
                    }

                    if (sign > 0) {
                        step = 1;
                    }
                    sign = -1;
                }
            }

            private int f(@SuppressWarnings("unused") int currentStep) {
                /*
                 * Move one step constantly. Increase the step if we want to
                 * adjust more quickly.
                 */
                return 1;
            }
        }
    }

}

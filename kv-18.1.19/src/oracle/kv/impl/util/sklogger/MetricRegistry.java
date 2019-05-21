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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.util.ScheduleStart;
import oracle.kv.impl.util.sklogger.CustomGauge.GaugeCalculator;

/**
 * Registry metrics to MetricRegistry, and install {@link MetricProcessor} such
 * as {@link SkLogger} to handle these registered metrics repeatedly at given
 * interval.
 * <pre>
 * Usage example:
 * {@code
 * static Counter requestCount = MetricRegistry.getCounter("requestCount");
 * static PerfQuantile requestLatency =
 *      MetricRegistry.getPerfQuantile("requestLatency");
 * MetricRegistry.defaultRegistry.addMetricProcessor(SkLogger);
 * MetricRegistry.defaultRegistry.startProcessors(MONITOR_INTERVAL);
 * }
 * It will use SkLogger to repeat logging requestCount and requestLatency on
 * MONITOR_INTERVAL interval.
 * </pre>
 */
public class MetricRegistry {
    /**
     * The default registry.
     */
    public static final MetricRegistry defaultRegistry = new MetricRegistry();

    private final ScheduledExecutorService executor;
    private Future<?> collectorFuture;
    private final ConcurrentHashMap<String, Metric<?>> registeredMetrics =
        new ConcurrentHashMap<String, Metric<?>>();
    /* Map instead of List could help resolving duplicated processor */
    private final ConcurrentHashMap<String, MetricProcessor> metricProcessors =
        new ConcurrentHashMap<String, MetricProcessor>();

    public MetricRegistry() {
        executor = new ScheduledThreadPoolExecutor(1,
                                                   new MetricThreadFactory());
    }

    // Convenience methods for defaultRegistry

    /**
     * Get LongGauge and register it to {@link #defaultRegistry}.
     * @see LongGauge#LongGauge(String, String...)
     */
    public static LongGauge getLongGauge(final String name,
                                         String... labelNames) {
       return defaultRegistry.register(new LongGauge(name, labelNames));
    }

    /**
     * Get CustomGauge and register it to {@link #defaultRegistry}.
     * @see CustomGauge#CustomGauge(String, GaugeCalculator)
     */
    public static CustomGauge getCustomGauge(String name,
                                             GaugeCalculator gaugeCalculator) {
        return defaultRegistry.register(new CustomGauge(name, gaugeCalculator));
    }

    /**
     * Get Counter and register it to {@link #defaultRegistry}.
     * @see Counter#Counter(String, String...)
     */
    public static Counter getCounter(String name, String... labelNames) {
        return defaultRegistry.register(new Counter(name, labelNames));
    }

    /**
     * Get SizeQuantile and register it to {@link #defaultRegistry}.
     * @see SizeQuantile#SizeQuantile(String, String...)
     */
    public static SizeQuantile getSizeQuantile(String name,
                                               String... labelNames) {
        return defaultRegistry.register(new SizeQuantile(name, labelNames));
    }

    /**
     * Get PerfQuantile and register it to {@link #defaultRegistry}.
     * @see PerfQuantile#PerfQuantile(String, int, String...)
     */
    public static PerfQuantile getPerfQuantile(String name,
                                               int maxTracked,
                                               String... labelNames) {
        return defaultRegistry.register(new PerfQuantile(name, maxTracked,
                                                         labelNames));
    }

    /**
     * Get SizeQuantile and register it to {@link #defaultRegistry}.
     * @see SizeQuantile#SizeQuantile(String, double[], String...)
     */
    public static SizeQuantile getSizeQuantile(String name,
                                               double[] quantiles,
                                               String... labelNames) {
        return defaultRegistry.register(new SizeQuantile(name, quantiles,
                                                         labelNames));
    }

    /**
     * Get Histogram and register it to {@link #defaultRegistry}.
     * @see Histogram#Histogram(String, long[], String...)
     */
    public static Histogram getHistogram(String name,
                                         long[] upperBounds,
                                         String... labelNames) {
        return defaultRegistry.register(new Histogram(name, upperBounds,
                                                      labelNames));
    }

    public LongGauge register(LongGauge metric) {
        addMetricData(metric);
        return metric;
    }

    public Counter register(Counter metric) {
        addMetricData(metric);
        return metric;
    }

    public Histogram register(Histogram metric) {
        addMetricData(metric);
        return metric;
    }

    public SizeQuantile register(SizeQuantile metric) {
        addMetricData(metric);
        return metric;
    }

    public PerfQuantile register(PerfQuantile metric) {
        addMetricData(metric);
        return metric;
    }

    public CustomGauge register(CustomGauge metric) {
        addMetricData(metric);
        return metric;
    }

    /**
     * Register metric and don't allow registering the same metric name.
     */
    public void addMetricData(Metric<?> metric) {
        final String statsName = metric.getStatsName();
        final Metric<?> data = registeredMetrics.putIfAbsent(statsName, metric);
        if (data != null) {
            //TODO allow duplicated register for same type with same labels?
            throw new IllegalArgumentException(
                "Already registered name: " + statsName);
        }
    }

    public void unregister(String statsName) {
        registeredMetrics.remove(statsName);
    }

    /**
     * Add {@link MetricProcessor} to handle registered metrics.
     */
    public void addMetricProcessor(MetricProcessor processor) {
        metricProcessors.put(processor.getName(), processor);
    }

    /**
     * Start all installed {@link MetricProcessor} to handle registered metrics
     * repeatedly at the given configuredIntervalMs interval.
     */
    public synchronized void startProcessors(long configuredIntervalMs) {
        if (collectorFuture != null) {
            collectorFuture.cancel(true);
        }
        //TODO wrap to ScheduleStart?
        final long nowMs = System.currentTimeMillis();
        final long delayMs = ScheduleStart.calculateDelay(configuredIntervalMs,
                                                          nowMs);
        collectorFuture =
            executor.scheduleAtFixedRate(new ProcessMetricTask(),
                                         delayMs,
                                         configuredIntervalMs,
                                         TimeUnit.MILLISECONDS);
    }

    public synchronized void stopProcessors() {
        if (collectorFuture != null) {
            collectorFuture.cancel(true);
        }
    }

    //TODO support stream API instead of getAll?
    public List<MetricFamilySamples<?>>
        getAllMetricFactory(String watcherName) {

        final List<MetricFamilySamples<?>> metricFamilys =
            new ArrayList<MetricFamilySamples<?>>();
        for (Metric<?> data : registeredMetrics.values()) {
            MetricFamilySamples<?> sample;
            if (data instanceof RateMetric) {
                sample = ((RateMetric<?, ?>) data).collectSinceLastTime(
                    watcherName);
            } else {
                sample = data.collect();
            }
            metricFamilys.add(sample);
        }
        return metricFamilys;
    }

    /*
     * Task to collect MetricFamilySamples from registered metrics and start
     * all MetricProcessor to handle these collected MetricFamilySamples.
     */
    private class ProcessMetricTask implements Runnable {

        @Override
        public void run() {
            try {
                for (MetricProcessor processor : metricProcessors.values()) {
                    for (Metric<?> data : registeredMetrics.values()) {
                        MetricFamilySamples<?> sample;
                        if (data instanceof RateMetric) {
                            sample =
                                ((RateMetric<?, ?>) data).collectSinceLastTime(
                                    processor.getName());
                        } else {
                            sample = data.collect();
                        }
                        /*
                         * Usually, it has only one MetricProcessor and process
                         * is quickly enough, to be simple, we don't start
                         * MetricProcessor concurrently.
                         */
                        processor.process(sample);
                    }
                }
            } catch (Exception e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    private class MetricThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(r, "MetricThread");
            t.setDaemon(true);
            return t;
        }
    }
}

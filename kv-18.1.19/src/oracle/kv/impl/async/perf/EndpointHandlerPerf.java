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

package oracle.kv.impl.async.perf;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Collects and shows performance metrics for an endpoint handler.
 */
public class EndpointHandlerPerf {

    private final Logger logger;

    private static final long rateUpdatePeriodMillis = 100;
    private static final long loggingMonitorPeriodMillis = 1000;

    /* Dialog throughput metrics */
    private final CountMetrics dialogStartMetric =
        new CountMetrics("dialog start", rateUpdatePeriodMillis);
    private final CountMetrics dialogDropMetric =
        new CountMetrics("dialog drop", rateUpdatePeriodMillis);
    private final CountMetrics dialogFinishMetric =
        new CountMetrics("dialog finish", rateUpdatePeriodMillis);
    private final CountMetrics dialogAbortMetric =
        new CountMetrics("dialog abort", rateUpdatePeriodMillis);

    /* Dialog latency metrics */
    private final DialogLatency dialogFinishLatency =
        new DialogLatency("dialog finish latency");
    private final DialogLatency dialogAbortLatency =
        new DialogLatency("dialog abort latency");
    private final DialogDetail dialogDetail =
        new DialogDetail();

    private volatile Future<?> updateFuture = null;
    private volatile Future<?> monitorFuture = null;

    /**
     * Constructs a dialog endpoint handler perf.
     */
    public EndpointHandlerPerf(Logger logger) {
        this.logger = logger;
    }

    /**
     * Schedule perf tasks.
     */
    public void schedule(ScheduledExecutorService executor) {
        if (PerfCondition.SYSTEM_PERF_ENABLED.holds()) {
            try {
                updateFuture = executor.scheduleAtFixedRate(
                        new RateMetricsUpdater(),
                        rateUpdatePeriodMillis,
                        rateUpdatePeriodMillis,
                        TimeUnit.MILLISECONDS);
                monitorFuture = executor.scheduleAtFixedRate(
                        new LoggingMonitor(),
                        loggingMonitorPeriodMillis,
                        loggingMonitorPeriodMillis,
                        TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                logger.log(Level.WARNING,
                        "Unable to start updating and monitoring tasks " +
                        "for perf: ", e);
            }
        }
    }

    /**
     * Close this perf.
     */
    public void close() {
        if (updateFuture != null) {
            updateFuture.cancel(false);
        }
        if (monitorFuture != null) {
            monitorFuture.cancel(false);
        }
    }

    /**
     * Called when a dialog is started.
     *
     * @return {@code true} if detailed perf is enabled
     */
    public boolean onDialogStarted() {
        long count = dialogStartMetric.incrementAndGet();
        return PerfFilter.SYSTEM_SAMPLING.accept(count);
    }

    /**
     * Called when a dialog is dropped.
     */
    public void onDialogDropped() {
        dialogDropMetric.incrementAndGet();
    }

    /**
     * Called when a dialog is finished.
     *
     * @param dperf the perf of the dialog, maybe null if not sampled
     */
    public void onDialogFinished(DialogPerf dperf) {
        dialogFinishMetric.incrementAndGet();
        dialogFinishLatency.update(dperf);
        dialogDetail.update(dperf);
    }

    /**
     * Called when a dialog is aborted.
     *
     * @param dperf the perf of the dialog, maybe null if not sampled
     */
    public void onDialogAborted(DialogPerf dperf) {
        dialogAbortMetric.incrementAndGet();
        dialogAbortLatency.update(dperf);
        dialogDetail.update(dperf);
    }

    /**
     * Returns the string representation of the performance metrics.
     */
    public String getPerfString() {
        return
            dialogStartMetric.get() + "\n" +
            dialogDropMetric.get() + "\n" +
            dialogFinishMetric.get() + "\n" +
            dialogAbortMetric.get() + "\n" +
            dialogFinishLatency.get() + "\n" +
            dialogAbortLatency.get() + "\n" +
            dialogDetail.get() + "\n";
    }

    /**
     * Periodically updates the rate metrics.
     */
    public class RateMetricsUpdater implements Runnable {

        @Override
        public void run() {
            dialogStartMetric.update();
            dialogDropMetric.update();
            dialogFinishMetric.update();
            dialogAbortMetric.update();
        }
    }

    /**
     * Periodically logs the metrics.
     */
    public class LoggingMonitor implements Runnable {

        @Override
        public void run() {
            if (logger.isLoggable(Level.INFO)) {
                logger.log(Level.INFO, "Perf stats from async:\n{0}\n",
                        getPerfString());
            }
        }
    }
}

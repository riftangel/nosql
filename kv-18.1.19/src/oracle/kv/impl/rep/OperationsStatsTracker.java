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

package oracle.kv.impl.rep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.measurement.EnvStats;
import oracle.kv.impl.measurement.JVMStats;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.kv.impl.measurement.ReplicationState;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.ScheduleStart;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.util.sklogger.PerfQuantile;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.utilint.Latency;
import com.sleepycat.utilint.LatencyStat;
import com.sleepycat.utilint.StatsTracker;

/**
 * Stats pertaining to a single replication node.
 */
public class OperationsStatsTracker {

    private static final int WAIT_FOR_HANDLE = 100;
    private final AgentRepository monitorBuffer;
    private final ScheduledExecutorService collector;
    private Future<?> collectorFuture;
    protected List<Listener> listeners = new ArrayList<Listener>();

    /*
     * The actual tracker. This is a member variable that can be replaced
     * if/when new parameters are set.
     */
    private volatile SummarizingStatsTracker tracker;

    /* Timestamp for the start of all operation tracking. */
    private long trackingStart;
    /* Timestamp for the end of the last collection period. */
    private long lastEnd;
    /* End of log at the time environment stats were last collected. */
    private long lastEndOfLog = 0;
    /* Configuration used to collect stats. */
    private final StatsConfig config = new StatsConfig().setClear(true);

    private final Logger logger;
    private final RepNodeService repNodeService;

    private ParameterMap rnParamsMap;
    private ParameterMap globalParamsMap;
    private GlobalParamsListener globalParamsListener;
    private RNParamsListener rnParamsListener;
    /**
     * Log no more than 1 threshold alert every 5 minutes.
     */
    private static final int LOG_SAMPLE_PERIOD_MS = 5 * 60 * 1000;

    /**
     * The max number of types of threshold alerts that will be logged;
     * for example, 'single op interval latency above ceiling',
     * 'multi-op interval throughput below floor', etc.
     */
    private static final int MAX_LOG_TYPES = 5;

    /**
     * OperationsStatsTracker will use this watcherName to get metric elements
     * rate of change.
     */
    private static final String WATCHER_NAME =
        OperationsStatsTracker.class.getName();

    /**
     * Encapsulates the logger used by this class. When this logger is used,
     * for each type of PerfEvent, the rate at which this logger writes
     * records corresponding to the given type will be bounded; to prevent
     * overwhelming the store's log file.
     */
    private final RateLimitingLogger<String> eventLogger;

    /** Maintain the history needed to track JVM stats. */
    private final JVMStats.Tracker jvmStatsTracker = new JVMStats.Tracker();

    /**
     */
    public OperationsStatsTracker(RepNodeService repNodeService,
                                  ParameterMap rnParamsMap,
                                  ParameterMap globalParamsMap,
                                  AgentRepository monitorBuffer) {

        this.repNodeService = repNodeService;
        this.monitorBuffer = monitorBuffer;
        RepNodeService.Params params = repNodeService.getParams();
        this.logger =
            LoggerUtils.getLogger(OperationsStatsTracker.class, params);
        this.eventLogger = new RateLimitingLogger<String>
            (LOG_SAMPLE_PERIOD_MS, MAX_LOG_TYPES, logger);
        ThreadFactory factory = new CollectorThreadFactory
            (logger, params.getRepNodeParams().getRepNodeId());
        collector = new ScheduledThreadPoolExecutor(1, factory);
        globalParamsListener = new GlobalParamsListener();
        rnParamsListener = new RNParamsListener();
        initialize(rnParamsMap, globalParamsMap);
    }

    /**
     * For unit test only, effectively a no-op stats tracker, to disable stats
     * tracking.
     */
    public OperationsStatsTracker() {
        tracker = new SummarizingStatsTracker(null, 0, 0, 0, 1000);
        monitorBuffer = null;
        collector = null;
        collectorFuture = null;
        logger = null;
        eventLogger = null;
        repNodeService = null;
        trackingStart = 0;
    }

    /**
     * Used for initialization during constructions and from newParameters()
     * NOTE: newParameters() results in loss of cumulative stats and reset of
     * trackingStart.
     */
    private void initialize(ParameterMap newRNParamsMap,
                            ParameterMap newGlobalParamsMap) {
        if (newRNParamsMap != null) {
            rnParamsMap = newRNParamsMap.copy();
        }
        if (newGlobalParamsMap != null) {
            globalParamsMap = newGlobalParamsMap.copy();
        }
        if (collectorFuture != null) {
            logger.info("Cancelling current operationStatsCollector");
            collectorFuture.cancel(true);
        }

        tracker = new SummarizingStatsTracker
            (logger,
             rnParamsMap.get(ParameterState.SP_ACTIVE_THRESHOLD).asInt(),
             ParameterUtils.getThreadDumpIntervalMillis(rnParamsMap),
             rnParamsMap.get(ParameterState.SP_THREAD_DUMP_MAX).asInt(),
             ParameterUtils.getMaxTrackedLatencyMillis(rnParamsMap));

        DurationParameter dp =
            (DurationParameter) globalParamsMap.getOrDefault(
                ParameterState.GP_COLLECTOR_INTERVAL);
        collectorFuture = ScheduleStart.scheduleAtFixedRate(collector,
                                                            dp,
                                                            new CollectStats(),
                                                            logger);
        lastEnd = System.currentTimeMillis();
        trackingStart = lastEnd;
    }

    public StatsTracker<OpCode> getStatsTracker() {
        return tracker;
    }

    synchronized public void newRNParameters(ParameterMap oldMap,
                                             ParameterMap newMap) {

        /*
         * Caller ensures that the maps are different, check for
         * differences that matter to this class.  Re-init if *any* of the
         * parameters are different.
         */
        if (paramsDiffer(oldMap, newMap, ParameterState.SP_THREAD_DUMP_MAX) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_ACTIVE_THRESHOLD) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_MAX_LATENCY) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_DUMP_INTERVAL)) {
            initialize(newMap, null /* newGlobalParamsMap */);
        }
    }

    synchronized public void newGlobalParameters(ParameterMap oldMap,
                                                 ParameterMap newMap) {

        /*
        * Caller ensures that the maps are different, check for
        * differences that matter to this class.  Re-init if *any* of the
        * parameters are different.
        */
        if (paramsDiffer(oldMap, newMap,
                         ParameterState.GP_COLLECTOR_INTERVAL)) {
            initialize(null /* newRNParamsMap */, newMap);
        }
    }

    /**
     * @return true if map1 and map2 have different param values.
     */
    private boolean paramsDiffer(ParameterMap map1,
                                 ParameterMap map2,
                                 String param) {
        return (!(map1.get(param).equals(map2.get(param))));
    }

    public void close() {
        collectorFuture.cancel(true);
    }

    /**
     * Invoked by the async collection job and at rep node close.
     */
    synchronized public void pushStats() {

        logger.fine("Collecting latency stats");
        long useStart = lastEnd;
        long useEnd = System.currentTimeMillis();

        final StatsPacket packet =
            new StatsPacket(useStart, useEnd,
                            repNodeService.getRepNodeId().getFullName(),
                            repNodeService.getRepNodeId().getGroupName());

        /* Gather up all the base, per operation stats. */
        for (OpCode op: OpCode.values()) {
            LatencyStat stat = tracker.getIntervalLatency().get(op);

            /*
             * Interval latencies timestamps are set to reflect this period
             * only, while cumulative latencies timestamps reflect the entire
             * lifetime of the stats tracker. That way, cumulative and
             * interval latency throughputs are correct.
             */
            packet.add(new LatencyInfo(op.getIntervalMetric(),
                                       useStart, useEnd,
                                       stat.calculate()));

            stat = tracker.getCumulativeLatency().get(op);
            packet.add(new LatencyInfo(op.getCumulativeMetric(),
                                       trackingStart, useEnd,
                                       stat.calculate()));
        }

        /* Add the summary stats. */
        LatencyInfo latencyInfo =
            new LatencyInfo(PerfStatType.USER_SINGLE_OP_INT,
                            useStart, useEnd,
                            tracker.getSingleOpsIntervalStat());
        packet.add(latencyInfo);
        latencyInfo = new LatencyInfo(PerfStatType.USER_SINGLE_OP_CUM,
                                      trackingStart, useEnd,
                                      tracker.getSingleOpsCumulativeStat());
        packet.add(latencyInfo);

        latencyInfo = new LatencyInfo(PerfStatType.USER_MULTI_OP_INT,
                                      useStart, useEnd,
                                      tracker.getMultiOpsIntervalStat());
        packet.add(latencyInfo);
        latencyInfo = new LatencyInfo(PerfStatType.USER_MULTI_OP_CUM,
                                      trackingStart, useEnd,
                                      tracker.getMultiOpsCumulativeStat());
        packet.add(latencyInfo);

        Latency latency =
            tracker.getAggregatedIntervalStat(DataRequest.READ_SINGLE,
                                           WATCHER_NAME);
        latencyInfo = new LatencyInfo(PerfStatType.USER_SINGLE_READ_INT,
                                      useStart, useEnd,
                                      latency);
        packet.add(latencyInfo);

        latency = tracker.getAggregatedCumulativeStat(DataRequest.READ_SINGLE);
        latencyInfo = new LatencyInfo(PerfStatType.USER_SINGLE_READ_CUM,
                                      trackingStart, useEnd,
                                      latency);
        packet.add(latencyInfo);

        latency = tracker.getAggregatedIntervalStat(DataRequest.WRITE_SINGLE,
                                                 WATCHER_NAME);
        latencyInfo = new LatencyInfo(PerfStatType.USER_SINGLE_WRITE_INT,
                                      useStart, useEnd,
                                      latency);
        packet.add(latencyInfo);

        latency = tracker.getAggregatedCumulativeStat(DataRequest.WRITE_SINGLE);
        latencyInfo = new LatencyInfo(PerfStatType.USER_SINGLE_WRITE_CUM,
                                      trackingStart, useEnd,
                                      latency);
        packet.add(latencyInfo);

        latency = tracker.getAggregatedIntervalStat(DataRequest.READ_MULTI,
                                                    WATCHER_NAME);
        latencyInfo = new LatencyInfo(PerfStatType.USER_MULTI_READ_INT,
                                      useStart, useEnd,
                                      latency);
        packet.add(latencyInfo);

        latency = tracker.getAggregatedCumulativeStat(DataRequest.READ_MULTI);
        latencyInfo = new LatencyInfo(PerfStatType.USER_MULTI_READ_CUM,
                                      trackingStart, useEnd,
                                      latency);
        packet.add(latencyInfo);

        latency = tracker.getAggregatedIntervalStat(DataRequest.WRITE_MULTI,
                                                    WATCHER_NAME);
        latencyInfo = new LatencyInfo(PerfStatType.USER_MULTI_WRITE_INT,
                                      useStart, useEnd,
                                      latency);
        packet.add(latencyInfo);

        latency = tracker.getAggregatedCumulativeStat(DataRequest.WRITE_MULTI);
        latencyInfo = new LatencyInfo(PerfStatType.USER_MULTI_WRITE_CUM,
                                      trackingStart, useEnd,
                                      latency);
        packet.add(latencyInfo);

        /* Get the table throughput and size stats */
        packet.set
            (repNodeService.getRepNode().getTableManager().getTableInfo());

        packet.add
            (new ReplicationState(useStart, useEnd, getReplicationState()));

        if (repNodeService.getParams().getRepNodeParams().
            getCollectEnvStats()) {
            ReplicatedEnvironment repEnv =
                repNodeService.getRepNode().getEnv(WAIT_FOR_HANDLE);

            /*
             * Check if the env is open; this method may be called after the
             * repNodeService has stopped.
             */
            if ((repEnv != null) && (repEnv.isValid())) {
                EnvironmentStats envStats = repEnv.getStats(config);

                /*
                 * Collect environment stats if there has been some app
                 * activity, or if there has been some env maintenance related
                 * write activity, independent of the app, say due to cleaning,
                 * checkpointing, replication, etc.
                 */
                if (envStats.getEndOfLog() != lastEndOfLog) {
                    packet.add(new EnvStats(useStart, useEnd, envStats));
                    ReplicatedEnvironmentStats repStats =
                        repEnv.getRepStats(config);
                    packet.add(new RepEnvStats(useStart, useEnd, repStats));
                    lastEndOfLog = envStats.getEndOfLog();
                }
            }
            packet.add(jvmStatsTracker.createStats(useStart, useEnd));
        }

        RequestHandlerImpl reqHandler = repNodeService.getReqHandler();
        Map<String, AtomicInteger> exceptionStat =
            reqHandler.getAndResetExceptionCounts();
        for(Entry<String, AtomicInteger> entry : exceptionStat.entrySet()) {
            packet.add(entry.getKey(), entry.getValue().get());
        }

        packet.setActiveRequests(reqHandler.getActiveRequests());
        packet.setTotalRequests(reqHandler.getAndResetTotalRequests());

        lastEnd = useEnd;

        logThresholdAlerts(Level.WARNING, packet);

        /* TODO bug: I think there is a small window between get and clear. */
        tracker.clearLatency();

        monitorBuffer.add(packet);
        sendPacket(packet);
        logger.fine(packet.toString());
    }

    private State getReplicationState() {
        State state = State.UNKNOWN;
        try {
            final ReplicatedEnvironment env =
                repNodeService.getRepNode().getEnv(WAIT_FOR_HANDLE);
            if (env != null) {
                try {
                    state = env.getState();
                } catch (IllegalStateException ise) {
                    /* State cannot be queried if detached. */
                    state = State.DETACHED;
                }
            }
        } catch (EnvironmentFailureException ignored) /* CHECKSTYLE_OFF */ {
            /* The environment is invalid. */
        } /* CHECKSTYLE_ON */
        return state;
    }

    /**
     * Simple Runnable to send latency stats to the service's monitor agent
     */
    private class CollectStats implements Runnable {

        @Override
        public void run() {
            try {
                pushStats();
            } catch(Exception e) {
                /*
                 * Capture any exceptions thrown by the stats task and prevent
                 * it from killing the job; we want to ensure that stats
                 * collection keeps going on.
                 */
                logger.severe("push RN operation stats error: " + e.toString());
            }
        }
    }

    /**
     * Collector threads are named KVAgentMonitorCollector and log uncaught
     * exceptions to the monitor logger.
     */
    private class CollectorThreadFactory extends KVThreadFactory {
        private final RepNodeId repNodeId;

        CollectorThreadFactory(Logger logger, RepNodeId repNodeId) {
            super(null, logger);
            this.repNodeId = repNodeId;
        }

        @Override
        public String getName() {
            return  repNodeId + "_MonitorAgentCollector";
        }
    }

    /**
     * This stats tracker adds these customizations:
     *
     * - operations are recorded both in a per-op-type stat, and in a
     * single-op or multi-op summary stat. The summary stats preserve
     * 95th/99th, because those values would be lost if we try to rollup the
     * individual op stats.
     * 
     * - Also use a PerfQuantile to track requests aggregated
     * read/write-single/multiple metrics
     *
     * - NOP ops are excluded.
     */
    public static class SummarizingStatsTracker extends StatsTracker<OpCode> {

        private static final String RN_OPS_PERF_FIELD_NAME = "aggregatedPerf";
        private static final String[] DATA_REQUEST_LABELS =
            new String[] { "opType", "actionMode" };

        private final LatencyStat singleOpsInterval;
        private final LatencyStat singleOpsCumulative;
        private final LatencyStat multiOpsInterval;
        private final LatencyStat multiOpsCumulative;
        /**
         * Tracks requests aggregated read/write-single/multiple metrics
         */
        private PerfQuantile aggregatedPerf;

        public SummarizingStatsTracker(Logger stackTraceLogger,
                                       int activeThreadThreshold,
                                       long threadDumpIntervalMillis,
                                       int threadDumpMax,
                                       int maxTrackedLatencyMillis) {
            super(OpCode.values(), stackTraceLogger, activeThreadThreshold,
                  threadDumpIntervalMillis, threadDumpMax,
                  maxTrackedLatencyMillis);

            singleOpsInterval = new LatencyStat(maxTrackedLatencyMillis);
            singleOpsCumulative = new LatencyStat(maxTrackedLatencyMillis);
            multiOpsInterval = new LatencyStat(maxTrackedLatencyMillis);
            multiOpsCumulative = new LatencyStat(maxTrackedLatencyMillis);
            /*
             * Use the new sklogger package metric to track aggregated
             * read/write-single/multiple metrics. In the long term, we might
             * use sklogger package metric for all metrics.
             */
            aggregatedPerf = new PerfQuantile(RN_OPS_PERF_FIELD_NAME,
                                              maxTrackedLatencyMillis,
                                              DATA_REQUEST_LABELS);
            for(DataRequest type : DataRequest.values()) {
                aggregatedPerf.labels(type.getValue());
            }
        }

        /**
         * Note that markFinish may be called with a null op type.
         */
        @Override
        public void markFinish(OpCode opType, long startTime, int numRecords) {

            super.markFinish(opType, startTime, numRecords);
            if (numRecords == 0) {
                return;
            }

            if (opType == null) {
                return;
            }

            long elapsed = System.nanoTime() - startTime;
            long avgElapsed = elapsed / numRecords;
            PerfStatType ptype = opType.getIntervalMetric();
            for (PerfStatType parent = ptype.getParent(); parent != null;
                    parent = parent.getParent()) {

                if (parent.equals(PerfStatType.USER_SINGLE_OP_INT)) {
                    singleOpsInterval.set(elapsed);
                    singleOpsCumulative.set(elapsed);
                } else if (parent.equals(PerfStatType.USER_MULTI_OP_INT)) {
                    /*
                     * TODO Do we change this all multi stats to per operation
                     * instead of per request as below read/write multi stats?
                     */ 
                    multiOpsInterval.set(numRecords, elapsed);
                    multiOpsCumulative.set(numRecords, elapsed);
                } else if (parent.equals(PerfStatType.USER_SINGLE_READ_INT)) {
                    aggregatedPerf.labels(DataRequest.READ_SINGLE.values).
                        observeNanoLatency(elapsed, 1);
                } else if (parent.equals(PerfStatType.USER_SINGLE_WRITE_INT)) {
                    aggregatedPerf.labels(DataRequest.WRITE_SINGLE.values).
                        observeNanoLatency(elapsed, 1);
                } else if (parent.equals(PerfStatType.USER_MULTI_READ_INT)) {
                    aggregatedPerf.labels(DataRequest.WRITE_MULTI.values).
                        observeNanoLatency(avgElapsed, numRecords);
                } else if (parent.equals(PerfStatType.USER_MULTI_WRITE_INT)) {
                    aggregatedPerf.labels(DataRequest.READ_MULTI.values).
                        observeNanoLatency(avgElapsed, numRecords);
                }
            }
        }

        /**
         * Should be called after each interval latency stat collection, to
         * reset for the next period's collection.
         */
        @Override
        public void clearLatency() {
            super.clearLatency();
            singleOpsInterval.clear();
            multiOpsInterval.clear();
        }

        public Latency getSingleOpsIntervalStat() {
            return singleOpsInterval.calculate();
        }

        public Latency getSingleOpsCumulativeStat() {
            return singleOpsCumulative.calculate();
        }

        public Latency getMultiOpsIntervalStat() {
            return multiOpsInterval.calculate();
        }

        public Latency getMultiOpsCumulativeStat() {
            return multiOpsCumulative.calculate();
        }

        public Latency getAggregatedIntervalStat(DataRequest tag,
                                                 String watcherName) {
            return aggregatedPerf.labels(tag.values).
                rateSinceLastTime(watcherName).getLatency();
        }

        public Latency getAggregatedCumulativeStat(DataRequest tag) {
            return aggregatedPerf.labels(tag.values).rate().getLatency();
        }

        public PerfQuantile getAggregatedPerf() {
            return aggregatedPerf;
        }
    }

    /**
     * For data request operations metric aggregation labels.
     */
    public enum DataRequest {
        READ_SINGLE("read", "single"),
        WRITE_SINGLE("write", "single"),
        READ_MULTI("read", "multiple"),
        WRITE_MULTI("write", "multiple"),
        NOP("nop", "nop");

        private final String[] values;

        private DataRequest(final String ...values) {
            this.values = values;
        }

        public String[] getValue() {
            return values;
        }
    }

    /**
     * An OperationsStatsTracker.Listener can be implemented by clients of this
     * interface to recieve stats when they are collected.
     */
    public interface Listener {
        void receiveStats(StatsPacket packet);
    }

    public void addListener(Listener lst) {
        listeners.add(lst);
    }

    public void removeListener(Listener lst) {
        listeners.remove(lst);
    }

    private void sendPacket(StatsPacket packet) {
        for (Listener lst : listeners) {
            lst.receiveStats(packet);
        }
    }

    private void logThresholdAlerts(Level level, StatsPacket packet) {

        /* Better to not log at all than risk a possible NPE. */
        if (logger == null || eventLogger == null ||
            repNodeService == null || level == null || packet == null) {
            return;
        }
        if (!logger.isLoggable(level)) {
            return;
        }

        final RepNodeParams params = repNodeService.getRepNodeParams();
        if (params == null) {
            return;
        }

        final int ceiling = params.getLatencyCeiling();
        final int floor = params.getThroughputFloor();
        final long threshold = params.getCommitLagThreshold();
        final long lag = PerfEvent.getCommitLagMs(packet.getRepEnvStats());

        /* For single operation within a given time interval */
        final LatencyInfo singleOpIntervalLatencyInfo =
                        packet.get(PerfStatType.USER_SINGLE_OP_INT);

        if (PerfEvent.latencyCeilingExceeded(
                          ceiling, singleOpIntervalLatencyInfo)) {
            eventLogger.log("single-op-interval-latency", level,
                            "single op interval latency above ceiling [" +
                            ceiling + "]");
        }

        if (PerfEvent.throughputFloorExceeded(
                          floor, singleOpIntervalLatencyInfo)) {
            eventLogger.log("single-op-interval-throughput",
                            level, "single op interval throughput below " +
                            "floor [" + floor + "]");
        }

        /* For multiple operations within a given time interval */
        final LatencyInfo multiOpIntervalLatencyInfo =
            packet.get(PerfStatType.USER_MULTI_OP_INT);

        if (PerfEvent.latencyCeilingExceeded(
                          ceiling, multiOpIntervalLatencyInfo)) {
            eventLogger.log("multi-op-interval-latency", level,
                            "multi-op interval latency above ceiling [" +
                            ceiling + "]");
        }

        if (PerfEvent.throughputFloorExceeded(
                          floor, multiOpIntervalLatencyInfo)) {
            eventLogger.log("multi-op-interval-throughput", level,
                            "multi-op interval throughput below floor [" +
                            floor + "]");
        }

        /* For commit lag averaged over the collection period */
        if (PerfEvent.commitLagThresholdExceeded(lag, threshold)) {
            eventLogger.log("replica-lag", level,
                            "replica lag exceeds threshold [replicaLagMs=" +
                            lag + " threshold=" + threshold + "]");
        }
    }

    public ParameterListener getGlobalParamsListener() {
        return globalParamsListener;
    }

    public ParameterListener getRNParamsListener() {
        return rnParamsListener;
    }

    /**
     * Global parameter change listener.
     */
    private class GlobalParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            newGlobalParameters(oldMap, newMap);
        }
    }

    /**
     * RepNode parameter change listener.
     */
    private class RNParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            newRNParameters(oldMap, newMap);
        }
    }
}

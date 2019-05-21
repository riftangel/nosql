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
package oracle.kv.impl.measurement;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;

import oracle.kv.impl.util.sklogger.PerfQuantile;
import oracle.kv.impl.util.sklogger.PerfQuantile.RateResult;

import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * Dump of Java Virtual Machine information.
 */
public class JVMStats implements ConciseStats, Serializable {

    /** Escape valve for stopping the pause checker. */
    public static volatile boolean stopPauseChecker;

    private static final long serialVersionUID = 1L;

    /**
     * The largest tracked latency, in milliseconds.
     */
    private static final int MAX_TRACKED_LATENCY = 1000;

    /**
     * The type of internal GC notifications.  In Java 7 and later, this field
     * has the the same as the GARBAGE_COLLECTION_NOTIFICATION field in the
     * com.sun.management.GarbageCollectionNotificationInfo class.
     */
    private static final String GARBAGE_COLLECTION_NOTIFICATION =
        "com.sun.management.gc.notification";

    /**
     * Class for tracking JVM statistics over time, so that we can supply
     * counts and times since the last report, even though GC beans report
     * cumulative values.  Use an instance of this class to maintain the
     * history for returned stats.
     */
    public static class Tracker {

        /**
         * Map from GC name to information about the last GC count and time.
         */
        private final Map<String, CollectorTotal> collectorTotals =
            Collections.synchronizedMap(new HashMap<String, CollectorTotal>());

        /**
         * Whether we have checked for whether we can register for GC
         * notifications.
         */
        private boolean checkRegisterForGCNotifications;

        /** Map from GC name to information used to quantify GC performance. */
        private final Map<String, PerfQuantile> gcPerfQuantiles =
            Collections.synchronizedMap(new HashMap<String, PerfQuantile>());

        /** Check for JVM pauses. */
        private final PauseChecker pauseChecker = new PauseChecker();

        /** Creates a JVMStats instance for the specified time period. */
        public JVMStats createStats(long start, long end) {
            maybeRegisterForNotifications();
            return new JVMStats(start, end, collectorTotals, gcPerfQuantiles,
                                pauseChecker.quantile);
        }

        /** Register for JMX GC notifications if needed and supported. */
        private synchronized void maybeRegisterForNotifications() {
            if (checkRegisterForGCNotifications) {
                return;
            }
            checkRegisterForGCNotifications = true;
            for (GarbageCollectorMXBean gcbean :
                     ManagementFactory.getGarbageCollectorMXBeans()) {
                if (gcbean instanceof NotificationEmitter) {
                    ((NotificationEmitter) gcbean).addNotificationListener(
                        new GCListener(), null, null);
                }
            }
        }

        /** Processes JMX GC notifications. */
        private class GCListener implements NotificationListener {
            @Override
            public void handleNotification(Notification notification,
                                           Object handback) {
                if (!notification.getType().equals(
                        GARBAGE_COLLECTION_NOTIFICATION)) {
                    return;
                }

                final String gcName;
                final long gcDuration;

                /* Catch exceptions for malformed JMX notifications */
                try {
                    final CompositeData compositeData =
                        (CompositeData) notification.getUserData();
                    gcName = getGcName(compositeData);
                    gcDuration = getGcDuration(compositeData);
                } catch (Exception e) {
                    return;
                }
                PerfQuantile quantile;
                synchronized (gcPerfQuantiles) {
                    quantile = gcPerfQuantiles.get(gcName);
                    if (quantile == null) {
                        quantile = new PerfQuantile("gcLatencies",
                                                    MAX_TRACKED_LATENCY);
                        gcPerfQuantiles.put(gcName, quantile);
                    }
                }
                quantile.observe((int) gcDuration);
            }
        }
    }

    /**
     * Returns the GC name specified by the composite data for an internal GC
     * notification.
     */
    private static String getGcName(CompositeData compositeData) {

        /*
         * TODO: In Java 7 and later, change to:
         * final GarbageCollectionNotificationInfo info =
         *      GarbageCollectionNotificationInfo.from(
         *          (CompositeData) notification.getUserData());
         *  return info.getGcName();
         */

        return (String) compositeData.get("gcName");
    }

    /**
     * Returns the GC duration specified by the composite data for an internal
     * GC notification.
     */
    private static long getGcDuration(CompositeData compositeData) {

        /*
         * TODO: In Java 7 and later, change to:
         * final GarbageCollectionNotificationInfo info =
         *      GarbageCollectionNotificationInfo.from(
         *          (CompositeData) notification.getUserData());
         *  return info.getGcInfo().getDuration();
         */

        final CompositeDataSupport gcInfo =
            (CompositeDataSupport) compositeData.get("gcInfo");
        return (Long) gcInfo.get("duration");
    }

    /** Tallies information about total GC counts and times. */
    private static class CollectorTotal {
        private long count;
        private long time;

        /** Updates the count and returns the change since the last value. */
        synchronized long updateCount(long newCount) {
            final long change = newCount - count;
            count = newCount;
            return change;
        }

        /** Updates the time and returns the change since the last value. */
        synchronized long updateTime(long newTime) {
            final long change = newTime - time;
            time = newTime;
            return change;
        }
    }

    /**
     * Check for JVM pauses by sleeping a millisecond and reporting if the
     * sleep lasts longer than that.  Pauses are typically caused by GCs, but
     * could be caused by other things, say by pauses due to OS virtualization.
     */
    private static class PauseChecker implements Runnable {
        final PerfQuantile quantile =
            new PerfQuantile("pauses", MAX_TRACKED_LATENCY);

        PauseChecker() {
            final Thread t = new Thread(this, "KV Pause Checker");
            t.setDaemon(true);
            t.start();
        }

        @Override
        public void run() {
            while (!stopPauseChecker) {
                final long start = System.nanoTime();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    continue;
                }

                /*
                 * Only report pauses if we are sure there was at least a full
                 * millisecond delay, to avoid problems with platforms that
                 * don't have access to fine grain clocks.  Since we don't know
                 * where the pause started in the millisecond we were asleep,
                 * count half of that time as part of the millisecond.  We also
                 * need to add a half millisecond as part of the conversion
                 * from nanoseconds to milliseconds so that we get rounding
                 * rather than truncation.  As a result, just report the entire
                 * pause time if it is greater than 1.
                 */
                final long time =
                    NANOSECONDS.toMillis(System.nanoTime() - start);
                if (time > 1) {
                    quantile.observe((int) time);
                }
            }
        }
    }

    /**
     * Container class for garbage collector information.
     */
    public static class CollectorInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The name of the garbage collector this object represents.
         */
        private final String name;

        /**
         * The total number of collections that have occurred. This field is set
         * to the value returned by
         * {@link java.lang.management.GarbageCollectorMXBean#getCollectionCount
         * GarbageCollectorMXBean.getCollectionCount} at the time this object is
         * constructed.
         *
         * @see java.lang.management.GarbageCollectorMXBean#getCollectionCount
         * GarbageCollectorMXBean.getCollectionCount
         */
        private final long count;

        /**
         * The approximate accumulated collection elapsed time in milliseconds.
         * This field is set to the value returned by
         * {@link java.lang.management.GarbageCollectorMXBean#getCollectionTime
         * GarbageCollectorMXBean.getCollectionTime} at the time this object is
         * constructed.
         *
         * @see java.lang.management.GarbageCollectorMXBean#getCollectionTime
         * GarbageCollectorMXBean.getCollectionTime
         */
        private final long time;

        /**
         * The 95th percentile of times for individual GCs, or -1 if not known.
         */
        private final int percent95;

        /**
         * The 99th percentile of times for individual GCs, or -1 if not known.
         */
        private final int percent99;

        /** The max time for individual GCs, or -1 if not known. */
        private final int max;

        private CollectorInfo(GarbageCollectorMXBean gc,
                              Map<String, CollectorTotal> collectorTotals,
                              Map<String, PerfQuantile> perfQuantiles) {
            name = gc.getName();
            CollectorTotal total;
            synchronized (collectorTotals) {
                total = collectorTotals.get(name);
                if (total == null) {
                    total = new CollectorTotal();
                    collectorTotals.put(name, total);
                }
            }
            count = total.updateCount(gc.getCollectionCount());
            time = total.updateTime(gc.getCollectionTime());

            final PerfQuantile quantile = perfQuantiles.get(gc.getName());
            if (quantile != null) {
                final RateResult result =
                    quantile.rateSinceLastTime("gcLatency");
                percent95 = result.get95th();
                percent99 = result.get99th();
                max = result.getMax();
            } else {
                percent95 = -1;
                percent99 = -1;
                max = -1;
            }
        }

        /**
         * Returns the name of the GC.
         *
         * @return the GC name
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the number of times the GC was called in this period.
         *
         * @return the GC count
         */
        public long getCount() {
            return count;
        }

        /**
         * Returns the total amount of time in milliseconds spent by the GC in
         * this period.
         *
         * @return the total GC time
         */
        public long getTime() {
            return time;
        }

        /**
         * Returns the 95th percentile of times, in milliseconds, for
         * individual GCs in this period, or -1 if not known.
         *
         * @return the 95th percentile GC time or -1
         */
        public long get95th() {
            return percent95;
        }

        /**
         * Returns the 99th percentile of times, in milliseconds, for
         * individual GCs in this period, or -1 if not known.
         *
         * @return the 99th percentile GC time or -1
         */
        public long get99th() {
            return percent99;
        }

        /**
         * Returns the maximum time in milliseconds of individual GCs in this
         * period, or -1 if not known.
         *
         * @return the maximum GC time or -1
         */
        public long getMax() {
            return max;
        }

        void getFormattedStats(StringBuilder sb) {
            if (count > 0) {
                sb.append("\n");
                sb.append("GC ");
                sb.append(name);
                sb.append("\n\tcount=");
                sb.append(count);
                sb.append("\n\ttime=");
                sb.append(time);
                if (percent95 >= 0) {
                    sb.append("\n\t95th=");
                    sb.append(percent95);
                }
                if (percent99 >= 0) {
                    sb.append("\n\t99th=");
                    sb.append(percent99);
                }
                if (max >= 0) {
                    sb.append("\n\tmax=");
                    sb.append(max);
                }
            }
        }
    }

    private final long start;
    private final long end;

    /**
     * The amount of free memory in the Java Virtual Machine. This field is set
     * to the value returned by
     * {@link java.lang.Runtime#freeMemory() Runtime.freeMemory} at the time
     * this object is constructed.
     *
     * @see java.lang.Runtime#freeMemory Runtime.freeMemory
     */
    private final long freeMemory;

    /**
     * The maximum amount of memory that the Java virtual machine will attempt
     * to use. This field is set to the value returned by
     * {@link java.lang.Runtime#maxMemory() Runtime.maxMemory} (or for Zing,
     * a JVM-specific method) at the time this object is constructed.
     *
     * @see java.lang.Runtime#maxMemory() Runtime.maxMemory
     */
    private final long maxMemory;

    /**
     * The total amount of memory in the Java virtual machine. This field is set
     * to the value returned by
     * {@link java.lang.Runtime#totalMemory() Runtime.totalMemory} at the time
     * this object is constructed.
     *
     * @see java.lang.Runtime#totalMemory() Runtime.totalMemory
     */
    private final long totalMemory;

    /** The number of JVM pauses detected. */
    private final long pauseCount;

    /** The 95th percentile of JVM pause times, in milliseconds. */
    private final int pause95;

    /** The 99th percentile JVM pause times, in milliseconds. */
    private final int pause99;

    /** The maximum JVM pause time, in milliseconds. */
    private final int pauseMax;

    /**
     * Garbage collectors operating in the Java virtual machine.
     */
    private final List<CollectorInfo> collectors;

    /**
     * Constructor. The JVM information contained in this object is collected
     * at construction time.
     */
    private JVMStats(long start,
                     long end,
                     Map<String, CollectorTotal> collectorTotals,
                     Map<String, PerfQuantile> gcPerfQuantiles,
                     PerfQuantile pauseQuantile) {
    	this.start = start;
    	this.end = end;
        Runtime rt = Runtime.getRuntime();
        this.freeMemory = rt.freeMemory();
        this.maxMemory = JVMSystemUtils.getRuntimeMaxMemory();
        this.totalMemory = rt.totalMemory();
        final RateResult pauseResult =
            pauseQuantile.rateSinceLastTime("pause");
        pauseCount = pauseResult.getOperationCount();
        pause95 = pauseResult.get95th();
        pause99 = pauseResult.get99th();
        pauseMax = pauseResult.getMax();
        final List<GarbageCollectorMXBean> gcBeans =
            ManagementFactory.getGarbageCollectorMXBeans();
        collectors = new ArrayList<CollectorInfo>(gcBeans.size());
        for (GarbageCollectorMXBean gc : gcBeans) {
            collectors.add(
                new CollectorInfo(gc, collectorTotals, gcPerfQuantiles));
        }
    }

    /**
     * Returns the amount of free memory in the Java Virtual Machine.
     *
     * @return the amount of free memory
     */
    public long getFreeMemory() {
        return freeMemory;
    }

    /**
     * Returns the maximum amount of memory the Java Virtual Machine will
     * attempt to use.
     *
     * @return the maximum amount of memory
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Returns the total amount of memory in the Java Virtual Machine.
     *
     * @return the total amount of memory
     */
    public long getTotalMemory() {
        return totalMemory;
    }

    /**
     * Returns the number of JVM pauses detected in this period.
     *
     * @return the number of pauses
     */
    public long getPauseCount() {
        return pauseCount;
    }

    /**
     * Returns the 95th percentile of JVM pause times, in milliseconds.
     *
     * @return the 95th percentile pause time
     */
    public long getPause95th() {
        return pause95;
    }

    /**
     * Returns the 99th percentile of JVM pause times, in milliseconds.
     *
     * @return the 99th percentile pause time
     */
    public long getPause99th() {
        return pause99;
    }

    /**
     * Returns the maximum of JVM pause times, in milliseconds.
     *
     * @return the maximum pause time
     */
    public long getPauseMax() {
        return pauseMax;
    }

    /**
     * Returns information about garbage collectors operating in the Java
     * Virtual Machine.
     *
     * @return information about garbage collectors
     */
    public List<CollectorInfo> getCollectors() {
        return collectors;
    }

    /* -- From ConciseStats -- */

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public String getFormattedStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("Memory");
        sb.append("\n\tfreeMemory=");
        sb.append(freeMemory);
        sb.append("\n\tmaxMemory=");
        sb.append(maxMemory);
        sb.append("\n\ttotalMemory=");
        sb.append(totalMemory);
        if (pauseCount > 0) {
            sb.append("\n\tpauseCount=");
            sb.append(pauseCount);
        }
        if (pause95 > 0) {
            sb.append("\n\tpause95th=");
            sb.append(pause95);
        }
        if (pause99 > 0) {
            sb.append("\n\tpause99th=");
            sb.append(pause99);
        }
        if (pauseMax > 0) {
            sb.append("\n\tpauseMax=");
            sb.append(pauseMax);
        }
        for (CollectorInfo gc : collectors) {
            gc.getFormattedStats(sb);
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return "JVMStats[" + getFormattedStats() + "]";
    }
}

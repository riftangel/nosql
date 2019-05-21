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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.utilint.LongAvgRate;

/**
 * Collects and shows information about counts.
 *
 * The metrics measure a count (monotonically increasing) value and its
 * statistics. The value is incremented by calling incrementAndGet(). The
 * statistics are updated periodically by calling update() from a timer, and
 * can be obtained by calling get() which will return a string representation
 * of the metrics and reset the statistics.
 */
public class CountMetrics {

    private final String name;
    private final AtomicLong count = new AtomicLong();
    private final LongAvgRate averageRate;
    private volatile long resetCnt = 0;
    private volatile long resetTimeMs = System.currentTimeMillis();
    private volatile long currCnt = 0;

    public CountMetrics(String name, long periodMillis) {
        this.name = name;
        this.averageRate =
            new LongAvgRate("expoAvg",
                    periodMillis, /* averaging period */
                    TimeUnit.SECONDS /* report unit */);
    }

    /**
     * Increments the count by one and returns the updated value.
     */
    public long incrementAndGet() {
        return count.incrementAndGet();
    }

    /**
     * Updates the count metrics with current count value.
     */
    public void update() {
        long value = count.get();
        averageRate.add(value, System.currentTimeMillis());
        currCnt = value;
    }

    /**
     * Gets the string representation of metrics for monitoring.
     */
    public String get() {
        Snapshot snapshot = getSnapshot();
        reset();
        return String.format("[%s]%s", name, snapshot.toString());
    }

    /**
     * Returns a snapshot.
     */
    public Snapshot getSnapshot() {
        long avg = averageRate.get();
        float curr = getCurrentRate();
        return new Snapshot(avg, curr);
    }


    /**
     * A snap shot of metrics of the count.
     */
    public class Snapshot {

        private final long average;
        private final float current;

        private Snapshot(long average, float current) {
            this.average = average;
            this.current = current;
        }

        public long average() {
            return average;
        }

        public float current() {
            return current;
        }

        @Override
        public String toString() {
            return String.format("avg=%d, curr=%.1f", average, current);
        }
    }

    /**
     * Gets the rate between the current time and last reset.
     */
    private float getCurrentRate() {
        long currTimeMs = System.currentTimeMillis();
        long intervalMs = currTimeMs - resetTimeMs;
        if (intervalMs <= 0) {
            return 0;
        }
        return (currCnt - resetCnt) / (intervalMs / 1000f);
    }

    /**
     * Resets the count metrics.
     */
    private void reset() {
        resetCnt = currCnt;
        resetTimeMs = System.currentTimeMillis();
    }
}

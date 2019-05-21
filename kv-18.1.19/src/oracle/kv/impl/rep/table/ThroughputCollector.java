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

package oracle.kv.impl.rep.table;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.TableAccessException;
import oracle.kv.TableSizeLimitException;
import oracle.kv.ThroughputLimitException;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.ThroughputTracker;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.measurement.TableInfo;
import oracle.kv.impl.rep.admin.ResourceInfo.RateRecord;

/**
 * An object for collecting throughput data for a table. The collector records
 * throughput in two ways, in one seconds chunks and a running total. The data
 * collected in chunks can be aggregated across nodes with some accuracy. The
 * running total is better suited for monitoring throughput on a single node.
 *
 * The collector maintains a array of buckets. Each bucket represents one
 * second of throughput data. When data comes in, the current time (in
 * seconds) is used as an index into the array. The data is then recorded
 * into that bucket. The the buckets are updated, if the target bucket is
 * from an earlier second, the counts in that bucket are reset. Note that
 * since the index is based on time some buckets will be "skipped" over and
 * the buckets can appear out-of-order with regard to the second they
 * represent. This can be used to filter old buckets when collecting them
 * for aggregation.
 *
 * In addition to collecting data in buckets, the read/write bytes is summed
 * as single totals. When the totals are read (by the monitoring system) the
 * totals are reset.
 */
public class ThroughputCollector implements ThroughputTracker {
    /* Array size must be power of 2 */
    private static final int ARRAY_SIZE = 8;

    /* The mask is used to create an index from a time (in seconds). */
    private static final int INDEX_MASK = ARRAY_SIZE-1;

    /* Per read/write throughput is rounded up to this value */
    public final static int RW_BLOCK_SIZE = 1024;

    /* Use to convert bytes to GB */
    private static final long GB = 1024L * 1024L * 1024L;

    /*
     * The table and limit references are updated when the table metadata
     * is updated.
     */
    private volatile TableImpl table;

    private final RateBucket[] buckets = new RateBucket[ARRAY_SIZE];

    /* Running totals to support telemetry */
    private volatile long collectionStartMillis;
    private final AtomicInteger totalReadKB = new AtomicInteger();
    private final AtomicInteger totalWriteKB = new AtomicInteger();
    private final AtomicInteger readThroughputExceptions = new AtomicInteger();
    private final AtomicInteger writeThroughputExceptions = new AtomicInteger();
    private final AtomicInteger sizeExceptions = new AtomicInteger();
    private final AtomicInteger accessExceptions = new AtomicInteger();

    /* Reported read and write rates used for rate limiting */
    private final AtomicInteger readRate = new AtomicInteger();
    private final AtomicInteger writeRate = new AtomicInteger();

    /* Aggregate throughput tracker */
    private final ThroughputTracker aggregateTracker;

    // TODO - There is a risk of the table size being updated and the
    // sizeLimitExceeded set and never updated again if the aggregation service
    // fails. This will leave the table unavailable even if the size was
    // reduced. A suggestion is that we provide a timer to clear the flag
    // if the AS does not report for some period of time. Probably best done
    // in the RN or table manager.

    /*
     * Reported table size used for enforcing size limit. The value of -1
     * indicates that the size is now known.
     */
    private volatile long size = -1L;

    /* Updated when the limit changes or the size changes */
    private volatile boolean sizeLimitExceeded = false;

    /*
     * This is the difference between System.currentTimeMills() and
     * System.nanoTime() at the time of object creation. This is used to
     * synthesize the current second from nanoTime(). The nano time is
     * available in the request handler, and it avoids calling
     * currentTimeMills() in updateReadWriteBytes() which can be expensive.
     */
    private final long timeDeltaMills;

    ThroughputCollector(TableImpl table, ThroughputTracker aggregateTracker) {
        this.aggregateTracker = aggregateTracker;
        updateTable(table);

        for (int i = 0; i < ARRAY_SIZE; i++) {
            buckets[i] = new RateBucket();
        }
        final long currentMillis = System.currentTimeMillis();
        final long currentNanos = System.nanoTime();
        timeDeltaMills = currentMillis - NANOSECONDS.toMillis(currentNanos);
        collectionStartMillis = currentMillis;
    }

    /**
     * Updates the table instance. The table instance may change due to a
     * table metadata update.
     */
    public final void updateTable(TableImpl newTable) {
        assert newTable.isTop();
        assert newTable.getTableLimits() != null;
        assert newTable.getTableLimits().hasThroughputLimits() ||
               newTable.getTableLimits().hasSizeLimit();
        table = newTable;
        /* This will set the sizeLimitExceeded flag */
        updateSize(size);
    }

    /*
     * Updates the size and sizeLimitExceeded flag. Method is synchronized
     * to keep sizeLimitExceeded consistent since it is called during a
     * metadata update and a rate update.
     */
    private synchronized void updateSize(long newSize) {
        if (newSize >= 0) {
            size = newSize;
            final int sizeGB = (int)(size / GB);
            sizeLimitExceeded =
                    (sizeGB > table.getTableLimits().getSizeLimit());
        }
    }

    @Override
    public int addReadBytes(int bytes, boolean isAbsolute) {
        /* Record aggregated RN throughput */
        int readKB = aggregateTracker.addReadBytes(bytes, isAbsolute);

        /* Costs are double if using absolute consistency */
        if (isAbsolute) {
            readKB += readKB;
        }
        updateReadWriteBytes(System.nanoTime(), readKB, 0);
        return readKB;
    }

    @Override
    public int addWriteBytes(int bytes, int nIndexWrites) {
        /* Record aggregated RN throughput */
        final int writeKB = aggregateTracker.addWriteBytes(bytes, nIndexWrites);
        updateReadWriteBytes(System.nanoTime(), 0, writeKB);
        return writeKB;
    }

    /*
     * Updates the collector with the specified throughput information. The
     * bytes are accumulated in the "current" bucket.
     */
    private void updateReadWriteBytes(long timeNanos, int readKB, int writeKB) {

        /* Synthesize the current second from nanoSeconds() */
        final long currentTimeMillis =
                            NANOSECONDS.toMillis(timeNanos) + timeDeltaMills;
        final long second = currentTimeMillis / 1000;
        final int index = (int)second & INDEX_MASK;
        final RateBucket bucket = buckets[index];
        if (bucket != null) {
            bucket.updateReadWriteKB(second, readKB, writeKB);
        }

        /* Update the running totals */
        totalReadKB.addAndGet(readKB);
        totalWriteKB.addAndGet(writeKB);
    }

    /**
     * Checks if accessed is permitted and whether a limit has been exceeded.
     * If either accessed is denied or a limit exceeded a
     * ResourceExceededException is thrown. The type of access check is based
     * on the operation.
     */
    public void checkForLimitExceeded(InternalOperation internalOp) {

        /* Check access */
        final boolean performsWrite = internalOp.performsWrite();
        if (performsWrite) {
            /* If a delete skip the size check to allow size reduction. */
            checkAccess(true, !internalOp.isDelete());
        } else {
            checkAccess(false, false);
        }

        final TableLimits limits = table.getTableLimits();

        /*
         * Check read limit.
         */
        if (internalOp.performsRead()) {
            /* Clear the readRate to prevent reporting multiple failures */
            final int rr = readRate.getAndSet(0);
            final int readLimit = limits.getReadLimit();
            if (rr > readLimit) {
                readThroughputExceptions.incrementAndGet();
                throw new ThroughputLimitException(
                            table.getName(), rr, readLimit, 0, 0,
                            "Read throughput rate exceeded for table " +
                            table.getName() + ". Actual: " + rr +
                            " KB/Sec Limit " + readLimit + " KB/Sec");
            }
        }

        /* Only check write overage on writes */
        if (!performsWrite) {
            return;
        }

        /*
         * Check write limit.
         *
         * Clear the writeRate to prevent reporting multiple failures.
         */
        final int wr = writeRate.getAndSet(0);
        final int writeLimit = limits.getWriteLimit();
        if (wr > writeLimit) {
            writeThroughputExceptions.incrementAndGet();
            throw new ThroughputLimitException(
                        table.getName(), 0, 0, wr, writeLimit,
                        "Write throughput rate exceeded for table " +
                        table.getName() + ". Actual: " + wr +
                        " KB/Sec Limit: " + writeLimit + " KB/Sec");
        }


    }

    /*
     * Checks access to the table. If isWrite is true, the table must permit
     * read and write access, otherwise the table must permit read. If
     * checkSize is true the table size is also checked.
     */
    void checkAccess(boolean isWrite, boolean checkSize) {
        final TableLimits limits = table.getTableLimits();
        if (!limits.isReadAllowed()) {
            accessExceptions.incrementAndGet();
            throw new TableAccessException(
                    table.getName(), limits.isReadAllowed(),
                    "Access not permitted to table " + table.getName());
        }
        if (isWrite && !limits.isWriteAllowed()) {
            accessExceptions.incrementAndGet();
            throw new TableAccessException(
                    table.getName(), limits.isReadAllowed(),
                    "Table " + table.getName() + " is read-only");
        }
        if (checkSize && sizeLimitExceeded) {
            assert isWrite;
            assert size >= 0;
            sizeExceptions.incrementAndGet();
            /*
             * There is a tiny window between the sizeLimitExceeded test above
             * and here in which the size can change. It does not make the
             * exception invalid, but the reported size could be incorrect.
             * Not worth any effort to correct.
             */
            final int sizeGB = (int)(size / GB);
            final int limitGB = limits.getSizeLimit();
            throw new TableSizeLimitException(
                            table.getName(), sizeGB, limitGB,
                            "Size exceeded for table " + table.getName() +
                            ", size limit: " + limitGB +
                            "GB, table size: " + sizeGB + "GB");
        }
    }

    /**
     * Collects the rate records since the specified time. The records are
     * added to the specified resource info. Each RateBucket is scanned to
     * see if it contains data newer than since. The current bucket (indexed
     * by nowSec) is skipped.
     */
    void collectRateRecords(Set<RateRecord> records,
                            long sinceSec, long nowSec) {
        int currentIndex = (int)nowSec & INDEX_MASK;
        for (int i = 0; i < ARRAY_SIZE; i++) {
            /* Leave out the current record since it may be active */
            if (i == currentIndex) {
                continue;
            }

            /*
             * There is no need to filter out empty buckets. If there has not
             * been any activity during a given second, the bucket for that
             * second would not have been updated. It will remain an "old"
             * second and get filtered by the sinceSec check.
             *
             * Note that the check and creation of the RateRecord is not
             * atomic. This could lead to incorrect data. Since we are skipping
             * the current record this should be infrequent. It would require
             * additional synchronization with the request/response path to
             * close this window.
             */
            final RateBucket bucket = buckets[i];
            if (bucket.second.get() >= sinceSec) {
                records.add(new RateRecord(table,
                                           bucket.second.get(),
                                           bucket.readKB.get(),
                                           bucket.writeKB.get()));
            }
        }
    }

    /*
     * Resets the read and write rates.
     */
    public void resetReadWriteRates() {
        setReadWriteRates(0, 0);
    }

    /*
     * Records table size and throughput rates. Values are unaffected if the
     * new values are < 0;
     */
    public void report(long newSize, int newReadRate, int newWriteRate) {
        updateSize(newSize);
        setReadWriteRates(newReadRate, newWriteRate);
    }

    /*
     * Sets the read and write rates. The rates are unaffected if the new
     * rate is < 0;
     */
    private void setReadWriteRates(int newReadRate, int newWriteRate) {
        if (newReadRate >= 0) {
            readRate.set(newReadRate);
        }
        if (newWriteRate >= 0) {
            writeRate.set(newWriteRate);
        }
    }

    /**
     * Gets a TableInfo object. If there has been no activity since the last
     * time getTableInfo() was called, null is returned. Otherwise the TableInfo
     * object is filled-in with the data collected since that time.
     *
     * @return a TableInfo object or null
     */
    TableInfo getTableInfo(long currentTimeMillis) {
        final long startTime = collectionStartMillis;
        final long duration = currentTimeMillis - collectionStartMillis;
        /* Avoid math problems later */
        if (duration <= 0) {
            return null;
        }
        collectionStartMillis = currentTimeMillis;
        final int readKB = totalReadKB.getAndSet(0);
        final int writeKB = totalWriteKB.getAndSet(0);
        final int rte = readThroughputExceptions.getAndSet(0);
        final int wte = writeThroughputExceptions.getAndSet(0);
        final int se = sizeExceptions.getAndSet(0);
        final int ae = accessExceptions.getAndSet(0);
        if ((readKB > 0) || (writeKB > 0) ||
            (rte > 0) || (wte > 0) || (se > 0) || (ae > 0)) {
            return new TableInfo(table.getNamespaceName(),
                                 startTime, duration,
                                 readKB, writeKB, size,
                                 rte, wte, se, ae);
        }
        return null;
    }

    /**
     * An object to collect rate information for one second.
     */
    private class RateBucket {
        private final AtomicLong second = new AtomicLong();
        private final AtomicInteger readKB = new AtomicInteger();
        private final AtomicInteger writeKB = new AtomicInteger();

        private void updateReadWriteKB(long currentSecond,
                                       int currentReadKB,
                                       int currentWriteKB) {

            /*
             * If this is a new second set the bytes to the current bytes to act
             * as a reset, otherwise add the current bytes. Note that there is a
             * window between the second.getAndSet and read/write add/set calls
             * where data can be lost if two threads hit his gap at the same
             * time. This loss is not worth the cost of additional
             * synchronization.
             */
            final long oldSecond = second.getAndSet(currentSecond);
            if (oldSecond == currentSecond) {
                currentReadKB = readKB.addAndGet(currentReadKB);
                currentWriteKB = writeKB.addAndGet(currentWriteKB);
            } else {
                readKB.set(currentReadKB);
                writeKB.set(currentWriteKB);
            }

            /*
             * Check if this op has pushed us over a limit. If true, mark
             * the table so that the next operation will fail.
             */
            if (table.getTableLimits().throughputExceeded(currentReadKB,
                                                          currentWriteKB)) {
                setReadWriteRates(currentReadKB, currentWriteKB);
            }
        }

        @Override
        public String toString() {
            return "RateBucket[" + second + ", " + readKB + ", "
                   + writeKB + "]";
        }
    }

    @Override
    public String toString() {
        return "ThroughputCollector[" + table.getId() + "]";
    }
}
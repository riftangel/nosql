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

package oracle.kv;

import java.util.concurrent.TimeUnit;

import oracle.kv.table.WriteOptions;

/**
 * BulkWriteOptions is used to configure bulk write operations.
 *
 * The default values, documented in the setter methods, should be a good
 * choice for a wide range of conditions. If you should wish to fine tune the
 * bulk load operation further, you can use these values as a starting point
 * for a benchmark using your own application data and actual hardware.
 *
 * @since 4.0
 */
public class BulkWriteOptions extends WriteOptions {
   /*
    * TODO:
    *
    * 1) Allow for passing in (stream and shard) thread pools, so they can be
    * reused across batch loads?
    *
    * 2) Can such pools be shared across concurrent putBatch operations? The
    * current code relies on pool shutdown which requires exclusive use of
    * pools.
    */
    private final static int MIN_REQUEST_SIZE = 64 * 1024;

    private int bulkHeapPercent = 40;

    private int maxRequestSize = 512 * 1024;

    private int perShardParallelism = 3;

    private int streamParallelism = 1;

    /**
     * The options used to configure the bulk put operation.
     *
     * @param durability the durability to be used by the underlying
     * write operations that make up the bulk put.
     *
     * @param timeout the timeout associated with the underlying
     * write operations that make up the bulk put.
     *
     * @param timeoutUnit the units associated with the timeout
     */
    public BulkWriteOptions(Durability durability,
                            long timeout,
                            TimeUnit timeoutUnit) {
        super(durability, timeout, timeoutUnit);
    }

    /* Internal copy constructor. */
    public BulkWriteOptions(BulkWriteOptions options) {
        super(options);

        bulkHeapPercent = options.bulkHeapPercent;
        maxRequestSize = options.maxRequestSize;
        perShardParallelism = options.perShardParallelism;
        streamParallelism = options.streamParallelism;
    }

    /**
     * Create a {@code BulkWriteOptions} with default values.
     */
    public BulkWriteOptions() {
        super();
    }

    /**
     * Returns the percentage of Runtime.maxMemory() that can be used for
     * the operation.
     */
    public int getBulkHeapPercent() {
        return bulkHeapPercent;
    }

    /**
     * The percentage of Runtime.maxMemory() that can be used for the
     * operation. This heap is used to assemble batches of entries
     * associated with specific shards and partitions.
     * <p>
     * The default is 40%.
     * </p>
     */
    public void setBulkHeapPercent(int bulkHeapPercent) {

        if (bulkHeapPercent > 100) {
            throw new IllegalArgumentException
                ("Percentage:" + bulkHeapPercent + " cannot exceed 100");
        }

        if (bulkHeapPercent < 1 ) {
            throw new IllegalArgumentException
                ("Percentage:" + bulkHeapPercent + " cannot be less than 1");
        }
        this.bulkHeapPercent = bulkHeapPercent;
    }

    /**
     * Returns the max number of bytes of records in a single bulk put request.
     */
    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    /**
     * The max request size is used to limit the total number of bytes of
     * records in a single bulk put request.
     * <p>
     * The default is 512K.
     * </p>
     */
    public void setMaxRequestSize(int maxRequestSize) {

        if (maxRequestSize < MIN_REQUEST_SIZE) {
            throw new IllegalArgumentException
                ("Max request size:" + maxRequestSize + " cannot be less " +
                 "than " + MIN_REQUEST_SIZE);
        }
        this.maxRequestSize = maxRequestSize;
    }

    /**
     * The maximum number of threads that can concurrently write a batch
     * of entries to a single shard in the store.
     */
    public int getPerShardParallelism() {
        return perShardParallelism;
    }

    /**
     * Sets the maximum number of threads that can concurrently write it's
     * batch of entries to a single shard in the store.
     * <p>
     * The default value is 3 and allows for overlapping the reading of the
     * next batch with processing of the current batch at a server node.
     * Higher capacity networks and and storage nodes can allow for
     * higher parallelism.
     * </p>
     */
    public void setPerShardParallelism(int perShardParallelism) {
        if (perShardParallelism < 1 ) {
            throw new IllegalArgumentException
                ("Maximum number of threads per shard:" + perShardParallelism +
                 " cannot be less than 1");
        }
        this.perShardParallelism = perShardParallelism;
    }

    /**
     * Returns the maximum number of streams that can be read concurrently.
     * Each stream is read by a dedicated thread from a thread pool. This
     * setting  determines the size of the thread pool used for
     * reading streams.
     */
    public int getStreamParallelism() {
        return streamParallelism;
    }

    /**
     * Sets the maximum number of streams that can be read concurrently.
     * Each stream is read by a dedicated thread from a thread pool. This
     * setting determines the size of the thread pool used for reading
     * streams.
     * <p>
     * The default parallelism is 1. For streams with high overheads, say
     * because the I/O device underlying the stream is slow and there are
     * different I/O devices underlying each stream, a higher value would
     * be appropriate.
     * </p>
     */
    public void setStreamParallelism(int streamParallelism) {
        if (streamParallelism < 1 ) {
            throw new IllegalArgumentException
                ("Maximum number of streams concurrently read:" +
                 streamParallelism + " cannot be less than 1");
        }
        this.streamParallelism = streamParallelism;
    }
}


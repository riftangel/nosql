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

package oracle.kv.impl.async.dialog.nio;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.util.KVThreadFactory;

public class NioChannelThreadPool {

    private final static AtomicInteger sequencer = new AtomicInteger(0);

    private final Logger logger;
    private final int id;
    private final AtomicReferenceArray<NioChannelExecutor> executors;
    private final AtomicInteger index = new AtomicInteger();
    private final KVThreadFactory threadFactory;

    /**
     * Construct the thread pool.
     *
     * @param num the number of executors.
     */
    public NioChannelThreadPool(Logger logger, int num)
        throws IOException, InterruptedException {

        if (num <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                        "Number of executors should be positive, got %d",
                        num));
        }
        this.logger = logger;
        this.id = sequencer.incrementAndGet();
        this.executors = new AtomicReferenceArray<NioChannelExecutor>(num);
        this.threadFactory = new KVThreadFactory(
                NioChannelThreadPool.class.getName(), logger);
        final Thread[] threads = new Thread[num];
        for (int i = 0; i < num; ++i) {
            boolean success = false;
            try {
                final Thread thread = createExecutor(i);
                if (thread == null) {
                    throw new IllegalStateException(
                            "Concurrent initialization of executors " +
                            "during thread pool construction; " +
                            "something is wrong");
                }
                threads[i] = thread;
                success = true;
            } finally {
                if (!success) {
                    for (int j = 0; j < i; ++j) {
                        final NioChannelExecutor executor = executors.get(j);
                        if (executor != null) {
                            executor.shutdownNow();
                        }
                        threads[j].join();
                    }
                }
            }
        }
    }

    /**
     * Returns the id.
     *
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Returns an executor of the group.
     *
     * @return the executor
     */
    public NioChannelExecutor next() {
        return executors.get(Math.abs(
                index.getAndIncrement() % executors.length()));
    }

    /**
     * Shutdown the group.
     *
     * @param force true if not wait for queued tasks in the executors
     */
    public void shutdown(boolean force) {
        for (int i = 0; i < executors.length(); ++i) {
            final NioChannelExecutor executor = executors.get(i);
            if (executor != null) {
                if (force) {
                    executor.shutdownNow();
                } else {
                    executor.shutdown();
                }
            }
        }
    }

    private Thread createExecutor(int childId)
        throws IOException {

        final NioChannelExecutor prev = executors.get(childId);

        if (prev != null) {
            prev.shutdownNow();
        }

        NioChannelExecutor executor = new NioChannelExecutor(
                logger, this, childId);
        if (executors.compareAndSet(childId, prev, executor)) {
            Thread thread = threadFactory.newThread(executor);
            thread.start();
            logger.log(Level.FINE, "New executor started: {0}", executor);
            return thread;
        }
        return null;
    }
}

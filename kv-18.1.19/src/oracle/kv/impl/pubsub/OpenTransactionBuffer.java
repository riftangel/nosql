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

package oracle.kv.impl.pubsub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableVersionException;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.VLSN;

/**
 * Object maintaining a list of open transactions received from source. When a
 * transaction message is received, the whole txn in openTxnBuffer will be
 * processed as a whole by client defined callback.
 *
 * For each key transferred from the Partition Migration Service, openTxnBuffer
 * would treat each key as an single txn. The callback is fired for each txn.
 * The openTxnBuffer remembers VLSN of the last txn processed.
 */
class OpenTransactionBuffer {

    /* timeout to poll input queue */
    private static final int INPUT_QUEUE_POLL_TIMEOUT_MS = 1000;

    /* private logger */
    private final Logger logger;
    /* parent consumer */
    private final ReplicationStreamConsumer parent;
    /* open txn buffer indexed by transaction ID */
    private final Map<Long, List<DataEntry>> openTxnBuffer;
    /* FIFO queue from which to dequeue messages from replication stream */
    private final BlockingQueue<? extends DataEntry> inputQueue;
    /* FIFO queue to which to enqueue messages for client */
    private final BlockingQueue<? super StreamOperation> outputQueue;
    /* replication group this worker belongs to */
    private final RepGroupId repGroupId;

    /* true if user subscribes all tables */
    private final boolean streamAllTables;
    /*
     * Map of cached subscribed tables.
     *
     * The map is indexed by root table id string. If streaming is limited to a
     * set of tables, then the list of tables for the root table id string
     * contains all subscribed tables with that root table as their parent,
     * possibly including the root table. If streaming all tables, then the
     * list contains just the root table.
     *
     * If user specifies subscribed tables, it will be populated with cached
     * copy of subscribed tables when OTB is constructed, and any cached
     * table will be refreshed only when table version exception for that table
     * is raised during streaming. During refresh, if OTB detects that a
     * subscribed table is dropped at kvstore, copy of the dropped table will
     * be removed from cache. Since then OTB will no longer stream any ops
     * from the dropped table. Note even the user re-create a new table with the
     * same name later, it is not considered as the same subscribed table
     * that has been dropped, thus it wont be streamed.
     *
     * If user does not specify any subscribed table, OTB considers that user
     * would like to subscribe all tables at kvstore, both existing at the
     * time of subscription, and newly created tables during subscription.
     * OTB will not cache any table when OTB is constructed, instead, it will
     * populate the cache during streaming. If table version mismatch is
     * found, OTB will refresh cached table. During refresh if the table is
     * found to have been dropped, OTB will remove it from cache, and will no
     * longer stream any ops from the dropped tables. Note in this case, if
     * user re-create a new tale with the same table, the newly table will be
     * streamed since the user subscribes all the tables.
     *
     */
    private final Map<String, List<TableImpl>> cachedTables;

    /*
     * Map of table id and table name of dropped table, if a table is dropped
     * and detected during refresh, OTB will not stream any ops from that table.
     *
     * Note that it is only queried when streamAllTables is true, as a way of
     * distinguishing a dropped table missing from cachedTables, from a brand
     * new table that is never cached before.
     *
     * This set could grow arbitrarily large over time as additional tables are
     * dropped. This is the cost we pay to support subscribe all tables,
     * until we are able to include DDL in the replication stream when we are
     * able to tell exactly when the table has been dropped.
     *
     */

     /*
      * TODO: an LRU cache with bounded size to clear out old entries, with
      * idea that entries for older dropped tables will eventually no longer
      * appear in the stream.
      */
    private final Set<String> droppedTables;

    /* statistics */

    /* VLSN of last txn committed */
    private VLSN lastCommitVLSN;
    /* VLSN of last txn aborted */
    private VLSN lastAbortVLSN;
    /* max # of open txns in buffer since creation */
    private AtomicLong maxOpenTxn;

    /*
     * Below are incremental stats and record the change between updates.
     * Parent RSC will update these stats to PU periodically where they are
     * aggregated and added up to previous value.
     */

    /* # of committed ops */
    private AtomicLong numCommitOps;
    /* # of aborted ops */
    private AtomicLong numAbortOps;
    /* # of committed txns */
    private AtomicLong numCommitTxn;
    /* # of aborted txns */
    private AtomicLong numAbortTxn;

    /* worker thread for the buffer */
    private volatile TxnWorkerThread workerThread;

    OpenTransactionBuffer(ReplicationStreamConsumer parent,
                          RepGroupId repGroupId,
                          BlockingQueue<? extends DataEntry> inputQueue,
                          BlockingQueue<? super StreamOperation> outputQueue,
                          Collection<TableImpl> tables,
                          Logger logger) {

        this.parent = parent;
        this.repGroupId = repGroupId;
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.logger = logger;

        /* make a map of local copy of subscribed tables for quick lookup */
        cachedTables = new HashMap<>();
        if (tables == null) {
            /*
             * All tables subscribed.  We will add root tables to the table
             * cache as we encounter their root table IDs.
             */
            streamAllTables = true;
            final String trace = "will stream all tables";
            logger.info(lm(trace));
        } else {
            assert (!tables.isEmpty());
            streamAllTables = false;
            /* build map of subscribed tables */
            String trace = "will stream tables: ";
            for (TableImpl t : tables) {
                final String rootTableId = getTableId(t.getTopLevelTable());
                addCachedTable(rootTableId, t);
                trace += t.getFullName() + "(table id: " + t.getIdString() +
                         ", " + "root table id: " + rootTableId + ")" + ", ";
            }
            logger.info(lm(trace));
        }
        droppedTables = new HashSet<>();

        openTxnBuffer = new HashMap<>();

        lastCommitVLSN = VLSN.NULL_VLSN;
        lastAbortVLSN = VLSN.NULL_VLSN;

        numAbortOps = new AtomicLong(0);
        numCommitOps = new AtomicLong(0);
        numCommitTxn = new AtomicLong(0);
        numAbortTxn = new AtomicLong(0);
        maxOpenTxn = new AtomicLong(0);

        /* to be initialized in startWorker() */
        workerThread = null;
    }

    /**
     * Shut down otb completely
     */
    void close() {

        shutdownWorker();
        workerThread = null;

        clearTxnBuffer();
    }

    /**
     * Starts the worker thread
     */
    void startWorker() {
        /*
         * the worker thread is created and started at the start stream, and
         * re-retry in error
         */
        workerThread = new TxnWorkerThread("txn worker thread");
        workerThread.start();
    }

    /**
     * Gets the VLSN of last committed transaction
     *
     * @return last commit VLSN
     */
    VLSN getLastCommitVLSN() {
        return lastCommitVLSN;
    }

    /**
     * Gets VLSN of last aborted transaction
     *
     * @return last aborted VLSN
     */
    VLSN getLastAbortVLSN() {
        return lastAbortVLSN;
    }

    long getOpenTxns() {
        return openTxnBuffer.size();
    }

    long getMaxOpenTxns(){
        return maxOpenTxn.get();
    }

    /* Update and refresh incremental stats */

    long getCommitTxns() {
        return numCommitTxn.getAndSet(0);
    }

    long getAbortTxns() {
        return numAbortTxn.getAndSet(0);
    }

    long getCommitOps() {
        return numCommitOps.getAndSet(0);
    }

    long getAbortOps() {
        return numAbortOps.getAndSet(0);
    }

    /**
     * Shutdown the worker thread
     */
    private void shutdownWorker() {
        if (workerThread != null && !workerThread.isShutdown()) {
            workerThread.shutdownThread(logger);
        }
        logger.info(lm("OTB worker thread shutdown."));
    }

    /**
     * Refresh the buffer while keep the statistics
     *
     * This is called when a RSC is trying to reconnect to the feeder due to
     * errors, e.g. master transfer. Parent RSC will guarantee that the new
     * connection will stream from the last committed VLSN to subscriber. All
     * unprocessed ops in buffer will be discarded and re-streamed from feeder.
     */
    private void clearTxnBuffer() {

        /* clear input queue */
        inputQueue.clear();

        /* clear all open txns in buffer */
        openTxnBuffer.clear();

        logger.info(lm("Txn buffer refreshed"));
    }

    /*
     * Adds an operation to openTxnBuffer. Create an open txn if it is the first
     * operation in this txn.
     *
     * @param entry   a data entry
     */
    private synchronized void addEntry(DataEntry entry) {
        final long txnid = entry.getTxnID();
        final List<DataEntry> txn =
            openTxnBuffer.computeIfAbsent(txnid, k -> new ArrayList<>());
        txn.add(entry);

        if (maxOpenTxn.get() < openTxnBuffer.size()) {
            maxOpenTxn.set(openTxnBuffer.size());
        }

        parent.getRSCStat()
              .setLastMsgTimeMs(NoSQLSubscriptionImpl.getCurrTimeMs());
    }

    /* Aborts a txn from openTxnBuffer */
    private synchronized void abort(final DataEntry entry) {

        assert (DataEntry.Type.TXN_ABORT.equals(entry.getType()));

        final long txnid = entry.getTxnID();
        final List<DataEntry> txn = openTxnBuffer.remove(txnid);
        if (txn == null) {
            /*
             * feeder filler is unable to filter some commit/abort msg for
             * internal db or dup db. So it is possible we can see some
             * phantom commit/abort without an open txn in buffer. But we
             * wont receive PUT/DEL for such internal db entries, so there
             * is no open txn for such commit/abort in buffer.
             */
            logger.finest("Abort a non-existent txnid " + txnid + ", ignore.");
            return;
        }

        /* remove txn from openTxnBuffer and update openTxnBuffer stats */
        final long numOps =  txn.size();
        numAbortOps.addAndGet(numOps);
        numAbortTxn.getAndIncrement();
        lastAbortVLSN = entry.getVLSN();
        logger.fine(lm("Aborted txn " + txnid + " with vlsn " + lastAbortVLSN +
                       ", # of ops aborted " + numOps));
    }

    /* Commits an open txn from openTxnBuffer  */
    private synchronized void commit(final DataEntry entry)
        throws SubscriptionFailureException, InterruptedException {

        assert (DataEntry.Type.TXN_COMMIT.equals(entry.getType()));

        final long txnid = entry.getTxnID();
        final List<DataEntry> txn = openTxnBuffer.remove(txnid);
        if (txn == null) {
            /*
             * feeder filler is unable to filter some commit/abort msg for
             * internal db or dup db. So it is possible we can see some
             * phantom commit/abort without an open txn in buffer. But we
             * wont receive PUT/DEL for such internal db entries, so there
             * is no open txn for such commit/abort in buffer.
             */
            logger.finest("Ignore a non-existent txn id " + txnid);
            return;
        }

        commitHelper(txn);

        /* remove txn from openTxnBuffer and update openTxnBuffer stats */
        final long numOps =  txn.size();
        numCommitOps.addAndGet(numOps);
        numCommitTxn.getAndIncrement();
        lastCommitVLSN = entry.getVLSN();
        logger.fine(lm("Committed txn " + txnid + " with vlsn " +
                       lastCommitVLSN + ", # of ops committed " + numOps));
    }

    /* Commits a transaction and convert data entry to stream operations */
    private void commitHelper(List<DataEntry> allOps)
        throws SubscriptionFailureException, InterruptedException {
        for (DataEntry entry : allOps) {

            /* sanity check just in case */
            final DataEntry.Type type = entry.getType();
            if ((type != DataEntry.Type.PUT) &&
                (type != DataEntry.Type.DELETE)) {
                throw new IllegalStateException(
                    "Type " + type + " cannot be streamed to client.");
            }
            if (entry.getKey() == null) {
                throw new IllegalStateException("key cannot be null when being " +
                                                "deserialized.");
            }

            final StreamOperation msg = buildStreamOp(entry.getKey(),
                                                      entry.getValue(),
                                                      type,
                                                      entry.getVLSN());

            /* enqueue stream operation */
            enqueueMsg(msg);
            logger.finest(lm("committed op:  " + msg));
        }
    }

    /* Enqueues a msg to output queue */
    private void enqueueMsg(StreamOperation msg) throws InterruptedException {
        if (msg == null) {
            return;
        }

        /* put the message in output queue */
        while (true) {
            try {
                if (outputQueue.offer(msg,
                                      PublishingUnit.OUTPUT_QUEUE_TIMEOUT_MS,
                                      TimeUnit.MILLISECONDS)) {
                    break;
                }

                logger.finest(lm("Unable enqueue a stream operation for " +
                                 (PublishingUnit.OUTPUT_QUEUE_TIMEOUT_MS /
                                  1000) + " seconds, keep trying..."));

            } catch (InterruptedException e) {
                /* This might have to get smarter. */
                logger.warning(lm("Interrupted offering output queue, " +
                                  "throw and let main loop to capture it and " +
                                  "check shutdown"));
                throw e;
            }
        }
    }

    /**
     * Gets a list of tables for the key.  The list contains all subscribed
     * tables with the key's root table ID if streaming specific tables, or
     * just the root table if streaming all tables.
     */
    private List<TableImpl> getCachedTables(byte[] key) {
        final String rootTableId = getRootTableIdString(key);

        /* Check for valid table key */
        if (rootTableId == null) {
            logger.fine(lm("Unable to get root table id from key " +
                           Arrays.toString(key)));
            return null;
        }
        final List<TableImpl> tbList = cachedTables.get(rootTableId);

        if (!streamAllTables) {
            return tbList;
        }

        /* if stream all tables */
        if (tbList != null) {
            /* good, have cached copy */
            return tbList;
        }

        /*
         * Root table was dropped, won't stream any op for this table or its
         * children
         */
        if (droppedTables.contains(rootTableId)) {
            return null;
        }

        /* A root table id we see for the first time */
        final TableImpl table =
            parent.getPu().getTableMDManager()
                  .getTable(rootTableId, parent.getPu().isUseTblNameAsId());
        if (table == null) {
            /* already dropped, a short lived table */
            droppedTables.add(rootTableId);
            return null;
        }

        /* add the root table */
        addCachedTable(rootTableId, table);

        return cachedTables.get(rootTableId);
    }

    /* Builds stream operation */
    private StreamOperation buildStreamOp(byte[] key,
                                          byte[] value,
                                          DataEntry.Type type,
                                          VLSN vlsn)
        throws SubscriptionFailureException {

        /* check if the key belongs to a subscribed table */
        final List<TableImpl> tbList = getCachedTables(key);
        if (tbList == null) {
            /* not a subscribed table or dropped */
            return null;
        }

        /* this key is from a subscribed table, deserialize it */
        for (TableImpl t : tbList) {
            final StreamOperation op = deserialize(t, key, value, type, vlsn);
            if (op != null) {
                return op;
            }
        }

        /* cannot deserialize */
        return null;
    }

    /* deserialize row from given table, null if cannot deserialized */
    private StreamOperation deserialize(TableImpl table,
                                        byte[] key,
                                        byte[] value,
                                        DataEntry.Type type,
                                        VLSN vlsn) {
        try {
            return createMsg(table, key, value, type, vlsn);
        } catch (SubscribedTableVersionException stve) {

            final String rootTableId = getTableId(table.getTopLevelTable());
            final String tableId = getTableId(table);
            /*
             * Ask table md manager in publisher to refresh, if success,
             * make a local copy and replace the old one. If null, it means
             * table has been dropped. If fail to refresh, SFE will be raised
             * from table md manager and propagated to caller.
             */
            final TableImpl refresh = parent.getPu()
                                            .getTableMDManager()
                                            .refreshTable(
                                                stve.getSubscriberId(),
                                                stve.getTable(),
                                                stve.getRequiredVersion(),
                                                tableId);
            if (refresh != null) {
                replaceCachedTable(rootTableId, refresh);
                logger.info(lm("Subscriber " + stve.getSubscriberId()  +
                               " refreshed table " + table.getFullName() +
                               " from version " + stve.getCurrentVer() +
                               " to " + stve.getRequiredVersion()));
                /* we should not fail this time! */
                return createMsg(refresh, key, value, type, vlsn);
            }

            final String err =
                "Subscriber " + stve.getSubscriberId()  +
                " is unable to refreshed table " + table.getFullName() +
                " from version " + stve.getCurrentVer() +
                " to " + stve.getRequiredVersion() +
                ", the table is dropped at kvstore, and no ops from that " +
                "table will be streamed from now on.";

            removeCachedTable(rootTableId, tableId);
            droppedTables.add(tableId);

            /* signal user that table is dropped */
            try {
                parent.getPu().getSubscriber()
                      .onWarn(new IllegalArgumentException(err));

            } catch (Exception exp) {
                logger.warning(lm("Exception in executing " +
                                  "subscriber's onWarn(): " +
                                  exp.getMessage() + "\n" +
                                  LoggerUtils.getStackTrace(exp)));
            }
            logger.warning(lm(err));
            return null;
        }
    }

    /* Creates a message to be consumed by client */
    private StreamOperation createMsg(TableImpl table, byte[] key, byte[] value,
                                      DataEntry.Type type, VLSN vlsn)
        throws SubscribedTableVersionException {

        final StreamSequenceId sequenceId =
            new StreamSequenceId(vlsn.getSequence());
        /* a put operation */
        switch (type) {
            case PUT:
                /* a put operation */
                final Row row = deserializeRow(table, key, value);
                /* key was not associated with a table */
                if (row == null) {
                    return null;
                }
                return new StreamPutEvent(row, sequenceId,
                                          repGroupId.getGroupId());

            case DELETE:
                /* a delete operation */
                final PrimaryKey delKey = deserializePrimaryKey(table, key);
                /* key was not associated with a table */
                if (delKey == null) {
                    return null;
                }

                /*
                 * The parent table can deserialize the primary key from a
                 * child table, so need to check if table matches when not
                 * streaming all tables
                 */
                if (!streamAllTables &&
                    !table.getFullName().equals(delKey.getTable()
                                                      .getFullName())) {
                    return null;
                }
                return new StreamDelEvent(delKey, sequenceId,
                                          repGroupId.getGroupId());


            default:
                /* should never reach here */
                throw new AssertionError("Unrecognized type " + type);
        }
    }

    /*
     * De-serializes to Row from key, value byte arrays from a given table
     *
     * @param table   the target table to deserialize the key/value
     * @param key     key in byte array
     * @param value   value in byte array
     *
     * @return a row deserialized from the given key and value for the table,
     * or null if key/value information does not correspond to a row in the
     * specified table.
     *
     * @throws SubscribedTableVersionException the version in table impl does
     * not match that in the key
     */
    private Row deserializeRow(TableImpl table, byte[] key, byte[] value)
        throws SubscribedTableVersionException {

        try {
            /*
             * Get the target table from the root table when streaming all
             * tables since row deserialization requires the exact table, which
             * may be a child.
             */
            if (streamAllTables) {
                table = table.findTargetTable(key);
            }
            /* Deserialize complete row */
            return table.createRowFromBytes(key, value, table.isKeyOnly(),
                                            false/* do not add missing col */);
        } catch (TableVersionException tve) {
            /* need refresh table md */
            throw new SubscribedTableVersionException(
                parent.getPu().getSubscriberId(), repGroupId,
                table.getFullName(), tve.getRequiredVersion(),
                table.getTableVersion());
        }
    }

    /*
     * De-serializes to a PrimaryKey from key byte arrays from a given table
     *
     * @param table   the target table to deserialize the key/value
     * @param key     key in byte array
     *
     * @return a primary key deserialized from the given key bytes for the
     * table, or null if key information does not correspond to a primary key
     * in the specified table.
     *
     * @throws SubscribedTableVersionException the version in table impl does
     * not match that in the key
     */
    private PrimaryKey deserializePrimaryKey(TableImpl table, byte[] key)
        throws SubscribedTableVersionException {

        try {
            /* a primary key */
            return table.createPrimaryKeyFromKeyBytes(key);
        } catch (TableVersionException tve) {
            /* need refresh table md */
            throw new SubscribedTableVersionException(
                parent.getPu().getSubscriberId(), repGroupId,
                table.getFullName(), tve.getRequiredVersion(),
                table.getTableVersion());
        }
    }

    private String lm(String msg) {
        return "[OTB-" + parent.getConsumerId() + "]" + msg;
    }

    /*
     * Object that processes each entry from replication stream. If the entry
     * is a put or delete, the worker thread will just post it in the open txn
     * buffer, or creates a new open txn. If the entry is a commit, the worker
     * will close the open txn in buffer and send a list of ops to output
     * queue; if the entry is an abort, the worker will just close the txn and
     * forget about it.
     *
     * The worker thread is a thread running in parallel with the thread
     * running as a subscription client, both working together to create a
     * pipelined processing mode.
     */
    private class TxnWorkerThread extends StoppableThread {

        TxnWorkerThread(String workerId) {
            super("TxnWorkerThread-" + workerId);
        }

        @Override
        public void run() {

            logger.info(lm("Txn worker thread starts."));

            try {
                while (!isShutdown()) {

                    final DataEntry entry = inputQueue.poll(
                        INPUT_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    if (entry == null) {
                        logger.fine(lm("Unable to dequeue for " +
                                       (INPUT_QUEUE_POLL_TIMEOUT_MS / 1000) +
                                       " seconds"));
                        continue;
                    }

                    final DataEntry.Type type = entry.getType();
                    switch (type) {
                        case TXN_ABORT:
                            abort(entry);
                            break;
                        case TXN_COMMIT:
                            commit(entry);
                            break;
                        case PUT:
                        case DELETE:
                            addEntry(entry);
                            break;
                        default:
                            throw new AssertionError(type);
                    }
                }
            } catch (SubscriptionFailureException sfe) {
                logger.warning(lm("Txn worker thread has to close pu because " +
                                  sfe.getMessage()));
                parent.getPu().close(sfe);

            } catch (InterruptedException ie) {
                if (isShutdown()) {
                    logger.info(lm("Txn worker thread shut down by rsc"));
                }
            } finally {
                logger.info(lm("Txn worker thread exits."));
            }
        }

        @Override
        protected int initiateSoftShutdown() {
            final boolean alreadySet = shutdownDone(logger);
            logger.fine(lm("Signal to txn worker to shutdown, " +
                           "shutdown already signalled? " + alreadySet +
                           ", wait for " + INPUT_QUEUE_POLL_TIMEOUT_MS +
                           " ms let it exit"));
            return INPUT_QUEUE_POLL_TIMEOUT_MS;
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    private String getTableId(TableImpl t) {
        return PublishingUnit.getTableId(parent.getPu(), t);
    }

    private void addCachedTable(String rootTableId, TableImpl table) {
        final List<TableImpl> tbs = cachedTables.get(rootTableId);
        if (tbs != null) {
            tbs.add(table);
        } else {
            final List<TableImpl> list = new ArrayList<>();
            list.add(table);
            cachedTables.put(rootTableId, list);
        }
    }

    private void removeCachedTable(String rootTableId, String tableId) {
        final List<TableImpl> tbs = cachedTables.get(rootTableId);
        for (TableImpl tbl : tbs) {
            if (getTableId(tbl).equals(tableId)) {
                tbs.remove(tbl);
                break;
            }
        }
    }

    private void replaceCachedTable(String rootTableId, TableImpl table) {
        final List<TableImpl> tbs = cachedTables.get(rootTableId);
        for (int i = 0; i < tbs.size(); i++) {
            if (tbs.get(i).getId() == table.getId()) {
                tbs.set(i, table);
                return;
            }
        }
    }

    /* gets the root table id from key, return null if no valid root id */
    private static String getRootTableIdString(byte[] key) {
        final Key.BinaryKeyIterator keyIter = new Key.BinaryKeyIterator(key);
        if (keyIter.atEndOfKey()) {
            return null;
        }

        try {
            return keyIter.next();
        } catch (RuntimeException exp) {
            /* unable to get a valid root table id from key */
            return null;
        }
    }
}

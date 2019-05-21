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

package oracle.kv.impl.rep.migration;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.admin.RepNodeAdmin.MigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.MigrationManager.DBOperation;
import oracle.kv.impl.rep.migration.PartitionMigrations.TargetRecord;
import oracle.kv.impl.rep.migration.TransferProtocol.OP;
import oracle.kv.impl.rep.migration.TransferProtocol.TransferRequest;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingCollector;
import oracle.kv.util.PingCollector.RNNameHAPort;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Put;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;

/**
 * Partition migration target. This class is the destination side
 * of a source target pair.
 *
 * The migration process is initiated by the target which will attempt to
 * contact the source and send a migration request. This initial request
 * is the only time the target sends data to the source. All other
 * communication is one-way, source to target.
 *
 * Once a connection is established (migration stream) the source will send
 * records (keys and values) read from the partition's DB and client
 * operations targeted for the partition. Additional messages are needed to
 * handle client transactions.
 *
 * The source sends Ops until all records are read read and send to the
 * target. At that time the source will send an End of Data (EoD) message.
 * Once the target received the EoD Op it initiates the Transfer of Ownership
 * protocol (ToO).
 *
 * If the target encounters an error any time during the above steps
 * it may wait and retry. See the MigrationTarget.call() method.
 *
 * During a migration the target consists of two threads. One (Reader) will
 * read operation messages from the source migration stream. Most messages
 * result in Op objects being placed on a queue. The second "consumer" thread
 * (MigrationTarget) removes Ops from the queue and executes them. This
 * continues until an EoD message is encountered.
 *
 * The basic message sequence for a client (non-copy) operation is:
 *
 *      1. Op (Put, Delete) --> 2. Prepare --> 3. Resolution (Commit/Abort)
 *
 * On the source, the client operation's transaction is closed (Commit or
 * Abort) between sending the Prepare (2) and sending the Resolution (3).
 *
 * Since sending EoD is based reading the source DB records it can occur at any
 * time. The target needs to handle the possible cases where EoD interrupts
 * the client messages. They are:
 *
 * Case 1 - If EoD is before 1 the client operation will be rejected on the
 *          source node, to be retried, and the Op is never sent. The retry
 *          should be redirected to the target node. Since the Op is not sent,
 *          the target is not aware of this case.
 *
 * Case 2 - If EoD is between 1 and 2 the operation will be rejected on the
 *          source node as in case 1. The target will see the Op but no other
 *          messages for it. Since the operation was not committed on the
 *          source, it is OK to just drop it on the target.
 *
 * Case 3 - If EoD is between 2 and 3 the operation may commit, abort, or fail.
 *          The target will see the Op and the Prepare messages but no
 *          Resolution. Because of this the target does not know them client
 *          operation's outcome.
 *               a) If the Op is committed, it could have aborted (or failed)
 *                  and the data would be incorrectly written.
 *               b) If the Op is dropped, it could have committed on the source,
 *                  in which case the new partition will be missing that record.
 *          The only thing that can be done is to abandon the migration and
 *          start over again.
 *
 * Case 4 - EoD sent after 3 is the usual steady state while the DB copy is
 *          in progress.
 */
class MigrationTarget implements Callable<MigrationTarget> {

    private final Logger logger;

    private static final int SECOND_MS = 1000;

    /* Number of times to retry after an error. */
    private static final int MAX_ERRORS = 10;

    /* Retry wait period (ms) for when the source or target is busy */
    private final long waitAfterBusy;

    /* Retry wait period (ms) for when there is an error */
    private final long waitAfterError;

    /* Configuration for speedy writes */
    private static final TransactionConfig weakConfig =
        new TransactionConfig().
               setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY).
               setDurability(new Durability(Durability.SyncPolicy.NO_SYNC,
                                            Durability.SyncPolicy.NO_SYNC,
                                            Durability.ReplicaAckPolicy.NONE));

    /* Write options for TTL to avoid creating a new instance on each op */
    private final WriteOptions writeOptions;

    /* The partition this target is going to get */
    private final PartitionId partitionId;

    /* The current rep group the partition resides on */
    private final RepGroupId sourceRGId;

    /* The ID of the TargetRecord associated with this target */
    private final long recordId;

    private final RepNode repNode;

    private final MigrationManager manager;

    private final ReplicatedEnvironment repEnv;

    private final ReaderFactory readerFactory;

    private DataChannel channel = null;

    /* The new partition db */
    private Database partitionDb = null;

    /*
     * The following three flags define the state of the migration:
     *                 running          done      canceled
     * PENDING          false           false       false
     * RUNNING          true            false       false
     * SUCCEEDED          -             true         -
     * ERROR              -             false       true
     *
     * The legal transitions are:
     *
     *                                    setDone()
     *          setRunning()            -------------> SUCCEEDED
     * PENDING --------------> RUNNING /
     *     ^                      |    \-------------> ERROR
     *     |______________________|      setCanceled() via error() or
     *          setStopped()              cancel()
     */

    /*
     * True when the target is executing.
     */
    private volatile boolean running = false;

    /*
     * True when the migration is complete and the partition has been
     * made durable. Once set the migration can not be canceled.
     */
    private volatile boolean done = false;

    /* Guard to keep from being canceled while finishing ToO (see setDone()) */
    private volatile boolean inDone = false;

    /*
     * True when the migration is canceled. Could be set from an admin
     * command or because of an unrecoverable error.
     */
    private volatile boolean canceled = false;

    /* Exception that caused the migration to terminate */
    private Exception errorCause = null;

    /*
     * Time (in milliseconds) to wait before retrying the target after an error
     * or busy response.
     */
    private long retryWait = -1;

    /* True if the EOD marker was received */
    private volatile boolean eodReceived;

    /*
     * For logging. When available, this will be set to the master RN of the
     * source group. Otherwise it will be the source group name.
     */
    private String sourceName;

    /* statistics */
    private final long requestTime;
    private long startTime = 0;
    private long endTime = 0;
    private long operations = 0;
    private long copyOps = 0;
    private long copyBytes = 0;
    private long copyBatches = 0;
    private int attempts = 0;
    private int busyResponses = 0;
    private int errors = 0;

    MigrationTarget(TargetRecord record,
                    RepNode repNode,
                    MigrationManager manager,
                    ReplicatedEnvironment repEnv,
                    Params params) {

        partitionId = record.getPartitionId();
        sourceRGId = record.getSourceRGId();
        /* Until a connection is made, the source name is just the group name */
        sourceName = sourceRGId.getGroupName();
        recordId = record.getId();
        this.repNode = repNode;
        this.manager = manager;
        this.repEnv = repEnv;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        final RepNodeParams repNodeParams = params.getRepNodeParams();
        waitAfterBusy =  repNodeParams.getWaitAfterBusy();
        waitAfterError = repNodeParams.getWaitAfterError();
        readerFactory = new ReaderFactory();
        requestTime = System.currentTimeMillis();
        writeOptions = new WriteOptions().setUpdateTTL(true);
    }

    /**
     * Gets the source group ID.
     *
     * @return the source group ID
     */
    RepGroupId getSource() {
        return sourceRGId;
    }

  /**
   * Gets the ID of the TargetRecord associated with this target.
   *
   * @return the target record ID
   */
    long getRecordId() {
        return recordId;
    }

    /**
     * Gets statistics on this migration target.
     *
     * @return a statistics object
     */
    PartitionMigrationStatus getStatus() {
        return getStatus(getState().getPartitionMigrationState());
    }

    private PartitionMigrationStatus getStatus(PartitionMigrationState state) {
        return new PartitionMigrationStatus(state,
                                            partitionId.getPartitionId(),
                                            repNode.getRepNodeId().getGroupId(),
                                            sourceRGId.getGroupId(),
                                            operations,
                                            requestTime,
                                            startTime,
                                            endTime,
                                            attempts,
                                            busyResponses,
                                            errors);
    }

    /**
     * Gets the state of the migration. The admin will poll for status as part
     * of the ToO protocol. This returns SUCCESS iff the the EOD messages
     * is received and the partition has been made durable.
     *
     * @return migration state
     */
    MigrationState getState() {
        PartitionMigrationState state =
            done ? PartitionMigrationState.SUCCEEDED :
                canceled ? PartitionMigrationState.ERROR :
                    running ? PartitionMigrationState.RUNNING :
                         PartitionMigrationState.PENDING;
        return new MigrationState(state, errorCause);
    }

    /**
     * Gets the partition ID of this target.
     *
     * @return the partition ID
     */
    PartitionId getPartitionId() {
        return partitionId;
    }

    /**
     * Attempts to cancel the migration. If wait is true, this method will wait
     * on the target thread to exit. Returns true if the migration can be
     * canceled, otherwise false is returned. The migration can be canceled any
     * time before the final commit of the partition and switch to topology x.
     *
     * Note that this does not remove the record from the db, as this cancel
     * could be due to shutdown, and not a failure or admin cancel.
     *
     * @param wait wait flag
     * @return true if migration was canceled
     */
    synchronized boolean cancel(boolean wait) {
        if (done || inDone) {
            return false;
        }
        setCanceled(wait, new Exception("Migration canceled"));
        return true;
    }

    /**
     * Cancels the migration by setting the canceled flag and cleans up the
     * target.
     *
     * @param wait wait flag
     */
    private synchronized void setCanceled(boolean wait, Exception cause) {
        assert !done;

        canceled = true;
        errorCause = cause;
        cleanup(wait);
    }

    /**
     * Cancels the migration due to an unrecoverable condition.
     *
     * @param msg message to log
     * @param ex exception to log
     */
    private void error(String msg, Exception cause) {
        assert !Thread.holdsLock(this);

        logger.log(Level.WARNING, msg, cause);

        setCanceled(false, new Exception(msg, cause));
        try {
            /* On an unrecoverable error, remove the record from the db */
            manager.removeRecord(partitionId, recordId, false);
        } catch (DatabaseException de) {
            logger.log(Level.INFO,
                       "Exception attempting to remove migration record for " +
                       partitionId,
                       de);
        }
    }

    /**
     * Cleans up this target. If wait is true, this method will wait on
     * the target thread to exit.
     *
     * @param wait wait flag
     */
    private synchronized void cleanup(boolean wait) {
        setStopped();

        /*
         * If requested to wait and the DB is open, wait until it is closed.
         * This is done to avoid DB errors caused but the target still running
         * after a cancel. (A DB error will cause the env to be invalidated,
         * something to avoid.)
         *
         * By setting stopped (above) Reader.remove() will return null,
         * causing the main thread to exit and call cleanup(false) which will
         * then close the DB at an OK time.
         */
        if (wait && (partitionDb != null)) {
            try {
                logger.log(Level.INFO, "Waiting for {0} to exit", this);
                wait(2 * SECOND_MS);
            } catch (InterruptedException ie) {
                logger.log(Level.INFO, "Unexpected interrupt", ie);
            }
        }

        /* Close the channel if open. */
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception ex) {
                logger.log(Level.WARNING, "Exception closing channel", ex);
            }
            channel = null;
        }

        /*
         * If we are not done, remove the partition migration DB if one exists.
         * (Note that a check for done is unnecessary as setDone() closes
         * the DB which clears db)
         */
        if (partitionDb != null) {
            assert !done;

            final String dbName = partitionId.getPartitionName();
            logger.log(Level.INFO, "Removing migrated DB {0}", dbName);

            try {
                closeDB();
            } catch (Exception ex) {
                logger.log(Level.WARNING, "Exception closing DB", ex);
            }

            try {
                repEnv.removeDatabase(null, dbName);
            } catch (DatabaseNotFoundException dnfe) {
                /* Shouldn't happen, but if it does, not really bad */
            } catch (Exception ex) {
                logger.log(Level.WARNING, "Exception removing DB", ex);
            }

            /*
             * We need to clean any secondary databases that may have been
             * present and populated by records from this aborted migration.
             * Note that if cleaning is required, future partition migrations
             * (including the restart of this one) will be held off until the
             * cleaning is complete (see the TableManager#isBusyCleaning()
             * calls in MigrationManager and MigrationService).
             */
            repNode.getTableManager().notifyRemoval(partitionId);
        }
    }

    /**
     * Closes the partition migration DB if one exists.
     */
    private synchronized void closeDB() {
        if (partitionDb != null) {
            partitionDb.close();
            partitionDb = null;
        }

        /* A cleanup may be waiting for the DB to be closed */
        notifyAll();
    }

    /**
     * Sets the running flag. The running flag can be set to true only if
     * not done or canceled, otherwise it is set to false. Returns the value
     * of the running flag.
     *
     * @return running
     */
    private synchronized boolean setRunning() {
        assert !running;
        running = (done || canceled) ? false : true;
        return running;
    }

    /**
     * Sets the running flag to false.
     */
    private synchronized void setStopped() {
        running = false;
    }

    /**
     * Sets state to done (SUCCEEDED) and persists the partition DB.
     */
    private void setDone() {
        endTime = System.currentTimeMillis();

        if (logger.isLoggable(Level.INFO)) {
            printMigrationStats();
        }

        try {
            /*
             * The normal monitor can't be held when writing the db (in
             * persistTargetDurable()) so we guard from being canceled by
             * setting inDone to true. We can't just set done to true because
             * that will cause getState() to return SUCCESS before things
             * are made durable and everyone is informed.
             */
            synchronized (this) {
                /* Doh! Too bad, we were almost there! */
                if (canceled) {
                    return;
                }
                inDone = true;
            }
            closeDB();

            /*
             * ToO #5 & #6 - Make the partition durable and persist that the
             * transfer from the source is complete.
             *
             * Persisting the transfer complete will allow the target node
             * to accept forwarded client ops - ToO #7
             *
             * ToO #9 - Setting done to true will cause SUCCEEDED be returned
             * from getMigrationState()
             */
            done = persistTargetDurable();

        } finally {
            inDone = false;
        }
    }

    /*
     * Print Partition Migration stats in actual data movement
     */
    private void printMigrationStats() {

        final String avgBatchOps =
                String.format("%.1f", (float)copyOps / (float)copyBatches);
        final float avgBatchBytes = (float)copyBytes / (float)copyBatches;
        final String avgBatchSize = (avgBatchBytes < 1000) ?
                           String.format("%.1f B", avgBatchBytes) :
                           String.format("%.1f kB", avgBatchBytes / 1000);
        final long seconds = (endTime - startTime) / 1000;
        logger.log(Level.INFO,
                   "Migration of {0} complete, {1} operations, " +
                   "Avg copy batch {2} ops {3}, transfer time: {4} seconds",
                   new Object[]{partitionId, operations,
                                avgBatchOps, avgBatchSize, seconds});
    }

    /**
     * Main target loop. This may be called repeatedly during the life of the
     * partition migration, potentially in different threads. In the case of
     * a retry-able error or busy response from the source, this call will
     * return this object. In all other cases null is returned. If this
     * object is returned, getRetryWait() will return the time to wait before
     * attempting to retry.
     *
     * @return this object or null
     */
    @Override
    public MigrationTarget call() {
        while (setRunning()) {
            runMigration();

            final long waitTime = getRetryWait();

            /* done or canceled */
            if (waitTime < 0) {
                break;
            }

            if (waitTime > 0) {
                return this;
            }

            /*
             * waitTime == 0, indicating an error occurred after the EOD.
             * In this case immediately re-try because the source will
             * have closed the partition DB (at ToD #4) and is waiting for
             * resolution (ToO #12) which will never come. By retrying the
             * migration the source will detect the restart and re-instate
             * the partition. (Note that the source will respond with BUSY
             * while the partition is being restored)
             */
        }
        return null;
    }

    private void runMigration() {
        attempts++;
        startTime = System.currentTimeMillis();
        endTime = 0;
        operations = 0;
        copyOps = 0;
        copyBytes = 0;
        copyBatches = 0;
        eodReceived = false;

        /*
         * Try clause for catching exceptions. The loop will open a channel
         * to the source, create a db if needed, and read operations from
         * the channel to populate the db.
         */
        try {
            final DataInputStream stream = openChannel();

            TransferRequest.write(channel,
                                  partitionId.getPartitionId(),
                                  repNode.getRepNodeId());

            final Response response = TransferRequest.readResponse(stream);
            switch (response) {

                /* OK */
                case OK:
                    logger.log(Level.INFO, "Starting {0}", this);

                    createDb();

                    /*
                     * Read loop. This returns when done, or throws an
                     * IOException.
                     */
                    consumeOps(readerFactory.newReader(stream));
                    break;

                /*
                 * If BUSY, the source is suitable for migration but is
                 * currently unavailable. Force the usual retry logic
                 * but without a retry limit since we assume that the
                 * source will eventually be able to service the
                 * request.
                 */
                case BUSY:
                    // TODO - make use of this info
                    TransferRequest.readNumStreams(stream);
                    setBusyRetryWait("Source busy: " +
                                     TransferRequest.readReason(stream), true);
                    break;

                /*
                 * If UNKNOWN_SERVICE the node may be down/coming up or the
                 * partition is missing (from a previously failed migration).
                 * Treat as an error and retry, but not forever.
                 */
                case UNKNOWN_SERVICE:
                    setErrorRetryWait(new Exception("Unknown service: " +
                                           TransferRequest.readReason(stream)));
                    break;

                /* Fatal */
                case FORMAT_ERROR:
                    error("Fatal response " + response + " from source: " +
                          TransferRequest.readReason(stream), null);
                    break;
                case AUTHENTICATE:
                        /* should not occur in this context */
                        error("Authenticate response encountered outside of " +
                              "hello sequence", null);
                        break;
                case PROCEED:
                	/* should not occur in this context */
                	error("Proceed response encountered outside of hello " +
                              "sequence", null);
                	break;
                case INVALID:
                    error("Fatal response " + response + " from source: " +
                          TransferRequest.readReason(stream), null);
                    break;
            }
        } catch (IOException | DatabaseException e) {
            setErrorRetryWait(e);
        } catch (ServiceConnectFailedException scfe) {
            error("Failed to connect to migration service at " + sourceName +
                  " for " + partitionId, scfe);
        } catch (Exception ex) {
            error("Unexpected exception, exiting", ex);
        }  finally {
            /* End time will not be set on error */
            if (endTime == 0) {
                endTime = System.currentTimeMillis();
            }

            /*
             * Before the try block exits, one of the following will be called:
             *    setDone() on success,
             *    setRetryWait() on a retry-able error, or busy response, or
             *    error() on an non-retry-able error.
             */
            try {
                cleanup(false);
            } catch (InsufficientReplicasException | DiskLimitException ire) {
                /*
                 * These exceptions can happen since cleanup() calls
                 * TableManager#notifyRemoval() which does
                 * SecondaryInfoMap#persist(). When that fails, we will not be
                 * able to clean the secondary populated by the aborted
                 * migration. But this is OK since the JE will delete obsolete
                 * secondary when it is scanned. So simply move on.
                 */
            }
        }
    }

    /**
     * Sets the retry wait due to a busy condition. If isBusyResponse is true
     * busyResponses is incremented.
     */
    private void setBusyRetryWait(String reason, boolean isBusyResponse) {
        assert !done;

        if (canceled) {
            return;
        }

        if (isBusyResponse) {
            busyResponses++;
        }

        retryWait = waitAfterBusy;

        logger.log(Level.FINE,
                   "Migration of {0} from {1} did not start: {2}, " +
                   "retry in {3} ms",
                   new Object[]{partitionId, sourceName, reason, retryWait});
    }

    /**
     * Sets the retry wait due to a retryable error condition. If the number
     * of errors > MAX_ERRORS the migration will be canceled.
     */
    private void setErrorRetryWait(Exception ex) {
        assert !done;

        if (canceled) {
            return;
        }

        errors++;
        if (errors >= MAX_ERRORS) {
            error("Migration of " + partitionId + " failed. Giving up after " +
                  attempts + " attempt(s)", ex);
            return;
        }

        /*
         * If an error and the EOD was received then return 0 to force
         * an immediate restart
         */
        retryWait = eodReceived ? 0 : waitAfterError;

        logger.log(Level.FINE,
                   "Migration of {0} from {1} failed, reason: {2}, " +
                   "retry in {3} ms",
                   new Object[]{partitionId, sourceName,
                                ex.getLocalizedMessage(), retryWait});
    }

    /**
     * Gets the retry wait time in milliseconds. If the target is to be retried
     * the return value is >= 0, otherwise the value is -1.
     *
     * @return the retry wait time or -1
     */
    long getRetryWait() {
        assert !running;

        if (canceled || done) {
            return -1L;
        }
        assert retryWait >= 0;
        return retryWait;
    }

    /**
     * Establishes a channel with the partition source and creates an
     * input stream.
     *
     * @return an input stream
     * @throws IOException
     * @throws com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException
     */
    private synchronized DataInputStream openChannel()
        throws IOException, ServiceConnectFailedException {

        final Topology topo = repNode.getTopology();

        if (topo == null) {
            throw new IOException("Target node not yet initialized");
        }
        final PingCollector collector = new PingCollector(topo);
        final RNNameHAPort rnNameAndPort =
                collector.getMasterNamePort(sourceRGId);

        if (rnNameAndPort == null) {
            throw new IOException("Unable to get mastership status for " +
                                   sourceRGId.getGroupName());
        }
        final String haHostPort = rnNameAndPort.getHAHostPort();
        sourceName = rnNameAndPort.getFullName();

        /* getHAHostPort() returns null for a R1 node */
        if (haHostPort == null) {
            throw new IllegalStateException("Source node " + sourceName +
                                            " is running an incompatible " +
                                            "software version");
        }
        final InetSocketAddress sourceAddress =
                                        HostPortPair.getSocket(haHostPort);

        logger.log(Level.FINE,
                   "Opening channel to {0} to make migration request",
                   sourceAddress);

        final RepNodeParams repNodeParams = repNode.getRepNodeParams();
        final RepImpl repImpl = repNode.getEnvImpl(0L);
        if (repImpl == null) {
            throw new IllegalStateException("Attempt to migrate a partition " +
                                            "on a node that is not available");
        }

        final ConnectOptions connectOpts = new ConnectOptions().
            setTcpNoDelay(true).
            setReceiveBufferSize(0).
            setReadTimeout(repNodeParams.getReadWriteTimeout()).
            setOpenTimeout(repNodeParams.getConnectTImeout());

        channel = RepUtils.openBlockingChannel(
            sourceAddress, repImpl.getChannelFactory(), connectOpts);

        ServiceDispatcher.doServiceHandshake(channel,
                                             MigrationService.SERVICE_NAME);

        return new DataInputStream(Channels.newInputStream(channel));
    }

    /**
     * Opens or creates the partition DB.
     */
    private synchronized void createDb() {

        if (partitionDb != null) {
            return;
        }

        /* Retry until success. */
        final TransactionConfig txnConfig =
                    new TransactionConfig().setConsistencyPolicy(
                                NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        /* Create DB */
        while (partitionDb == null) {

            Transaction txn = null;
            try {
                txn = repEnv.beginTransaction(null, txnConfig);
                partitionDb =
                        repEnv.openDatabase(txn,
                                            partitionId.getPartitionName(),
                                            repNode.getPartitionDbConfig());
                txn.commit();
                txn = null;
                
            } catch (ReplicaWriteException rwe) {
                /* Could be transitioning from master to replica. */
                final String msg = "Attempted to start partition migration " +
                                   "target for " + partitionId + " but node " +
                                   "has become a replica";
                logger.log(Level.WARNING, msg, rwe);
                throw new IllegalStateException(msg, rwe);
            } catch (UnknownMasterException ume) {
                /* Could be transitioning from master to replica. */
                final String msg = "Attempted to start partition migration " +
                                   "target for " + partitionId + " but node " +
                                   "has lost master status";
                logger.log(Level.WARNING, msg, ume);
                throw new IllegalStateException(msg, ume);
            } finally {
                TxnUtil.abort(txn);
            }
        }
    }

    private void consumeOps(Reader reader) throws Exception {
        try {
            int count = 0;
            while (!done) {
                final Op op = reader.remove();

                if (op == null) {
                    throw new IOException("Reader returned null after " +
                                          count + " operations");
                }
                /*
                 * We retry the operation in the case of RUE because this can
                 * be thrown by the table manager when a secondary DB is being
                 * created and not everything is in a consistent state.
                 */
                int retries = 100;
                while (true) {
                    try {
                        op.execute();
                        break;
                    } catch (RNUnavailableException rue) {
                        if (retries-- <=0) {
                            throw rue;
                        }
                        Thread.sleep(10);
                    }
                }
                count++;
            }
        } finally {
            /*
             * Cleanup. Unresolved-unprepared txns may be leftover in normal
             * operation. Prepared and batch txns may remain after an error.
             */
            reader.abortAllTxns();
        }
    }

    /**
     * Persists the target record in the db. Note the monitor can't be
     * held during this operation as the db's triggers call back into the
     * manager.
     *
     * @return true if the operation was successful
     */
    private boolean persistTargetDurable() {
        assert !Thread.holdsLock(this);
        logger.log(Level.FINE,
                   "Persist target transfer durable for {0}", partitionId);

        final TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setConsistencyPolicy(
                                 NoConsistencyRequiredPolicy.NO_CONSISTENCY);
        txnConfig.setDurability(
                   new Durability(Durability.SyncPolicy.SYNC,
                                  Durability.SyncPolicy.SYNC,
                                  Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

        final Boolean success =
                manager.tryDBOperation(new DBOperation<Boolean>() {

            @Override
            public Boolean call(Database db) {

                Transaction txn = null;
                try {
                    txn = db.getEnvironment().beginTransaction(null, txnConfig);

                    final PartitionMigrations pm =
                                   PartitionMigrations.fetch(db, txn);

                    final TargetRecord record = pm.getTarget(partitionId);

                    if (record == null) {
                        throw new IllegalStateException(
                                        "Unable to find migration record for " +
                                        partitionId);
                    }
                    record.setStatus(
                                getStatus(PartitionMigrationState.SUCCEEDED));
   
                    pm.persist(db, txn, true);
                    txn.commit();
                    txn = null;
                    return true;
                } finally {
                    TxnUtil.abort(txn);
                }
            }
        }, true);

        if ((success == null) || !success) {
            return false;
        }

        /* With the migration record persisted, we can remove this target. */
        manager.removeTarget(partitionId);

        /*
         * This will update the local topology here (master) and update the
         * partition DBs. The replicas are updated through the DB triggers
         * from persisting the migration record.
         *
         * This is critical to complete, because if the local topo is not
         * updated no one will (source or target) will think they own the
         * partition. So fail the node if there is a problem and hopefully
         * the new master be correct.
         */
        manager.criticalUpdate();

        manager.setLastMigrationDuration(endTime - startTime);
        return true;
    }
    
    @Override
    public String toString() {
        return "MigrationTarget[" + partitionId + ", " + sourceName + ", " +
               attempts + ", " + running + ", " + done + ", " + canceled + "]";
    }

    /**
     * Encapsulates an operation.
     */
    private static abstract class Op {

        /**
         * Called to execute the operation by the consumer thread.
         */
        abstract void execute();
    }

    /**
     * Reader thread. This thread will read operations from the stream and
     * insert them onto the opQueue.
     */
    private class Reader implements Runnable {

        /*
         * Map of local transactions. No synchronization is needed since it is
         * onlt accessed by the read thread.
         */
        private final Map<Long, LocalTxn> txnMap = new HashMap<>();

        /* Max. number of operations in a single copy batch transaction */
        private final int MAX_BATCH_COPY_OPS = 100;

        /*
         * The transaction used to batch copy operations. This should only be
         * used for copy ops, and the transaction should not overlap other
         * transactions. This means the transaction should be committed before
         * some other transactional operation (put, delete) is started. This is
         * to maintain proper ordering between the on disk copy and client
         * operations.
         */
        private Transaction batchTxn = null;

        /* The number of copy operations made in the current batch txn. */
        private int batchCount = 0;

        /* The operation queue. This thread inserts ops, the target thread
         * removes them. Accesses to the queue must be synchronized.
         */
        private final Queue<Op> opQueue = new LinkedList<>();

        /*
         * The normal capacity of the op queue. This is overridden when the
         * consumer needs to wait for a transaction resolution.
         */
        private static final int DEFAULT_CAPACITY = 100;

        /* The capacity limit of the op queue. */
        private int capacity = DEFAULT_CAPACITY;

        private final DataInputStream stream;

        /* For general use to avoid constructing DatabaseEntrys in the OPS */
        private final DatabaseEntry keyEntry = new DatabaseEntry();
        private final DatabaseEntry valueEntry = new DatabaseEntry();

        Reader(DataInputStream stream) {
            this.stream = stream;
        }

        @Override
        public void run() {

            try {
                processStream();
            } catch (Exception ex) {
                /* If canceled, don't bother reporting. */
                if (!canceled) {
                    logger.log(Level.INFO,
                               "Exception processing migration stream for " +
                               partitionId, ex);
                }

                /*
                 * Clearing running will cause remove() and PrepareOp.execute()
                 * to exit so that the target thread can handle the issue.
                 */
                setStopped();
            }
        }

        /**
         * Processes operations from the migration stream. This method does not
         * return normally until an End Of Data operation is received on the
         * stream.
         *
         * If an exception is thrown, the migration should be aborted because
         * the state of the source and the data is unknown.
         *
         * @throws Exception if there were any problems encountered processing
         * the migration stream
         */
        private void processStream() throws Exception {

            while (running) {
                final OP op = OP.get(stream.readByte());

                if (op == null) {
                    throw new IOException("Bad op, or unexpected EOF");
                }
                operations++;

                switch (op) {
                    case COPY : {
                        copyOps++;
                        insert(new CopyOp(readDbEntry(),
                                          readDbEntry(),
                                          readExpirationTime()));
                        break;
                    }
                    case PUT : {
                        insert(new PutOp(readTxnId(),
                                         readDbEntry(),
                                         readDbEntry(),
                                         readExpirationTime()));
                        break;
                    }
                    case DELETE : {
                        insert(new DeleteOp(readTxnId(), readDbEntry()));
                        break;
                    }
                    case PREPARE : {
                        insert(new PrepareOp(readTxnId()));
                        break;
                    }
                    case COMMIT : {
                        resolve(readTxnId(), true);
                        break;
                    }
                    case ABORT: {
                        resolve(readTxnId(), false);
                        break;
                    }
                    case EOD : {
                        logger.log(Level.INFO,
                                   "Received EOD for {0}", partitionId);

                        eodReceived = true;

                        /*
                         * It is possible that a txn was started (via PUT or
                         * DELETE) but not resolved (a COMMIT or ABORT was never
                         * received). At this point the local txns in the map
                         * are those which have not been resolved.
                         *
                         * If the transaction has been prepared then we don't
                         * know if the operation has completed on the source.
                         * In this case the only option is to cancel
                         * the migration and start over.
                         *
                         * Unresolved txns which have not been prepared are
                         * safe to abort (later) at this target; these txns are
                         * also aborted on the source and will be re-tried by
                         * the client.
                         */
                        for (LocalTxn txn : txnMap.values()) {
                            assert !txn.resolved;
                            if (txn.prepared) {

                                /*
                                 * Log instead of throwing an exception
                                 * because even though this (rare) condition
                                 * results in an abort of the migration, it is
                                 * not unexpected.
                                 */
                                logger.log(Level.INFO,
                                           "Encountered prepared but " +
                                           "unresolved txn, stopping " +
                                           "migration for {0}", partitionId);
                                setStopped();
                                return;
                            }
                        }
                        insert(new EoD());
                        return;
                    }
                }
            }
        }

        /**
         * Reads a transaction ID from the migration stream.
         */
        private long readTxnId() throws IOException {
            return stream.readLong();
        }

        /**
         * Reads expiration time from the migration stream.
         */
        private long readExpirationTime() throws IOException {
            return stream.readLong();
        }

        /**
         * Reads a DB entry (as a byte array) from the migration stream.
         */
        private byte[] readDbEntry() throws IOException {
            final int size = stream.readInt();
            final byte[] bytes = new byte[size];
            stream.readFully(bytes);
            return bytes;
        }

        /**
         * Inserts an operation onto the queue. This method will block if the
         * queue is at capacity.
         */
        private void insert(Op op) {

            synchronized (opQueue) {
                if (!running) {
                    return;
                }
                opQueue.add(op);
                opQueue.notifyAll();

                while ((opQueue.size() > capacity) && running) {

                    try {
                        opQueue.wait(SECOND_MS);
                    } catch (InterruptedException ie) {
                        logger.log(Level.WARNING, "Unexpected interrupt", ie);
                    }
                }
            }
        }

        /**
         * Removes an operation from the queue. This method will block if the
         * queue is empty.
         */
        private Op remove() {

            synchronized (opQueue) {
                while (running) {
                    final Op op = opQueue.poll();
                    if (op != null) {
                        opQueue.notifyAll();
                        return op;
                    }

                    try {
                        opQueue.wait(SECOND_MS);
                    } catch (InterruptedException ie) {
                        logger.log(Level.WARNING, "Unexpected interrupt", ie);
                    }
                }
                return null;
            }
        }

        /**
         * Resolves a prepare operation.
         */
        private void resolve(long txnId, boolean commit) {

            final LocalTxn txn = txnMap.remove(txnId);
            assert txn != null;

            /*
             * If the consumer had reached the prepare operation, it is waiting
             * on the transaction. After marking it resolved wake it up.
             */
            synchronized (txn) {
                txn.resolve(commit);
                txn.notifyAll();
            }

            /*
             * Resolve dosen't place an op on the queue, so we need to wake
             * the consumer thread just in case it is waiting there.
             */
            synchronized (opQueue) {
                opQueue.notifyAll();
            }
        }

        /*
         * Gets the batch transaction and increments the batch count. If the
         * count is MAX_BATCH_COPY_OPS the current txn is committed and a new
         * transaction is started.
         */
        private Transaction getBatchTxn() {
            if (batchCount >= MAX_BATCH_COPY_OPS) {
                commitBatchTxn();
            }
            if (batchTxn == null) {
                batchTxn = repEnv.beginTransaction(null, weakConfig);
            }
            batchCount++;
            return batchTxn;
        }

        /*
         * Commits the batch transaction if there is one open and resets the
         * batch txn count.
         */
        private void commitBatchTxn() {
            if (batchTxn != null) {
                batchTxn.commit();
                batchTxn = null;
                batchCount = 0;
                copyBatches++;
            }
        }

        /*
         * Aborts all open transactions.
         */
        private void abortAllTxns() {
            /*
             * Aborts the batch transaction if it is open. This should only be
             * necessary if there is an error.
             */
            if (batchTxn != null) {
                assert !done;
                TxnUtil.abort(batchTxn);
                batchTxn = null;
                batchCount = 0;
            }

            /*
             * Aborts unresolved local txns. Note that if any were
             * prepared, it would be detected and handled in processStream().
             */
            for (LocalTxn txn : txnMap.values()) {
                txn.abort();
            }
        }

        @Override
        public String toString() {
            return "Reader[" + operations + ", " + opQueue.size() + "]";
        }

        /**
         * Copy operation (record read from the DB).
         */
        private class CopyOp extends Op {
            final byte[] key;
            final byte[] value;
            final long expirationTime;

            CopyOp(byte[] key, byte[] value, long expirationTime) {
                this.key = key;
                this.value = value;
                this.expirationTime = expirationTime;
            }

            @Override
            void execute() {
                /* Skip writing records to a dropped table */
                if (MigrationManager.isForDroppedTable(repNode, key)) {
                    return;
                }
                keyEntry.setData(key);
                valueEntry.setData(value);
                partitionDb.put(getBatchTxn(), keyEntry, valueEntry,
                                Put.OVERWRITE, getWriteOptions(expirationTime));
                copyBytes += value.length;
            }

            @Override
            public String toString() {
                return "CopyOp[" + key.length + ", " + value.length + "]";
            }
        }

        /**
         * An operation associated with source-side transactions. When the first
         * object created with the specified txn a new local transaction is
         * started. Subsequent creations with the same txn will be associated
         * with same local transaction.
         */
        private abstract class TxnOp extends Op {
            final LocalTxn txn;

            protected TxnOp(long txnId) {
                LocalTxn t = txnMap.get(txnId);

                if (t == null) {
                    t = new LocalTxn(txnId);
                    txnMap.put(txnId, t);
                }
                this.txn = t;
            }

            /**
             * Gets the local transaction for this operation. The first time
             * this is called a new local transaction will be created and
             * started.
             */
            protected Transaction getTransaction() {
                return txn.getTransaction();
            }
        }

        /**
         * Put operation (client write).
         */
        private class PutOp extends TxnOp {
            final byte[] key;
            final byte[] value;
            final long expirationTime;

            PutOp(long txnId, byte[] key, byte[] value,
                  long expirationTime) {
                super(txnId);
                this.key = key;
                this.value = value;
                this.expirationTime = expirationTime;
            }

            @Override
            void execute() {
                /* Skip writing records to a dropped table */
                if (MigrationManager.isForDroppedTable(repNode, key)) {
                    return;
                }
                keyEntry.setData(key);
                valueEntry.setData(value);
                partitionDb.put(getTransaction(), keyEntry, valueEntry,
                                Put.OVERWRITE, getWriteOptions(expirationTime));
            }

            @Override
            public String toString() {
                return "PutOp[" + txn.txnId + ", " + key.length + ", " +
                       value.length + "]";
            }
        }

        /**
         * Delete operation (client delete).
         */
        private class DeleteOp extends TxnOp {
            final byte[] key;

            DeleteOp(long txnId, byte[] key) {
                super(txnId);
                this.key = key;
            }

            @Override
            void execute() {
                keyEntry.setData(key);
                partitionDb.delete(getTransaction(), keyEntry);
            }

            @Override
            public String toString() {
                return "DeleteOp[" + txn.txnId + ", " + key.length + "]";
            }
        }

        /**
         * Prepare op. This operation indicates that a source-based transaction
         * is about to be committed. When the consumer reaches a PrepareOp it
         * must wait for it to be resolved before consuming any other
         * operations.
         *
         * While the consumer is waiting for the resolution the operation queue
         * becomes unbounded so that the read thread can continue and read the
         * resolution.
         */
        private class PrepareOp extends TxnOp {

            PrepareOp(long txnId) {
                super(txnId);
                txn.prepared = true;
            }

            @Override
            void execute() {

                /*
                 * Check if no transaction was started for this ID. This could
                 * happen due to key filtering at the source. In this case just
                 * exit.
                 */
                if (txn.transaction == null) {
                    logger.log(Level.FINE,
                               "Prepare with no txn for {0}, {1}",
                               new Object[]{txn, partitionId});
                    return;
                }

                /* Commit copy operations to maintain order. */
                commitBatchTxn();

                synchronized (txn) {
                    if (!txn.resolved) {
                        synchronized (opQueue) {

                            /*
                             * Make the queue unbounded so we can find the
                             * resolution message.
                             */
                            capacity = Integer.MAX_VALUE;
                            opQueue.notifyAll();
                        }
                    }
                    while (!txn.resolved && running) {
                        logger.log(Level.FINE,
                                   "Waiting for resolution of {0}, {1} {2} ops",
                                   new Object[]{txn, partitionId, operations});
                        try {
                            txn.wait(SECOND_MS);
                        } catch (InterruptedException ie) {
                            logger.log(Level.WARNING,
                                       "Unexpected interrupt", ie);
                        }
                    }
                }
                capacity = DEFAULT_CAPACITY;
                txn.finish();
            }

            @Override
            public String toString() {
                return "PrepareOp[" + txn + "]";
            }
        }

        /**
         * End of data marker.
         */
        private class EoD extends Op {

            @Override
            void execute() {
                /* Commit all remaining copy operations */
                commitBatchTxn();
                setDone();
            }

            @Override
            public String toString() {
                return "EoD[]";
            }
        }

        /**
         * Encapsulates a local transaction which is associated with a
         * transaction id.
         */
        private class LocalTxn {
            private final long txnId;
            private Transaction transaction = null;
            private boolean prepared = false;
            private boolean resolved = false;
            private boolean committed = false;

            LocalTxn(long txnId) {
                this.txnId = txnId;
            }

            /**
             * Gets the local transaction for this id. The first time this
             * is called a new local transaction will be created and started.
             */
            Transaction getTransaction() {
                /* Commit copy operations to prevent overlap. */
                commitBatchTxn();
                if (transaction == null) {
                    transaction = repEnv.beginTransaction(null, weakConfig);
                }
                return transaction;
            }

            /**
             * Marks the transaction as resolved.
             */
            void resolve(boolean commit) {
                assert prepared;
                assert !resolved;
                resolved = true;
                committed = commit;
            }

            /**
             * Completes the transaction.
             */
            void finish() {
                assert resolved;
                assert transaction != null;
                if (committed) {
                    transaction.commit();
                } else {
                    TxnUtil.abort(transaction);
                }
            }

            /**
             * Aborts the transaction if started.
             */
            void abort() {
                TxnUtil.abort(transaction);
            }

            @Override
            public String toString() {
                return "LocalTxn[" + txnId + ", " + transaction +
                       ", prepared=" + prepared + ", resolved=" + resolved +
                       ", committed=" + committed + "]";
            }
        }
    }

    private class ReaderFactory extends KVThreadFactory {

        ReaderFactory() {
            super(" migration stream reader for ", logger);
        }

        private Reader newReader(DataInputStream stream) {
            final Reader reader = new Reader(stream);
            newThread(reader).start();
            return reader;
        }
    }

    /**
     * Returns a JE WriteOptions object initialized with a TTL
     * if expirationTime is non-zero, otherwise null. If non-null
     * the instance returned is a singleton in this class.
     */
    private WriteOptions getWriteOptions(long expirationTime) {
        if (expirationTime == 0) {
            return null;
        }
        return writeOptions.setExpirationTime(expirationTime, null);
    }
}

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

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.pubsub.security.StreamClientAuthHandler;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.ShardTimeoutException;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.pubsub.SubscriptionInsufficientLogException;

import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.stream.BaseProtocol;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType;
import com.sleepycat.je.rep.subscription.Subscription;
import com.sleepycat.je.rep.subscription.SubscriptionAuthHandler;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;
import com.sleepycat.je.rep.subscription.SubscriptionStat;
import com.sleepycat.je.rep.subscription.SubscriptionStatus;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.VLSN;

/**
 * Object that represents a client to consume replication stream from source
 * kvstore.
 */
class ReplicationStreamConsumer {

    /* min ha protocol version which server should support */
    private static final int MIN_HA_PROTOCOL_VERSION = BaseProtocol.VERSION_7;

    /* subscription client uses external node type to connect feeder */
    private static final NodeType CLIENT_NODE_TYPE = NodeType.EXTERNAL;
    /* subscription client id starts with a fixed prefix */
    private static final String CONSUMER_ID_PREFIX = "RSC";
    /* to satisfy je rep config, but actually not used */
    private static final String DEFAULT_SUBSCRIBER_HOSTPORT =
        new HostPort("localhost", 0xffff).toString();
    /* statistics collection internal in ms */
    private static final long STAT_COLL_INTERVAL_MS = 1000;
    /* monitoring interval in ms */
    private static final long MONITORING_INTERVAL_MS = 100;
    /* onWarning signal interval in ms */
    private static final long SIGNAL_WARN_INTERVAL_MS = 1000 * 60 * 10;

    /* private logger */
    private final Logger logger;
    /* consumer id */
    private final String consumerId;
    /* parent publisher */
    private final PublishingUnit pu;
    /* replication group id of the source feeder */
    private final RepGroupId repGroupId;
    /* txn buffer for replication stream */
    private final OpenTransactionBuffer txnBuffer;
    /* monitoring thread */
    private final RSCMonitorThread monitorThread;
    /* directory used this consumer */
    private final String directory;
    /* statistics */
    private final ReplicationStreamConsumerStat stat;
    /* rep stream cbk */
    private final ReplicationStreamCbk replicationStreamCbk;
    /* true if the consumer has been shutdown */
    private final AtomicBoolean canceled;

    /* JE subscription configuration used in streaming */
    private volatile SubscriptionConfig subscriptionConfig;
    /* feeder host and port, updated when master migrates */
    private volatile ShardMasterInfo master;
    /* subscription client of replication stream */
    private volatile Subscription subscriptionClient;
    /* reauthentication handler */
    private final StreamClientAuthHandler authHandler;
    /* security properties */
    private final Properties securityProps;

    /* For unit tests */
    private TestHook<?> ileHook = null;

    /**
     * Creates a data consumer and initialize internal data structures and
     * start worker thread.
     *
     * @param pu                parent publishing unit
     * @param master            feeder node id and its host port pair
     * @param repGroupId        id of feeder's replication group
     * @param outputQueue       queue for all output messages
     * @param subscribedTables  subscribed tables, null if all tables
     * @param rootDir           root directory
     * @param feederFilter      subscription feeder filter
     * @param mode              stream mode
     * @param securityProps     properties for secure store, null for
     *                          non-secure store
     * @param logger            private logger
     *
     * @throws UnknownHostException if feeder node not accessible
     * @throws SubscriptionFailureException if invalid stream mode
     */
    ReplicationStreamConsumer(PublishingUnit pu,
                              ShardMasterInfo master,
                              RepGroupId repGroupId,
                              BlockingQueue<StreamOperation> outputQueue,
                              Collection<TableImpl> subscribedTables,
                              String rootDir,
                              NoSQLStreamFeederFilter feederFilter,
                              NoSQLStreamMode mode,
                              Properties securityProps,
                              Logger logger)
        throws UnknownHostException, SubscriptionFailureException {

        /* id of subscriber that uses the consumer */
        final String sid = (pu == null) ?
            /* unit test without pu */
            "TestNoSQLSubscriber" :
            /* get the external node name from pu */
            "subscription-" + pu.getCkptTableManager().getCkptTableName() +
            "-"  + pu.getSubscriberId();

        consumerId = CONSUMER_ID_PREFIX + "-" + sid + "-" + repGroupId;

        this.pu = pu;
        this.master = master;
        this.repGroupId = repGroupId;
        this.logger = logger;
        this.securityProps = securityProps;
        canceled = new AtomicBoolean(false);

        /*
         * TODO:
         * Now we need a writeable dir at client side, which is only used
         * to dump the traces. Elimination of this dependency requires
         * modification of JE subscription client.
         */
        directory = NoSQLPublisher.ensureDir(rootDir, consumerId, true);

        /* build JE subscription configuration */
        if (pu == null || !pu.isSecureStore()) {
            authHandler = null;
            /* non-secure store */
            subscriptionConfig =
                new SubscriptionConfig(consumerId,
                                       directory,
                                       DEFAULT_SUBSCRIBER_HOSTPORT,
                                       master.getMasterHostPort(),
                                       repGroupId.getGroupName(),
                                       null,
                                       CLIENT_NODE_TYPE);
        } else {
            /* secure store with authentication */
            final long reAuthInv = pu.getSubscriber().getSubscriptionConfig()
                                     .getReAuthIntervalMs();
            authHandler = StreamClientAuthHandler.getAuthHandler(
                new NameIdPair(consumerId), pu, reAuthInv, repGroupId, logger);

            subscriptionConfig =
                new SubscriptionConfig(consumerId,
                                       directory,
                                       DEFAULT_SUBSCRIBER_HOSTPORT,
                                       master.getMasterHostPort(),
                                       repGroupId.getGroupName(),
                                       null,
                                       CLIENT_NODE_TYPE,
                                       authHandler,
                                       securityProps);
        }

        /* set ha protocol version */
        subscriptionConfig.setMinProtocolVersion(MIN_HA_PROTOCOL_VERSION);

        /* set feeder filter */
        if (feederFilter != null) {
            subscriptionConfig.setFeederFilter(feederFilter);
        }

        /* build replication stream callback to replace default */
        final BlockingQueue<DataEntry> inputQueue =
            new ArrayBlockingQueue<>(subscriptionConfig
                                         .getInputMessageQueueSize());
        stat = new ReplicationStreamConsumerStat(this);
        replicationStreamCbk = new ReplicationStreamCbk(inputQueue, stat,
                                                        logger);
        subscriptionConfig.setCallback(replicationStreamCbk);

        /* set request type  */
        final EntryRequestType reqType;
        switch (mode) {
            case FROM_NOW:
                reqType = EntryRequestType.NOW;
                break;

            case FROM_EXACT_STREAM_POSITION:
            case FROM_EXACT_CHECKPOINT:
                reqType = EntryRequestType.DEFAULT;
                break;

            case FROM_STREAM_POSITION:
            case FROM_CHECKPOINT:
                reqType = EntryRequestType.AVAILABLE;
                break;

            default:
                final NoSQLSubscriberId id =
                    (pu == null) ? null : pu.getSubscriberId();
                throw new SubscriptionFailureException(
                    id, "Invalid stream mode " + mode);
        }
        subscriptionConfig.setStreamMode(reqType);

        /* client to be built in startClient */
        subscriptionClient = null;

        /* create txn agenda */
        txnBuffer = new OpenTransactionBuffer(this,
                                              repGroupId,
                                              inputQueue,
                                              outputQueue,
                                              subscribedTables,
                                              logger);

        monitorThread = new RSCMonitorThread();
    }

    /**
     * In test only
     *
     * Starts a consumer from the very beginning
     *
     * @throws InsufficientLogException  if source does not have the log to
     *                                   serve streaming from requested start
     *                                   vlsn
     * @throws InternalException         if other errors fail the subscription
     */
    public void start() throws InsufficientLogException, InternalException  {
        start(VLSN.FIRST_VLSN);
    }

    /**
     * Starts a consumer from a specific VLSN
     *
     * @param startVLSN   start vlsn to stream from
     *
     * @throws InsufficientLogException if source does not have the log to
     * serve streaming from requested start vlsn.
     * @throws SubscriptionFailureException if other errors that fail the
     * subscription
     */
    public void start(VLSN startVLSN)
        throws InsufficientLogException, SubscriptionFailureException {

        /* start all worker threads */
        logger.info(lm("Start RSC from vlsn " +
                       (startVLSN.getSequence() == Long.MAX_VALUE ? "<now>" :
                           startVLSN) + ")" +
                       " with request entry type " +
                       subscriptionConfig.getStreamMode()));

        txnBuffer.startWorker();

        /* start subscription client, null vlsn check must be done  */
        assert (startVLSN != null);
        try {
            subscriptionClient = startClient(startVLSN, subscriptionConfig);

            /* start monitoring */
            monitorThread.start();
            logger.info(lm("Subscription client starts from VLSN " +
                           subscriptionClient.getStatistics().getStartVLSN() +
                           " (req: " +
                           (startVLSN.getSequence() == Long.MAX_VALUE ?
                               "<now>" : startVLSN) + ")" +
                           " from node " + master.getMasterRepNodeId()));

        } catch (InsufficientLogException ile) {
            /* requested VLSN is not available, switch to partition transfer */
            logger.info(lm("Requested VLSN " + startVLSN +
                           " is not available at node " +
                           master.getMasterRepNodeId()));

            /*
             * just throw it to PU, PU will cancel the whole subscription,
             * shut down all RSC, and signal subscriber
             */
            throw ile;
        } catch (IllegalArgumentException | GroupShutdownException |
            InternalException | TimeoutException cause) {

            final String err = "Unable to start streaming from " +
                               master.getMasterRepNodeId() +
                               " due to error " +
                               cause.getMessage();
            logger.warning(lm(err));

            /*
             * just throw it to PU, PU will cancel the whole subscription,
             * shut down all RSC, and signal subscriber
             */
            final NoSQLSubscriberId sid = (pu == null) ?
                new NoSQLSubscriberId(1, 0) : pu.getSubscriberId();
            throw new SubscriptionFailureException(sid, err, cause);
        }
    }

    /**
     * Stops a replication stream client.
     *
     * @param logStat  true if dump stat in log
     */
    void cancel(boolean logStat) {

        /* avoid concurrent and recursive stop calls */
        if (!canceled.compareAndSet(false, true)) {
            return;
        }

        /* shutdown monitor before shutting down client */
        monitorThread.shutdownThread(logger);
        logger.info(lm("monitor thread has shutdown."));

        /* shutdown otb */
        txnBuffer.close();
        logger.info(lm("OTB has shutdown."));

        /* shutdown subscription client */
        if (subscriptionClient != null) {
            subscriptionClient.shutdown();
        }
        logger.info(lm("subscription client has shutdown."));

        if (logStat) {
            logger.info(lm("stats:" + stat.dumpStat()));
        }

        logger.info(lm("RSC has shut down"));
    }

    /**
     * Gets the statistics of the consumer
     *
     * @return  the statistics of the consumer
     */
    ReplicationStreamConsumerStat getRSCStat() {
        return stat;
    }

    @Override
    public String toString() {
        return "RSC: [" + "shard: " + repGroupId + ", " +
               "source node: " + master.getMasterRepNodeId() + "\n" +
               "source HA addr: " + master.getMasterHostPort() + "\n" +
               stat.dumpStat();
    }

    /* for test use only */
    ReplicationStreamCbk getRepStrCbk() {
        return replicationStreamCbk;
    }

    /**
     * Returns the replication group id for this consumer
     *
     * @return   the replication group id for this consumer
     */
    RepGroupId getRepGroupId() {
        return repGroupId;
    }

    /**
     * Returns consumer id
     *
     * @return consumer id
     */
    String getConsumerId() {
        return consumerId;
    }

    /**
     * Gets open txn buffer
     *
     * @return open txn buffer
     */
    OpenTransactionBuffer getTxnBuffer() {
        return txnBuffer;
    }

    /**
     * Gets parent publishing unit.
     *
     * @return  parent PU, or null in certain unit tests where no publishing
     * unit is created.
     */
    PublishingUnit getPu() {
        return pu;
    }

    /* unit test only */
    void setILEHook(TestHook<?> hook) {
        logger.info("ILETestHook set");
        ileHook = hook;
    }

    /**
     * Gets subscriber authentication handler
     *
     * @return subscriber authentication handler, null when no authentication
     * handler is needed for non-secure store.
     */
    SubscriptionAuthHandler getAuthHandler() {
        return authHandler;
    }

    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private String lm(String msg) {
        return "[" + consumerId + "] " + msg;
    }

    /* start subscription client */
    private Subscription startClient(VLSN startVLSN,
                                     SubscriptionConfig conf)
        throws InsufficientLogException, TimeoutException {

        /* for unit test, throw ILE */
        assert TestHookExecute.doHookIfSet(ileHook, null);

        /* build new replication stream client */
        final Subscription client = new Subscription(conf, logger);

        client.start(startVLSN);

        stat.setReqStartVLSN(startVLSN);
        stat.setAckedStartVLSN(client.getStatistics().getStartVLSN());

        return client;
    }

    /*
     * Private monitoring thread to keep track of subscription client status.
     *
     * If the subscription client fails, the monitor thread should analyze the
     * failure and retry if possible. For example, during master migration,
     * the subscription client will fail due to connection error, the
     * monitoring thread should obtain the new master from publisher and
     * restart the subscription client with new master.
     */
    private class RSCMonitorThread extends StoppableThread {

        /* max reconnect in one attempt to handle error */
        private final static int MAX_RETRY_IN_ONE_ATTEMPT = 10;

        /* stat update interval */
        private final long collIntvMs;
        /* shard timeout */
        private final long timeoutInMs;
        /* warning signal interval */
        private final long warningIntvMs;

        private int numAttempts;
        private int numSucc;
        private long lastStatCollectTimeMs;
        private long lastWarningTimeMs;

        RSCMonitorThread() {
            super("RSC-monitor-" + consumerId);

            collIntvMs = STAT_COLL_INTERVAL_MS;
            warningIntvMs = SIGNAL_WARN_INTERVAL_MS;

            if (pu == null) {
                /* unit test only */
                timeoutInMs = 60 * 1000;
            } else {
                timeoutInMs = pu.getShardTimeoutMs();
            }

            numAttempts = 0;
            numSucc = 0;
            lastStatCollectTimeMs = 0;
        }

        @Override
        public void run() {

            logger.info(lm("monitor thread starts."));

            Throwable cause = null;
            try {
                /* loop until thread shutdown or throw sfe or sile */
                while (!isShutdown()) {

                    updateStatistics();

                    final SubscriptionStatus status =
                        subscriptionClient.getSubscriptionStatus();
                    switch (status) {
                        case INIT:
                        case SUCCESS:
                            break;
                        case CONNECTION_ERROR:
                        case UNKNOWN_ERROR:
                            if (pu.isClosed()) {
                                /*
                                 * error because PU closed, no retry. monitor
                                 * will be shutdown
                                 */
                                continue;
                            }

                            if (subscriptionClient != null) {
                                subscriptionClient.shutdown();
                                logger.info(
                                    lm("subscription client shutdown, " +
                                       "status: " + status +
                                       ", statistics:" +
                                       dump(subscriptionClient
                                                .getStatistics())));
                            }

                            /* ask PU where we should start in reconnect */
                            final VLSN vlsn = pu.getReconnectVLSN(
                                repGroupId.getGroupId());
                            /* remember where we stop */
                            stat.setLastVLSNBeforeReconnect(vlsn);
                            handleErrorWithRetry(vlsn.getNext());
                            break;

                        case SECURITY_CHECK_ERROR:
                            /* no retry if security error */
                            if (subscriptionClient != null) {
                                subscriptionClient.shutdown();
                            }
                            /* cause of failure to pass to SFE */
                            final Throwable exp = authHandler.getCause();
                            final String err =
                                "Security check failed, subscription client " +
                                "shutdown without retry. Cause of failure: " +
                                exp.getMessage();

                            logger.warning(lm(err));
                            /* create SFE with cause of failure */
                            throw new SubscriptionFailureException(
                                pu.getSubscriberId(), err, exp);

                        default:
                    }

                    synchronized (this) {
                        this.wait(MONITORING_INTERVAL_MS);
                    }
                }

            } catch (InterruptedException ie) {
                cause = ie;
                /* rsc requires to shut down monitor, no need to escalate */
                logger.fine(lm("thread " + getName() + " is " +
                               "interrupted and exists."));
            } catch (SubscriptionInsufficientLogException sile) {
                cause = sile;
                logger.warning(lm("Unable to restart subscription client due " +
                                 "to insufficient log at server for " +
                                  "subscriber " + sile.getSubscriberId() +
                                  ", in shard " +
                                  repGroupId + ", the requested vlsn is  " +
                                  sile.getReqVLSN(repGroupId)));
            } catch (SubscriptionFailureException e) {
                cause = e;
                logger.warning(lm("Unable to restart subscription client" +
                                  " reason:" + e.getMessage()));
            } finally {

                /*
                 * fail to reconnect the feeder after retry, have to let PU
                 * close the whole subscription, and signal subscriber.
                 *
                 * PU will close each RSC when closing the subscription.
                 *
                 * In some unit tests, there is no PU.
                 */
                if (pu != null) {
                    pu.close(cause);
                }
                logger.info(lm("Monitor thread exits, " +
                               "during lifetime, # attempted " +
                               "connect: " + numAttempts +
                               ", # successful connects: " + numSucc));
            }
        }

        @Override
        public int initiateSoftShutdown() {
            /* wake up the thread and give it a bit time to exit */
            synchronized (this) {
                this.notify();
            }
            final boolean alreadySet = shutdownDone(logger);
            final int waitMs = (int)(2 * MONITORING_INTERVAL_MS);
            logger.fine(lm("Signal RSC monitor thread to shutdown, " +
                           "shutdown already signalled? " + alreadySet +
                           "wait for  " + waitMs + " ms let it exit"));
            return waitMs;
        }

        @Override
        protected void cleanup() {
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }

        private String lm(String msg) {
            return "[RSC-MON-" + consumerId + "] " + msg;
        }

        private String dump(SubscriptionStat s) {
            return "[" +
                   "start vlsn: " + s.getStartVLSN() +
                   ", high vlsn: " + s.getHighVLSN() +
                   ", # msg received: " + s.getNumMsgReceived() +
                   ", # msg responded: " + s.getNumMsgResponded() +
                   ", # ops processed: " + s.getNumOpsProcessed() +
                   ", # txn committed: " + s.getNumTxnCommitted() +
                   ", # txn aborted: " + s.getNumTxnAborted()  +
                   "]";
        }

        /*
         * Monitor thread retry to create a stream to server after it gets a
         * new master HA from publisher.
         *
         * @param vlsn   start vlsn
         *
         * @throws SubscriptionInsufficientLogException
         * @throws SubscriptionFailureException
         */
        private void handleErrorWithRetry(VLSN startVLSN)
            throws SubscriptionInsufficientLogException,
            SubscriptionFailureException {


            numAttempts++;
            logger.info(lm("Restart subscription client (# attempts:" +
                           " " + numAttempts + ") from " + startVLSN));

            /*
             * first shut down txn buffer worker thread and stop all activity,
             * and get the last committed VLSN the txn buffer has processed,
             * use that VLSN as the start stream point of new client.
             */
            txnBuffer.close();

            txnBuffer.startWorker();
            logger.fine(lm("txn worker restarted."));

            /* txn buffer ready, waiting for client to stream from feeder */
            int numRetry = 0;
            while (!canceled.get()) {
                try {

                    numRetry++;
                    if (numRetry <= MAX_RETRY_IN_ONE_ATTEMPT) {
                        logger.info(lm("Attempt (" + numAttempts + ")" +
                                       " will restart client: " +
                                       "# of retry " + numRetry +
                                       ", limit " +
                                       MAX_RETRY_IN_ONE_ATTEMPT));
                    } else {
                        final String err =
                            "Attempt (" + numAttempts + ")" +
                            "fails to start client after trying " +
                            MAX_RETRY_IN_ONE_ATTEMPT + " times, throw " +
                            "SubscriptionFailureException to " +
                            "terminate subscription.";
                        logger.info(lm(err));
                        throw new SubscriptionFailureException(
                            pu.getSubscriberId(), err);
                    }

                    signalOnWarn(System.currentTimeMillis());

                    /* refresh to a new master HA host port */
                    master = pu.getMasterInfo(repGroupId, master);

                    if (master == null) {
                        final String err = "Subscriber " +
                                           pu.getSubscriberId() +
                                           " is unable to get any master " +
                                           "HA for shard " + repGroupId;

                        logger.warning(lm(err));
                        /* will be captured and retry */
                        throw new UnknownHostException(err);
                    }

                    final String feederHostPort = master.getMasterHostPort();
                    logger.info(lm("refreshed master HA for group " +
                                   repGroupId + ", master: " +
                                   master.getMasterRepNodeId() +
                                   ", HA: " + feederHostPort));

                    /* build a new config with new feeder host port */
                    if (pu == null || !pu.isSecureStore()) {
                        /* unit test or non-secure store */
                        subscriptionConfig = new SubscriptionConfig(
                            consumerId, directory, DEFAULT_SUBSCRIBER_HOSTPORT,
                            feederHostPort, repGroupId.getGroupName(), null,
                            CLIENT_NODE_TYPE);
                    } else {
                        /* secure store */
                        subscriptionConfig = new SubscriptionConfig(
                            consumerId, directory, DEFAULT_SUBSCRIBER_HOSTPORT,
                            feederHostPort, repGroupId.getGroupName(), null,
                            CLIENT_NODE_TYPE, authHandler, securityProps);
                    }
                    subscriptionConfig.setCallback(replicationStreamCbk);

                    subscriptionClient = startClient(startVLSN,
                                                     subscriptionConfig);

                    logger.info(lm("Subscriber " + pu.getSubscriberId() +
                                   " creates a new client to shard " +
                                   repGroupId + " to RN " +
                                   master.getMasterRepNodeId() +
                                   " at " + feederHostPort +
                                   " last committed vlsn before reconnect " +
                                   stat.getLastVLSNBeforeReconnect() +
                                   " and new stream will start from " +
                                   startVLSN));
                    numSucc++;
                    return;

                } catch (GroupShutdownException cause) {
                    /* no need to retry */
                    final String err = "Subscriber " + pu.getSubscriberId() +
                                       " is unable to start client from " +
                                       startVLSN + " at " +
                                       ((master == null) ?
                                           " from unknown master " :
                                           " at " +
                                           master.getMasterRepNodeId()) +
                                       ", reason " + cause.getMessage();
                    logger.warning(lm(err));
                    throw new SubscriptionFailureException(pu.getSubscriberId(),
                                                           err, cause);
                } catch (InsufficientLogException ile) {
                    /* no enough logs, no need to retry */
                    final String err = "Subscriber " + pu.getSubscriberId() +
                                       " is unable to start client " +
                                       "from vlsn " + startVLSN + " " +
                                       "due to insufficient log at shard " +
                                       repGroupId + " of store " +
                                       pu.getStoreName();
                    logger.warning(lm(err));

                    final SubscriptionInsufficientLogException sile =
                        new SubscriptionInsufficientLogException(
                            pu.getSubscriberId(), err);

                    sile.addInsufficientLogShard(repGroupId, startVLSN);

                    throw sile;

                } catch (UnknownHostException | IllegalArgumentException |
                    TimeoutException | InternalException  cause) {

                    /* retry on these exceptions */
                    final String err =
                        "Subscriber " + pu.getSubscriberId() +
                        " is unable to start client from  vlsn " + startVLSN +
                        ((master == null) ? " from unknown master " :
                            " at " + master.getMasterRepNodeId()) +
                        ", will refresh master and retry, " +
                        "cause: " + cause.getMessage() +
                        "\n" + LoggerUtils.getStackTrace(cause);

                    logger.warning(lm(err));
                }
            }
        }

        /* periodically update stats */
        private synchronized void updateStatistics() {

            final long curr = NoSQLSubscriptionImpl.getCurrTimeMs();
            if ((curr - lastStatCollectTimeMs) < collIntvMs) {
                return;
            }

            stat.setNumSuccReconn(numSucc);

            /* will signal onWarn if shard timeout */
            signalOnWarn(curr);

            /* update pu stat if pu exists, skip for unit test without pu */
            if (pu != null) {
                final SubscriptionStatImpl puStat =
                    (SubscriptionStatImpl) pu.getStatistics();
                puStat.updateShardStat(repGroupId, stat);
            }

            lastStatCollectTimeMs = curr;
        }

        private void signalOnWarn(long curr) {

            final long lastMsgTime = stat.getLastMsgTimeMs();
            if (!isShardDown(curr, lastMsgTime) || !needWarn(curr)) {
                return;
            }

            final String msg = "Shard timeout";

            try {
                pu.getSubscriber().onWarn(
                    new ShardTimeoutException(repGroupId.getGroupId(),
                                              lastMsgTime,
                                              timeoutInMs,
                                              msg));
            } catch (Exception exp) {
                logger.warning(lm("Exception in executing " +
                                  "subscriber's onWarn(): " +
                                  exp.getMessage() + "\n" +
                                  LoggerUtils.getStackTrace(exp)));
            }


            lastWarningTimeMs = curr;
            logger.warning(lm("Shard time out in streaming, " +
                              "shard: " + repGroupId.getGroupId() +
                              ", last msg time: " +
                              pu.getDateFormatter().get().format(lastMsgTime)));
        }

        private boolean isShardDown(long curr, long ts) {
            return (ts > 0) && ((curr - ts) > timeoutInMs);
        }

        private boolean needWarn(long curr) {
            return lastWarningTimeMs == 0 /* first warning */ ||
                   (curr - lastWarningTimeMs) > warningIntvMs;

        }
    }
}

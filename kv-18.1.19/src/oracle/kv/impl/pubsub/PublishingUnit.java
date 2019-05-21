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
import java.text.DateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.pubsub.security.StreamClientAuthHandler;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.CheckpointFailureException;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.pubsub.SubscriptionInsufficientLogException;
import oracle.kv.stats.SubscriptionMetrics;
import oracle.kv.table.TableAPI;

import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.utilint.VLSN;

/**
 * Object presents a single publisher unit that services a single subscriber.
 * This PublishingUnit is created when NoSQLPublisher accepts a subscriber and
 * create a subscription for the subscriber. The PublishingUnit is destroyed
 * when a subscription is canceled by subscriber and NoSQLPublisher no longer
 * serves the subscriber.
 */
public class PublishingUnit {

    /* max wait time to enqueue or dequeue a stream op */
    final static int OUTPUT_QUEUE_TIMEOUT_MS = 1000;
    /* size of queue in terms of number of stream operations */
    final static int OUTPUT_QUEUE_SIZE_PER_SHARD = 1024 * 10;

    /* parent publisher */
    private final NoSQLPublisher parent;
    /* private logger */
    private final Logger logger;
    /* a handle of source kvstore */
    private final KVStoreImpl kvstore;
    /* data consumers for all shards */
    private final Map<RepGroupId, ReplicationStreamConsumer> consumers;
    /* shard timeout in ms */
    private final long shardTimeoutMs;
    /* thread safe date format */
    private final ThreadLocal<DateFormat> df;
    /* parent publisher directory */
    private final String parentDir;
    /* true if the unit has been closed */
    private final AtomicBoolean closed;
    /* properties for secure store, null for non-secure store */
    private final Properties securityProp;

    /* handle to schedule stat collection thread */
    private volatile ScheduledFuture<?> schedulerHandle;
    /* FIFO queue of stream messages to be consumed by subscriber */
    private volatile BlockingQueue<StreamOperation> outputQueue;
    /* subscriber using the Publisher */
    private volatile NoSQLSubscriber subscriber;
    /* id of subscriber using the publishing unit */
    private volatile NoSQLSubscriberId si;
    /* shards PU covers, for a single member group, PU covers all shards */
    private volatile Set<RepGroupId> shards;
    /* subscription created by the pu after successful onSubscribe() */
    private volatile NoSQLSubscription subscription;
    /* subscription statistics */
    private volatile SubscriptionStatImpl statistics;
    /* checkpoint table used */
    private volatile CheckpointTableManager ckptTableManager;
    /* true if a new ckpt table is created and used */
    private volatile boolean newCkptTable;

    /*
     * A map to hold impl of all subscribed tables, which will be used in
     * de-serialization of rows. The map is null if user is trying to
     * subscribe all tables in this case, we do not prepare any impl at the
     * time when subscription is created, since new table will be created any
     * time during subscription.
     */
    private volatile Map<String, TableImpl> tables;


    /*-- For test and internal use only. ---*/
    /* true if enable checkpoint in subscription, default is true */
    private boolean enableCkpt;
    /* true if enable feeder filter */
    private boolean enableFeederFilter;

    /*
     * unit test only, true use table name as id. In unt tests where table
     * created by TableBuilder set id 0 and table API encode table name in
     * the key bytes. The default is false;
     */
    private final boolean tblNameAsId;

    /**
     * Builds a publishing unit for a subscription
     *
     * @param publisher  parent publisher
     * @param logger     logger
     */
    private PublishingUnit(NoSQLPublisher publisher,
                           Logger logger) {
        this(publisher, (KVStoreImpl) publisher.getKvs(),
             publisher.getDefaultShardTimeoutMs(),
             publisher.getJEHASecurityProp(), publisher.getPubRootDir(),
             publisher.useTableNameAsId(), logger);
    }

    /**
     * Unit test only
     */
    PublishingUnit(NoSQLPublisher parent,
                   KVStoreImpl kvstore,
                   long shardTimeoutMs,
                   Properties securityProp,
                   String parentDir,
                   boolean tblNameAsId,
                   Logger logger) {

        this.parent = parent;
        this.kvstore = kvstore;
        this.shardTimeoutMs = shardTimeoutMs;
        this.securityProp = securityProp;
        this.parentDir = parentDir;
        this.tblNameAsId = tblNameAsId;
        this.logger = logger;

        /* will be init a subscriber establishes a NoSQL Subscription */
        outputQueue = null;
        tables = null;
        consumers = new HashMap<>();
        subscriber = null;
        si = null;
        shards = null;
        closed = new AtomicBoolean(false);
        schedulerHandle = null;
        subscription = null;
        ckptTableManager = null;
        enableCkpt = true;
        newCkptTable = false;
        df = ThreadLocal.withInitial(
            FormatUtils::getDateTimeAndTimeZoneFormatter);
    }

    /**
     * Services a request for subscription. It shall not throw any exception
     * to caller.
     *
     * @param pub    parent publisher
     * @param sub    an instance of NoSQLSubscriber
     * @param logger logger
     */
    public static void doSubscribe(NoSQLPublisher pub,
                                   NoSQLSubscriber sub,
                                   Logger logger) {

        final PublishingUnit pu = new PublishingUnit(pub, logger);
        pu.subscribe(sub);
    }

    /**
     * Internally create a subscription
     */
    void subscribe(NoSQLSubscriber s) {

        /*
         * All exceptions should be taken care of inside the function, and
         * delivered to Subscriber via onError.
         */
        try {
            final NoSQLSubscriptionConfig config = s.getSubscriptionConfig();

            subscriber = s;
            si = config.getSubscriberId();

            /* verify store in init position matches those in publisher */
            checkInitPosition(config);

            /* if a single member group, it contains all shards from topology */
            shards =
                NoSQLSubscriptionConfig.computeShards(si,
                                                      kvstore.getTopology());
            statistics = new SubscriptionStatImpl(si, shards);
            enableFeederFilter = config.isFeederFilterEnabled();
            enableCkpt = config.isCkptEnabled();
            ckptTableManager =
                new CheckpointTableManager(kvstore, si,
                                           config.getCkptTableName(), logger);
            verifyCkptTable(config);

            /* prepare the unit */
            prepareUnit(config);

            SubscriptionInsufficientLogException sile = null;

            /* determine where to start */
            final StreamPosition initPos = getStartStreamPos(config);

            /* start all replication stream consumers */
            for (RepGroupId gid : shards) {
                final ReplicationStreamConsumer c = consumers.get(gid);
                final VLSN vlsn = initPos.getShardPosition(gid.getGroupId())
                                         .getVLSN();

                try {
                    c.start(vlsn);
                } catch (InsufficientLogException ile) {
                    if (sile == null) {
                        sile = new SubscriptionInsufficientLogException(
                            si, "Unable to subscribe due to insufficient logs" +
                                " in at least one shard in source store with" +
                                " stream mode " + config.getStreamMode());
                    }
                    sile.addInsufficientLogShard(gid, vlsn);
                    continue;
                }

                logger.info(lm("RSC has started for " + gid +
                               " from vlsn " + vlsn + " with mode " +
                               config.getStreamMode()));
                statistics.setReqStartVLSN(gid,
                                           c.getRSCStat().getReqStartVLSN());
                statistics.setAckStartVLSN(gid,
                                           c.getRSCStat().getAckedStartVLSN());
            }

            /* throw ile if insufficient logs  */
            if (sile != null) {
                throw sile;
            }

            /* get actual start position */
            final StreamPosition startPos = getActualStartPos();
            /* create a handle to NoSQLSubscription */
            subscription = NoSQLSubscriptionImpl.get(this, s, startPos,
                                                     enableCkpt, logger);
            if (parent == null) {
                /* unit test only */
                logger.info(lm("Unit test only, no parent publisher."));
            } else {
                /*
                 * Note here we add a PU into the map, officially it becomes an
                 * active subscription, no longer an init subscription, but this
                 * subscription is still counted in the init counter in
                 * publisher, until it is decremented after we return from
                 * subscribe(). Consequently, there is a small window that we
                 * may over count the number of subscriptions in publisher
                 * and make over-conservative (false negative) decision to
                 * reject new subscriptions. That window should be small.
                 */
                parent.addPU(ckptTableManager.getCkptTableName(), this);
                /* ensure the publisher is not closed during initialization */
                if (parent.isClosed()) {
                    throw new SubscriptionFailureException(si,
                                                           "NoSQLPublisher " +
                                                           "has been closed " +
                                                           "during " +
                                                           "initialization, " +
                                                           "need to close and" +
                                                           " exit.",
                                                           null);
                }
            }
            logger.info(lm("Subscription to store " + getStoreName() +
                           " is created for subscriber" + si));

            /*
             * update ckpt table after subscription is successful, the
             * checkpoint may or may not equal to the requested stream
             * position, depending on the stream mode
             */
            if (config.isCkptEnabled()) {
                doCheckpoint(startPos.prevStreamPosition());
            }

            /*
             * Now we have a full-blown subscription instance to pass to the
             * subscriber.
             *
             * Per Rule Subscription.2 in reactive streams spec, subscription
             * should allow subscriber to call Subscription.request
             * synchronously from within onNext or onSubscribe.
             *
             * https://github.com/reactive-streams/reactive-streams-jvm/tree/v1.0.0#specification
             *
             * User may call Subscription.request in Subscriber.onSubscribe
             * below. Since at this time no subscription worker is available,
             * the subscription will create a separate subscription worker
             * thread to start streaming, and then return immediately.
             */
            try {
                s.onSubscribe(subscription);
            } catch (Exception exp) {
                /* catch all exceptions from user's onSubscribe */
                throw new SubscriptionFailureException(si,
                                                       "Exception raised in " +
                                                       "onSubscribe",
                                                       exp);
            }
        } catch (SubscriptionInsufficientLogException sile) {
            final String err = "Cannot subscribe because store " +
                               getStoreName() +
                               " does not have sufficient logs files:\n";
            final Set<RepGroupId> isls = sile.getInsufficientLogShards();
            final StringBuilder sb = new StringBuilder();
            for (RepGroupId id : isls) {
                sb.append("shard ").append(id).append(":");
                sb.append(" requested start vlsn ").append(sile.getReqVLSN(id));
                sb.append("\n");
            }
            logger.warning(lm(err + sb.toString()));

            /* signal subscriber and close pu */
            close(sile);
        } catch (SubscriptionFailureException sfe) {
            logger.warning(lm("Subscription failed because " +
                           sfe.getMessage()));

            /* signal subscriber and close pu */
            close(sfe);
        }

        /* we shall not any exception other than the above */
    }

    /**
     * Returns shards covered by this PU, which is available after
     * call of subscriber() is done successfully. If not, returns null.
     *
     * @return  shards covered by this PU
     */
    Set<RepGroupId> getCoveredShards() {
        return shards;
    }

    /**
     * Unit test only.
     *
     * @return true if use table name as id in some unit test
     */
    boolean isUseTblNameAsId() {
        return tblNameAsId;
    }


    /* determine where to start stream */
    private StreamPosition getStartStreamPos(NoSQLSubscriptionConfig config)
        throws SubscriptionFailureException {

        /* determine where to start */
        StreamPosition initPos;

        if (!enableCkpt) {
            /* checkpoint disabled */
            return (config.getInitialPosition() != null) ?
                config.getInitialPosition() :
                StreamPosition.getInitStreamPos(getStoreName(),
                                                getStoreId(),
                                                shards);
        }

        final NoSQLStreamMode mode = config.getStreamMode();
        switch (mode) {
            case FROM_CHECKPOINT:
            case FROM_EXACT_CHECKPOINT:
                if (newCkptTable) {
                    /* if a new ckpt table, no need to read it */
                    initPos = StreamPosition.getInitStreamPos(getStoreName(),
                                                              getStoreId(),
                                                              shards);
                    logger.info(lm("a new checkpoint table is created, start " +
                                   "stream from the position: " + initPos));
                } else {
                    /* if existing ckpt table, fetch checkpoint from it */
                    initPos = getLastCheckpoint().nextStreamPosition();
                    logger.info(lm("start stream from the next position " +
                                   "after the last checkpoint: " + initPos));
                }
                break;

            case FROM_STREAM_POSITION:
            case FROM_EXACT_STREAM_POSITION:
                initPos = getInitPosInConf(config);
                logger.info(lm("start stream from position " + initPos));
                break;

            case FROM_NOW:
                /* note init pos does not matter */
                initPos = StreamPosition.getInitStreamPos(getStoreName(),
                                                          getStoreId(),
                                                          shards);
                /*
                 * set a meaningless VLSN. Since NULL_VLSN is widely used
                 * through out the code with different semantics, probably
                 * it is better to use Long.MAX_VALUE instead of NULL_VLSN.
                 */
                for (StreamPosition.ShardPosition sp :
                    initPos.getAllShardPos()) {
                    sp.setVlsn(new VLSN(Long.MAX_VALUE));
                }
                logger.info(lm("start stream with mode " + mode));
                break;

            default:
                throw new SubscriptionFailureException(
                    si, "Invalid stream mode " + mode);
        }

        return initPos;
    }

    /**
     * Closes a publisher unit, shut down all connection to source and free
     * resources. The close can be called 1) from parent publisher when
     * it is closed by user; 2) from parent publisher when it is unable to
     * create a subscription; 3) from its subscription where it is canceled
     * by user; 4) from underlying child threads when a serious error has
     * happened and the subscription has to be terminated.
     *
     * @param cause  cause to close the publishing unit, null if
     *               normally closed by user without error.
     */
    public void close(Throwable cause) {

        if (!closed.compareAndSet(false, true)) {
            return;
        }

        logger.info(lm("Closing PU starts, cause: " +
                       (cause == null ? "none" : cause)));

        /* if have a running subscription, cancel it */
        if (subscription != null) {
            ((NoSQLSubscriptionImpl)subscription).cancel(cause);
        }

        /* shut down all rep stream consumers */
        shutDownConsumers();

        /* close bridge queue between consumers and subscriber */
        if (outputQueue != null && !outputQueue.isEmpty()) {
            outputQueue.clear();
        }
        outputQueue = null;

        /* ask parent forget about me */
        if (parent != null) {
            parent.removePU(ckptTableManager.getCkptTableName());
        }

        /* finally signal subscriber */
        assert (subscriber != null);
        if (cause != null) {
            /* close pu due to errors, signal subscriber */
            try {
                subscriber.onError(cause);
            } catch (Exception exp) {
                /* just log, we will close pu anyway */
                logger.warning(lm("Exception in executing " +
                                  "subscriber's onError: " +
                                  exp.getMessage() + "\n" +
                                  LoggerUtils.getStackTrace(exp)));

            }
            logger.info(lm("PU closed due to error " + cause.getMessage() +
                           "\n" + LoggerUtils.getStackTrace(cause)));
        } else {
            logger.info(lm("PU closed normally."));
        }
    }

    /**
     * Gets subscription statistics
     *
     * @return subscription statistics
     */
    public SubscriptionMetrics getStatistics() {
        return statistics;
    }

    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Gets subscriber using the publisher unit
     *
     * @return subscriber using the publisher unit
     */
    public NoSQLSubscriber getSubscriber() {
        return subscriber;
    }

    /**
     * Retrieves the login handle from kvstore. It should be called only when
     * store is a secure store.
     */
    public LoginHandle getLoginHandle(RepGroupId gid) {
        final LoginManager lm = KVStoreImpl.getLoginManager(kvstore);
        final ShardMasterInfo master = getMasterInfo(gid, null);
        if (master == null) {
            throw new IllegalStateException("Cannot find master for shard " +
                                            gid);
        }

        return lm.getHandle(master.getMasterRepNodeId());
    }

    /**
     * Returns a handle to kvstore
     *
     * @return a handle to kvstore
     */
    public KVStoreImpl getKVStore() {
        return kvstore;
    }

    /**
     * Returns the security credentials from parent publisher
     *
     * @return the security credentials from parent publisher
     */
    public NoSQLPublisher.SecurityCred getSecurityCred() {
        return parent.getSecurityCred();
    }

    /**
     * Gets a reference to kvstore
     *
     * @return a reference to kvstore
     */
    KVStoreImpl getKvstore() {
        return kvstore;
    }

    /**
     * Gets the output queue of the unit
     *
     * @return the output queue, null if not initialized
     */
    BlockingQueue<? extends StreamOperation> getOutputQueue() {
        return outputQueue;
    }

    /**
     * Gets the table md manager from parent publisher
     *
     * @return the table md manager from parent publisher
     */
    PublisherTableMDManager getTableMDManager() {
        return parent.getPublisherTableMDManager();
    }

    /**
     * Gets subscriber index using the publishing unit
     *
     * @return subscriber index using the publishing unit
     */
    NoSQLSubscriberId getSubscriberId() {
        return si;
    }

    /**
     * Internal use in test only
     * <p>
     * We should always get store name and id from the parent
     * publisher. In some unit test the parent publisher might
     * not be available, fetch them from kvstore in that case.
     */
    String getStoreName() {
        if (parent == null) {
            return kvstore.getTopology().getKVStoreName();
        }
        return parent.getStoreName();
    }

    /* stat collection */
    void doStatCollection() {

        /* collect RSC stats */
        for(ReplicationStreamConsumer c : consumers.values()) {
            final RepGroupId id = c.getRepGroupId();
            statistics.updateShardStat(id, c.getRSCStat());
        }

        /* collect other stats */
        final NoSQLSubscriptionImpl sub = (NoSQLSubscriptionImpl)subscription;
        statistics.setTotalConsumedOps(sub.getNumStreamedOps());
    }

    /**
     * Gets master HA hostport from topology manager
     *
     * @param gid                 id of given shard
     * @param currMasterInfo      current master info

     * @return  shard master info, null if cannot find master
     */
    ShardMasterInfo getMasterInfo(RepGroupId gid,
                                  ShardMasterInfo currMasterInfo) {

        if (parent != null) {
            /* normal cases, all query should to go topology manager  */
            try {
                return parent.getPublisherTopoManager()
                             .buildMasterInfo(gid, currMasterInfo);
            } catch (FaultException fe) {
                /* return null and let caller handle the failure */
                logger.warning(lm("Cannot build master for shard " + gid +
                                  ", reason " + fe.getMessage()));
                return null;
            }
        }

        /*
         * below are for unit test only, get master feeder HA hostport without
         * a parent publisher
         */

        final PublisherTopoManager topoManager =
            new PublisherTopoManager(getStoreName(), kvstore, logger);
        return topoManager.buildMasterInfo(gid, currMasterInfo);
    }

    /**
     * Gets the last checkpoint made by the subscription
     *
     * @return the last checkpoint
     */
    StreamPosition getLastCheckpoint() {
        return ckptTableManager.fetchCheckpoint(shards);
    }

    /**
     * Internal use in test only
     */
    Topology getTopology() {
        return (kvstore == null) ? null : kvstore.getTopology();
    }

    /**
     * Internal use in test only
     */
    TableAPI getTableAPI() {
        return (kvstore == null) ? null : kvstore.getTableAPI();
    }

    /**
     * Internal use in test only
     */
    Map<RepGroupId, ReplicationStreamConsumer> getConsumers() {
        return consumers;
    }

    /**
     * Internal use in test only
     */
    ReplicationStreamConsumer getConsumer(RepGroupId gid) {
        return consumers.get(gid);
    }

    long getShardTimeoutMs() {
        return shardTimeoutMs;
    }

    ThreadLocal<DateFormat> getDateFormatter() {
        return df;
    }

    /**
     * Returns true if kvstore is a secure store
     *
     * @return true if kvstore is a secure store, false otherwise.
     */
    boolean isSecureStore() {
        /* in unit test without publisher, always non-secure store */
        return parent != null && parent.isSecureStore();
    }

    CheckpointTableManager getCkptTableManager() {
        return ckptTableManager;
    }

    VLSN getReconnectVLSN(int shardId) {

        if (subscription == null) {
            /* no subscription yet or subscription has gone */
            return VLSN.NULL_VLSN;
        }

        final StreamPosition currPos = subscription.getCurrentPosition();
        final StreamPosition.ShardPosition sp =
            currPos.getShardPosition(shardId);
        final VLSN ret = (sp == null ? VLSN.NULL_VLSN : sp.getVLSN());
        logger.fine(lm("Shard " + shardId + " should start from " + ret +
                       " when reconnect."));
        return ret;
    }

    /**
     * Gets the table id string, consider a special unit test case
     */
    static String getTableId(PublishingUnit pu, TableImpl t) {

        if (pu == null  || /* unit test without pu*/
            pu.tblNameAsId /* unit test using table builder */) {
            /*
             * for unit test only.
             *
             * In some unit tests the table created by TableBuilder which sets
             * the id 0, and table API encode table name in the key bytes.
             */
            return t.getFullName();
        }
        /* normal cases */
        return t.getIdString();
    }

    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private String lm(String msg) {
        return "[PU-" +
               (ckptTableManager == null ? "<na>" :
                   ckptTableManager.getCkptTableName()) +
               "-" + si + "] " + msg;
    }

    /* verify store in config matches those in publisher */
    private void checkInitPosition(NoSQLSubscriptionConfig config)
        throws SubscriptionFailureException {

        /* bypass check if init stream position not specified */
        if (config.getInitialPosition() == null) {
            return;
        }

        final String storeInConfig = config.getInitialPosition().getStoreName();
        final long storeIdInConfig = config.getInitialPosition().getStoreId();

        if (!storeInConfig.equals(getStoreName())) {
            throw new SubscriptionFailureException(si,
                "Store  " + storeInConfig + " in init stream position " +
                "does not match the store in publisher: " + getStoreName());
        }

        final long storeId = kvstore.getTopology().getId();
        if (storeIdInConfig != storeId) {
            throw new SubscriptionFailureException(si,
                "Store id " + storeIdInConfig + " in stream position " +
                "does not match the store id in publisher: " + storeId);
        }
    }

    /* clean up all consumers */
    private void shutDownConsumers() {
        /* shutdown all consumers */
        for (ReplicationStreamConsumer client : consumers.values()) {
            client.cancel(true);
        }
        consumers.clear();
        logger.info(lm("All RSC shut down."));

        /* cancel stat collection */
        if (schedulerHandle != null && !schedulerHandle.isCancelled()) {
            schedulerHandle.cancel(false);
            schedulerHandle = null;
        }
    }

    /* prepare publisher and make it ready to stream */
    private void prepareUnit(NoSQLSubscriptionConfig config)
        throws SubscriptionFailureException {

        final Topology topology = kvstore.getTopology();
        final int numShards;

        logger.info(lm("PU starts with covered shards: " + shards));

        if (shards == null || shards.isEmpty()) {
            numShards = topology.getRepGroupIds().size();
        } else {
            numShards = Math.min(topology.getRepGroupIds().size(),
                                 shards.size());
        }
        outputQueue =
            new ArrayBlockingQueue<>(numShards * OUTPUT_QUEUE_SIZE_PER_SHARD);

        /*
         * ensure subscribed tables all valid, happens at the beginning
         * of subscription when a publishing unit is created.
         *
         * When a table is removed in during an ongoing subscription, it
         * is transparent to subscriber and subscriber will not receive
         * any updates from that table. If a table is added in the mid
         * of subscription, it is transparent to subscriber too, since
         * the new table is not subscribed. The only exception is that
         * when user subscribes all tables without specifying any table
         * name, subscriber will receive updates to the new table when
         * it is created and populated.
         */
        final Set<String> tableNames = config.getTables();
        if (tableNames != null && !tableNames.isEmpty()) {
            /* user subscribes some tables */
            tables = new HashMap<>();
            for (String table : tableNames) {

                /* ask table md manager for md, create a local copy */
                TableImpl tableImpl = getTable(config.getSubscriberId(), table);

                if (tableImpl == null) {
                    final String err = "Table " + table + " not found at " +
                                       "store";

                    logger.warning(lm(err));
                    throw new SubscriptionFailureException(si, err);
                }

                tables.put(tableImpl.getFullName(), tableImpl);
            }
            logger.info(lm("PU subscribed tables: " + tables.keySet() +
                           ", table names: " + tableNames));

        } else {
            /* user subscribes all tables */
            logger.info(lm("PU subscribed all tables"));
        }

        /*
         * ensure directory exists for the subscription. User may re-create a
         * subscription on the same publisher after cancel an existing one, so
         * allow pre-exist directory. Duplicate subscription wont take place
         * because feeder will reject all duplicate connections.
         */
        final String directory;
        try {
            directory = NoSQLPublisher.ensureDir(parentDir,
                                                 config.getCkptTableName(),
                                                 true);
            logger.info(lm("Directory ok at " + directory));
        } catch (Exception cause) {
            /* capture all exceptions and raise pfe to user */
            final String err = "Fail to ensure directory (parent dir: " +
                               parentDir + ", child: " +
                               config.getCkptTableName() + ")";
            throw new SubscriptionFailureException(si, err, cause);
        }

        for (RepGroupId gid : shards) {
            final ShardMasterInfo master = getMasterInfo(gid, null);
            if (master == null) {
                final String err = "Unable to get master info for shard " + gid;
                throw new SubscriptionFailureException(si, err);
            }

            try {
                final ReplicationStreamConsumer consumer =
                    new ReplicationStreamConsumer(this,
                                                  master,
                                                  gid,
                                                  outputQueue,
                                                  ((tables == null) ?
                                                      null : tables.values()),
                                                  directory,
                                                  getFilter(),
                                                  config.getStreamMode(),
                                                  securityProp,
                                                  logger);
                if (config.getILETestHook() != null) {
                    /* unit test only */
                    consumer.setILEHook(config.getILETestHook());
                }
                if (config.getTestToken() != null) {
                    /* unit test only */
                    final StreamClientAuthHandler authHandler =
                        (StreamClientAuthHandler)consumer.getAuthHandler();
                    authHandler.setToken(config.getTestToken());
                }

                consumers.put(gid, consumer);
                logger.info(lm("RSC ready for " + gid + " to " + master));
            } catch (UnknownHostException uhe) {
                final String err = "Unknown host for shard " + gid +
                                   ": " + master;
                logger.warning(lm(err));

                throw new SubscriptionFailureException(si, err, uhe);
            }
        }

        logger.info(lm("PU preparation done."));
    }

    /* Creates feeder filter from subscribed tables */
    private NoSQLStreamFeederFilter getFilter() {

        /* no feeder filter if disabled */
        if (!enableFeederFilter) {
            return null;
        }

        final NoSQLStreamFeederFilter filter;
        if (tables == null || tables.isEmpty()) {
            /* get a feeder filter passing all tables */
            filter = NoSQLStreamFeederFilter.getFilter();
        } else {
            /* get a feeder filter passing selected tables */
            filter = NoSQLStreamFeederFilter.getFilter(
                new HashSet<>(tables.values()));
        }

        logger.info(lm("Feeder filter created with subscribed table: " +
                       ((tables == null || tables.isEmpty()) ?
                           "all user tables" : tables.keySet()) +
                       ", id strings " + filter.toString()));
        return filter;
    }

    /* gets a table impl from table md */
    private TableImpl getTable(NoSQLSubscriberId sid, String table) {

        /* unit test only, no parent publisher */
        if (parent == null) {
            return (TableImpl) kvstore.getTableAPI().getTable(table).clone();
        }

        /* normal cases, all query should to go tableMD manager  */
        return parent.getPublisherTableMDManager().getTable(sid, table);
    }

    /*
     * We should always get store name and id from the parent
     * publisher. In some unit test the parent publisher might
     * not be available, fetch them from kvstore in that case.
     */
    private long getStoreId() {
        if (parent == null) {
            return kvstore.getTopology().getId();
        }
        return parent.getStoreId();
    }

    private void verifyCkptTable(NoSQLSubscriptionConfig config) {

        if (!enableCkpt) {
            return;
        }

        try {
            if (ckptTableManager.isCkptTableExists()) {
                return;
            }

            if (config.useNewCheckpointTable()) {
                /* ok, we can create a new ckpt table */
                final boolean multiSubs = (si.getTotal() > 1);
                ckptTableManager.createCkptTable(multiSubs);
                newCkptTable = true;
                return;
            }
        } catch (FaultException fe) {
            final String err = "Subscription " + config.getSubscriberId() +
                               " gets fault exception when attempt to " +
                               "ensure checkpoint table " +
                               ckptTableManager.getCkptTableName() +
                               " at store " + getStoreName() +
                               ", cause " + fe.getFaultClassName();
            logger.warning(lm(err));
            throw new SubscriptionFailureException(config.getSubscriberId(),
                                                   err, fe);
        } catch (UnauthorizedException ue) {
            final String err = "Subscription " + config.getSubscriberId() +
                               " has insufficient privilege to create table " +
                               ckptTableManager.getCkptTableName() +
                               " at store " + getStoreName() +
                               ", cause " + ue.getMessage();
            logger.warning(lm(err));
            throw new SubscriptionFailureException(config.getSubscriberId(),
                                                   err, ue);
        }

        /* ckpt table does not exist and we are not allowed to create it */
        final String err = "Subscription " + config.getSubscriberId() +
                           " configured to use existing checkpoint " +
                           "table " +
                           ckptTableManager.getCkptTableName() +
                           " while table not found at " +
                           getStoreName();
        logger.warning(lm(err));
        throw new SubscriptionFailureException(config.getSubscriberId(),
                                               err);
    }

    private void doCheckpoint(StreamPosition pos)
        throws SubscriptionFailureException {

        try {
            ckptTableManager.updateCkptTableInTxn(pos);
        } catch (CheckpointFailureException cfe) {
            final String err = "Unable to update checkpoint table " +
                               cfe.getCheckpointTableName() +
                               " with the initial stream position " +
                               pos;
            logger.warning(lm(err));
            throw new SubscriptionFailureException(si, err, cfe);
        }
    }

    /* get init stream position from config */
    private StreamPosition getInitPosInConf(NoSQLSubscriptionConfig config) {

        /* use position specified in config */
        StreamPosition initPos = config.getInitialPosition();
        if (initPos == null) {
            initPos = StreamPosition.getInitStreamPos(getStoreName(),
                                                      getStoreId(),
                                                      shards);
            logger.info(lm("initial position not specified, " +
                           "will start stream from position " + initPos));
        } else {
            /* sanity check if user specifies an init position */
            for (RepGroupId gid : shards) {
                /* missing shard in init position */
                if (initPos.getShardPosition(gid.getGroupId()) == null) {
                    throw new SubscriptionFailureException(
                        si, "Incomplete user specified init stream " +
                            "position, missing position for " +
                            "shard " + gid);
                }
            }

            /*
             * user should specify a valid start vlsn for each shard covered
             * by this PU, if not, it is an user error and user need to fix
             * it to subscribe.
             */
            for (StreamPosition.ShardPosition p : initPos.getAllShardPos()) {
                final int gid = p.getRepGroupId();
                if (!shards.contains(new RepGroupId(gid))) {
                    /*
                     * user specified a position of shard PU does not cover,
                     * dump warning and ignore.
                     */
                    logger.warning(lm("Uncovered shard " + gid +
                                      " in init stream position, shards " +
                                      "covered: " + shards));
                    continue;
                }

                if (p.getVLSN().equals(VLSN.NULL_VLSN)) {
                    throw new SubscriptionFailureException(
                        si, "User specified start VLSN for shard " +
                            p.getRepGroupId() + " is null");
                }
            }
            logger.fine(lm("PU to start stream from a given init " +
                           "position " + initPos));
        }

        return initPos;
    }

    /* Collects actual start position for each shard */
    private StreamPosition getActualStartPos() {
        final StreamPosition ret = new StreamPosition(getStoreName(),
                                                      getStoreId());
        final Map<RepGroupId, VLSN> actualStart = statistics.getAckStartVLSN();
        for (RepGroupId rid : shards) {
            ret.setShardPosition(rid.getGroupId(),  actualStart.get(rid));
        }
        return ret;
    }
}

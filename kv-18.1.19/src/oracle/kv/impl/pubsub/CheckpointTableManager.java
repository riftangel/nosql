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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.pubsub.CheckpointFailureException;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.WriteOptions;

import com.sleepycat.je.utilint.VLSN;

/**
 * Object represent the checkpoint table used in subscription.
 */
public class CheckpointTableManager {

    /** checkpoint table write timeout in ms */

    //TODO: hardcoded for now, may make it configurable in publisher config
    // if necessary
    final static int CKPT_TIMEOUT_MS = KVStoreConfig.DEFAULT_REQUEST_TIMEOUT;

    /** retry interval in ms */
    private final static int RETRY_INTERVAL_MS = 3000;
    /** maximum number of attempts */
    private final static int MAX_NUM_ATTEMPTS = 20;

    /**
     * The checkpoint table is a user table persisted in source kvstore which
     * records the checkpoint of each shard for a given subscription. Any
     * subscription need to be eligible to read and write the checkpoint
     * table associated with that subscription.
     *
     * The checkpoint table schema example for subscription group id s1
     *
     * subscription |   shard id  |   vlsn   |  timestamp in ms
     * -----------------------------------------------------------
     *     s1       |     rg1     |   1024   |  232789789781789
     *     s1       |     rg2     |   2048   |  258246582346578
     *     s1       |     rg3     |   3076   |  234527348572834
     * ------------------------------------------------------------
     *
     * Note
     * 1) All rows of a checkpoint table are clustered on single shard with
     * the shard key. For example, all rows for subscription "s1" are all
     * clustered on a single shard computed from shard key "s1".
     *
     * 2) The checkpoint table is a user table specified by user in
     * subscription configuration (@see NoSQLSubscriptionConfig).
     *
     * 3) Updates to the checkpoint table cannot be subscribed.
     *
     */

    /* write option to put checkpoint row in kvstore */
    private final static WriteOptions CKPT_WRITE_OPT =
        new WriteOptions(Durability.COMMIT_SYNC,
                         CKPT_TIMEOUT_MS, /* max time to do a checkpoint */
                         TimeUnit.MILLISECONDS);

    private final static String CKPT_TABLE_SUBSCRIPTION_FIELD_NAME =
        "subscription";
    final static String CKPT_TABLE_SHARD_FIELD_NAME = "shard_id";
    final static String CKPT_TABLE_VLSN_FIELD_NAME = "vlsn";
    final static String CKPT_TABLE_TS_FIELD_NAME = "timestamp";

    /* private logger */
    private final Logger logger;
    /* name of checkpoint table */
    private final String ckptTableName;
    /* subscriber that uses this checkpoint table */
    private final NoSQLSubscriberId sid;
    /* a handle of source kvstore */
    private final KVStoreImpl kvstore;

    public CheckpointTableManager(KVStoreImpl kvstore,
                                  NoSQLSubscriberId sid,
                                  String ckptTableName,
                                  Logger logger) {
        this.kvstore = kvstore;
        this.sid = sid;
        this.ckptTableName = ckptTableName;
        this.logger = logger;
    }

    /**
     * Creates checkpoint table. Throw an exception if the table exists or it
     * fails to create the table.
     *
     * @param kvs             handle to kvstore
     * @param ckptTableName   name of checkpoint table
     *
     * @throws IllegalArgumentException checkpoint table already exists at
     * store or fail to create the checkpoint
     *
     * @throws UnauthorizedException user is not eligible to create the
     * checkpoint table at kvstore
     */
    public static void createCkptTable(KVStore kvs, String ckptTableName)
        throws IllegalArgumentException, UnauthorizedException {
        createCkptTable(kvs, ckptTableName, false);
    }

    /**
     * Fetches a checkpoint and build a stream position from checkpoint table
     *
     * @param shards  shards of checkpoint
     *
     * @return the last persisted checkpoint for a subscription group, or
     * null if no such checkpoint exists in kvstore
     */
    public StreamPosition fetchCheckpoint(Set<RepGroupId> shards)
        throws SubscriptionFailureException {

        final TableAPI tableAPI = kvstore.getTableAPI();

        Table table;
        int attempt = 0;
        /* Check for the table first */
        while (true) {
            attempt++;
            table = tableAPI.getTable(ckptTableName);
            if (table != null) {
                logger.fine("Ensure table " + ckptTableName +
                            " exists after " + attempt + " attempts");
                break;
            }

            if (attempt < MAX_NUM_ATTEMPTS) {
                /* Table might not be ready, wait and retry */
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException e) {
                    final String err = "Interrupted when waiting for metadata" +
                                       " of " + ckptTableName +
                                       " to be available in table api";
                    throw new SubscriptionFailureException(sid, err);
                }
            } else {
                /* no checkpoint table exists for the given subscriber */
                final String err = "After " + attempt + " attempts " +
                                   " table " + ckptTableName +
                                   " still not found at store, terminate " +
                                   "subscription" +
                                   kvstore.getTopology().getKVStoreName();
                logger.warning(lm(err));
                throw new SubscriptionFailureException(sid, err);
            }
        }

        StreamPosition position;
        attempt = 0;
        while (true) {
            try {
                attempt++;
                position = getPosFromCkptTable(kvstore, shards, table);
                logger.fine(lm("After " + attempt + " attempts, subscription " +
                               sid + " read checkpoint position " +
                               position + " from table " + ckptTableName));
                break;
            } catch (StoreIteratorException sie) {
                if (attempt < MAX_NUM_ATTEMPTS) {
                    /* Table might not be ready, wait and retry */
                    try {
                        Thread.sleep(RETRY_INTERVAL_MS);
                    } catch (InterruptedException e) {
                        final String err = "Interrupted when waiting for " +
                                           ckptTableName + " to be ready";
                        throw new SubscriptionFailureException(sid, err);
                    }
                } else {
                    final String err = "After " + attempt + " attempts, " +
                                       "subscription " + sid +
                                       "still cannot read checkpoint table " +
                                       ckptTableName + " from store " +
                                       kvstore.getTopology().getKVStoreName() +
                                       ", terminate subscription.";
                    logger.warning(lm(err));
                    throw new SubscriptionFailureException(sid, err, sie);
                }
            } catch (UnauthorizedException ue) {
                final String err = "Subscription " + sid +
                                   " is unauthorized to read from checkpoint " +
                                   "table " + ckptTableName;
                logger.warning(lm(err));
                throw new SubscriptionFailureException(sid, err, ue);
            }
        }

        /* check if we miss any shards */
        for (RepGroupId gid : shards) {
            if (position.getShardPosition(gid.getGroupId()) == null) {
                /* missing shard from checkpoint table, fix it */
                position.setShardPosition(gid.getGroupId(), VLSN.NULL_VLSN);
            }
        }

        return position;
    }

    /**
     * Creates checkpoint table
     */
    void createCkptTable(boolean multiSubs) {
        createCkptTable(kvstore, ckptTableName, multiSubs);
        logger.info(lm("Checkpoint table " + ckptTableName + " created."));
    }

    /**
     * Checks if checkpoint table exists
     *
     * @return true if checkpoint exists for the given subscriber
     */
    boolean isCkptTableExists() {
        return isTableExists(kvstore.getTableAPI(), ckptTableName);
    }

    /**
     * Updates the checkpoint in kvstore with given stream position. If the
     * checkpoint cannot be updated or dropped, CFE is raised to caller
     *
     * @param pos  stream position to checkpoint
     *
     * @throws CheckpointFailureException if fail to update the checkpoint
     * table in single transaction
     */
    void updateCkptTableInTxn(StreamPosition pos)
        throws CheckpointFailureException {

        try {

            final TableAPI tableAPI = kvstore.getTableAPI();
            /*
             * since start from a position, need ensure that ALL rows
             * are successfully written into the update table. If not,
             * exception is raised to caller.
             */
            final Set<Row> ckptRows = createCkptRows(pos);

            /* write rows in transaction */
            final List<TableOperation> ops = new ArrayList<>();
            final TableOperationFactory f = tableAPI.getTableOperationFactory();
            for (Row row : ckptRows) {
                ops.add(f.createPut(row, null, true));
            }
            tableAPI.execute(ops, CKPT_WRITE_OPT);

            logger.info(lm("Checkpoint table updated successfully to " +
                           "position " + pos));

        } catch (UnauthorizedException ue) {
            final String err = "Subscriber " + sid + " is not authorized to " +
                               "write table " + ckptTableName;
            throw new CheckpointFailureException(sid, ckptTableName, err, ue);
        } catch (FaultException fe) {
            /*
             * With FaultException, there is no guarantee whether operation
             * completed successfully
             */
            throw new CheckpointFailureException(sid, ckptTableName,
                                                 "Unable to ensure the " +
                                                 "checkpoint table is updated" +
                                                 " successfully.", fe);
        } catch (TableOpExecutionException toee) {
            /* a sure failure */
            throw new CheckpointFailureException(sid, ckptTableName,
                                                 "Fail to persist the " +
                                                 "checkpoint table.", toee);
        }
    }

    String getCkptTableName() {
        return ckptTableName;
    }

    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private String lm(String msg) {
        return "[CkptMan-" + ckptTableName + "-" + sid + "] " + msg;
    }

    /*
     * Constructs checkpoint rows from stream position
     */
    private Set<Row> createCkptRows(StreamPosition streamPos) {

        final Set<Row> rows = new HashSet<>();

        final TableAPI tableAPI = kvstore.getTableAPI();
        final Table table = tableAPI.getTable(ckptTableName);
        final Collection<StreamPosition.ShardPosition> allPos =
            streamPos.getAllShardPos();
        for (StreamPosition.ShardPosition pos : allPos) {
            final int shardId = pos.getRepGroupId();
            final long vlsn = pos.getVLSN().getSequence();
            final Row row = table.createRow();
            row.put(CKPT_TABLE_SUBSCRIPTION_FIELD_NAME, ckptTableName);
            row.put(CKPT_TABLE_SHARD_FIELD_NAME, shardId);
            row.put(CKPT_TABLE_VLSN_FIELD_NAME, vlsn);
            row.put(CKPT_TABLE_TS_FIELD_NAME, System.currentTimeMillis());
            rows.add(row);
        }

        return rows;
    }

    private static StreamPosition getPosFromCkptTable(KVStoreImpl kvs,
                                                      Set<RepGroupId> shards,
                                                      Table table)
        throws UnauthorizedException, StoreIteratorException {

        final String storeName = kvs.getTopology().getKVStoreName();
        final long storeId = kvs.getTopology().getId();
        final StreamPosition position = new StreamPosition(storeName, storeId);
        final PrimaryKey pkey = table.createPrimaryKey();
        final TableIterator<Row> iter = kvs.getTableAPI().tableIterator(pkey,
                                                                        null,
                                                                        null);
        while (iter.hasNext()) {
            final Row row = iter.next();
            final int shardId = row.get(CKPT_TABLE_SHARD_FIELD_NAME)
                                   .asInteger()
                                   .get();
            final RepGroupId gid = new RepGroupId(shardId);
            if (!shards.contains(gid)) {
                /* filter out shards I do not cover */
                continue;
            }
            final long vlsn = row.get(CKPT_TABLE_VLSN_FIELD_NAME)
                                 .asLong()
                                 .get();
            position.setShardPosition(shardId, new VLSN(vlsn));
        }

        return position;
    }

    private static String getCreateCkptTableDDL(String tableName,
                                                boolean multiSubs) {

        final String ifNotExists = multiSubs ? " IF NOT EXISTS " : " ";
        return
            "CREATE TABLE" + ifNotExists + tableName +
            " (" +
            CKPT_TABLE_SUBSCRIPTION_FIELD_NAME + " STRING, " +
            CKPT_TABLE_SHARD_FIELD_NAME + " INTEGER, " +
            CKPT_TABLE_VLSN_FIELD_NAME + " LONG, " +
            CKPT_TABLE_TS_FIELD_NAME + " LONG, " +
            "PRIMARY KEY " +
            "(" +
            "SHARD(" + CKPT_TABLE_SUBSCRIPTION_FIELD_NAME + "), " +
            CKPT_TABLE_SHARD_FIELD_NAME + ")" +
            ")";
    }

    private static boolean isTableExists(TableAPI tableAPI,
                                         String tableName) {
        /* Check for the table first */
        final Table table = tableAPI.getTable(tableName);
        return table != null;
    }

    /*
     * Creates checkpoint table. Throw an exception if the table exists.
     *
     * @param kvs             handle to kvstore
     * @param ckptTableName   name of checkpoint table
     * @param multiSubs       true if multiple subscribers
     *
     * @throws IllegalArgumentException checkpoint table already exists at
     * store or fail to create the checkpoint
     *
     * @throws UnauthorizedException user is not eligible to create the
     * checkpoint table at kvstore
     */
    private static void createCkptTable(KVStore kvs,
                                        String ckptTableName,
                                        boolean multiSubs)
        throws IllegalArgumentException, UnauthorizedException {

        final Table t = kvs.getTableAPI().getTable(ckptTableName);

        if (t != null) {
            /*
             * If single subscriber, we do not expect a checkpoint table, but
             * if multi-subscribers, other subscriber may just create it so it
             * is Ok.
             */
            if (multiSubs) {
                return;
            }
            /* single subscriber */
            throw new IllegalArgumentException("Existing checkpoint table " +
                                               ckptTableName);
        }

        /*
         * Exception UnauthorizedException will be thrown if not eligible to
         * create the checkpoint table
         */
        final StatementResult result =
            kvs.executeSync(getCreateCkptTableDDL(ckptTableName, multiSubs));
        if (!result.isSuccessful()) {
            throw new IllegalArgumentException(result.getErrorMessage());
        }
    }
}

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

package pubsub;


import java.util.Arrays;
import java.util.UUID;
import java.util.logging.Logger;

import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLPublisherConfig;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.stats.SubscriptionMetrics;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/**
 * This is to demonstrate the NoSQL Stream publish/subscriber feature in
 * Oracle NoSQL Database. In particular, it demonstrate the feature by
 * allowing user to doSubscribe a table "User" from source kvstore.
 *
 * The NoSQL Stream feature provides APIs which can be used to define a
 * Subscription to receive all logical changes including both table row puts
 * and deletes made to a NoSQL store, on the order that these changes are
 * applied to single primary key in NoSQL store.
 *
 * <p>
 * Example invocations:
 *
 * <p>
 * To create a table at source kv store
 *
 * <code>
 * java -cp ... pubsub.NoSQLStreamExample create-table \
 *              -store <store> -host <host> -port <port>
 * </code>
 *
 * <p>
 * To load a table at source kv store
 *
 * <code>
 * java -cp ... pubsub.NoSQLStreamExample load-table \
 *              -store <store> -host <host> -port <port> \
 *              -num <number of rows>
 * </code>
 *
 * <p>
 * To start the subscription from table "User"
 *
 * <code>
 * java -cp ... pubsub.NoSQLStreamExample subscribe \
 *              -store <store> -host <host> -port <port> -num <number of rows>
 * </code>
 *
 * <p>
 * To clean up
 *
 * <code>
 * java -cp ... pubsub.NoSQLStreamExample cleanup \
 *              -store <store> -host <host> -port <port>
 * </code>
 *
 * <p>
 * Work flow of subscribing a table, which is ReactiveStream compatible.
 *
 * step 1: create a publisher configuration with store name and helper
 * hosts;
 *
 * step 2: create a publisher with publisher configuration in 1);
 *
 * step 3: create a subscription configuration with subscribed tables,
 * initial stream position, and other required info;
 *
 * step 4: create a subscriber with the configuration in 3);
 *
 * step 5: doSubscribe with publisher in step 2) and start streaming.
 */
public class NoSQLStreamExample {

    /* # of rows loaded into noise table which is not subscribed */
    private static final int NUM_ROWS_IN_NOISE_TABLE = 1000;
    /* shard timeout in ms */
    private static final int PUBLISHER_SHARD_TIMEOUT_MS = 60000;
    /* max concurrent subscriptions in publisher  */
    private static final int PUBLISHER_MAX_SUBSCRIPTIONS = 1;
    /* max subscription allowed time before forced termination */
    private static final long MAX_SUBSCRIPTION_TIME_MS = Long.MAX_VALUE;
    /* name of checkpoint table used in example */
    private static final String CKPT_TABLE_NAME = "StreamExampleCkptTable";
    /* private logger */
    private final Logger logger = Logger.getLogger(this.getClass().getName());

    /* table to subscribe */
    private static final String TABLE_NAME = "User";
    private static final String CREATE_USER_TABLE_DDL =
        "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " " +
        "(firstName STRING, " +
        " lastName STRING, " +
        " state STRING, " +
        " userID INTEGER, " +
        " PRIMARY KEY (userID))";

    /* noise table to test filtering */
    private static final String NOISE_TABLE_NAME = "Product";
    private static final String CREATE_NOISE_TABLE_DDL =
        "CREATE TABLE IF NOT EXISTS " + NOISE_TABLE_NAME + " " +
        "(desc STRING, " +
        " pid INTEGER, " +
        " PRIMARY KEY (pid))";
    private static final String[] STATES = {
        "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD",
        "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH",
        "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"};

    private final String[] argv;
    private final int nArgs;

    private String storeName = "";
    private String host = "";
    private String port = "";
    private int num = 0;
    /* no checkpoint by default */
    private int ckptIntv = 0;

    /* stream mode */
    private NoSQLStreamMode streamMode = NoSQLStreamMode.FROM_CHECKPOINT;

    public static void main(final String args[]) throws Exception {

        final NoSQLStreamExample runTest = new NoSQLStreamExample(args);
        runTest.run();
    }

    private NoSQLStreamExample(final String[] argv) {
        this.argv = argv;
        nArgs = argv.length;
        if (nArgs == 0) {
            usage("missing arguments");
        }
        num = 0;
    }

    private void run() throws Exception {
        final String thisArg = argv[0];

        parseArgs();
        if (storeName.isEmpty() || host.isEmpty() || port.isEmpty()) {
            usage("missing argument, -store -host -port cannot be empty");
        }

        if (thisArg.equals("create-table")) {
            doCreate();
        }

        if (thisArg.equals("load-table")) {
            if (num == 0) {
                usage("missing argument -num is not specified");
            }
            doLoad();
        }

        if (thisArg.equals("subscribe")) {
            doSubscribe();
        }

        if (thisArg.equals("cleanup")) {
            doCleanup();
        }
    }

    private static void usage(final String message) {
        if (message != null) {
            System.err.println("\n" + message + "\n");
        }

        System.err.println("usage: + NoSQLStreamExample");
        System.err.println
            ("\t[create-table | load-table | subscribe | cleanup]\n" +
             "\t-store <instance name>\n" +
             "\t-table <table name>\n" +
             "\t-host <host name>\n" +
             "\t-port <port number>\n" +
             "\t-num <number of rows>\n" +
             "\t-checkpoint <checkpoint interval in number of rows> \n" +
             "\t-from [now | checkpoint | exact_checkpoint] \n");

        System.exit(1);
    }

    private void parseArgs() {

        int argc = 1;
        while (argc < nArgs) {
            final String thisArg = argv[argc++];
            switch (thisArg) {
                case "-store":
                    if (argc < nArgs) {
                        storeName = argv[argc++];
                    } else {
                        usage("-store requires an argument");
                    }
                    break;
                case "-host":
                    if (argc < nArgs) {
                        host = argv[argc++];
                    } else {
                        usage("-host requires an argument");
                    }
                    break;
                case "-port":
                    if (argc < nArgs) {
                        port = argv[argc++];
                    } else {
                        usage("-port requires an argument");
                    }
                    break;
                case "-num":
                    if (argc < nArgs) {
                        num = Integer.valueOf(argv[argc++]);
                    } else {
                        usage("-num requires an argument");
                    }
                    break;
                case "-checkpoint":
                    if (argc < nArgs) {
                        ckptIntv = Integer.valueOf(argv[argc++]);
                    } else {
                        usage("-checkpoint requires an argument");
                    }
                    break;
                case "-from":
                    if (argc < nArgs) {
                        final String mode = argv[argc++];
                        switch (mode) {
                            case "now":
                                streamMode = NoSQLStreamMode.FROM_NOW;
                                break;
                            case "checkpoint":
                                streamMode = NoSQLStreamMode.FROM_CHECKPOINT;
                                break;
                            case "exact_checkpoint":
                                streamMode =
                                    NoSQLStreamMode.FROM_EXACT_CHECKPOINT;
                                break;
                            default:
                                usage("-from requires a valid argument from" +
                                      "[now | checkpoint | exact_checkpoint]");
                                break;
                        }
                    } else {
                        usage("-from requires an argument");
                    }
                    break;
                default:
                    usage("Unknown argument: " + thisArg);
            }
        }
    }

    private void doCreate() throws Exception {
        final String hostPort = host + ":" + port;
        try (
            final KVStore store =
                KVStoreFactory.getStore(new KVStoreConfig(storeName,
                                                          hostPort))) {

            createTableHelper(store,
                              TABLE_NAME,
                              CREATE_USER_TABLE_DDL);
            createTableHelper(store, NOISE_TABLE_NAME,
                              CREATE_NOISE_TABLE_DDL);

        } catch (FaultException fe) {
            String msg = "Error in creating tables in " +
                         " Oracle NoSQL store [" + storeName + "] at " +
                         host + ":" + port;
            logger.severe(msg);
            throw fe;
        }
    }

    private void doLoad() throws Exception {

        final String hostPort = host + ":" + port;

        try (
            final KVStore store =
                KVStoreFactory.getStore(new KVStoreConfig(storeName,
                                                          hostPort))) {

            final TableAPI tableAPI = store.getTableAPI();
            final Table tbl = tableAPI.getTable(TABLE_NAME);
            if (tbl == null) {
                logger.warning("Table " + TABLE_NAME + " does not exist, " +
                               "please run example with create-table first.");
                return;
            }

            final int batch = 10000;
            long start = System.currentTimeMillis();
            final long beginning = start;
            logger.info("Start loading table " + TABLE_NAME + " with " + num +
                        " rows");
            for (int i = 0; i < num; i++) {
                /* Insert row */
                final Row row = tbl.createRow();
                row.put("userID", i);
                row.put("firstName", "firstName-" + UUID.randomUUID());
                row.put("lastName", "lastName-" + UUID.randomUUID());
                row.put("state", STATES[i % STATES.length]);
                tableAPI.put(row, null, null);

                if (i > 0 && (i % batch) == 0) {
                    logger.info(batch + " rows loaded, elapsed timed in ms: " +
                                (System.currentTimeMillis() - start) +
                                ", total number of rows loaded: " + i);
                    start = System.currentTimeMillis();
                }
            }
            logger.info("Table " + TABLE_NAME + " loaded with rows:" + num +
                        ", total elapsed time in seconds: " +
                        ((System.currentTimeMillis() - beginning) / 1000));

            start = System.currentTimeMillis();
            final Table noiseTable = tableAPI.getTable(NOISE_TABLE_NAME);
            if (noiseTable == null) {
                logger.warning("Table " + NOISE_TABLE_NAME +
                               " does not exist,  please run example with " +
                               "create-table first.");
                return;
            }
            logger.info("Start loading table " + NOISE_TABLE_NAME +
                        " with " + NUM_ROWS_IN_NOISE_TABLE + " rows");
            for (int i = 0; i < NUM_ROWS_IN_NOISE_TABLE; i++) {
                /* Insert row */
                final Row row = noiseTable.createRow();
                row.put("pid", i);
                row.put("desc", "product description-" + UUID.randomUUID());
                tableAPI.put(row, null, null);
            }
            logger.info("Table " + NOISE_TABLE_NAME + " loaded with rows:" +
                        NUM_ROWS_IN_NOISE_TABLE + ", elapsed time " +
                        ((System.currentTimeMillis() - start) / 1000) +
                        " secs.");
        } catch (FaultException | IllegalStateException exp) {
            String msg = "Error in loading tables in " +
                         " Oracle NoSQL store [" + storeName + "] at " +
                         host + ":" + port;
            logger.severe(msg);
            throw exp;
        }
    }

    /* Subscribes a table. The work flow is ReactiveStream compatible */
    private void doSubscribe() throws Exception {

        final String hostPort = host + ":" + port;
        final String rootPath = ".";
        NoSQLPublisher publisher = null;
        try {
            /* step 1 : create a publisher configuration */
            final NoSQLPublisherConfig publisherConfig =
                new NoSQLPublisherConfig.Builder(
                    new KVStoreConfig(storeName, hostPort), rootPath)
                    .setMaxConcurrentSubs(PUBLISHER_MAX_SUBSCRIPTIONS)
                    .setShardTimeoutMs(PUBLISHER_SHARD_TIMEOUT_MS)
                    .build();

            String trace =
                "\n=========================================" +
                "\n=      PUBLISHER CONFIGURATION          =" +
                "\n=========================================" +
                "\nstore name: " + publisherConfig.getStoreName() +
                "\nhelper host:" +
                Arrays.toString(publisherConfig.getHelperHosts()) +
                "\ndirectory: " + publisherConfig.getRootPath() +
                "\nshard timeout in ms: " +
                publisherConfig.getShardTimeoutMs() +
                "\nmax concurrent subscriptions: " +
                publisherConfig.getMaxConcurrentSubs();
            logger.fine(trace);

            /* step 2 : create a publisher */
            publisher = NoSQLPublisher.get(publisherConfig, logger);
            trace =
                "NoSQLPublisher created for store " +
                publisher.getStoreName() +
                " (id: " + publisher.getStoreId() + ") at host " +
                Arrays.toString(publisherConfig.getHelperHosts());
            logger.info(trace);

            /* step 3: create a subscription configuration */
            final NoSQLSubscriptionConfig subscriptionConfig =
                /* stream with specified mode */
                new NoSQLSubscriptionConfig.Builder(CKPT_TABLE_NAME)
                .setSubscribedTables(TABLE_NAME)
                .setStreamMode(streamMode)
                .build();
            trace =
                "\n========================================" +
                "\n=    SUBSCRIPTION CONFIGURATION        =" +
                "\n========================================" +
                "\nsubscribed table: " + TABLE_NAME +
                "\ninitial position: " +
                (subscriptionConfig.getInitialPosition() == null ?
                    "beginning " : subscriptionConfig.getInitialPosition()) +
                "\ncheckpoint table " + CKPT_TABLE_NAME +
                "\nstream mode " + subscriptionConfig.getStreamMode();
            logger.fine(trace);

            /* step 4: create a subscriber */
            final NoSQLStreamSubscriberExample subscriber =
                new NoSQLStreamSubscriberExample(subscriptionConfig, num,
                                                 ckptIntv);
            trace = "Subscriber created to stream " + num + " operations.";
            logger.info(trace);

            /* step 5: doSubscribe with publisher and start stream */
            final long start = System.currentTimeMillis();
            publisher.subscribe(subscriber);
            if (!subscriber.isSubscriptionSucc()) {
                logger.severe("Subscription failed for " +
                              subscriber.getSubscriptionConfig()
                                        .getSubscriberId() +
                              ", reason " + subscriber.getCauseOfFailure());

                throw new RuntimeException("fail to subscribe");
            }
            trace = "Start stream " + num + " operations from table " +
                    TABLE_NAME;
            logger.info(trace);

            subscriber.getSubscription().request(num);

            /*
             * wait for stream done, throw exception if it cannot finish
             * within max allowed elapsed time
             */
            final long s = System.currentTimeMillis();
            while (subscriber.getStreamOps() < num) {
                final long elapsed = System.currentTimeMillis() - s;
                if (elapsed >= MAX_SUBSCRIPTION_TIME_MS) {
                    throw new RuntimeException("Not done within max allowed " +
                                               "elapsed time");
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted!");
                }
            }

            /* step 6: clean up */
            subscriber.getSubscription().cancel();
            publisher.close(true);
            trace = "Publisher closed normally.";
            logger.info(trace);

            /* finally dump summary and stats */
            final long total = subscriber.getStreamOps();
            final long elapsedMs =  (System.currentTimeMillis() - start);
            final long opsPerSec = total * 1000 / elapsedMs;
            trace =
                "\n====================" +
                "\n=    SUMMARY       =" +
                "\n====================" +
                "\ntotal # streamed ops: " + total +
                "\ntotal # processed rows: " + subscriber.getProcessOps() +
                "\nelapsed time in ms: " + elapsedMs +
                "\nthroughput ops/sec: " + opsPerSec;
            logger.info(trace);

            final SubscriptionMetrics metrics =
                subscriber.getSubscription().getSubscriptionMetrics();
            logger.fine(dumpStat(metrics));
        } catch (Exception exp) {
            String msg = "Error: " + exp.getMessage();
            logger.warning(msg);
            if (publisher != null) {
                publisher.close(exp, false);
                logger.warning("Publisher closed with error.");
            }
            throw exp;
        } finally {
           logger.fine("Test done.");
        }
    }

    private void doCleanup() {
        final String hostPort = host + ":" + port;
        try (
            final KVStore store =
                KVStoreFactory.getStore(new KVStoreConfig(storeName,
                                                          hostPort))) {


            /* drop small table first */
            dropTableHelper(store, CKPT_TABLE_NAME);
            dropTableHelper(store, NOISE_TABLE_NAME);

            /* drop the main user table, may take a while */
            dropTableHelper(store, TABLE_NAME);
        } catch (FaultException fe) {
            String msg = "Error in dropping tables in " +
                         " Oracle NoSQL store [" + storeName + "] at " +
                         host + ":" + port;
            logger.warning(msg);
            throw fe;
        }
    }

    private void createTableHelper(KVStore store, String table, String ddl) {

        final TableAPI tableAPI = store.getTableAPI();
        final Table tbl = tableAPI.getTable(table);
        if (tbl != null) {
            logger.warning("Table " + table + " already exist");
            return;
        }

        try {
            final ExecutionFuture result = store.execute(ddl);
            result.get();
            logger.info("Table " + table + " created successfully");
        } catch (Exception exp) {
            logger.warning("Fail to create table " + table +
                           ", reason: " + exp.getMessage());
        }
    }

    private void dropTableHelper(KVStore store, String table) {

        final TableAPI tableAPI = store.getTableAPI();
        final Table tbl = tableAPI.getTable(table);
        if (tbl == null) {
            logger.warning("Table " + table + " does not exist");
            return;
        }

        try {
            final long start = System.currentTimeMillis();
            final String ddl = "DROP TABLE " + table;
            final ExecutionFuture result = store.execute(ddl);
            result.get();
            logger.info("Table " + table + " dropped, elapsed time in ms: " +
                        (System.currentTimeMillis() - start));
        } catch (Exception exp) {
            logger.warning("Fail to drop table " + table +
                           ", reason: " + exp.getMessage());
        }
    }

    private String dumpStat(SubscriptionMetrics stat) {
        return
            "\n=========================================" +
            "\n=      SUBSCRIPTION STATISTICS          =" +
            "\n=========================================" +
            "\ntotal consumed ops: " + stat.getTotalConsumedOps() +
            "\ncommitted ops: " + stat.getTotalCommitOps() +
            "\naborted ops: " + stat.getTotalAbortOps() +
            "\ncommitted txns: " + stat.getTotalCommitTxns() +
            "\naborted txns: " + stat.getTotalAbortTxns() +
            "\nreconnections: " + stat.getTotalReconnect() +
            "\nrequested start VLSN: " + stat.getReqStartVLSN() +
            "\nacknowledged start VLSN: " + stat.getAckStartVLSN() +
            "\nlast committed txn VLSN: " + stat.getLastCommitVLSN() +
            "\nlast aborted txn VLSN: " + stat.getLastAbortVLSN() +
            "\n=========================================";
    }
}

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

package parallelscan;

import java.util.Arrays;
import java.util.List;

import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.Value;
import oracle.kv.stats.DetailedMetrics;

/**
 * This is a simple example that demonstrates the Parallel Scan feature of
 * KVStore.storeIterator(). It can load "user" records into a KVStore and then
 * retrieve them using Parallel Scan.
 * <p>
 * Record keys are formatted like this:
 * <code>
 * /user/userNNN
 * </code>
 * and all Values are a fixed zero-filled 1024 byte array.
 * <p>
 * There are command line options to load, scan with a key range, and
 * scan with a client-side user search string (mimicking a SQL "WHERE" clause).
 * <p>
 * Use the -storeIteratorThreads option to specify the number of Parallel Scan
 * threads to use. The default value for this option is 1, which indicates
 * non-Parallel Scan.
 * <p>
 * At the end of each retrieval operation, the number of matching records and
 * the per-shard DetailedMetrics (number of records and scan time for the
 * shard) are shown.
 * <p>
 * Example invocations:
 * <p>
 * To load 50000 records:
 *
 * <code>
 * java -cp ... parallelscan.ParallelScanExample \
 *              -store <store> -host <host> -port <port> \
 *              -load 50000
 *
 * </code>
 * <p>
 * <code>
 * <p>
 * To specify a key range to scan only those users whose user id starts with
 * "1":
 *
 * <code>
 * java -cp ... parallelscan.ParallelScanExample \
 *              -store <store> -host <host> -port <port> \
 *              -startUser 1 -endUser 1
 * </code>
 * <p>
 * To add a client-side filter (similar to a SQL WHERE clause in a full table
 * scan) to find all records with keys containing "99" in the user id:
 *
 * <code>
 * java -cp ... parallelscan.ParallelScanExample \
 *              -store <store> -host <host> -port <port> \
 *              -where 99
 * </code>
 * <p>
 * <code>
 *
 * <p>
 * If the security for the store is enabled, use the oracle.kv.security
 * property to specify a user login file. For example,
 * <pre><code>
 *  &gt; java -Doracle.kv.security=$KVHOME/kvroot/security/user.security \
 *          hadoop.hive.table.LoadRmvTable
 * </code></pre>
 * You can also disable the security of the store. For kvlite, you can use the
 * following command to start a non-secure store.
 * <pre><code>
 *  &gt; java -jar KVHOME/lib/kvstore.jar kvlite -secure-config disable
 * </code></pre>
 * More details are available at <a href="https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/18.1&id=NSKVJ-GUID-C1E4D281-2285-498A-BC12-858A5DFCF4F7">
 * kvlite Utility Command Line Parameter Options</a> and
 * <a href="https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/18.1&id=NSSEC-GUID-6C5875FE-9765-45E6-9EC7-0D6EAEE3F95D">
 * Oracle NoSQL Database Security Guide</a>.
 */
public class ParallelScanExample {

    private final KVStore store;

    private int nStoreIteratorThreads = 1;
    private String where = null;
    private int startUser = -1;
    private int endUser = -1;
    private int nToLoad = -1;

    public static void main(final String args[]) {
        try {
            ParallelScanExample runTest =
                new ParallelScanExample(args);
            runTest.run();
        } catch (FaultException e) {
        	e.printStackTrace();
        	System.out.println("Please make sure a store is running.");
            System.out.println("The error could be caused by a security " +
                    "mismatch. If the store is configured secure, you " +
                    "should specify a user login file " +
                    "with system property oracle.kv.security. " +
                    "For example, \n" +
                    "\tjava -Doracle.kv.security=<user security login " +
                    "file> parallelscan.ParallelScanExample\n" +
                    "KVLite generates the security file in " +
                    "$KVHOME/kvroot/security/user.security ");
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    ParallelScanExample(final String[] argv) {

        String storeName = "";
        String hostName = "";
        String hostPort = "";

        final int nArgs = argv.length;
        int argc = 0;

        if (nArgs == 0) {
            usage(null);
        }

        while (argc < nArgs) {
            final String thisArg = argv[argc++];

            if (thisArg.equals("-store")) {
                if (argc < nArgs) {
                    storeName = argv[argc++];
                } else {
                    usage("-store requires an argument");
                }
            } else if (thisArg.equals("-host")) {
                if (argc < nArgs) {
                    hostName = argv[argc++];
                } else {
                    usage("-host requires an argument");
                }
            } else if (thisArg.equals("-port")) {
                if (argc < nArgs) {
                    hostPort = argv[argc++];
                } else {
                    usage("-port requires an argument");
                }
            } else if (thisArg.equals("-storeIteratorThreads")) {
                if (argc < nArgs) {
                    nStoreIteratorThreads = Integer.parseInt(argv[argc++]);
                } else {
                    usage("-storeIteratorThreads requires an argument");
                }
            } else if (thisArg.equals("-where")) {
                if (argc < nArgs) {
                    where = argv[argc++];
                } else {
                    usage("-where requires an argument");
                }
            } else if (thisArg.equals("-startUser")) {
                if (argc < nArgs) {
                    startUser = Integer.parseInt(argv[argc++]);
                } else {
                    usage("-startUser requires an argument");
                }
            } else if (thisArg.equals("-endUser")) {
                if (argc < nArgs) {
                    endUser = Integer.parseInt(argv[argc++]);
                } else {
                    usage("-endUser requires an argument");
                }
            } else if (thisArg.equals("-load")) {
                if (argc < nArgs) {
                    nToLoad = Integer.parseInt(argv[argc++]);
                } else {
                    usage("-load requires an argument");
                }
            } else {
                usage("Unknown argument: " + thisArg);
            }
        }

        store = KVStoreFactory.getStore
            (new KVStoreConfig(storeName, hostName + ":" + hostPort));
    }

    private void usage(final String message) {
        if (message != null) {
            System.err.println("\n" + message + "\n");
        }

        System.err.println("usage: " + getClass().getName());
        System.err.println
            ("\t-store <instance name>\n" +
             "\t-host <host name>\n" +
             "\t-port <port number>\n" +
	     "\t[-load <# records to load>]\n" +
             "\t[-storeIteratorThreads <number storeIterator threads>\n" +
             "\t\t(default: 1)]\n" +
             "\t[-where <string to search for in key>]\n" +
             "\t[-startUser <startUser#>]\n" +
             "\t[-endUser <endUser#>]\n");
        System.exit(1);
    }

    private void run() {
        try {
	    doRun();
        } finally {
            store.close();
        }
    }

    private void doRun() {
        try {
	    if (nToLoad < 0) {
		doStoreIteration();
	    } else {
		doLoad();
	    }
        } catch (Exception e) {
            System.err.println
                ("storeIteratorThread caught: " + e);
            e.printStackTrace();
        }
    }

    private void doLoad() {
        final Value dummyData = Value.createValue(new byte[1024]);
        for (int i = 0; i < nToLoad; i++) {
            final String uid = "user" + i;
            store.put(Key.createKey(Arrays.asList("user", uid)), dummyData);
        }
    }

    private void doStoreIteration() {
        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().
            setMaxConcurrentRequests(nStoreIteratorThreads);

        Key useParent = null;
        KeyRange useSubRange = null;
        if (startUser > 0 || endUser > 0) {
            useParent = Key.createKey("user");
            useSubRange =
                new KeyRange((startUser > 0 ? ("user" + startUser) : null),
                             true,
                             (endUser > 0 ? ("user" + endUser) : null),
                             true);
        }

        final long start = System.currentTimeMillis();
        final ParallelScanIterator<KeyValueVersion> iter =
            store.storeIterator(Direction.UNORDERED, 0 /* batchSize */,
                                useParent,
                                useSubRange,
                                null, /* depth */
                                null, /* consistency */
                                0 /* timeout */,
                                null,
                                storeIteratorConfig);

        /* Key format: "/user/userNNN/-/ */
        int cnt = 0;
        try {
            while (iter.hasNext()) {
                final KeyValueVersion kvv = iter.next();
                final List<String> majorKeys = kvv.getKey().getMajorPath();
                final String userId = majorKeys.get(1);
                if (where == null) {
                    cnt++;
                } else {
                    if (userId.indexOf(where) > 0) {
                        cnt++;
                    }
                }
            }
        } finally {
            iter.close();
        }

        final long end = System.currentTimeMillis();

        System.out.println(cnt + " records found in " +
                           (end - start) + " milliseconds.");

        final List<DetailedMetrics> shardMetrics = iter.getShardMetrics();
        for (DetailedMetrics dmi : shardMetrics) {
            System.out.println(dmi);
        }
    }
}

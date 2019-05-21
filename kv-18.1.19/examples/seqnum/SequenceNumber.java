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

package seqnum;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.Version;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/**
 * A simple implementation of a Sequence Number Generator. Numbers are
 * unique and monotonically incremented in the domain determined by key.
 *
 * This example creates a single table, called "sequenceNumbers" that can be
 * used for any number of named sequences. A sequence is implemented as a
 * single record in this table and its primary key is the name of the sequence.
 * A sequence's value is a single long value, atomically incremented as needed.
 *
 * The important aspects of this example are use of putIfVersion() for
 * atomicity as well as a retry loop to handle potential collisions.
 *
 * This is a simple implementation intended to demonstrate the concept. It may
 * not scale well in the face of a large number of concurrent accesses. Also
 * note this class is not thread safe.
 *
 * Use the KVStore instance name, host and port for running this program:
 *
 * <pre>
 * java seqnum.SequenceNumber -store &lt;instance name&gt; \
 *                            -host  &lt;host name&gt;     \
 *                            -port  &lt;port number&gt;
 *
 * </pre>
 *
 * For all examples the default instance name is kvstore, the default host name
 * is localhost and the default port number is 5000.  These defaults match the
 * defaults for running kvlite, so the simplest way to run the examples along
 * with kvlite is to omit all parameters.
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
 *
 *
 */
public class SequenceNumber {
    private static ReadOptions READ_OPTIONS =
        new ReadOptions(Consistency.ABSOLUTE, 3, TimeUnit.SECONDS);
    private static PrimaryKey PRIMARY_KEY;

    private final TableAPI tableAPI;
    private final Table table;
    private final String name;
    private final int noOfRetries;
    private Version lastKnownVersion;
    private long sequenceNumber;


    /**
     * Creates an instance of the sequence number generator.
     *
     * @param store The store name.
     * @param name The name of the sequence
     * @param noOfRetries The number of attempts to synchronize with the store
     * @param initialValue The starting value of the sequence (the first
     *                     value returned will be an increment of this one.
     */
    public SequenceNumber(KVStore store, String name, int noOfRetries,
        long initialValue) {

        StatementResult res = store.executeSync("CREATE TABLE IF NOT EXISTS " +
            "sequenceNumbers (name STRING, value LONG, PRIMARY KEY ( name ))");

        if ( !res.isSuccessful() ) {
            throw new RuntimeException("Failed to create table " +
                "sequenceNumbers: " + res.getErrorMessage());
        }

        tableAPI = store.getTableAPI();
        table = tableAPI.getTable("sequenceNumbers");
        this.name = name;
        this.noOfRetries = noOfRetries;

        PrimaryKey primaryKey = table.createPrimaryKey();
        primaryKey.put("name", name);
        PRIMARY_KEY = primaryKey;

        /* Save the initialValue in the store. */
        Row row = table.createRow();
        row.put("name", name);
        row.put("value", initialValue);
        lastKnownVersion = tableAPI.putIfAbsent(row, null, null);
        sequenceNumber = initialValue;
        if (lastKnownVersion == null) {
            /* If value already present get the last sequence number. */
            readLastSequenceNumber();
        }
    }

    /**
     * Reads the value of the sequence. This is stored as the maximum
     * value with a given name.
     */
    private void readLastSequenceNumber() {
        Row r = tableAPI.get(PRIMARY_KEY, READ_OPTIONS);

        sequenceNumber = r.get("value").asLong().get();
        lastKnownVersion = r.getVersion();
    }

    /**
     * Returns the next number in the sequence after it synchronizes with
     * the store, making a maximum of noOfRetries attempts.<br/>
     *
     * There are three possible outcomes when calling this method:<br/>
     *  - the next number in the sequence is returned.<br/>
     *  - the maximum number of retries is reached for trying to
     *  synchronize with the store and a RuntimeException is thrown<br/>
     *  - other exception if thrown by the store <br/>
     * @return the next number in the sequence
     * @throws java.lang.RuntimeException, oracle.kv.DurabilityException
     *      oracle.kv.RequestTimeoutException oracle.kv.FaultException
     *      oracle.kv.ConsistencyException
     */
    public long incrementAndGet() {
        for (int i = 0; i < noOfRetries; i++) {
            sequenceNumber++;

            /* Try to put the next sequence number with the
            lastKnownVersion. */
            Row row = table.createRow();
            row.put("name", name);
            row.put("value", sequenceNumber);

            assert lastKnownVersion != null : "lastKV == null";

            Version newVersion =
                tableAPI.putIfVersion(row, lastKnownVersion, null, null);
            if (newVersion == null) {
                /* Put was unsuccessful get the one in the store. */
                readLastSequenceNumber();
            } else {
                /* Put was successful. */
                lastKnownVersion = newVersion;
                return sequenceNumber;
            }
        }

        throw new RuntimeException("Reached maximum number of retries.");
    }

    /* Main entry point when running the example */
    public static void main(String[] args) {
        try {
            String storeName = "kvstore";
            String hostName = "localhost";
            String hostPort = "5000";

            final int nArgs = args.length;
            int argc = 0;

            while (argc < nArgs) {
                final String thisArg = args[argc++];

                if (thisArg.equals("-store")) {
                    if (argc < nArgs) {
                        storeName = args[argc++];
                    } else {
                        usage("-store requires an argument");
                    }
                } else if (thisArg.equals("-host")) {
                    if (argc < nArgs) {
                        hostName = args[argc++];
                    } else {
                        usage("-host requires an argument");
                    }
                } else if (thisArg.equals("-port")) {
                    if (argc < nArgs) {
                        hostPort = args[argc++];
                    } else {
                        usage("-port requires an argument");
                    }
                } else {
                    usage("Unknown argument: " + thisArg);
                }
            }

            /* Connect to the store. */
            KVStore store = KVStoreFactory.getStore
                (new KVStoreConfig(storeName, hostName + ":" + hostPort));

            /* Initialize the generator, use the key for the sequence namespace. */
            SequenceNumber sn =
                new SequenceNumber(store, "example", 5, 0);
            System.out.println("Create Sequence Number Generator - key based");

            /* Get a new number. */
            long firstSequenceNumber = sn.incrementAndGet();
            System.out.println(firstSequenceNumber);

            /* ... and another one... */
            long secondSequenceNumber = sn.incrementAndGet();
            System.out.println(secondSequenceNumber);

            /* ... and a few more. */
            for (int i = 0; i < 4; i++) {
                long l = sn.incrementAndGet();
                System.out.println(l);
            }
        } catch (FaultException e) {
        	e.printStackTrace();
        	System.out.println("Please make sure a store is running.");
            System.out.println("The error could be caused by a security " +
                    "mismatch. If the store is configured secure, you " +
                    "should specify a user login file " +
                    "with system property oracle.kv.security. " +
                    "For example, \n" +
                    "\tjava -Doracle.kv.security=<user security login " +
                    "file> seqnum.SequenceNumber\n" +
                    "KVLite generates the security file in " +
                    "$KVHOME/kvroot/security/user.security ");
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    private static void usage(String message) {
        System.out.println("\n" + message + "\n");
        System.out.println("usage: SequenceNumber");
        System.out.println("\t-store <instance name> (default: kvstore)\n" +
            "\t-host <host name> (default: localhost)\n" +
            "\t-port <port number> (default: 5000)\n");
        System.exit(1);
    }
}

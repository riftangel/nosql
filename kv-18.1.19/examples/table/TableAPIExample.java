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

package table;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

/**
 * Executes examples that demonstrate some of the features of tables and
 * secondary indexes in Oracle NoSQL Database.
 * <p>
 * See the <a href="package-summary.html">package documentation</a> for
 * instructions on how to build and run this example. It requires a running
 * Oracle NoSQL Database instance.  The usage is:
 *
 * <pre>
 *   java -cp .:&lt;path-to-kvclient.jar&gt; table.TableAPIExample
 *                            [-list]      // list available examples \
 *                            [-help | -?] // get usage message \
 *                            [-store &lt;instance name&gt;] \
 *                            [-host  &lt;host name&gt;]     \
 *                            [-port  &lt;port number&gt;]   \
 *                            example1 example2 ...
 *
 * </pre>
 *
 * If no specific example is specified, then runs all available examples.
 * <p>
 * The examples use either specified store, host, and port or, when unspecified,
 * the same defaults of <i>kvstore</i>, <i>localhost</i>, and <i>5000</i>as in
 * <code>KVLite</code> datastore configuration.
 * <p>
 * The available example names can be listed by:
 *
 * <pre>
 * java -cp .:&lt;path-to-kvclient.jar&gt; table.TableAPIExample -list
 * </pre>
 * <p>
 * <h3>Example Cases</h3>
 * <ul>
 * <li>SimpleReadWrite: Creates a very simple table and does simple put and get
 * of a row.
 * <li>IndexReadWrite: Uses simple and composite indexes to demonstrate index
 * scans.
 * <li>ShardKeys: Uses a table that has a composite primary key and a defined
 * shard key. This demonstrates the ability to ensure that rows with the same
 * shard key are stored in the same shard and are therefore accessible in an
 * atomic manner. Such rows can also be accessed using the various "multi*" API
 * operations.
 * <li>UseChildTable: Uses a parent/child table relationship to demonstrate how
 * to put and get to/from a child table. It also uses an index on the child
 * table to retrieve child rows via the index.
 * <li>ComplexFields: Uses a table with complex fields (Record, Array, Map) to
 * demonstrate input and output of rows in tables with such fields.
 * <li>ArrayIndex: Uses an index on the array from the complex field example
 * table to demonstrate use of an array index.
 * <li>TTL: Inserts and updates records with expiration time.
 * <li>Select: Shows the API for executing and accessing the results of select
 * queries.
 * </ul>
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
 */
public class TableAPIExample {
    private static KVStore store;
    private static Map<String, Example> examples;

    /**
     * statically enumerate available examples indexed by logical name.
     * Use a case-insensitive map.
     */
    static {
        examples =
            new TreeMap<String, Example>(String.CASE_INSENSITIVE_ORDER);

        addExample("SimpleReadWrite", new SimpleReadWriteExample());
        addExample("IndexReadWrite", new IndexReadAndWrite());
        addExample("ShardKeys", new ShardKeysExample());
        addExample("ChildTable", new ChildTableExample());
        addExample("ComplexField", new ComplexFieldExample());
        addExample("ArrayIndex", new ArrayIndexExample());
        addExample("MapIndex", new MapIndexExample());
        addExample("TimeToLive", new TTLExample());
        addExample("Select", new SelectExample());
        addExample("ParentChildJoin", new ParentChildJoin());
        addExample("AggregateFunctions", new AggregateFunction());
        addExample("GroupBy", new GroupByExample());
        checkMoreExamples();
    }

    /**
     * Register additional examples specified by setting the
     * more.table.examples system property.  The value of the system property
     * should includes one or more comma-separated entries each of which
     * specifies an example name and the fully qualified example class name,
     * separated by a colon.
     */
    private static void checkMoreExamples() {
        final String moreExamples = System.getProperty("more.table.examples");
        if (moreExamples == null) {
            return;
        }
        for (String entry : moreExamples.split(",")) {
            entry = entry.trim();
            if (entry.isEmpty()) {
                continue;
            }
            try {
                final int colon = entry.indexOf(':');
                final String name = entry.substring(0, colon);
                final String classname = entry.substring(colon + 1);
                addExample(name,
                           (Example) Class.forName(classname).newInstance());
            } catch (Exception e) {
                warn("Problem adding example: " + entry);
            }
        }
    }

    /**
     * Run the examples.
     */
    public static void main(String args[]) {
        Collection<String> exampleNames = new ArrayList<String>();
        try {
            exampleNames = parseAndInit(args);
        } catch (Throwable ex) {
            error(ex, 1);
        }

        Example example = null;
        for (String exampleName : exampleNames) {
            if ((example = resolveExample(exampleName)) != null) {
                try {
                    log("---- Running example [" + exampleName + "] ----------");
                    example.init(store);
                    example.call();
                } catch (Throwable t) {
                    error(t.getMessage());
                } finally {
                    try {
                        if (example != null) {
                            example.end();
                        }
                    } catch (Throwable t) {
                        error("Stopping example [" + exampleName + "]", t);
                    }
                }
            } else {
                if (exampleName.startsWith("-")) {
                    warn("Unrecognized option [" + exampleName + "]");
                } else {
                    warn("Unknown Example [" + exampleName + "]");
                }
                usage(null, 0);
            }
        }
    }

    /**
     * Determines store parameters and name of the examples. Initializes a store
     * against which all examples execute.
     *
     * @return name of examples.
     */
    private static Collection<String> parseAndInit(String argv[]) {
        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";

        /**
         * If no arguments are provided, initialize store with default
         * parameters and return all example names to execute.
         */
        if (argv.length == 0) {
            store = initStore(storeName, hostName, hostPort);
            list("Running all available examples", System.out, 0);
            return examples.keySet();
        }

        int i = 0;
        for (; i < argv.length; i++) {
            String arg = argv[i];
            if ("-store".equals(arg)) {
                if (i < argv.length - 1) {
                    storeName = argv[++i];
                } else {
                    usage("-store requires an argument", 1);
                }
            } else if ("-host".equals(arg)) {
                if (i < argv.length - 1) {
                    hostName = argv[++i];
                } else {
                    usage("-host requires an argument", 1);
                }
            } else if ("-port".equals(arg)) {
                if (i < argv.length - 1) {
                    hostPort = argv[++i];
                } else {
                    usage("-port requires an argument", 1);
                }
            } else if (arg.equals("-?") || arg.equals("-help")) {
                usage(null, 1);
            } else if ("-list".equals(arg)) {
                list("Available examples are:", System.out, 1);
            } else {
                if (arg.contains("-")) {
                    usage(("Invalid flag: " + arg), 1);
                }
                break;
            }
        }

        store = initStore(storeName, hostName, hostPort);

        if (i == argv.length) {
            list("***INFO: No example specified. Running all examples",
                 System.out,
                 0);
            return examples.keySet();
        }
        String[] exampleNames = new String[argv.length - i];
        System.arraycopy(argv, i, exampleNames, 0, exampleNames.length);

        return Arrays.asList(exampleNames);
    }

    /**
     * Initialize a store.
     * @param storeName
     * @param host
     * @param hostPort
     * @return the store
     */
    private static KVStore initStore(String storeName, String host,
            String hostPort) {
        try {
            return KVStoreFactory.getStore(new KVStoreConfig(storeName, host
                    + ":" + hostPort));
        } catch (FaultException ex) {
            if (ex.toString().indexOf("Could not contact any RepNode") != -1) {
                String msg = "Can not open connection to "
                    + " Oracle NoSQL store [" + storeName + "] at " + host
                    + ':' + hostPort
                    + ".\nPlease make sure a store is running."
                    + "\nThe error could be caused by a security "
                    + "mismatch. If the store is configured secure, you "
                    + "should specify a user login file "
                    + "with system property oracle.kv.security. "
                    + "For example, \n"
                    + "\tjava -Doracle.kv.security=<user security login "
                    + "file> table.TableAPIExample\n"
                    + "KVLite generates the security file in "
                    + "$KVHOME/kvroot/security/user.security ";
                usage(msg, 1);
            }
        }
        return null;
    }

    private static void usage(String message, int exitCode) {
        if (message != null) {
            System.err.println("\n" + message + "\n");
        }
        System.err.print("usage: " + TableAPIExample.class.getName());
        System.err.print(" [-help|-?]");
        System.err.println(" [-list]");
        System.err.print("\t[-store <store name>]");
        System.err.print(" [-host <host name>]");
        System.err.println(" [-port <port number>]");
        System.err.println("\t[example1 example2 ...]");

        System.err.println("\nwhere");
        System.err.println("\t-list lists available examples");
        System.err.println("\t-store <instance name>   name of the store "
                + "(default: kvstore)");
        System.err.println("\t-host <host name>        name of the host "
                + "(default: localhost)");
        System.err.println("\t-port <port number>      the store's port "
                + "(default: 5000)");

        list("\nAvailable examples are:", System.err, 0);
        System.err.println("If no example name is provided, then runs all "
                         + "examples.");

        if (exitCode > 0) {
            System.exit(1);
        }
    }

    private static void list(String message,
                             PrintStream output,
                             int exitCode) {
        output.println(message);
        for (Map.Entry<String, Example> s : examples.entrySet()) {
            output.println("\t" + s.getKey() + "\t-- "
                    + s.getValue().getShortDescription());
        }
        if (exitCode > 0) {
            System.exit(exitCode);
        }
    }

    /**
     * Adds given example with given name.
     *
     * @param name logical name of a example
     * @param example a example
     */
    protected static void addExample(String name, Example example) {
        examples.put(name, example);
    }

    /**
     * Returns a example for the given name (case-insensitive).
     *
     * @param exampleName logical name of a example
     * @return null if no example of given logical name exists.
     */
    private static Example resolveExample(String exampleName) {
        return examples.get(exampleName);
    }

    private static void log(String message) {
        System.out.println(message);
    }

    private static void warn(String message) {
        System.err.println("***WARN: " + message);
    }

    private static void error(String message) {
        error(message, null, 0);
    }

    private static void error(Throwable t, int exitCode) {
        error(t.getMessage(), null, exitCode);
    }

    private static void error(String message, Throwable t ) {
        error(message, t, 0);
    }

    private static void error(String message, Throwable t, int exitCode) {
        if (t != null) {
            t.printStackTrace();
        }
        System.err.println("***ERROR: " + message
                + (t != null ? ":" + t.getMessage() : ""));
        if (exitCode > 0) {
            System.exit(exitCode);
        }
    }
}

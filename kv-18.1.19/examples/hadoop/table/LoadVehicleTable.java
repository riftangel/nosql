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

package hadoop.table;

import java.security.SecureRandom;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;

import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

/**
 * Class that creates an example table in a given NoSQL Database store and
 * then uses the Table API to populate the table with sample records. The
 * table that is created consists of a variety of NoSQL Table data types;
 * but includes only primitive types.
 * <p>
 * To run this class, type a command like the following:
 * <pre><code>
 *  &gt; cd  &lt;KVEXAMPLES&gt;
 *  &gt; java -classpath lib/kvclient.jar \
 *                   [:lib/oraclepki.jar:lib/osdt_cert.jar:lib/oadt_core.jar] \
 *                    :examples hadoop.table.LoadVehicleTable \
 *         -store &lt;storename&gt; -host &lt;hostname&gt; -port &lt;port&gt; \
 *         -nops &lt;number-of-rows-to-generate&gt; \
 *        [-security &lt;login-properties-file&gt;] [-delete]
 * </code></pre>
 * where the additional JAR files (oraclepki.jar, osdt_cert.jar, and
 * oadt_core.jar) are required only if the password is stored in an Oracle
 * Wallet; which is available only in the enterprise edition of Oracle NoSQL
 * Database.
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
 * <p>
 * <p>
 * <br>
 * This class demonstrates the use of Oracle NoSQL Table DDL
 * (<em>Data Definition Langue</em>) to create a table named
 * <code>vehicleTable</code> with the following format:
 * <pre><code>
 * CREATE TABLE IF NOT EXISTS vehicleTable (type STRING, \
 *                                          make STRING, \
 *                                          model STRING, \
 *                                          class STRING, \
 *                                          color STRING, \
 *                                          price DOUBLE, \
 *                                          count INTEGER), \
 *                     PRIMARY KEY (SHARD(type, make, model), class))
 * </code></pre>
 */
public final class LoadVehicleTable {

    private static final SecureRandom secureRandom = new SecureRandom();

    private final KVStore store;
    private final TableAPI tableAPI;
    private final Table table;

    private long nOps = 10; /* The default number of rows, if not specified. */
    private long nRowsAdded;

    private boolean deleteExisting = false;

    private static final String TABLE_NAME = "vehicleTable";

    /*
     * Create the table with the following SHARD and PRIMARY keys:
     *   shardKey : [ "type", "make", "model" ]
     *   primaryKey : [ "type", "make", "model", "class" ]
     */
    private static final String STATEMENT =
        "CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
        " (" +
           "type STRING," +
           "make STRING," +
           "model STRING," +
           "class STRING," +
           "color STRING," +
           "price DOUBLE," +
           "count INTEGER," +
        "PRIMARY KEY (SHARD(type, make, model), class))";

    public static void main(final String[] args) {
        try {
            final LoadVehicleTable loadData = new LoadVehicleTable(args);
            loadData.run();
        } catch (FaultException e) {
        	e.printStackTrace();
        	System.out.println("Please make sure a store is running.");
            System.out.println("The error could be caused by a security " +
                    "mismatch. If the store is configured secure, you " +
                    "should specify a user login file " +
                    "with system property oracle.kv.security. " +
                    "For example, \n" +
                    "\tjava -Doracle.kv.security=<user security login " +
                    "file> hadoop.table.LoadVehicleTable\n" +
                    "KVLite generates the security file in " +
                    "$KVHOME/kvroot/security/user.security ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Parses command line args and opens the KVStore.
     */
    private LoadVehicleTable(final String[] argv) {

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

            if ("-store".equals(thisArg)) {
                if (argc < nArgs) {
                    storeName = argv[argc++];
                } else {
                    usage("-store requires an argument");
                }
            } else if ("-host".equals(thisArg)) {
                if (argc < nArgs) {
                    hostName = argv[argc++];
                } else {
                    usage("-host requires an argument");
                }
            } else if ("-port".equals(thisArg)) {
                if (argc < nArgs) {
                    hostPort = argv[argc++];
                } else {
                    usage("-port requires an argument");
                }
            } else if ("-nops".equals(thisArg)) {
                if (argc < nArgs) {
                    nOps = Long.parseLong(argv[argc++]);
                } else {
                    usage("-nops requires an argument");
                }
            } else if ("-security".equals(thisArg)) {
                if (argc < nArgs) {
                    System.setProperty(
                        KVSecurityConstants.SECURITY_FILE_PROPERTY,
                        argv[argc++]);
                } else {
                    usage("-security requires an argument");
                }
            } else if ("-delete".equals(thisArg)) {
                deleteExisting = true;
            } else {
                usage("Unknown argument: " + thisArg);
            }
        }
        nRowsAdded = nOps;

        store = KVStoreFactory.getStore
            (new KVStoreConfig(storeName, hostName + ":" + hostPort));

        tableAPI = store.getTableAPI();
        createTable();
        table = tableAPI.getTable(TABLE_NAME);
        if (table == null) {
            final String msg =
                "Store does not contain table [name=" + TABLE_NAME + "]";
            throw new RuntimeException(msg);
        }
    }

    private void usage(final String message) {
        if (message != null) {
            System.out.println("\n" + message + "\n");
        }

        System.out.println("usage: " + getClass().getName());
        System.out.println
            ("\t-store <instance name>\n" +
             "\t-host <host name>\n" +
             "\t-port <port number>\n" +
             "\t-nops <total records to create>\n" +
             "\t[-security <login properties file>]\n" +
             "\t-delete (default: false) [delete all existing data]\n");
        System.exit(1);
    }

    private void run() {
        if (deleteExisting) {
            deleteExistingData();
        }

        doLoad();
    }

    private void createTable() {
        try {
            final StatementResult result = store.executeSync(STATEMENT);
            if (result.isSuccessful()) {
                System.out.println("table created [" + TABLE_NAME + "]");
            } else if (result.isCancelled()) {
                System.out.println("table creation CANCELLED [" +
                                   TABLE_NAME + "]");
            } else {
                if (result.isDone()) {
                    System.out.println("table creation FAILED:\n\t" +
                                       STATEMENT);
                    System.out.println("ERROR:\n\t" +
                                       result.getErrorMessage());
                } else {
                    System.out.println("table creation IN PROGRESS:\n\t" +
                                       STATEMENT);
                    System.out.println("STATUS:\n\t" + result.getInfo());
                }
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:");
            e.printStackTrace();
        } catch (FaultException e) {
            System.out.println("Failure on statement execution:");
            e.printStackTrace();
        }
    }

    private void doLoad() {
        for (long i = 0; i < nOps; i++) {
            addRow();
        }
        displayRow(table);
        final long nRowsTotal = nRowsInTable();
        if (nOps > nRowsAdded) {
            System.out.println(
                nOps + " records requested, " +
                nRowsAdded + " unique records actually added " +
                "[" + (nOps - nRowsAdded) + " duplicates], " +
                nRowsTotal + " records total in table");
        } else {
            System.out.println(
                nOps + " records requested, " +
                nRowsAdded + " unique records added, " +
                nRowsTotal + " records total in table");
        }
        store.close();
    }

    private void addRow() {

        final int typeIndx = secureRandom.nextInt(TYPES.length);
        final int makeIndx = secureRandom.nextInt(MAKES.length);
        final int classIndx = secureRandom.nextInt(CLASSES.length);
        final int colorIndx = secureRandom.nextInt(COLORS.length);

        final String type  = TYPES[typeIndx];
        final String make  = MAKES[makeIndx];
        String vClass = CLASSES[classIndx];
        String color = COLORS[colorIndx];

        String[] models = MODELS_FORD_AUTO;
        float priceMult = 2.0f;

        if ("suv".equals(type)) {
            priceMult = 4.0f;
            if ("Chrysler".equals(make)) {
                models = MODELS_CHRYSLER_SUV;
            } else if ("GM".equals(make)) {
                models = MODELS_GM_SUV;
            } else {
                /* Default to make "Ford" */
                models = MODELS_FORD_SUV;
            }
        } else if ("truck".equals(type)) {
            priceMult = 3.0f;
            if ("Chrysler".equals(make)) {
                models = MODELS_CHRYSLER_TRUCK;
            } else if ("GM".equals(make)) {
                models = MODELS_GM_TRUCK;
            } else {
                /* Default to make "Ford" */
                models = MODELS_FORD_TRUCK;
            }
        } else {
            /* Default to type "auto" */
            if ("Chrysler".equals(make)) {
                models = MODELS_CHRYSLER_AUTO;
            } else if ("GM".equals(make)) {
                models = MODELS_GM_AUTO;
            }
        }
        final int modelIndx = secureRandom.nextInt(models.length);
        final String model = models[modelIndx];

        final float basePrice = 10371.59f;
        final float deltaPrice = secureRandom.nextFloat();
        final double price = (priceMult * basePrice) + deltaPrice;

        final int count = secureRandom.nextInt(100);

        final Row row = table.createRow();

        row.put("type", type);
        row.put("make", make);
        row.put("model", model);
        row.put("class", vClass);
        row.put("color", color);
        row.put("price", price);
        row.put("count", count);

        /* Row may exist. If so, then change it slightly to avoid overwrite. */
        final PrimaryKey dupKey = row.createPrimaryKey();
        final Row dupRow = tableAPI.get(dupKey, null);
        if (dupRow != null) {
            final int indx0 = secureRandom.nextInt(DUP_CLASS_SUFFIXES.length);
            final int indx1 = secureRandom.nextInt(COLORS.length);
            final String class2nd = DUP_CLASS_SUFFIXES[indx0];
            final String color2nd = COLORS[indx1];
            vClass = vClass + "-" + class2nd;
            color = color + "-on-" + color2nd;

            row.put("class", vClass);
            row.put("color", color);

            /* Still may have duplicate row. Note it and move on. */
            final PrimaryKey dupKey1 = row.createPrimaryKey();
            final Row dupRow1 = tableAPI.get(dupKey1, null);
            if (dupRow1 != null) {
                nRowsAdded--;
            }
        }
        tableAPI.putIfAbsent(row, null, null);
    }

    private void deleteExistingData() {

        /* Get an iterator over all the primary keys in the table. */
        final TableIterator<PrimaryKey> itr =
            tableAPI.tableKeysIterator(table.createPrimaryKey(), null, null);

        /* Delete each row from the table. */
        long cnt = 0;
        while (itr.hasNext()) {
            tableAPI.delete(itr.next(), null, null);
            cnt++;
        }
        itr.close();
        System.out.println(cnt + " records deleted");
    }

    private long nRowsInTable() {
        final TableIterator<PrimaryKey> itr =
            tableAPI.tableKeysIterator(table.createPrimaryKey(), null, null);
        long cnt = 0;
        while (itr.hasNext()) {
            itr.next();
            cnt++;
        }
        itr.close();
        return cnt;
    }

    /*
     * Convenience method for displaying output when debugging.
     */
    private void displayRow(Table tbl) {
        final TableIterator<Row> itr =
            tableAPI.tableIterator(tbl.createPrimaryKey(), null, null);
        while (itr.hasNext()) {
            System.out.println(itr.next());
        }
        itr.close();
    }

    /* Static values used to populate the table that is created above. */

    private static final String[] TYPES = {"auto", "truck", "suv"};
    private static final String[] MAKES = {"Ford", "GM", "Chrysler"};

    private static final String[] MODELS_FORD_AUTO =
        {"Focus", "Taurus", "Fiesta", "Edge"};
    private static final String[] MODELS_FORD_TRUCK = {"F150", "F250", "F350"};
    private static final String[] MODELS_FORD_SUV =
        {"Escape", "Expedition", "Explorer"};

    private static final String[] MODELS_GM_AUTO =
        {"Camaro", "Corvette", "Impala", "Malibu"};
    private static final String[] MODELS_GM_TRUCK =
        {"Sierra", "Silverado1500", "Silverado2500"};
    private static final String[] MODELS_GM_SUV =
        {"Tahoe", "Equinox", "Blazer"};

    private static final String[] MODELS_CHRYSLER_AUTO =
        {"Sebring", "Imperial", "Lebaron", "PTCruiser"};
    private static final String[] MODELS_CHRYSLER_TRUCK =
        {"Ram1500", "Ram2500", "Ram3500"};
    private static final String[] MODELS_CHRYSLER_SUV =
        {"Aspen", "Pacifica", "Journey"};

    private static final String[] CLASSES =
        {"4WheelDrive", "AllWheelDrive", "FrontWheelDrive", "RearWheelDrive"};

    private static final String[] COLORS =
        {"red", "blue", "green", "yellow", "white", "black"};

    private static final String[] DUP_CLASS_SUFFIXES =
        {"4cylinder", "6cylinder", "hybrid", "diesel"};
}

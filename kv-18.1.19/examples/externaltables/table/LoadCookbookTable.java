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

package externaltables.table;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

import externaltables.UserInfo;

/**
 * Class that creates sample records and uses the Table API to populate a
 * NoSQL Database with those records.
 *
 * This class assumes that a table of the following name and format has been
 * created and added to the store:
 * <code>
 *   kv -> table create -name cookbookTable
 *   cookbookTable -> add-field -name email   -type STRING
 *   cookbookTable -> add-field -name name    -type STRING
 *   cookbookTable -> add-field -name gender  -type STRING
 *   cookbookTable -> add-field -name address -type STRING
 *   cookbookTable -> add-field -name phone   -type STRING
 *   cookbookTable -> primary-key -field email
 *   cookbookTable -> exit
 *   kv -> plan add-table -wait -name cookbookTable
 * </code>
 * Rather than executing the commands above interactively in the CLI,
 * the examples/table/create_vehicle_table.kvs script can instead be used
 * in the following way:
 *
 * <code>
 *   > cd KVHOME
 *   > java -jar lib/kvcli.jar -host <hostname> -port <port> -store <storename>
 *
 *   kv-> load -file examples/externaltables/table/create_cookbook_table.kvs
 * </code>
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
public final class LoadCookbookTable {

    private final KVStore store;
    private final TableAPI tableAPI;
    private final Table table;

    private long nOps = 10;

    private boolean deleteExisting = false;

    static final String TABLE_NAME = "cookbookTable";
    static final String USER_OBJECT_TYPE = "user";

    public static void main(final String[] args) {
        try {
            final LoadCookbookTable loadData = new LoadCookbookTable(args);
            loadData.run();
        } catch (FaultException e) {
        	 e.printStackTrace();
             System.out.println(
                     "Please make sure a store is running " +
                     "and a table is created.");
             System.out.println("The error could be caused by a security " +
                     "mismatch. If the store is configured secure, you " +
                     "should specify a user login file " +
                     "with system property oracle.kv.security. " +
                     "For example, \n" +
                     "\tjava -Doracle.kv.security=<user security login file>" +
                     " externaltables.table.LoadCookbookTable\n" +
                     "KVLite generates the security file in " +
                     "$KVHOME/kvroot/security/user.security ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Parses command line args and opens the KVStore.
     */
    private LoadCookbookTable(final String[] argv) {

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
            } else if ("-delete".equals(thisArg)) {
                deleteExisting = true;
            } else {
                usage("Unknown argument: " + thisArg);
            }
        }

        store = KVStoreFactory.getStore
            (new KVStoreConfig(storeName, hostName + ":" + hostPort));

        tableAPI = store.getTableAPI();

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
             "\t-delete (default: false) [delete all existing data]\n");
        System.exit(1);
    }

    private void run() {
        if (deleteExisting) {
            deleteExistingData();
        }

        doLoad();
    }

    private void doLoad() {
        for (long i = 0; i < nOps; i++) {
            addRow(i);
        }
        displayRow(table);
        System.out.println(nOps + " new records added");
        store.close();
    }

    private void addRow(final long i) {

        final String email = "user" + i + "@example.com";

        final UserInfo userInfo = new UserInfo(email);
        final String gender = (i % 2 == 0) ? "F" : "M";
        final int mod = (int) (i % 10);

        /* Pad the number for nicer column alignment. */
        String iStr = String.format("%03d", i);
        userInfo.setGender(gender);
        userInfo.setName((("F".equals(gender)) ? "Ms." : "Mr.") +
                         " Number-" + iStr);
        userInfo.setAddress(iStr + " Example St, Example Town, AZ");
        userInfo.setPhone("000.000.0000".replace('0', (char) ('0' + mod)));

        final Row row = table.createRow();

        row.put("email", userInfo.getEmail());
        row.put("name", userInfo.getName());
        row.put("gender", userInfo.getGender());
        row.put("address", userInfo.getAddress());
        row.put("phone", userInfo.getPhone());

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
}

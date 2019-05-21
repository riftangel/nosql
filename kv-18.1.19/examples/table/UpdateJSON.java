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

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;

import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;


public class UpdateJSON {
    private String dataFile = "person_contacts.json";
    private String defaulthost = "localhost:5000";
    private String helperhosts[];
    private String storeName = "kvstore";


    private static void usage() {
        String msg = "Creates a table and loads data into it from\n";
        msg += "an external file containing one or more JSON\n";
        msg += "objects. The objects must conform to the table\n";
        msg += "schema. Table rows are then updated so that\n";
        msg += "zipcodes for all home addresses in Boston are\n";
        msg += "modified updated. Update is performed 3 different\n";
        msg += "ways so as to illustrate the ways to query JSON\n";
        msg += "data in Oracle NoSQL Database.\n";
        msg += "\nCommand line options: \n";
        msg += "-store <storename>\n";
        msg += "\tName of the store. Defaults to 'kvstore'\n";
        msg += "-hostport <hostname>:<port>\n";
        msg += "\tStore location. Defaults to 'localhost:5000'\n";
        msg += "-file <filename>\n";
        msg += "\tFile containing row data. Defaults to ";
        msg += "person_contacts.json";


        System.out.println(msg);
        System.exit(0);
    }

    public static void main(String args[]) {
        UpdateJSON uj = new UpdateJSON();

        uj.run(args);
    }

    private void run(String args[]) {
        parseArgs(args);

        KVStoreConfig kconfig =
            new KVStoreConfig(storeName,
                              helperhosts);
        KVStore kvstore = KVStoreFactory.getStore(kconfig);

        defineTable(kvstore);
        loadTable(kvstore, dataFile);
        displayTable(kvstore);
        updateTableWithoutQuery(kvstore);
        createIndex(kvstore);
        updateTableWithIndex(kvstore);
        updateTableUsingSQLQuery(kvstore);
        displayTable(kvstore);
    }





    // Drops the example table if it exists. This removes all table
    // data and indexes. The table is then created in the store.
    // The loadTable() method is used to populate the newly created
    // table with data.
    private void defineTable(KVStore kvstore) {
        System.out.println("Dropping table....");
        String statement = "DROP TABLE IF EXISTS personContacts";
        boolean success = runDDL(kvstore, statement);

        if (success) {
            statement =
                  "CREATE TABLE personContacts (" +
                  "account INTEGER," +
                  "person JSON," +
                  "PRIMARY KEY(account))";
            System.out.println("Creating table....");
            success = runDDL(kvstore, statement);
            if (!success) {
                System.out.println("Table creation failed.");
                System.exit(-1);
            }
        }

    }




    // Creates a JSON index. This method must be
    // run before updateTableWithIndex() is run.
    private void createIndex(KVStore kvstore) {
        System.out.println("Creating index....");
        String statement = "CREATE INDEX IF NOT EXISTS ";
        statement += "idx_home_city on personContacts ";
        statement += "(person.address.home.city AS String)";
        runDDL(kvstore, statement);
    }




    // Executes DDL statements (such as are found in defineTable()
    // and createIndex()) in the store.
    private boolean runDDL(KVStore kvstore, String statement) {
        StatementResult result = null;
        boolean success = false;

        try {
            result = kvstore.executeSync(statement);
            displayResult(result, statement);
            success = true;
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:\n" + e.getMessage());
        } catch (FaultException e) {
            System.out.println
                ("Statement couldn't be executed, please retry: " + e);
        }

        return success;
    }




    // Utility method. Given a MapValue and a field name,
    // return the field as a MapValue. Used by
    // updateTableWithoutQuery()
    private MapValue getMV(MapValue mv, String field) {
        FieldValue fv = null;
        if ((fv = mv.get(field)) != null)
            return fv.asMap();
        return null;
    }

    // Update the zip code found on all Boston home addresses
    // to "02102-1000"
    //
    // Because we are not using an index, we must iterate over
    // every row in the table, modifying the rows with Boston home
    // addresses.
    private void updateTableWithoutQuery(KVStore kvstore) {
        TableAPI tableH = kvstore.getTableAPI();
        Table myTable = tableH.getTable("personContacts");

        PrimaryKey pkey = myTable.createPrimaryKey();
        TableIterator<Row> iter =
            tableH.tableIterator(pkey, null, null);
        try {
            while (iter.hasNext()) {
                int account = 0;
                Row row = iter.next();
                FieldValue fv = null;
                try {
                    account = row.get("account").asInteger().get();
                    MapValue mv = row.get("person").asMap();

                    MapValue mvaddress = getMV(mv, "address");
                    if (mvaddress != null) {
                        MapValue mvhome = getMV(mvaddress, "home");
                        if (mvhome != null) {
                            fv = mvhome.get("city");
                            if (fv != null) {
                                if (fv.toString()
                                        .equalsIgnoreCase("Boston"))
                                    updateZipCode(tableH,
                                                  row,
                                                  "home",
                                                  "02102-1000");
                            }
                        }
                    }
                } catch (ClassCastException cce) {
                    System.out.println("Data error: ");
                    System.out.println("Account " + account +
                         "has a missing or incomplete person field");
                    // If this is thrown, then the "person" field
                    // doesn't exist for the row.
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }

        System.out.println("Updated a table without using a query.");
    }




    // Update the zip code found on all Boston home addresses
    // to "02102-1000"
    //
    // Because we have an index available to us, we only have to look
    // at those rows which have person.address.home.city = Boston.
    // All other rows are skipped during the read operation.
    private void updateTableWithIndex(KVStore kvstore) {
        TableAPI tableH = kvstore.getTableAPI();
        Table myTable = tableH.getTable("personContacts");

        // Construct the IndexKey.
        Index homeCityIdx = myTable.getIndex("idx_home_city");
        IndexKey homeCityIdxKey = null;

        // If NullPointerException is thrown by createIndexKey(),
        // it means that the required index has not been created.
        // Run the createIndex() method before running this method.
        homeCityIdxKey = homeCityIdx.createIndexKey();

        // Return only those entries with a home city of "Boston"
        homeCityIdxKey.put("person.address.home.city", "Boston");

        // Iterate over the returned table rows. Because we're
        // using an index, we're guaranteed that
        // person.address.home.city exists and equals Boston
        // for every table row seen here.
        TableIterator<Row> iter =
            tableH.tableIterator(homeCityIdxKey, null, null);
        try {
            while (iter.hasNext()) {
                Row row = iter.next();
                updateZipCode(tableH, row, "home", "02102-1000");
            }
        } finally {
            if (iter != null) {
            iter.close();
            }
        }
        System.out.println("Updated a table using an index.");
    }




    // Update the zip code found on all Boston home addresses
    // to "02102-1000"
    //
    // This query works with or without an index. If an index is
    // available, it is automatically used. For larger datasets,
    // the read operation will be faster with an index because only
    // the rows where person.address.home.city=Boston are returned
    // for the read.
    private void updateTableUsingSQLQuery(KVStore kvstore) {
        TableAPI tableH = kvstore.getTableAPI();
        Table myTable = tableH.getTable("personContacts");

        String query = "select * from personContacts p ";
        query += "where p.person.address.home.city=\"Boston\"";

        StatementResult result = kvstore.executeSync(query);

        for (RecordValue rv : result) {
            Row row = myTable.createRowFromJson(rv.toString(), false);
            updateZipCode(tableH, row, "home", "02102-1000");
        }
        System.out.println("Updated a table using a SQL Query to read.");
    }





    // Updates the zipcode for the proper address (either "home"
    // or "work" in this example).
    //
    // The calling method must guarantee that this row contains a
    // home address which refers to the correct city.
    private void updateZipCode(TableAPI tableH, Row row,
            String addrType, String newzip) {

        MapValue homeaddr = row.get("person").asMap()
                               .get("address").asMap()
                               .get(addrType).asMap();
        // If the zip field does not exist in the home address,
        // it is created with the newzip value. If it currently
        // exists, it is updated with the new value.
        homeaddr.put("zip", newzip);

        // Write the updated row back to the store.
        // Note that if this was production code, we
        // should be using putIfVersion() when
        // performing this write to ensure that the row
        // has not been changed since it was originally read.
        tableH.put(row, null, null);
    }





    // Loads the contents of the sample data file into
    // the personContacts table. The defineTable() method
    // must have been run at least once (either in this
    // runtime, or in one before it) before this method
    // is run.
    //
    // JSON parsers ordinarily expect one JSON Object per file.
    // Our sample data contains multiple JSON Objects, each of
    // which represents a single table row. So this method
    // implements a simple, custom, not particularly robust
    // parser to read the input file, collect JSON Objects,
    // and load them into the table.
    private void loadTable(KVStore kvstore, String file2load) {
        TableAPI tableH = kvstore.getTableAPI();
        Table myTable = tableH.getTable("personContacts");

        BufferedReader br = null;
        FileReader fr = null;

        try {
          String jObj = "";
          String currLine;
          int pCount = 0;
          boolean buildObj = false;
          boolean beganParsing = false;

          fr = new FileReader(file2load);
          br = new BufferedReader(fr);

          // Parse the example data file, loading each JSON object
          // found there into the table.
          while ((currLine = br.readLine()) != null) {
              pCount += countParens(currLine, '{');

              // Empty line in the data file
              if (currLine.length() == 0)
                  continue;

              // Comments must start at column 0 in the
              // data file.
              if (currLine.charAt(0) == '#')
                  continue;

              // If we've found at least one open paren, it's time to
              // start collecting data
              if (pCount > 0) {
                  buildObj = true;
                  beganParsing = true;
              }

              if (buildObj) {
                  jObj += currLine;
              }

              // If our open and closing parens balance (the count
              // is zero) then we've collected an entire object
              pCount -= countParens(currLine, '}');
              if (pCount < 1)
                  buildObj = false;

              // If we started parsing data, but buildObj is false
              // then that means we've reached the end of a JSON
              // object in the input file. So write the object
              // to the table, which means it is written to the
              // store.
              if (beganParsing && !buildObj) {
                  Row row = myTable.createRowFromJson(jObj, false);
                  tableH.put(row, null, null);
                  jObj = "";
              }

          }

          System.out.println("Loaded sample data " + file2load);

        } catch (FileNotFoundException fnfe) {
            System.out.println("File not found: " + fnfe);
            System.exit(-1);
        } catch (IOException ioe) {
            System.out.println("IOException: " + ioe);
            System.exit(-1);
        } finally {
            try {
                if (br != null)
                    br.close();
                if (fr != null)
                    fr.close();
            } catch (IOException iox) {
                System.out.println("IOException on close: " + iox);
            }
        }
    }

    // Used by loadTable() to know when a JSON object
    // begins and ends in the input data file.
    private int countParens(String line, char p) {
        int c = 0;
        for( int i=0; i < line.length(); i++ ) {
            if( line.charAt(i) == p ) {
                    c++;
            }
        }

        return c;
    }

    // Dumps the entire table to the command line.
    // Output is unformatted.
    private void displayTable(KVStore kvstore) {
        TableAPI tableH = kvstore.getTableAPI();
        Table myTable = tableH.getTable("personContacts");

        PrimaryKey pkey = myTable.createPrimaryKey();
        TableIterator<Row> iter = tableH.tableIterator(pkey, null,
                null);
        try {
            while (iter.hasNext()) {
                Row row = iter.next();
                System.out.println("\nAccount: " +
                        row.get("account").asInteger());
                if (row.get("person").isNull()) {
                    System.out.println("No person field");
                } else {
                    System.out.println(row.get("person").asMap());
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    // Displays the results of an executeSync() call.
    private void displayResult(StatementResult result,
            String statement) {
        System.out.println("===========================");
        if (result.isSuccessful()) {
            System.out.println("Statement was successful:\n\t" +
                    statement);
            System.out.println("Results:\n\t" + result.getInfo());
        } else if (result.isCancelled()) {
            System.out.println("Statement was cancelled:\n\t" +
                    statement);
        } else {
             // statement wasn't successful: may be in error, or may
             // still be in progress.
            if (result.isDone()) {
                System.out.println("Statement failed:\n\t" +
                        statement);
                System.out.println("Problem:\n\t" +
                        result.getErrorMessage());
            } else {
                System.out.println("Statement in progress:\n\t" +
                        statement);
                System.out.println("Status:\n\t" +
                        result.getInfo());
            }
        }
    }

    // Parse command line arguments
    private void parseArgs(String[] args)
    {
        final int nArgs = args.length;
        int argc = 0;
        ArrayList<String> hhosts = new ArrayList<String>();

        while (argc < nArgs) {
            final String thisArg = args[argc++];

            if (thisArg.equals("-store")) {
                if (argc < nArgs) {
                    storeName = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals("-hostport")) {
                if (argc < nArgs) {
                    hhosts.add(args[argc++]);
                } else {
                    usage();
                }
            } else if (thisArg.equals("-file")) {
                if (argc < nArgs) {
                    dataFile = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals("?") ||
                    thisArg.equals("help")) {
                usage();
            } else {
                usage();
            }
        }

        if (hhosts.isEmpty()) {
            helperhosts = new String [] {defaulthost};
        } else {
            helperhosts = new String[hhosts.size()];
            helperhosts = hhosts.toArray(helperhosts);
        }
    }
}

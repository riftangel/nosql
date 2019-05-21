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

package oracle.kv.util.expimp;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.avro.Schema;

import com.sleepycat.util.PackedInteger;

import oracle.kv.BulkWriteOptions;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.EntryStream;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.Key;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StatementResult;
import oracle.kv.UnauthorizedException;
import oracle.kv.Value;
import oracle.kv.Value.Format;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KeyValueVersionInternal;
import oracle.kv.impl.api.avro.AvroDdl;
import oracle.kv.impl.api.avro.AvroDdl.AddSchemaOptions;
import oracle.kv.impl.api.avro.AvroDdl.SchemaSummary;
import oracle.kv.impl.api.avro.AvroSchemaMetadata;
import oracle.kv.impl.api.avro.AvroSchemaStatus;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.KVStoreLogin.CredentialsProvider;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.table.TableAPI;
import oracle.kv.util.expimp.ExitHandler.ExitCode;

/**
 * Abstract implementation of Import functionality which is used to move data
 * from the export stores to Oracle NoSql database. Currently the import utility
 * supports Oracle Storage Cloud Service and Local File System as a valid
 * export store.
 *
 * For performing import, this utility will consume the export package generated
 * by the export utility. Import utility will not create any target Oracle NoSql
 * store. Its users responsibility to ensure that a target Oracle NoSql store
 * is created before performing an import against the store.
 *
 * Using the import utility:
 * 1) The user can import all the schema definitions and the data (table, avro
 * and none format data) from the export store to the Oracle NoSql store.
 * The schema definitions include the table definitions, avro schemas and the
 * index definitions. The import utility first imports all schema definitions
 * and then imports the user data into the Oracle NoSql store.
 * 2) The user can import individual table in which case the corresponding table
 * schemas, index definitions and the table data will get imported.
 * 3) The TTL of the record is imported.
 *
 * The store need not be quiescent when import utility is run. It is possible
 * that the user runs the Import utility on a non empty Oracle NoSql store. For
 * this reason, the import utility will first check if the table definition is
 * already present in the nosql store. Only if the table is not already present
 * in the target nosql store, the Import utility will create the table in the
 * store. Indexes are created after the tables are created. If the index field
 * is missing in the table definition, the corresponding index creation is
 * skipped. For the avro schemas, the same rule applies. Only those avro schemas
 * which are not already present in the target Oracle NoSql store will get
 * imported. The application data (table, avro and none format data) is then
 * imported into the Oracle NoSql store. While importing a table data, if the
 * structure of the data does not match the structure of the table schema
 * (example: table primary key incompatibility), the corresponding table data
 * is not imported and is captured in reject-record-file.
 *
 * Exit code can be used as a diagnostic to determine the state of import. The
 * exit codes have the following meaning:
 *
 * 0   (EXIT_OK) -- No errors found during import.
 * 100 (EXIT_USAGE) -- Illegal import command usage.
 * 101 (EXIT_NOPERM) -- Unauthorized access to the Oracle Cloud Storage Service
 * 102 (EXIT_EXPSTR_NOCONNECT) -- The Oracle Cloud Storage Service could not be
 *      accessed using the service connection parameters
 * 103 (EXIT_NOCONNECT) -- The source NoSql store could not be connected using
 *      the given store-name and helper hosts.
 * 104 (EXIT_UNEXPECTED) -- The import utility experienced an unexpected error.
 * 106 (EXIT_NOREAD) -- The export package has no read permissions.
 * 112 (EXIT_NOEXPPACKAGE) -- The export package needed for import not found in
 *      the path provided. For Oracle CloudStorage Service, this means the
 *      required container not found
 * 109 (EXIT_INVALID_EXPORT_STORE) -- Invalid export store type. Valid export
 *      stores are local and object_store
 * 110 (EXIT_SECURITY_ERROR) -- Error loading security file.
 */
public abstract class AbstractStoreImport implements CredentialsProvider {

    private KVStore store;
    @SuppressWarnings("deprecation")
    private final oracle.kv.avro.AvroCatalog catalog;
    private final TableAPI tableAPI;
    private final AvroDdl avroDdl;

    /*
     * Mapping between the avro schema Id and the avro schema.
     */
    private static Map<Integer, Schema> schemaIndexMap;

    /*
     * General utility used to convert table schema definitions and table
     * index definitions into table DDLs and table index DDLs.
     */
    private final JSONToDDL jsonToDDL;

    /*
     * Mapping between the table full name and the table instance.
     */
    private final Map<String, TableImpl> tableMap;

    /*
     * Mapping between the table full name and its parent table full name
     */
    private final Map<String, String> tableParent;

    /*
     * Mapping between the table full name and the avro writer schema
     */
    private final Map<String, String> tableWriterSchemas;

    /*
     * Mapping between the avro name and the avro format data writer schema
     */
    private final Map<Integer, Schema> avroWriterSchemas;

    private final Set<String> mismatchKeySchemaTables;

    /*
     * Retry count to load ddls
     */
    private static final int RETRY_COUNT = 10;

    /*
     * Sleep time period before trying to load the ddl
     */
    private static final int RETRY_SLEEP_PERIOD = 5000;

    /*
     * The TTL will get imported relative to this date which is chosen by the
     * user if ttlImportType is 'RELATIVE'. This value is null if ttlImportType
     * is 'ABSOLUTE'. Date should of the format YYYY-MM-DD HH:MM:SS and is
     * always in UTC.
     */
    private String importTtlRelativeDate;
    private long referenceTimeMs = 0;

    /*
     * Bulk put tuning parameters
     */
    private int streamParallelism;
    private int perShardParallelism;
    private int bulkHeapPercent;

    private static enum ImportState {
        RECORD_IMPORTED,
        FILE_SCAN_COMPLETE,
        RECORD_SKIPPED
    }

    /*
     * The exported files present in the export package are segmented in chunks
     * of 1GB. This set keeps track of all the file segments that have been
     * successfully imported into the target kvstore. This is used to aid in
     * resuming import in case of a failure. On an import retry, all the file
     * segments that have already been imported will be skipped.
     */
    private final Set<String> loadedFileSegments;

    /*
     * Each LOB in the export package is imported into the target kvstore in a
     * separate thread. This list holds the status (future instances) of all
     * the worker threads importing the lob bytes into the kvstore.
     */
    private final List<FutureHolder> lobFutureList;

    /*
     * Asynchronous thread pool
     */
    private final ExecutorService threadPool;
    private static final int NUM_THREADS = 20;

    /*
     * List of entry streams used by the BulkPut API used to import all the
     * data from export package into the target kvstore.
     */
    private final List<EntryStream<KeyValueVersion>> fileStreams;

    /*
     * Hook to induce schema changes. Used for testing
     */
    public TestHook<String> CHANGE_HOOK;

    /*
     * Logger variables
     */
    private final Logger logger;
    private final LogManager logManager;

    /*
     * Boolean variable which determines if the import output from CLI should
     * be in json format
     */
    private final boolean json;

    /*
     * For test purpose
     */
    public static boolean printToConsole = true;

    /*
     * Test flag
     */
    public static boolean testFlag = false;

    /*
     * Variables used only for testing purpose
     */
    private static final String testTableName = "Table1.Table2.Table3";
    private static final String testTableChunkSequence = "abcdefghijlk";

    /*
     *  Use for authentication
     */
    private KVStoreLogin storeLogin;
    private LoginCredentials loginCreds;

    class CustomLogManager extends LogManager {
        CustomLogManager() {
            super();
        }
    }

    /**
     * Exit import with the appropriate exit code.
     *
     * @param resetLogHandler if true resets the log handler.
     * @param exitCode
     * @param ps
     * @param message message to be dispalyed in CLI output on exit
     */
    protected void exit(boolean resetLogHandler,
                        ExitCode exitCode,
                        PrintStream ps,
                        String message) {

        if (resetLogHandler) {
            flushLogs();
            logManager.reset();
        }

        if (threadPool != null) {
            threadPool.shutdown();
        }

        if (getJson()) {
            /*
             * Display import output in json format
             */
            ExitHandler.displayExitJson(ps, exitCode, message, "import");
        } else {
            String exitMsg = exitCode.getDescription();

            if (message != null) {
                exitMsg += " - " + message;
            }

            if ((exitCode.equals(ExitCode.EXIT_OK) && printToConsole) ||
                    !exitCode.equals(ExitCode.EXIT_OK)) {
                ps.println(exitMsg);
            }
        }

        if (!exitCode.equals(ExitCode.EXIT_OK)) {
            System.exit(exitCode.value());
        }
    }

    @SuppressWarnings("deprecation")
    public AbstractStoreImport(String storeName,
                               String[] helperHosts,
                               String userName,
                               String securityFile,
                               boolean json) {

        this.json = json;
        logger = Logger.getLogger(AbstractStoreExport.class.getName() +
                                  (int)(Math.random()*1000));
        logger.setUseParentHandlers(false);

        logManager = new CustomLogManager();
        logManager.reset();
        logManager.addLogger(logger);

        prepareAuthentication(userName, securityFile);

        KVStoreConfig kconfig = new KVStoreConfig(storeName, helperHosts);
        kconfig.setSecurityProperties(storeLogin.getSecurityProperties());

        try {
            store = KVStoreFactory.getStore(kconfig, loginCreds,
                KVStoreLogin.makeReauthenticateHandler(this));
        } catch (IllegalArgumentException iae) {
            exit(false, ExitCode.EXIT_NOCONNECT, System.err, null);
        } catch (KVSecurityException kse) {
            exit(false, ExitCode.EXIT_NOCONNECT, System.err, null);
        } catch (FaultException fe) {
            exit(false, ExitCode.EXIT_NOCONNECT, System.err, null);
        }

        catalog = store.getAvroCatalog();
        tableAPI = store.getTableAPI();
        avroDdl = new AvroDdl(store);

        schemaIndexMap = new HashMap<Integer, Schema>();
        tableMap = new HashMap<String, TableImpl>();
        tableParent = new HashMap<String, String>();
        tableWriterSchemas = new HashMap<String, String>();
        avroWriterSchemas = new HashMap<Integer, Schema>();
        mismatchKeySchemaTables = new HashSet<String>();
        jsonToDDL = new JSONToDDL(tableAPI, this);
        loadedFileSegments = new HashSet<String>();
        importTtlRelativeDate = null;

        /*
         * Default values for bulk put tuning parameters
         */

        /*
         * Assuming a ideal state of 40Gbit downlink and 20 concurrent streams,
         * this configuration gives good performance which is theoretically
         * 40Gbit / (8 bits * 20 streams) = 25MB/sec. Depending on the actual
         * configuration in cloud object storage, this parameter can be tuned
         * via the config file.
         */
        streamParallelism = 20;

        /*
         * Bulk put performance tests had the best performance result with
         * Shardparallelism = 3, heap percentage = 40%.
         */
        perShardParallelism = 3;
        bulkHeapPercent = 40;

        fileStreams = new ArrayList<EntryStream<KeyValueVersion>>();

        lobFutureList = new ArrayList<FutureHolder>();
        threadPool = Executors.newFixedThreadPool(NUM_THREADS,
                                                  new KVThreadFactory("Import",
                                                                      null));
    }

    public boolean getJson() {
        return json;
    }

    /**
     * Set the relative date for TTL when the TTL import type is 'RELATIVE'
     */
    public void setImportTtlRelativeDate(String importTtlRelativeDate) {
        this.importTtlRelativeDate = importTtlRelativeDate;
    }

    /**
     * Set the bulk put streamParallelism parameter
     * @see BulkWriteOptions
     */
    public void setStreamParallelism(int streamParallelism) {
        this.streamParallelism = streamParallelism;
    }

    /**
     * Set the bulk put perShardParallelism parameter
     * @see BulkWriteOptions
     */
    public void setPerShardParallelism(int perShardParallelism) {
        this.perShardParallelism = perShardParallelism;
    }

    /**
     * Set the bulk put heap percent parameter
     * @see BulkWriteOptions
     */
    public void setBulkHeapPercent(int bulkHeapPercent) {
        this.bulkHeapPercent = bulkHeapPercent;
    }

    /**
     * Get the reference time for ttl that will be used during import
     */
    private long getReferenceTime() {

        if (referenceTimeMs != 0) {
            return referenceTimeMs;
        }

        referenceTimeMs = System.currentTimeMillis();

        if (importTtlRelativeDate != null) {
            SimpleDateFormat formatter =
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            formatter.setLenient(false);

            try {
                Date referenceDate = formatter.parse(importTtlRelativeDate);
                referenceTimeMs = referenceDate.getTime();
            } catch (ParseException e) {
                /*
                 * The check for valid format of the relative date should be
                 * done when the config.xml file is parsed in
                 * ExportImportMain.java, so a format error here is unexpected.
                 */
                throw new IllegalStateException("Invalid date format: " +
                                                importTtlRelativeDate +
                                                ": " + e);
            }
        }

        return referenceTimeMs;
    }

    /**
     * Check and set SSL connection
     */
    private void prepareAuthentication(final String user,
                                       final String securityFile) {

        storeLogin = new KVStoreLogin(user, securityFile);
        try {
            storeLogin.loadSecurityProperties();
        } catch (IllegalArgumentException iae) {
            exit(false, ExitCode.EXIT_SECURITY_ERROR, System.err, null);
        }

        /* Needs authentication */
        if (storeLogin.foundSSLTransport()) {
            try {
                loginCreds = storeLogin.makeShellLoginCredentials();
            } catch (IOException e) {
                exit(false, ExitCode.EXIT_SECURITY_ERROR, System.err, null);
            }
        }
    }

    @Override
    public LoginCredentials getCredentials() {
        return loginCreds;
    }

    /**
     * Sets the handler for the logger
     */
    protected void setLoggerHandler() {
        setLoggerHandler(logger);
    }

    /**
     * Import the schema definition into the target kvstore. The json schema
     * may correspond to either an avro schema or table definition.
     *
     * @param schemaJsonString
     */
    protected void importSchema(String schemaJsonString) {

        /*
         * Get the schema type of the schema definition. This can be either 'T'
         * representing table definition or 'A' representing Avro schema
         * definition.
         */
        String schemaType =
            schemaJsonString
            .substring(0, schemaJsonString.indexOf(",")).trim();

        String jsonString =
            schemaJsonString
            .substring(schemaJsonString.indexOf(",") + 1,
                       schemaJsonString.length()).trim();

        switch (schemaType) {

            case "A": importAvroSchema(jsonString);
                      break;

            case "T": importTableSchema(jsonString);
                      break;

            default : break;
        }
    }

    /**
     * Import the avro schema in json format into the target kvstore
     *
     * Format of the avro jsonString: [A, SchemaId, A/D: AvroJsonSchema]
     * where: 'A' signifies this is an avro schema.
     *        'SchemaId' unique schemaId of the avro schema.
     *        'A/D' signifies whether this was enabled of disabled schema during
     *              export
     *        'AvroJsonSchema' The actual avro schema in json format.
     *
     * @param avroJsonString
     */
    private void importAvroSchema(String avroJsonString) {

        /*
         * Get the avro schema info: [A, SchemaId, A/D]
         */
        String avroInfo =
            avroJsonString.substring(0, avroJsonString.indexOf(":")).trim();

        /*
         * Get the avro schema json string.
         */
        String schemaString =
            avroJsonString.substring(avroJsonString.indexOf(":") + 1,
                                     avroJsonString.length()).trim();

        String[] schemaIdStatus = avroInfo.split(",");
        Integer schemaId = 0;

        try {
            schemaId = Integer.parseInt(schemaIdStatus[0].trim());
        } catch (NumberFormatException nfe) {
            logger.log(Level.SEVERE, "Exception retrieving the avro schema " +
                       "from export package", nfe);
            return;
        }

        Schema schema = new Schema.Parser().parse(schemaString);
        String schemaName = schema.getFullName();

        avroWriterSchemas.put(schemaId, schema);

        logger.info("Importing Avro schema: " + schemaName);

        SchemaSummary schemaSummary =
            avroDdl.getSchemaSummaries(true).get(schemaName);

        /*
         * Check if a schema with the same name is already present in the
         * target kvstore.
         */
        if (schemaSummary != null) {

            logger.info("Avro schema with name" + schemaName + " already " +
                        "exists in the KVStore.");

            int id = schemaSummary.getId();
            Schema existingSchema =
                avroDdl.getAllSchemas(true).get(id).getSchema();

            schemaIndexMap.put(schemaId, existingSchema);
            return;
        }

        String schemaStatus = schemaIdStatus[1].trim();

        /*
         * The schema with the same name is not present in the target
         * kvstore. Import the avro schema.
         */
        importAvroSchema(schemaString, schemaId, schemaStatus);
    }

    /**
     * Import the avro schema into the target kvstore
     *
     * @param schemaString The actual avro schema in json format
     * @param schemaId unique schemaId of the avro schema
     * @param schemaStatus schemaStatus is either A(Active)/D(Disabled)
     */
    private void importAvroSchema(String schemaString,
                                  Integer schemaId,
                                  String schemaStatus) {

        AvroSchemaStatus status = AvroSchemaStatus.ACTIVE;

        if (schemaStatus.equals("D")) {
            status = AvroSchemaStatus.DISABLED;
        }

        AvroSchemaMetadata schemaMetadata =
            new AvroSchemaMetadata(status, System.currentTimeMillis(),
                                   getAdminUser(), getAdminMachine());

        /*
         * Set the schema evolve option to false
         */
        AddSchemaOptions options = new AddSchemaOptions(false, true);

        try {
            avroDdl.addSchema(schemaMetadata, schemaString,
                              options, KVVersion.CURRENT_VERSION);

            Schema schema = new Schema.Parser().parse(schemaString);
            schemaIndexMap.put(schemaId, schema);
        } catch (AdminFaultException afe) {

            logger.log(Level.SEVERE, "Exception loading avro schema " +
                       schemaString, afe);
        } catch (IllegalCommandException ice) {

            logger.log(Level.SEVERE, "Exception loading avro schema.", ice);
        }
    }

    private static String getAdminUser() {
        return "";
    }

    private static String getAdminMachine() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "";
        }
    }

    /**
     * Imports the table schema definition and all the tables index definitions
     * into the target kvstore
     *
     * Format of the table schemaJsonString: [T, TableFullName: TableJsonSchema]
     */
    private void importTableSchema(String tableJsonString) {

        /*
         * Get the table name from the json string
         */
        String tableName =
            tableJsonString.substring(0, tableJsonString.indexOf(":")).trim();

        logger.info("Importing Table and index schema definitions for " +
                    tableName);

        /*
         * The table is not present in the target kvstore. Import the table
         * schema definition and all its index definitions.
         */
        String tableSchema =
            tableJsonString.substring(tableJsonString.indexOf(":") + 1,
                                      tableJsonString.length()).trim();

        importTableSchema(tableSchema, tableName);
    }

    /**
     * Imports the table schema definition and all the tables index definitions
     * into the target kvstore
     *
     * Format of the table schemaJsonString: [T, TableFullName: TableJsonSchema]
     *
     * @param schemaJsonString The table schema definition. It also contains
     * all its index definitions.
     * @param tableNames The list of table names the user wants to import.
     * Import the table schema definition only if the table is present in this
     * list.
     */
    protected void importTableSchema(String schemaJsonString,
                                     List<String> tableNames) {

        /*
         * Get the schema type of the schema definition. This can be either 'T'
         * representing table definition or 'A' representing Avro schema
         * definition.
         */
        String schemaType =
            schemaJsonString.substring(0, schemaJsonString.indexOf(",")).trim();

        if (schemaType.equals("A")) {
            return;
        }

        /*
         * Table schema definition found. Retrieve the table name.
         */
        String jsonString =
            schemaJsonString.substring(schemaJsonString.indexOf(",") + 1,
                                       schemaJsonString.length()).trim();

        String tableName =
            jsonString.substring(0, jsonString.indexOf(":")).trim();

        /*
         * tableNames list contains the list of all tables the user wants to
         * import. If this table is not in this list, don't import this table
         * schema definition.
         */
        if (!tableNames.contains(tableName)) {
            return;
        }

        tableNames.remove(tableName);

        logger.info("Importing Table and index schema definitions for " +
                    tableName);

        /*
         * The table is not present in the target kvstore. Import the table
         * schema definition and all its index definitions.
         */
        String tableSchema =
            jsonString.substring(jsonString.indexOf(":") + 1,
                                 jsonString.length()).trim();

        importTableSchema(tableSchema, tableName);
    }

    /**
     * Given the table name and the json schema definition, import all the table
     * DDls and the index DDLs.
     *
     * @param jsonSchema The actual table schema in json format. It also
     * contains all the table index definitions.
     * @param tableName
     */
    private void importTableSchema(String jsonSchema, String tableName) {

        /*
         * Get all the table and its index DDL definitions. The first entry
         * in this list is the table DDL followed by all the tables index
         * DDLs
         */
        List<String> ddlSchemas = jsonToDDL.getTableDDLs(jsonSchema, tableName);

        if (ddlSchemas == null) {
            /*
             * Cant load DDL for table due to one of the following:
             * 1) The table is already present.
             * 2) Parent table(s) needs to be loaded first.
             * 3) The parent table keys are missing in the child table json
             *    schema definition.
             */
            return;
        }

        StatementResult result = null;
        boolean loadingTable = true;

        /*
         * First load table DDL and then its index DDLs
         */
        for (String ddlSchema : ddlSchemas) {

            /*
             * Retry a few times on failure
             */
            for (int i = 0; i < RETRY_COUNT; i++) {
                try {
                    result = store.executeSync(ddlSchema);
                    displayResult(result, ddlSchema);

                    if (loadingTable && result.isSuccessful()) {

                        tableMap.put(tableName,
                                     (TableImpl)tableAPI.getTable(tableName));
                        loadingTable = false;
                    }

                    break;
                } catch (IllegalArgumentException e) {
                    logger.log(Level.SEVERE,
                        "Invalid statement: " + ddlSchema, e);

                    break;
                } catch (UnauthorizedException ue) {
                    logger.log(Level.WARNING,
                        "User does not have sufficient privileges to " +
                        "create the schema: " + ddlSchema);

                    break;
                } catch (FaultException | KVSecurityException e) {
                    if (i == (RETRY_COUNT - 1)) {
                        logger.log(Level.SEVERE,
                            "Statement couldn't be executed: " + ddlSchema, e);
                    } else {
                        try {
                            logger.log(Level.WARNING,
                                "Loading ddl failed. Retry #" +
                                (i + 1) + ": " + ddlSchema);
                            Thread.sleep(RETRY_SLEEP_PERIOD);
                        } catch (InterruptedException e1) {
                            logger.log(Level.SEVERE,
                                "Exception loading ddl: " + ddlSchema, e1);
                        }
                    }
                }
            }
        }
    }

    /**
     * General utility to check if a table schema has already been imported
     * in the target kvstore
     *
     * @param tableName
     * @return true of table schema already imported. False otherwise.
     */
    protected boolean isTableSchemaImported(String tableName) {

        if (tableMap.containsKey(tableName)) {
            return true;
        }

        return false;
    }

    /**
     * Display the status of table and index DDL execution.
     *
     * @param result The result of table/index DDL statement execution
     * @param statement The actual table/index DDL statement
     */
    private void displayResult(StatementResult result, String statement) {

        StringBuilder sb = new StringBuilder();

        if (result.isSuccessful()) {

            sb.append("Statement was successful:\n\t" + statement + "\n");
            sb.append("Results:\n\t" + result.getInfo() + "\n");
        } else if (result.isCancelled()) {

            sb.append("Statement was cancelled:\n\t" + statement + "\n");
        } else {

            if (result.isDone()) {

                sb.append("Statement failed:\n\t" + statement + "\n");
                sb.append("Problem:\n\t" + result.getErrorMessage() + "\n");
            } else {

                sb.append("Statement in progress:\n\t" + statement + "\n");
                sb.append("Status:\n\t" + result.getInfo() + "\n");
            }
        }

        logger.info(sb.toString());
    }

    /**
     * Create Entry Streams from multiple data file segments in the export
     * package. Add each of the EntryStream into the list holding all the
     * EntryStreams. This method is called for each of the file segments present
     * in the export package.
     *
     * @param in Input stream for the file segment in the export store
     * @param fileName name of the file in the export store
     * @param chunkSequence The file stored in export store is segmented into
     *        many chunks. FileName + chunksequence uniquely identifies a file
     *        segment
     * @param isTableData true if the file contains table data. False otherwise
     */
    protected void populateEntryStreams(BufferedInputStream in,
                                        String fileName,
                                        String chunkSequence,
                                        boolean isTableData) {

        String fileSegmentName = fileName + "-" + chunkSequence;

        /*
         * Check if the file segment from the export store is already imported
         */
        if (isLoaded(fileSegmentName)) {

            logger.info("File segment " + fileName + " with chunk sequence " +
                        chunkSequence + " already loaded in the target " +
                        "KVStore.");

            return;
        }

        /*
         * Create an entry stream to read the key/value data from the file
         * segment in the export package
         */
        FileEntryStream stream =
            new FileEntryStream(in, fileName, chunkSequence, isTableData);

        /*
         * Populate the KV EntryStreams needed for bulk put
         */
        fileStreams.add(stream);
    }

    public void addTableParent(String cTableName, String pTableName) {
        tableParent.put(cTableName, pTableName);
    }

    public void addTableMap(String tableName, TableImpl tableImpl) {
        tableMap.put(tableName, tableImpl);
    }

    public void addKeyMismatchTable(String tableName) {
        mismatchKeySchemaTables.add(tableName);
    }

    /**
     * BulkPut capability is used to provide a high performance backend for the
     * Import process. This method is called after populateEntryStreams is
     * called for all the file segments in the export package.
     */
    protected void performBulkPut() {

        logger.info("Performing Bulk Put of data.");

        try {
            if (fileStreams != null && !fileStreams.isEmpty()) {
                BulkWriteOptions writeOptions =
                    new BulkWriteOptions(null, 0, null);

                /*
                 * Sets the maximum number of streams that can be read
                 * concurrently by bulk put
                 */
                writeOptions.setStreamParallelism(streamParallelism);

                /*
                 * Sets the maximum number of threads that can concurrently
                 * write it's batch of entries to a single shard in the
                 * store
                 */
                writeOptions.setPerShardParallelism(perShardParallelism);

                writeOptions.setBulkHeapPercent(bulkHeapPercent);
                KVStoreImpl storeImpl = (KVStoreImpl)store;

                /*
                 * Import all the data from the export store into the target
                 * kvstore using bulk put.
                 */
                storeImpl.put(fileStreams, getReferenceTime(), writeOptions);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception in bulk put.", e);
        }
    }

    /**
     * Return a number present in the FileEntryStream. If a number is not
     * found, NumberFormatException is thrown.
     */
    public long getNumber(BufferedInputStream bin,
                          String fileName,
                          String chunkSequence) throws Exception {

        List<Integer> byteList = new ArrayList<Integer>();

        while (true) {
            int l = bin.read();

            if (l == -1) {
                return -1;
            }

            if (l == 32) {
                break;
            }
            byteList.add(l);
        }

        byte[] numberBytes = new byte[byteList.size()];
        int x = 0;

        for (int l : byteList) {
            numberBytes[x] = (byte)l;
            x++;
        }

        String numberString = new String(numberBytes,
                                         StandardCharsets.UTF_8);
        long number = 0;

        try {
            number = Long.parseLong(numberString);
        } catch (NumberFormatException nfe) {

            logger.log(Level.SEVERE, "Cant parse number. File segment " +
                       fileName + "-" + chunkSequence +
                       " seems to be corrupted.", nfe);

            /*
             * If number cant be obtained while parsing the file segments,
             * Exception is thrown which is caught by getNextTableData()/
             * getNextKVData() methods
             */
            throw new Exception();
        }

        return number;
    }

    /**
     * The stream that supplies the data (Key/Value pair) to be batched and
     * loaded into the store.
     */
    class FileEntryStream implements EntryStream<KeyValueVersion> {

        /*
         * The input stream for the file in the export store holding the
         * key/value data
         */
        private final BufferedInputStream bin;

        /*
         * The file segment being imported
         */
        private final String fileName;

        /*
         * True if this file segment contains table data
         */
        private final boolean isTableData;

        /*
         * Map containing the tableIds as key and the location of the tableId
         * in the key full path as the value
         */
        private Map<String, Integer> tableIds;

        /*
         * Identifies particular segment of the file in the export store that is
         * being imported
         */
        private final String chunkSequence;

        private boolean isCompleted;
        private KeyValueVersion kv;

        private boolean tableSchemaSame = false;
        private boolean tableSchemaChecked = false;

        public FileEntryStream(BufferedInputStream bin,
                               String fileName,
                               String chunkSequence,
                               boolean isTableData) {

            this.bin = bin;
            this.fileName = fileName;
            this.chunkSequence = chunkSequence;
            this.isCompleted = true;
            this.isTableData = isTableData;
        }

        @Override
        public String name() {
            return fileName;
        }

        /*
         * Returns the next entry in the stream holding non-table data or null
         * if at the end of the stream
         */
        @Override
        public KeyValueVersion getNext() {

            if (isTableData) {
                ImportState retNum = getNextTableData();

                /*
                 * Record is either expired or the checksum of record does not
                 * match
                 */
                if (retNum == ImportState.RECORD_SKIPPED) {
                    do {
                        retNum = getNextTableData();
                    } while (retNum == ImportState.RECORD_SKIPPED);
                }

                return retNum == ImportState.RECORD_IMPORTED ? kv : null;
            }

            ImportState retNum = getNextKV();

            /*
             * Record is either expired or the checksum of record does not
             * match
             */
            if (retNum == ImportState.RECORD_SKIPPED) {
                do {
                    retNum = getNextKV();
                } while (retNum == ImportState.RECORD_SKIPPED);
            }

            return retNum == ImportState.RECORD_IMPORTED ? kv : null;
        }

        /**
         * Get the tableId strings and their location in the key full path
         * for the table
         */
        private void getTableIds(TableImpl table) {

            Map<Integer, String> tempMap = new HashMap<Integer, String>();
            tableIds = new HashMap<String, Integer>();
            TableImpl tempTable = table;

            int tableNumber = 0;
            while (tempTable != null) {
                String idString = tempTable.getIdString();
                tempTable = (TableImpl)tempTable.getParent();
                int numKeys = 0;
                if (tempTable != null) {
                    numKeys = tempTable.getPrimaryKeySize();
                }
                tempMap.put(tableNumber, numKeys + "," + idString);
                tableNumber++;
            }

            for (Map.Entry<Integer, String> entry : tempMap.entrySet()) {
                int idx = entry.getKey();
                int numParentTables = tableNumber - 1 - idx;
                String[] keysAndTableId = entry.getValue().split(",");
                int numKeys = Integer.parseInt(keysAndTableId[0]);
                String tableId = keysAndTableId[1];
                tableIds.put(tableId, numKeys + numParentTables);
            }
        }

        /**
         * Set the tableIds (obtained using getTableIds) in the fullKey
         */
        private void setTableIds(List<String> fullKey) {

            for (Map.Entry<String, Integer> entry : tableIds.entrySet()) {
                String tableId = entry.getKey();
                int index = entry.getValue();
                fullKey.set(index, tableId);
            }
        }

        private ImportState getNextTableData() {

            byte[] keyBytes;
            byte[] valueBytes;
            byte[] expiryTimeBytes;
            byte[] checksumBytes;

            try {
                if (testFlag && fileName.equals(testTableName) &&
                    chunkSequence.contains(testTableChunkSequence)) {

                    testFlag = false;
                    throw new Exception("Test Exception");
                }

                /*
                 * Get the length of the record key
                 */
                int kLength = (int)getNumber(bin, fileName, chunkSequence);

                /*
                 * Reached end of the file segment
                 */
                if (kLength == -1) {
                    return ImportState.FILE_SCAN_COMPLETE;
                }

                TableImpl table = tableMap.get(fileName);

                if (mismatchKeySchemaTables.contains(table.getFullName())) {
                    String msg = "Mismatch between the primary key of the " +
                        "table schema in the target nosql store and the " +
                        "table data being imported. Skipping the record.";

                    throw new Exception(msg);
                }

                /*
                 * Get the record keyBytes
                 */
                keyBytes = new byte[kLength];
                for (int x = 0; x < kLength; x++) {
                    keyBytes[x] = (byte)bin.read();
                }

                /*
                 * Get the length of the record value
                 */
                int vLength = (int)getNumber(bin, fileName, chunkSequence);

                /*
                 * Get the record value bytes
                 */
                valueBytes = new byte[vLength];
                for (int x = 0; x < vLength; x++) {
                    valueBytes[x] = (byte)bin.read();
                }

                /*
                 * Get the value format
                 */
                int ordinal = bin.read();
                Format valueFormat = Value.Format.values()[ordinal];

                /*
                 * Get the length of the expiry time bytes
                 */
                int expiryTimeLength =
                    (int)getNumber(bin, fileName, chunkSequence);
                long expiryTimeMs = 0;

                if (expiryTimeLength > 0) {
                    /*
                     * Get the expiry time bytes packed in binary format using
                     * PackedInteger
                     */
                    expiryTimeBytes = new byte[expiryTimeLength];
                    for (int x = 0; x < expiryTimeLength; x++) {
                        expiryTimeBytes[x] = (byte)bin.read();
                    }

                    /*
                     * Convert the expiryTime in bytes to expiryTime in hours
                     * using PackedInteger utility method
                     */
                    long expiryTimeHours =
                        PackedInteger.readInt(expiryTimeBytes, 0);

                    /*
                     * Convert expiryTime in hours to expiryTime in ms
                     */
                    expiryTimeMs = expiryTimeHours * 60L * 60L * 1000L;
                }

                /*
                 * Get the length of the checksum bytes
                 */
                int checksumLength =
                    (int)getNumber(bin, fileName, chunkSequence);

                /*
                 * Get the checksum bytes packed in binary format using
                 * PackedInteger
                 */
                checksumBytes = new byte[checksumLength];
                for (int x = 0; x < checksumLength; x++) {
                    checksumBytes[x] = (byte)bin.read();
                }

                /*
                 * Convert the checksum in bytes to checksum number using
                 * PackedInteger utility method
                 */
                long storedCheckSum = PackedInteger.readLong(checksumBytes, 0);

                /*
                 * Calculate the checksum of the record
                 */
                long checkSum =
                    Utilities.getChecksum(keyBytes, valueBytes);

                /*
                 * If the checksum of the record during import does not match
                 * the checksum of the record calculated during export, skip
                 * the record and get the next table record.
                 */
                if (storedCheckSum != checkSum) {
                    return ImportState.RECORD_SKIPPED;
                }

                /*
                 * Skip the record if it has already expired
                 */
                if (expiryTimeLength > 0 && getReferenceTime() > expiryTimeMs) {
                    return ImportState.RECORD_SKIPPED;
                }

                String schemaString = tableWriterSchemas.get(fileName);

                if (valueBytes.length != 0 && schemaString == null) {
                    return ImportState.RECORD_SKIPPED;
                }

                Schema readerSchema = table.getSchema();
                Schema writerSchema = null;

                if (schemaString != null) {
                    writerSchema = new Schema.Parser().parse(schemaString);
                }

                if (!tableSchemaChecked) {
                    tableSchemaChecked = true;

                    if ((writerSchema != null &&
                         writerSchema.equals(readerSchema)) ||
                         valueBytes.length == 0) {

                        /*
                         * Reader and writer schemas are the same. Data will
                         * be imported as normal key value pairs obtained from
                         * the export package and not using rows. The key will
                         * not have the correct table Ids.  Get the tableIds
                         * and their location in the key.
                         */
                        getTableIds(table);
                        tableSchemaSame = true;
                    }
                }

                if (tableSchemaSame) {

                    Key key = Key.fromByteArray(keyBytes);
                    List<String> fullPath = key.getFullPath();

                    /*
                     * Set the tableIds in the correct position in the full
                     * path for the key
                     */
                    setTableIds(fullPath);

                    List<String> majorPath = new ArrayList<String>();
                    List<String> minorPath = new ArrayList<String>();
                    int majorPathLength = key.getMajorPath().size();

                    /*
                     * Extract major path of key from the full path
                     */
                    for (int i = 0; i < majorPathLength; i++) {
                        majorPath.add(fullPath.get(i));
                    }

                    /*
                     * Extract minor path of key from the full path
                     */
                    for (int i = majorPathLength; i < fullPath.size(); i++) {
                        minorPath.add(fullPath.get(i));
                    }

                    key = Key.createKey(majorPath, minorPath);

                    /*
                     * Set the first byte of valueBytes with the table version
                     */
                    if (valueBytes.length != 0)  {
                        valueBytes[0] = (byte)table.getTableVersion();
                    }

                    Value value =
                        Value.internalCreateValue(valueBytes, valueFormat);

                    if (expiryTimeMs > 0) {
                        kv = new KeyValueVersionInternal(key,
                                                         value,
                                                         expiryTimeMs);
                    } else {
                        kv = new KeyValueVersion(key, value);
                    }

                    return ImportState.RECORD_IMPORTED;
                }

                BinaryKeyIterator keyIter = new BinaryKeyIterator(keyBytes);
                RowImpl importedRow = table.createRow();

                Iterator<String> pkIter = table.getPrimaryKey().iterator();

                boolean isKeyFilled =
                    table.createImportRowFromKeyBytes(importedRow, keyIter, pkIter);

                if (!isKeyFilled || !keyIter.atEndOfKey()) {
                    String msg = "Mismatch between the primary key of the " +
                        "table schema in the target nosql store and the " +
                        "table data being imported. Skipping the record.";

                    throw new Exception(msg);
                }

                if (valueBytes.length == 0) {

                    Key key = importedRow.getPrimaryKey(false);
                    Value value = importedRow.createValue();

                    kv = new KeyValueVersion(key, value);
                    return ImportState.RECORD_IMPORTED;
                }

                /*
                 * Deserialize a record value encoded in avro.
                 */
                table.createExportRowFromValueSchema(writerSchema,
                                                     null,
                                                     importedRow,
                                                     valueBytes,
                                                     0,
                                                     table.getTableVersion(),
                                                     valueFormat);

                Key key = importedRow.getPrimaryKey(false);
                Value value = importedRow.createValue();
                if (expiryTimeMs > 0) {
                    kv = new KeyValueVersionInternal(key, value, expiryTimeMs);
                } else {
                    kv = new KeyValueVersion(key, value);
                }

                return ImportState.RECORD_IMPORTED;
            } catch (Exception e) {
                /*
                 * Import of this file segment was not completed. Set
                 * isCompleted to false to record this fact.
                 */
                isCompleted = false;
                logger.log(Level.SEVERE, "File segment " +
                           fileName + " import failed", e);
                return ImportState.FILE_SCAN_COMPLETE;
            }
        }

        @SuppressWarnings("deprecation")
        private ImportState getNextKV() {

            byte[] keyBytes;
            byte[] valueBytes;
            byte[] checksumBytes;

            try {
                /*
                 * Get the dataFormat. Its either 'A' for avro format data, 'N'
                 * for data in none format and 'L' for a LOB key reference
                 */
                int dataFormat = bin.read();

                /*
                 * Reached end of the file segment
                 */
                if (dataFormat == -1) {
                    return ImportState.FILE_SCAN_COMPLETE;
                }

                /*
                 * None format data
                 */
                if (dataFormat == 78) {

                    /*
                     * Get the length of the record key
                     */
                    int kLength = (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the record key bytes
                     */
                    keyBytes = new byte[kLength];
                    for (int x = 0; x < kLength; x++) {
                        keyBytes[x] = (byte)bin.read();
                    }

                    /*
                     * Get the length of the record value
                     */
                    int vLength = (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the record value bytes
                     */
                    valueBytes = new byte[vLength];
                    for (int x = 0; x < vLength; x++) {
                        valueBytes[x] = (byte)bin.read();
                    }

                    Value value = Value.createValue(valueBytes);

                    /*
                     * Get the length of the checksum bytes
                     */
                    int checksumLength =
                        (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the checksum bytes packed in binary format using
                     * PackedInteger
                     */
                    checksumBytes = new byte[checksumLength];
                    for (int x = 0; x < checksumLength; x++) {
                        checksumBytes[x] = (byte)bin.read();
                    }

                    /*
                     * Convert the checksum in bytes to checksum number using
                     * PackedInteger utility method
                     */
                    long storedCheckSum =
                        PackedInteger.readLong(checksumBytes, 0);

                    /*
                     * Calculate the checksum of the record
                     */
                    long checkSum =
                        Utilities.getChecksum(keyBytes, valueBytes);

                    /*
                     * If the checksum of the record during import does not
                     * match the checksum of the record calculated during
                     * export, skip the record and get the next KV record.
                     */
                    if (storedCheckSum != checkSum) {
                        return ImportState.RECORD_SKIPPED;
                    }

                    Key key = Key.fromByteArray(keyBytes);
                    kv = new KeyValueVersion(key, value);
                    return ImportState.RECORD_IMPORTED;
                }

                /*
                 * Avro format data
                 */
                else if (dataFormat == 65) {

                    int schemaId = (int)getNumber(bin, fileName, chunkSequence);

                    boolean importRecord = true;

                    Schema schema = schemaIndexMap.get(schemaId);
                    oracle.kv.avro.JsonAvroBinding binding = null;

                    try {
                        binding = catalog.getJsonBinding(schema);
                    } catch(oracle.kv.avro.UndefinedSchemaException use) {
                        importRecord = false;
                    }

                    /*
                     * Get the length of the record key
                     */
                    int kLength = (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the record key bytes
                     */
                    keyBytes = new byte[kLength];
                    for (int x = 0; x < kLength; x++) {
                        keyBytes[x] = (byte)bin.read();
                    }

                    /*
                     * Get the length of the record value
                     */
                    int vLength = (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the record value bytes
                     */
                    valueBytes = new byte[vLength];
                    for (int x = 0; x < vLength; x++) {
                        valueBytes[x] = (byte)bin.read();
                    }

                    Value value = Value.fromByteArray(valueBytes);
                    oracle.kv.avro.JsonRecord record = null;

                    try {
                        record =
                            (binding != null) ? binding.toObjectForImport(value,
                            avroWriterSchemas.get(schemaId)) : null;

                        value =
                            (binding != null) ? binding.toValue(record) : null;
                    } catch (oracle.kv.avro.SchemaNotAllowedException snae) {
                        importRecord = false;
                    } catch (IllegalArgumentException iae) {
                        importRecord = false;
                    }

                    /*
                     * Get the length of the checksum bytes
                     */
                    int checksumLength =
                        (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the checksum bytes packed in binary format using
                     * PackedInteger
                     */
                    checksumBytes = new byte[checksumLength];
                    for (int x = 0; x < checksumLength; x++) {
                        checksumBytes[x] = (byte)bin.read();
                    }

                    /*
                     * Convert the checksum in bytes to checksum number using
                     * PackedInteger utility method
                     */
                    long storedCheckSum =
                        PackedInteger.readLong(checksumBytes, 0);

                    /*
                     * Calculate the checksum of the record
                     */
                    long checkSum =
                        Utilities.getChecksum(keyBytes, valueBytes);

                    /*
                     * If the checksum of the record during import does not
                     * match the checksum of the record calculated during
                     * export, skip the record and get the next KV record.
                     */
                    if (storedCheckSum != checkSum) {
                        importRecord = false;
                    }

                    Key key = Key.fromByteArray(keyBytes);

                    if (!importRecord) {
                        return ImportState.RECORD_SKIPPED;
                    }

                    kv = new KeyValueVersion(key, value);
                    return ImportState.RECORD_IMPORTED;
                }

                /*
                 * LOB key reference
                 */
                else if (dataFormat == 76) {

                    /*
                     * Get the length of the LOB key
                     */
                    int kLength = (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the LOB key bytes
                     */
                    keyBytes = new byte[kLength];
                    for (int x = 0; x < kLength; x++) {
                        keyBytes[x] = (byte)bin.read();
                    }

                    /*
                     * Get the length of the LOB fileName
                     */
                    int vLength = (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the LOB fileName
                     */
                    valueBytes = new byte[vLength];
                    for (int x = 0; x < vLength; x++) {
                        valueBytes[x] = (byte)bin.read();
                    }

                    /*
                     * Get the length of the checksum bytes
                     */
                    int checksumLength =
                        (int)getNumber(bin, fileName, chunkSequence);

                    /*
                     * Get the checksum bytes packed in binary format using
                     * PackedInteger
                     */
                    checksumBytes = new byte[checksumLength];
                    for (int x = 0; x < checksumLength; x++) {
                        checksumBytes[x] = (byte)bin.read();
                    }

                    /*
                     * Convert the checksum in bytes to checksum number using
                     * PackedInteger utility method
                     */
                    long storedCheckSum =
                        PackedInteger.readLong(checksumBytes, 0);

                    /*
                     * Calculate the checksum of the record
                     */
                    long checkSum =
                        Utilities.getChecksum(keyBytes, valueBytes);

                    /*
                     * If the checksum of the record during import does not
                     * match the checksum of the record calculated during
                     * export, skip the record and get the next KV record.
                     */
                    if (storedCheckSum != checkSum) {
                        return ImportState.RECORD_SKIPPED;
                    }

                    Key lobKey = Key.fromByteArray(keyBytes);
                    String lobFileName = new String(valueBytes,
                                                    StandardCharsets.UTF_8);

                    logger.info("Importing LOB: " + lobFileName);

                    /*
                     * Get the LOB data input stream from the export store
                     */
                    InputStream in = getLobInputStream(lobFileName);

                    if (in == null) {

                        logger.warning("LOB file " + lobFileName +
                                       " not found.");

                        return ImportState.RECORD_SKIPPED;
                    }

                    logger.info("Spawning a thread to import LOB: " +
                                lobFileName);

                    /*
                     * LOB data is not imported using Bulk Put. A separate
                     * thread is spawned which imports the LOB stream into the
                     * target KVStore using KVStore.putLOB() API
                     */
                    Future<Boolean> lobImportTask =
                        threadPool.submit(new PutLobTask(in, lobKey));

                    FutureHolder holder = new FutureHolder(lobImportTask);
                    lobFutureList.add(holder);

                    return ImportState.RECORD_SKIPPED;
                }

            } catch (Exception e) {
                /*
                 * Import of this file segment was not completed. Set
                 * isCompleted to false to record this fact.
                 */
                isCompleted = false;
                logger.log(Level.SEVERE, "File segment " +
                    fileName + " import failed", e);
                return ImportState.FILE_SCAN_COMPLETE;
            }

            return ImportState.FILE_SCAN_COMPLETE;
        }

        /*
         * Invoked by the loader to indicate that all the entries supplied by
         * the stream have been processed. The callback happens sometime after
         * getNext() method returns null and all entries supplied by the stream
         * have been written to the store.
         *
         * When this method is invoked by the BulkPut API, it is used as an
         * indicator that a file segment has been successfully imported into
         * the target KVStore.
         */
        @Override
        public void completed() {

            if (isCompleted) {
                logger.info("File segment: " + fileName + " with chunk " +
                            "sequence " + chunkSequence + " successfully " +
                            "imported.");

                if (loadedFileSegments != null) {
                    loadedFileSegments.add(fileName + "-" + chunkSequence);
                }
            } else {
                logger.warning("Not all records in File segment: " + fileName +
                               " with chunk " + "sequence " + chunkSequence +
                               " got imported.");
            }
        }

        @Override
        public void keyExists(KeyValueVersion entry) {}

        /**
         * Exception encountered while adding an entry to the store
         */
        @Override
        public void catchException(RuntimeException runtimeException,
                                   KeyValueVersion kvPair) {
            isCompleted = false;
        }
    }

    /**
     * Perform any task that needs to be completed post import
     */
    protected void doPostImportWork() {

        /*
         * LOB data is imported in a separate thread using KVStore.putLOB().
         * Wait for all these threads to complete their execution.
         */
        waitForLobTasks();

        /*
         * Write the status file with the list of all the file segments that
         * has been successfully imported into the target KVStore.
         */
        writeStatusFile(loadedFileSegments);
    }

    /**
     * Load the list of file segments that has been already been imported into
     * the kvstore. During import process, the import of these file segments
     * will be skipped
     */
    protected void loadStatusFile(String fileSegmentName) {
        loadedFileSegments.add(fileSegmentName);
    }

    /**
     * Check if the given file segment has already been imported
     */
    private boolean isLoaded(String fileSegmentName) {

        if (loadedFileSegments != null &&
                loadedFileSegments.contains(fileSegmentName)) {
            return true;
        }
        return false;
    }

    public TableImpl getTableImpl(String tableName) {
        return tableMap.get(tableName);
    }

    public void putTableWriterSchema(String tableName, String writerSchema) {
        tableWriterSchemas.put(tableName, writerSchema);
    }

    public void logMessage(String message, Level level) {
        logger.log(level, message);
    }

    /**
     * This class holds a Future<Integer> and exists so that a
     * BlockingQueue<FutureHolder> can be used to indicate which Worker thread
     * tasks need to be waited upon.  A FutureHolder with a null future
     * indicated the end of input and the waiting thread can exit.
     */
    class FutureHolder {
        Future<Boolean> future;

        public FutureHolder(Future<Boolean> future) {
            this.future = future;
        }

        public Future<Boolean> getFuture() {
            return future;
        }
    }

    /**
     * LOB data is imported in a separate thread using KVStore.putLOB().
     * Wait for all these threads to complete their execution.
     */
    private void waitForLobTasks() {

        logger.info("Waiting for PutLobTask threads to complete execution.");

        try {
            for (FutureHolder holder : lobFutureList) {
                holder.getFuture().get();
            }
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Exception importing LOB.", e);
        } catch (InterruptedException ie) {
            logger.log(Level.SEVERE, "Exception importing LOB.", ie);
        }
    }

    /**
     * A callable responsible for importing the LOB data into KVStore
     */
    public class PutLobTask implements Callable<Boolean> {

        private final InputStream in;
        private final Key lobKey;

        public PutLobTask(InputStream in, Key lobKey) {
            this.in = in;
            this.lobKey = lobKey;
        }

        @Override
        public Boolean call() {

            logger.info("PutLobTask thread spawned.");

            try {
                store.putLOB(lobKey, in,
                             Durability.COMMIT_WRITE_NO_SYNC,
                             5, TimeUnit.SECONDS);
            } catch (DurabilityException e) {

                logger.log(Level.SEVERE, "Error importing LOB with key " +
                           lobKey.toString(), e);

                return false;
            } catch (RequestTimeoutException e) {

                logger.log(Level.SEVERE, "Error importing LOB with key " +
                           lobKey.toString(), e);

                return false;
            } catch (ConcurrentModificationException e) {

                logger.log(Level.SEVERE, "Error importing LOB with key " +
                           lobKey.toString(), e);

                return false;
            } catch (KVSecurityException kvse) {

                logger.log(Level.SEVERE, "Error importing LOB with key " +
                           lobKey.toString(), kvse);

                return false;
            } catch (FaultException e) {

                logger.log(Level.SEVERE, "Error importing LOB with key " +
                           lobKey.toString(), e);

                return false;
            } catch (IOException e) {

                logger.log(Level.SEVERE, "Error importing LOB with key " +
                           lobKey.toString(), e);

                return false;
            }

            return true;
        }
    }

    protected void induceImportChange(String when) {

        assert TestHookExecute.doHookIfSet(CHANGE_HOOK, when);
    }

    /******************* Abstract Methods **************************/

    /**
     * Imports the data/metadata from the abstract store to the target KVStore
     */
    abstract void doImport();

    /**
     * Imports the given tables schema and data from the abstract store to the
     * target KVStore
     */
    abstract void doTableImport(String[] tableNames);

    /**
     * Get the given LOB input stream from the abstract store
     */
    abstract InputStream getLobInputStream(String lobFileName);

    /**
     * Write the status file (list of file segments successfully imported) into
     * the abstract store
     */
    abstract void writeStatusFile(Set<String> fileSegments);

    /**
     * Sets the log handler
     */
    abstract void setLoggerHandler(Logger logger);

    /**
     * Flush all the log streams
     */
    abstract void flushLogs();
}

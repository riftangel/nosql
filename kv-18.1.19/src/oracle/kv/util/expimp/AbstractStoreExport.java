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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.Value.Format;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.ParallelScanIterator;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.StoreIteratorException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.avro.AvroDdl;
import oracle.kv.impl.api.avro.SchemaData;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.KVStoreLogin.CredentialsProvider;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.util.expimp.CustomStream.CustomInputStream;
import oracle.kv.util.expimp.CustomStream.CustomOutputStream;
import oracle.kv.util.expimp.ExitHandler.ExitCode;

/**
 * Abstract implementation of Export functionality which is used to move data
 * from Oracle NoSql database to export stores. Currently the export utility
 * supports Oracle Storage Cloud Service and Local File System as a valid
 * export store.
 *
 * Using the export utility, the user can export all the data/metadata to the
 * export store:
 * 1. Application created data (excluding security data) will be exported.
 * 2. Schema definitions are exported.
 *    a. Schema definitions are table definitions, avro schemas and the
 *       index definitions.
 *    b. The schema definitions are exported in json format.
 *    c. The exported table definitions will also include the table owner
 *       which is null if the table has no owner.
 * 3. TTL of every table record is exported.
 * 4. Derived data such as Index data and statistics will not be exported,
 *    but the schema definition includes index definitions, which can be used
 *    at import time.
 * 5. Instead of exporting the entire store, the user can choose to export
 *    individual tables, in which case the corresponding table schemas,
 *    index definitions and the table data will get exported.
 *
 * The export utility does not manage:
 * 1. Security data (such as user definitions) are excluded from the export.
 *    The user must load and manage identities independently of export.
 * 2. Store deployment information; the export utility focuses only on
 *    application data.
 * 3. Incremental export (exporting records since a given time) is not
 *    supported, since the underlying store cannot support this.
 *
 * Export Entire Store:  ParallelScanIterator is used to saturate the disks of
 * all the RNs in the KVStore. A fan out of the store iterator is performed
 * based on whether a data is Avro format, None format or if it belongs to
 * a particular Table. The data from each data format and tables are streamed
 * directly into the external export store. When we encounter a LOB key, get
 * the corresponding LOB data from KVStore using KVStore.getLOB(lobKeyString).
 * A separate thread is  spawned to stream the lob data into the Abstract Store.
 *
 * Export Individual Table(s): User can export a subset of tables from Oracle
 * NoSql Store to the external export store. KVStore.tableIterator with
 * ParallelScan capability is used to fetch all data from a particular table.
 * The data bytes are then streamed (spawned in a separate thread) into the
 * destination store. Along with table data, corresponding table definitions
 * and its index definitions are also exported.
 *
 * Exit code can be used as a diagnostic to determine the state of export. The
 * exit codes have the following meaning:
 *
 * 0   (EXIT_OK) -- No errors found during export.
 * 100 (EXIT_USAGE) -- Illegal export command usage.
 * 101 (EXIT_NOPERM) -- Unauthorized access to the Oracle Cloud Storage Service
 * 102 (EXIT_EXPSTR_NOCONNECT) -- The Oracle Cloud Storage Service could not be
 *      accessed using the service connection parameters
 * 103 (EXIT_NOCONNECT) -- The source NoSql store could not be connected using
 *      the given store-name and helper hosts.
 * 104 (EXIT_UNEXPECTED) -- The export utility experienced an unexpected error.
 * 105 (EXIT_NOWRITE) -- The export package has no write permissions.
 * 107 (EXIT_CONTAINER_EXISTS) -- The container already exists in the Object
 *      Store. Delete the container or use another container name.
 * 108 (EXIT_NO_EXPORT_FOLDER) -- Export folder with the given name does not
 *      exist.
 * 109 (EXIT_INVALID_EXPORT_STORE) -- Invalid export store type. Valid export
 *      stores are local and object_store.
 * 110 (EXIT_SECURITY_ERROR) -- Error loading security file.
 * 111 (EXIT_NOSQL_NOPERM) -- User has no read permissions on the object.
 */
public abstract class AbstractStoreExport implements CredentialsProvider {

    private final String storeName;
    private final String[] helperHosts;
    private final KVStoreConfig kconfig;
    private KVStore store;
    private final TableAPI tableAPI;
    private final KeySerializer keySerializer;
    private final AvroDdl avroDdl;

    /*
     * Map that holds all the RecordStreams responsible for streaming
     * data/metadata from kvstore to external export store
     */
    private final RecordStreamMap recordStreamMap;

    /*
     * Holds instances of all the topmost level tables in source NoSql store.
     * The key to the map is the unique table id (tableImpl.getIdString()).
     */
    private final Map<String, TableImpl> tableMap;

    /*
     * Mapping between the tables full name and table instances. Contains all
     * the tables in the source NoSql store.
     */
    private final Map<String, TableImpl> nameToTableMap;

    /*
     * Mapping between the tables full name and the version of the table at the
     * time of export
     */
    private final Map<String, Integer> exportedTableVersion;

    /*
     * Mapping between avro schema ID and the avro schema name. Holds all the
     * latest evolved versions of the avro schemas at the time of export.
     */
    private final Map<Integer, String> schemaIdToNameMap;

    /*
     * Mapping between avro schema name and the avro schema. Holds only the
     * highest evolved version of the avro schema (at the time of export).
     */
    private final Map<String, Integer> schemaNameToLatestIdMap;
    private final Map<String, SchemaData> schemaNameToLatestSchemaMap;

    /*
     * Read consistency used for export. Default is Time consistency with
     * permissible lag of 1 minute
     */
    private String consistencyType;

    /*
     * Permissible time lag when consistencyType is Time consistency
     */
    private long timeLag;

    /*
     * Cache of table reader and writer schemas
     */
    private final DataSchemaCache schemaCache;

    private final Map<String, FutureHolder> taskFutureMap;

    /*
     * Asynchronous thread pool
     */
    private final ExecutorService threadPool;

    /*
     * An ArrayBlockingQueue is used to synchronize the producer and consumers.
     * Holds the state of all the threads that are streaming the bytes from
     * Oracle NoSql store into the external export store.
     */
    BlockingQueue<FutureHolder> taskWaitQueue;

    /*
     * A utility thread that waits for tasks (stream data from source nosql
     * store to export store) to complete and aggregates status of the writer
     * threads for output and success/error reporting. If any task fails,
     * the taskWaiter will exit prematurely. The main thread must detect this.
     */
    Future<Boolean> taskWait;

    /*
     * Holds the state of all the threads that are streaming the lob bytes from
     * Oracle NoSql store into the external export store.
     */
    List<FutureHolder> lobTaskWaitList;

    /*
     * This is the maximum number of finished WriteTasks. Limit it to
     * avoid running out of memory if the producer is faster than the consumer.
     * With this limit, the maximum number of CustomStreams that are referenced
     * at any point are:
     * - 1 in-use CustomStream per export type fanout (that is, one per table,
     *   other data, schemafiles)
     * - TASK_QUEUE_SIZE finished, flushed CustomStreams.
     */
    private static final int TASK_QUEUE_SIZE = 40;

    /*
     * Index of the lob file being exported.
     */
    private int lobFileNumber = 0;

    /*
     * Identifiers for the data retrieved from the store iterator.
     */
    private static final String tableDataIdentifier = "T";
    private static final String avroDataIdentifier = "A";
    private static final String noneDataIdentifier = "N";
    private static final String lobDataReferenceIdentifier = "L";

    /*
     * Schema definitions (Avro and table schemas) will get exported in
     * SchemaDefinition file in the export package. Lob file references, AVRO
     * format, None format data will get exported in 'OtherData' file. The
     * table data will get exported in files whose name is same as the table
     * name. For example: Table EMPLOYEE data will get exported in EMPLOYEE file
     * in the export package.
     */
    private static final String schemaFileName = "SchemaDefinition";
    private static final String otherDataFile = "OtherData";

    /*
     * The size of the file segment stored in the export store set to 1GB
     */
    private static long maxFileSegmentSize = 1000 * 1000 * 1000;

    /*
     * Hook to induce schema changes. Used for testing
     */
    public static TestHook<String> CHANGE_HOOK;

    /*
     * Variables used only during testing
     */
    public static Boolean testFlag = false;
    private static final String testTable = "Table4";

    /*
     * Logger variables
     */
    private final Logger logger;
    private final LogManager logManager;

    /*
     *  Use for authentication
     */
    private KVStoreLogin storeLogin;
    private LoginCredentials loginCreds;

    /*
     * Boolean variable which determines if the export output from CLI should
     * be in json format
     */
    private final boolean json;

    /*
     * For test purpose
     */
    public static boolean printToConsole = true;

    class CustomLogManager extends LogManager {
        CustomLogManager() {
            super();
        }
    }

    /**
     * Set size of the file segment. Used only for test purposes.
     */
    public static void setMaxFileSegmentSize(long testSize) {
        maxFileSegmentSize = testSize;
    }

    /**
     * Exit export with the appropriate exit code.
     *
     * @param resetLogHandler if true resets the log handler.
     * @param exitCode
     * @param ps
     * @param message message to be displayed in CLI output on exit
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
             * Display export output in json format
             */
            ExitHandler.displayExitJson(ps, exitCode, message, "export");
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

    public AbstractStoreExport(String storeName,
                               String[] helperHosts,
                               String userName,
                               String securityFile,
                               boolean json) {

        this.storeName = storeName;
        this.helperHosts = helperHosts;
        this.json = json;
        logger = Logger.getLogger(AbstractStoreExport.class.getName() +
                 (int)(Math.random()*1000));
        logger.setUseParentHandlers(false);

        logManager = new CustomLogManager();
        logManager.reset();
        logManager.addLogger(logger);

        prepareAuthentication(userName, securityFile);

        kconfig = new KVStoreConfig(storeName, helperHosts);
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

        tableAPI = store.getTableAPI();

        recordStreamMap = new RecordStreamMap();
        tableMap = new HashMap<String, TableImpl>();
        nameToTableMap = new HashMap<String, TableImpl>();
        exportedTableVersion = new HashMap<String, Integer>();
        keySerializer = KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        schemaIdToNameMap = new HashMap<Integer, String>();
        schemaNameToLatestIdMap = new HashMap<String, Integer>();
        schemaNameToLatestSchemaMap = new HashMap<String, SchemaData>();
        avroDdl = new AvroDdl(store);
        schemaCache = new DataSchemaCache();
        consistencyType = "TIME";
        timeLag = 60000;

        threadPool = Executors.newCachedThreadPool(new KVThreadFactory("Export",
                                                                       null));

        taskFutureMap = new HashMap<String, FutureHolder>();

        taskWaitQueue = new ArrayBlockingQueue<FutureHolder>(TASK_QUEUE_SIZE);
        lobTaskWaitList =
            new ArrayList<FutureHolder>(TASK_QUEUE_SIZE);
        taskWait = threadPool.submit(new TaskWaiter());
    }

    public void setConsistencyType(String consistencyType) {
        this.consistencyType = consistencyType;
    }

    public void setTimeLag(int timeLag) {
        this.timeLag = timeLag;
    }

    public Consistency getConsistency() {

        switch (consistencyType) {

            case "ABSOLUTE":
                return Consistency.ABSOLUTE;
            case "NONE":
                return Consistency.NONE_REQUIRED;
            default:
                return new Consistency.Time(timeLag,
                                            TimeUnit.MILLISECONDS,
                                            10,
                                            TimeUnit.SECONDS);
        }
    }

    public boolean getJson() {
        return json;
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
     * Exports the schema definitions and the data of the specified tables.
     *
     * @param tableNames The name of the tables whose schema definition and
     * data will get exported.
     */
    public void exportTable(String[] tableNames) {

        StringBuilder hhosts = new StringBuilder();

        for (int i = 0; i < helperHosts.length - 1; i++) {
            hhosts.append(helperHosts[i] + ",");
        }

        hhosts.append(helperHosts[helperHosts.length - 1]);

        logger.info("Starting export of table(s) from store " + storeName +
            ", helperHosts=" + hhosts.toString());

        /*
         * Calculate start time of export in UTC
         */
        Date startDate = new Date();

        SimpleDateFormat simpleDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String startDateString = simpleDateFormat.format(startDate);

        /*
         * Hook to induce schema changes before exporting table schemas
         */
        assert TestHookExecute.doHookIfSet(CHANGE_HOOK, "TABLE,1");
        exportTableSchemas(tableNames);

        /*
         * Hook to induce schema changes after exporting table schemas
         */
        assert TestHookExecute.doHookIfSet(CHANGE_HOOK, "TABLE,2");

        Set<String> uniqueTables = new HashSet<String>();

        for (String tableName : tableNames) {

            TableImpl table = nameToTableMap.get(tableName);

            /*
             * If the table does not exist in the source nosql store, log the
             * message and continue exporting other table data.
             */
            if (table == null) {
                logger.warning("Table " + tableName +
                               " not present in the store");
                continue;
            }

            if (table.dontExport()) {
                continue;
            }

            if (!uniqueTables.add(tableName)) {
                logger.info("Data for table " + tableName +
                            " already exported. Skipping duplicate entry.");
                continue;
            }

            logger.info("Exporting " + tableName + " table data.");

            if (tableAPI.getTable(tableName) == null) {
                logger.warning("Table " + tableName + " deleted midway of " +
                               "export.");
                continue;
            }

            PrimaryKey pKey = table.createPrimaryKey();
            TableIteratorOptions iterateOptions =
                new TableIteratorOptions(Direction.UNORDERED,
                                         getConsistency(),
                                         0,
                                         null);

            TableAPIImpl tableAPIImpl = (TableAPIImpl)tableAPI;
            TableIterator<KeyValueVersion> tableIterator = null;

            try {

                tableIterator =
                    tableAPIImpl.tableKVIterator(pKey, null, iterateOptions);

                /*
                 * Induce schema changes when table iterator is importing data
                 * to export store. testFlag variable set to true only in tests
                 */
                if (testFlag == true) {
                    if (tableName.equals(testTable)) {
                        assert TestHookExecute
                            .doHookIfSet(CHANGE_HOOK, "TABLE,3");
                        testFlag = false;
                    }
                }

                /*
                 * Iterate over all the table data using table iterator and
                 * stream the bytes to the external export store.
                 *
                 * Format of the exported table record bytes:
                 * [RecordKeyLength RecordKey RecordValueLength RecordValueBytes
                 * Checksum]
                 */
                while (tableIterator.hasNext()) {

                    KeyValueVersion kvv = tableIterator.next();
                    long expirationTime = kvv.getExpirationTime();

                    long recordLength = 0;

                    Key key = kvv.getKey();
                    Value value = kvv.getValue();
                    Version version = kvv.getVersion();
                    Format valueFormat = value.getFormat();

                    byte[] keyBytes = keySerializer.toByteArray(key);
                    byte[] valueBytes = value.getValue();

                    int exportTableVersion =
                        exportedTableVersion.get(tableName);

                    if (valueBytes.length != 0 &&
                            (valueBytes[0] != exportTableVersion)) {

                        ValueVersion vv = new ValueVersion(value, version);
                        RowImpl rRow = table.createRowFromKeyBytes(keyBytes);
                        createExportRowFromValueVersion(
                            table,
                            rRow,
                            vv,
                            exportTableVersion);

                        Key rKey = rRow.getPrimaryKey(false);
                        Value rValue = rRow.createValue();

                        keyBytes = keySerializer.toByteArray(rKey);
                        valueBytes = rValue.getValue();
                        valueFormat = rValue.getFormat();
                    }

                    String keyLength = keyBytes.length + " ";
                    byte[] keyLengthBytes =
                        keyLength.getBytes(StandardCharsets.UTF_8);

                    recordLength += keyLengthBytes.length + keyBytes.length;

                    String valueLength = valueBytes.length + " ";
                    byte[] valueLengthBytes =
                        valueLength.getBytes(StandardCharsets.UTF_8);

                    recordLength += valueLengthBytes.length + valueBytes.length;

                    /* Value format */
                    byte[] valueFormatBytes =
                        new byte[]{(byte)valueFormat.ordinal()};
                    recordLength += 1;

                    /*
                     * Convert row expiration time in milliseconds to hours
                     */
                    int expiryTimeInHours =
                        (int)(expirationTime/(60 * 60 * 1000));

                    byte[] expiryTimePacked = null;
                    byte[] expiryBufLengthBytes = null;
                    byte[] zeroExpiryTimeLengthBytes = null;

                    if (expiryTimeInHours > 0) {
                        /*
                         * Convert row expiration time in hours to binary using
                         * PackedInteger. This allows the expiration time of
                         * record to be stored more compactly in the export
                         * package.
                         */
                        expiryTimePacked =
                            new byte[PackedInteger
                                     .getWriteIntLength(expiryTimeInHours)];
                        PackedInteger.writeInt(expiryTimePacked,
                                               0,
                                               expiryTimeInHours);

                        String expiryBufLength = expiryTimePacked.length + " ";
                        expiryBufLengthBytes =
                            expiryBufLength.getBytes(StandardCharsets.UTF_8);


                        recordLength += expiryBufLengthBytes.length +
                                        expiryTimePacked.length;
                    } else {
                        String zeroExpirationEntry = expiryTimeInHours + " ";
                        zeroExpiryTimeLengthBytes =
                            zeroExpirationEntry
                            .getBytes(StandardCharsets.UTF_8);
                        recordLength += zeroExpiryTimeLengthBytes.length;
                    }

                    /*
                     * Calculate the checksum of the record bytes.
                     */
                    long checkSum = Utilities.getChecksum(keyBytes, valueBytes);

                    /*
                     * Convert the checksum in long format to binary using
                     * PackedInteger
                     */
                    byte[] checkSumPacked =
                        new byte[PackedInteger.getWriteLongLength(checkSum)];
                    PackedInteger.writeLong(checkSumPacked, 0, checkSum);

                    String checksumLength = checkSumPacked.length + " ";
                    byte[] checksumLengthBytes =
                        checksumLength.getBytes(StandardCharsets.UTF_8);

                    recordLength +=
                        checksumLengthBytes.length + checkSumPacked.length;

                    /*
                     * Check if the record bytes can be transferred to record
                     * stream
                     */
                    recordStreamMap.canAddRecord(tableName, recordLength);

                    /*
                     * Stream the table data bytes to the external export store.
                     */
                    recordStreamMap.insert(tableName, keyLengthBytes);
                    recordStreamMap.insert(tableName, keyBytes);
                    recordStreamMap.insert(tableName, valueLengthBytes);
                    recordStreamMap.insert(tableName, valueBytes);
                    recordStreamMap.insert(tableName, valueFormatBytes);
                    if (expiryTimeInHours > 0) {
                        recordStreamMap.insert(tableName, expiryBufLengthBytes);
                        recordStreamMap.insert(tableName, expiryTimePacked);
                    } else {
                        recordStreamMap.insert(tableName,
                                            zeroExpiryTimeLengthBytes);
                    }
                    recordStreamMap.insert(tableName, checksumLengthBytes);
                    recordStreamMap.insert(tableName, checkSumPacked);

                }
            } catch (RequestTimeoutException rte) {
                logger.log(Level.SEVERE, "Table iterator experienced an " +
                           "Operation Timeout. Check the network " +
                           "connectivity to the shard.", rte);
                exit(true, ExitCode.EXIT_UNEXPECTED, System.err, null);
            } catch(StoreIteratorException sie) {
                logger.log(Level.WARNING, "Table " + tableName +
                           " either dropped midway of export or the user " +
                           "does not have read privilege on the table.", sie);
            } catch (RuntimeException e) {
                logger.log(Level.SEVERE, "User has no read permissions on " +
                           "the table " + tableName, e);
                exit(true, ExitCode.EXIT_NOSQL_NOPERM, System.err, null);
            } finally {
                if (tableIterator != null) {
                    tableIterator.close();
                }
            }
        }

        /*
         * Flush any remaining bytes in all the streams to the external export
         * store.
         */
        recordStreamMap.flushAllStreams();

        /*
         * Wait for all the threads streaming the data to the external export
         * store to complete.
         */
        try {
            waitForTasks();
        } catch (Exception e) {
            exit(true, ExitCode.EXIT_UNEXPECTED, System.err, null);
        }

        recordStreamMap.performPostExportWork();
        doTableMetadataDiff();

        /*
         * Calculate end time of export in UTC
         */
        Date endDate = new Date();
        String endDateString = simpleDateFormat.format(endDate);

        StringBuilder exportStat = new StringBuilder();

        exportStat.append("StoreName: " + storeName + "\n");
        exportStat.append("Helper Hosts: " + hhosts.toString() + "\n");
        exportStat.append("Export start time: " + startDateString + "\n");
        exportStat.append("Export end time: " + endDateString + "\n");

        generateExportStats(exportStat.toString());

        String expCompleteMsg = "Completed exporting table(s) from store " +
            storeName + ", helperHosts=" + hhosts.toString();
        logger.info(expCompleteMsg);

        exit(true, ExitCode.EXIT_OK, System.out, expCompleteMsg);
    }

    /**
     * Exports the table schema definitions in json format.
     *
     * @param tableNames The names of the tables whose schema definitions will
     *        be exported.
     */
    private void exportTableSchemas(String[] tableNames) {

        for (String tableName : tableNames) {

            if (nameToTableMap.containsKey(tableName)) {

                logger.info("Table schema with the name " + tableName +
                            " already exported. Skipping duplicate entry.");
                continue;
            }

            TableImpl tableImpl = (TableImpl)tableAPI.getTable(tableName);

            /*
             * If the table does not exist in the source nosql store, log the
             * message and continue exporting remaining table schemas.
             */
            if (tableImpl == null) {

                logger.warning("Table with the name " + tableName +
                               " does not exists in the store.");
                continue;
            }

            if (tableImpl.dontExport()) {
                continue;
            }

            nameToTableMap.put(tableName, tableImpl);

            String tableJson = "";
            int tableVersionBefore = 0;
            int tableVersionAfter = 0;

            /*
             * Ensure the table version did not change while retrieving the
             * table schema definition.
             */
            do {
                tableVersionBefore = tableImpl.numTableVersions();
                tableJson = tableImpl.toJsonString(false);
                tableVersionAfter = tableImpl.numTableVersions();
            } while (tableVersionBefore != tableVersionAfter);

            logger.info("Exporting table schema: " + tableName +
                        ". TableVersion: " + tableVersionAfter);

            exportedTableVersion.put(tableName, tableVersionAfter);

            String tableSchemaRecord = tableDataIdentifier + ", " +
                                       tableName + ": " +
                                       tableJson + "\n";

            byte[] tableSchemaBytes =
                tableSchemaRecord.getBytes(StandardCharsets.UTF_8);

            /*
             * Check if the schema bytes can be transferred to the record
             * stream
             */
            recordStreamMap.canAddRecord(schemaFileName,
                                         tableSchemaBytes.length);

            /*
             * Stream the schema bytes to the external export store
             */
            recordStreamMap.insert(schemaFileName, tableSchemaBytes);
        }
    }

    /**
     * Exports all the data/metadata from the Oracle NoSql store to the export
     * store. Application created data (excluding security data) is exported.
     * The metadata exported includes table schemas, avro schemas and table
     * index definitions.
     */
    @SuppressWarnings("deprecation")
    public void export() {

        StringBuilder hhosts = new StringBuilder();

        for (int i = 0; i < helperHosts.length - 1; i++) {
            hhosts.append(helperHosts[i] + ",");
        }

        hhosts.append(helperHosts[helperHosts.length - 1]);

        logger.info("Starting export of store " + storeName +
                    ", helperHosts=" + hhosts.toString());

        /*
         * Calculate start time of export in UTC
         */
        Date startDate = new Date();

        SimpleDateFormat simpleDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String startDateString = simpleDateFormat.format(startDate);

        /*
         * Hook to induce schema changes before exporting table schemas
         */
        assert TestHookExecute.doHookIfSet(CHANGE_HOOK, "TABLE,1");
        exportTableSchemas();

        /*
         * Hook to induce schema changes after exporting table schemas
         */
        assert TestHookExecute.doHookIfSet(CHANGE_HOOK, "TABLE,2");

        /*
         * Hook to induce schema changes before exporting avro schemas
         */
        assert TestHookExecute.doHookIfSet(CHANGE_HOOK, "AVRO,1");
        exportAvroSchemas();

        /*
         * Hook to induce schema changes after exporting avro schemas
         */
        assert TestHookExecute.doHookIfSet(CHANGE_HOOK, "AVRO,2");

        logger.info("Exporting all the data. Retrieving all the records from " +
                    "the KVStore using ParallelScanIterator");

        /*
         * Use multi-threading for store iteration and limit the number
         * of threads (degree of parallelism) to 5.
         */
        final StoreIteratorConfig sc = new StoreIteratorConfig().
            setMaxConcurrentRequests(5);

         /*
          * ParallelScanIterator used to saturate the disks of all the RNS in
          * the KVStore.
          */
        ParallelScanIterator<KeyValueVersion> iter =
            store.storeIterator(Direction.UNORDERED,
                                0,
                                null,
                                null,
                                null,
                                getConsistency(),
                                0,
                                null,
                                sc);

        try {
            while (iter.hasNext()) {

                KeyValueVersion kvversion = iter.next();
                Key key = kvversion.getKey();

                Value value = kvversion.getValue();
                Version version = kvversion.getVersion();
                ValueVersion vv = new ValueVersion(value, version);

                /*
                 * If value length is 0, there is no format byte that allows us
                 * to find the format of data. Use table membership to check if
                 * key only data belongs to a key only table.
                 */
                if (value.getValue().length == 0) {

                    /*
                     * Try to export the key only data assuming it is belongs to
                     * a key only table. Returns 0 if it belonged to a key only
                     * table.
                     */
                    int retValue =
                        exportTableRecord(key,
                                          vv,
                                          kvversion.getExpirationTime());

                    /*
                     * If -1 is returned, key only data belongs to a NONE format
                     * key only data. Export it as a NONE format data.
                     */
                    if (retValue == -1) {
                        exportNoneFormatRecord(key, value);
                    }

                    continue;
                }

                /*
                 * Fan out the store iterator depending on the format of the
                 * value: Table format, None format and Avro format.
                 */
                if (Format.isTableFormat(value.getFormat())) {
                    exportTableRecord(key, vv, kvversion.getExpirationTime());
                } else if (value.getFormat().equals(Value.Format.NONE)){
                    exportNoneFormatRecord(key, value);
                } else if (value.getFormat().equals(Value.Format.AVRO)) {
                    exportAvroRecord(key, value);
                }
            }
        } catch (RequestTimeoutException rte) {
            logger.log(Level.SEVERE, "Store iterator experienced an " +
                       "Operation Timeout. Check the following:\n1) " +
                       "Network connectivity to the shard is good.\n2) " +
                       "User executing the export has READ_ANY " +
                       "privilege.", rte);
            exit(true, ExitCode.EXIT_UNEXPECTED, System.err, null);
        } finally {
            iter.close();
        }

        logger.info("All the records retrieved from the KVStore.");

        /*
         * At this point all the data bytes in the kvstore has been transfered
         * to the RecordStream. Need to wait for all the worker threads that
         * are transferring the data bytes to the export store to complete their
         * execution.
         */

        /*
         * LOB bytes are streamed to the export store in a separate thread.
         * Wait for these threads to complete transferring the LOB bytes to the
         * RecordStream.
         */
        try {
            waitForLobTasks();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception exporting the lob to " +
                       "external export store.", e);
        }

        /*
         * Flush any remaining bytes in the RecordStream.
         */
        recordStreamMap.flushAllStreams();

        /*
         * Wait for the worker threads to transfer all the bytes in the record
         * stream to the export store.
         */
        try {
            waitForTasks();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception exporting the data to " +
                       "external export store. Halting export.", e);
            exit(true, ExitCode.EXIT_UNEXPECTED, System.err, null);
        }

        recordStreamMap.performPostExportWork();

        /*
         * Perform a metadata (table and avro schema) diff at the end of export
         * to check if there has been any metadata change. Report any metadata
         * change as warnings to the user.
         */
        doMetadataDiff();

        /*
         * Calculate end time of export in UTC
         */
        Date endDate = new Date();
        String endDateString = simpleDateFormat.format(endDate);

        String expCompleteMsg = "Completed export of store " + storeName +
                                ", helperHosts=" + hhosts.toString();

        StringBuilder exportStat = new StringBuilder();

        exportStat.append("StoreName: " + storeName + "\n");
        exportStat.append("Helper Hosts: " + hhosts.toString() + "\n");
        exportStat.append("Export start time: " + startDateString + "\n");
        exportStat.append("Export end time: " + endDateString + "\n");

        generateExportStats(exportStat.toString());

        logger.info(expCompleteMsg);

        exit(true, ExitCode.EXIT_OK, System.out, expCompleteMsg);
    }

    /**
     * Exports table schema definitions in json format
     */
    private void exportTableSchemas() {

        /*
         * Export all the top level table schema definitions
         */
        for (Map.Entry<String, Table> entry :
                tableAPI.getTables().entrySet()) {

            TableImpl tableImpl = (TableImpl) entry.getValue();

            if (tableImpl.dontExport()) {
                continue;
            }

            tableMap.put(tableImpl.getIdString(), tableImpl);

            String tableJson = "";
            int tableVersionBefore = 0;
            int tableVersionAfter = 0;

            /*
             * Ensure the table version did not change while retrieving the
             * table schema definition.
             */
            do {
                tableVersionBefore = tableImpl.numTableVersions();
                tableJson = tableImpl.toJsonString(false);
                tableVersionAfter = tableImpl.numTableVersions();
            } while (tableVersionBefore != tableVersionAfter);

            String tableFullName = tableImpl.getFullName();
            exportedTableVersion.put(tableFullName, tableVersionAfter);

            logger.info("Exporting table schema: " + tableFullName +
                        ". TableVersion: " + tableVersionAfter);

            String tableSchema = tableDataIdentifier + ", " +
                                 tableFullName + ": " +
                                 tableJson + "\n";

            byte[] tableSchemaBytes =
                tableSchema.getBytes(StandardCharsets.UTF_8);

            /*
             * Check if the schema bytes can be transferred to RecordStream
             */
            recordStreamMap.canAddRecord(schemaFileName,
                                         tableSchemaBytes.length);

            /*
             * Stream the schema bytes to the external export store
             */
            recordStreamMap.insert(schemaFileName, tableSchemaBytes);

            /*
             * If this table has any child tables, export the child table
             * schema definitions.
             */
            exportChildTableSchemas(tableImpl);
        }
    }

    /**
     * Exports given tables child table schemas in json format
     */
    private void exportChildTableSchemas(TableImpl parent) {

        for (Map.Entry<String, Table> entry :
                parent.getChildTables().entrySet()) {

            TableImpl tableImpl = (TableImpl) entry.getValue();

            if (tableImpl.dontExport()) {
                continue;
            }

            String tableJson = "";
            int tableVersionBefore = 0;
            int tableVersionAfter = 0;

            /*
             * Ensure the table version did not change while retrieving the
             * table schema definition.
             */
            do {
                tableVersionBefore = tableImpl.numTableVersions();
                tableJson = tableImpl.toJsonString(false);
                tableVersionAfter = tableImpl.numTableVersions();
            } while (tableVersionBefore != tableVersionAfter);

            String tableFullName = tableImpl.getFullName();
            exportedTableVersion.put(tableFullName, tableVersionAfter);

            logger.info("Exporting table schema: " + tableFullName +
                        ". TableVersion: " + tableVersionAfter);

            String tableSchema = tableDataIdentifier + ", " +
                                 tableFullName + ": " +
                                 tableJson + "\n";

            byte[] tableSchemaBytes =
                tableSchema.getBytes(StandardCharsets.UTF_8);

            /*
             * Check if the schema bytes can be transferred to RecordStream
             */
            recordStreamMap.canAddRecord(schemaFileName,
                                         tableSchemaBytes.length);

            /*
             * Stream the schema bytes to the external export store
             */
            recordStreamMap.insert(schemaFileName, tableSchemaBytes);

            /*
             * If this table has any child tables, export the child table
             * schema definitions recursively.
             */
            exportChildTableSchemas(tableImpl);
        }
    }

    /**
     * Exports Avro schema definitions in json format
     */
    private void exportAvroSchemas() {

        /*
         * Retrieve all the avro schemas (including disabled schemas) from
         * the kvstore
         */
        SortedMap<Integer, SchemaData> schemas = avroDdl.getAllSchemas(true);

        for (Map.Entry<Integer, SchemaData> entry : schemas.entrySet()) {
            int schemaId = entry.getKey();
            SchemaData schemaData = entry.getValue();
            Schema schema = schemaData.getSchema();

            String schemaName = schema.getFullName();
            schemaIdToNameMap.put(schemaId, schemaName);

            /*
             * A schema can evolve any number of times. Keep track of the latest
             * schema id for a given schemaName.
             */
            Integer latestSchemaId = schemaNameToLatestIdMap.get(schemaName);

            if (latestSchemaId == null || schemaId > latestSchemaId) {
                schemaNameToLatestIdMap.put(schemaName, schemaId);
                schemaNameToLatestSchemaMap.put(schemaName, schemaData);
            }
        }

        /*
         * Export the most recent evolved version of all the avro schemas.
         */
        for (Map.Entry<String, SchemaData> entry :
                schemaNameToLatestSchemaMap.entrySet()) {

            String schemaName = entry.getKey();
            SchemaData schemaData = entry.getValue();

            Integer schemaId = schemaNameToLatestIdMap.get(schemaName);
            Schema schema = schemaData.getSchema();
            String code = schemaData.getMetadata().getStatus().getCode();

            logger.info("Exporting Avro schema: " + schemaName +
                        ". SchemaId: " + schemaId);

            String avroSchema = avroDataIdentifier + ", " +
                                schemaId + ", " +
                                code + ": " +
                                schema.toString() + "\n";

            byte[] avroSchemaBytes =
                avroSchema.getBytes(StandardCharsets.UTF_8);

            /*
             * Check if the schema bytes can be transferred to RecordStream
             */
            recordStreamMap.canAddRecord(schemaFileName,
                                         avroSchemaBytes.length);

            /*
             * Stream the schema bytes to the external export store
             */
            recordStreamMap.insert(schemaFileName, avroSchemaBytes);
        }
    }

    /**
     * Exports the avro format record. Key and value are exported as bytes.
     *
     * Format of the exported Avro record bytes:
     * [A ReaderSchemaId RecordKeyLength RecordKeyBytes RecordValueLength
     * RecordValueBytes ChecksumBytesLength ChecksumBytes] where 'A' denotes
     * this is a Avro record
     */
    private void exportAvroRecord(Key key, Value value) {

        long recordLength = 0;

        String identifier = avroDataIdentifier;
        byte[] identifierBytes = identifier.getBytes(StandardCharsets.UTF_8);

        recordLength += identifierBytes.length;

        byte[] keyBytes = key.toByteArray();
        String keyLength = keyBytes.length + " ";
        byte[] keyLengthBytes = keyLength.getBytes(StandardCharsets.UTF_8);

        recordLength += keyLengthBytes.length + keyBytes.length;

        byte[] valueBytes = value.getValue();
        String valueLength = valueBytes.length + " ";
        byte[] valueLengthBytes = valueLength.getBytes(StandardCharsets.UTF_8);

        recordLength += valueLengthBytes.length + valueBytes.length;

        /*
         * Get the schemaId of the avro schema that wrote this record.
         */
        int writerSchemaId = PackedInteger.readSortedInt(valueBytes, 0);

        /*
         * Get the schemaName of the avro schema that wrote this record.
         */
        String schemaName = schemaIdToNameMap.get(writerSchemaId);

        /*
         * Avro record schema not found. Probably the schema was loaded post
         * avro schema export. Ignore such records.
         */
        if (schemaName == null) {
            return;
        }

        /*
         * Get the schema Id of this schema at the time of export. The
         * readerSchemaId can be different from writerSchemaId if this schema
         * went through schema evolution.
         */
        int readerSchemaId = schemaNameToLatestIdMap.get(schemaName);

        String schemaString = readerSchemaId + " ";
        byte[] schemaStringBytes =
            schemaString.getBytes(StandardCharsets.UTF_8);

        recordLength += schemaStringBytes.length;

        /*
         * Calculate the checksum of the record bytes.
         */
        long checkSum = Utilities.getChecksum(keyBytes, valueBytes);

        /*
         * Convert the checksum in long format to binary using PackedInteger
         */
        byte[] checkSumPacked =
            new byte[PackedInteger.getWriteLongLength(checkSum)];
        PackedInteger.writeLong(checkSumPacked, 0, checkSum);

        String checksumLength = checkSumPacked.length + " ";
        byte[] checksumLengthBytes =
            checksumLength.getBytes(StandardCharsets.UTF_8);

        recordLength +=
            checksumLengthBytes.length + checkSumPacked.length;

        /*
         * Check if the record bytes can be transferred to RecordStream
         */
        recordStreamMap.canAddRecord(otherDataFile, recordLength);

        /*
         * Stream the avro record bytes to the external export store.
         */
        recordStreamMap.insert(otherDataFile, identifierBytes);
        recordStreamMap.insert(otherDataFile, schemaStringBytes);
        recordStreamMap.insert(otherDataFile, keyLengthBytes);
        recordStreamMap.insert(otherDataFile, keyBytes);
        recordStreamMap.insert(otherDataFile, valueLengthBytes);
        recordStreamMap.insert(otherDataFile, valueBytes);
        recordStreamMap.insert(otherDataFile, checksumLengthBytes);
        recordStreamMap.insert(otherDataFile, checkSumPacked);
    }

    /**
     * Exports the None format record and LOB file reference.
     * Key and value are exported as bytes.
     *
     * Format of the exported None format record bytes:
     * [N RecordKeyLength RecordKeyBytes RecordValueLength RecordValueBytes
     * ChecksumBytesLength ChecksumBytes] where 'N' denotes this is a
     * None format record.
     *
     * Format of the exported LOB file reference:
     * [L LOBKeyLength LOBKeyBytes LOBFileNameLength LOBFileName
     * ChecksumBytesLength ChecksumBytes] where 'L' signifies this is a LOB
     * file reference.
     */
    private void exportNoneFormatRecord(Key key, Value value) {

        long recordLength = 0;

        String identifier = noneDataIdentifier;
        byte[] identifierBytes = identifier.getBytes(StandardCharsets.UTF_8);

        byte[] keyBytes = key.toByteArray();
        String keyLength = keyBytes.length + " ";
        byte[] keyLengthBytes = keyLength.getBytes(StandardCharsets.UTF_8);

        byte[] valueBytes = value.getValue();
        String valueLength = valueBytes.length + " ";
        byte[] valueLengthBytes = valueLength.getBytes(StandardCharsets.UTF_8);

        String lobName = "";
        String keyString = key.toString();

        /*
         * Check if the key has a .lob suffix. If true, this key references
         * LOB data.
         */
        if (keyString.endsWith(kconfig.getLOBSuffix())) {

            lobName = "LOBFile" + ++lobFileNumber;

            identifier = lobDataReferenceIdentifier;
            identifierBytes = identifier.getBytes(StandardCharsets.UTF_8);

            valueBytes = lobName.getBytes(StandardCharsets.UTF_8);
            valueLength = valueBytes.length + " ";
            valueLengthBytes = valueLength.getBytes(StandardCharsets.UTF_8);
        }

        recordLength += identifierBytes.length;
        recordLength += keyLengthBytes.length + keyBytes.length;
        recordLength += valueLengthBytes.length + valueBytes.length;

        /*
         * Calculate the checksum of the record bytes.
         */
        long checkSum = Utilities.getChecksum(keyBytes, valueBytes);

        /*
         * Convert the checksum in long format to binary using PackedInteger
         */
        byte[] checkSumPacked =
            new byte[PackedInteger.getWriteLongLength(checkSum)];
        PackedInteger.writeLong(checkSumPacked, 0, checkSum);

        String checksumLength = checkSumPacked.length + " ";
        byte[] checksumLengthBytes =
            checksumLength.getBytes(StandardCharsets.UTF_8);

        recordLength += checksumLengthBytes.length + checkSumPacked.length;

        /*
         * Check if the record bytes can be transferred to RecordStream
         */
        recordStreamMap.canAddRecord(otherDataFile, recordLength);

        /*
         * Stream the record bytes to the external export store.
         */
        recordStreamMap.insert(otherDataFile, identifierBytes);
        recordStreamMap.insert(otherDataFile, keyLengthBytes);
        recordStreamMap.insert(otherDataFile, keyBytes);
        recordStreamMap.insert(otherDataFile, valueLengthBytes);
        recordStreamMap.insert(otherDataFile, valueBytes);
        recordStreamMap.insert(otherDataFile, checksumLengthBytes);
        recordStreamMap.insert(otherDataFile, checkSumPacked);

        /*
         * If this is a LOB key, spawn a separate thread to transfer the LOB
         * data bytes from Oracle NoSql store to export store.
         */
        if (keyString.endsWith(kconfig.getLOBSuffix())) {

            logger.info("LOB found. Spawning a thread to export the LOB " +
                        lobName);

            InputStreamVersion istreamVersion =
                store.getLOB(key,
                             Consistency.NONE_REQUIRED,
                             5,
                             TimeUnit.SECONDS);

            InputStream stream = istreamVersion.getInputStream();

            Future<Boolean> future =
                threadPool.submit(new WriteLobTask(lobName, stream));

            lobTaskWaitList.add(new FutureHolder(future));
        }
    }

    /**
     * Exports the table record. Key and value are exported as bytes.
     *
     * Format of the exported Table record bytes:
     * [RecordKeyLength RecordKeyBytes RecordValueLength RecordValueBytes
     * ExpirationTimeBytesLength ExpirationTimeInBytes ChecksumBytesLength
     * ChecksumBytes]
     */
    private int exportTableRecord(Key key,
                                  ValueVersion vv,
                                  long expirationTime) {

        /*
         * keyIter is used to iterate over the fields in key. The first field
         * in key is the IdString of the top most level table
         */
        BinaryKeyIterator keyIter = new BinaryKeyIterator(key.toByteArray());

        /*
         * Get hold of the top most level table
         */
        String tableIdString = keyIter.next();
        TableImpl topTable = tableMap.get(tableIdString);

        /*
         * Check if table data introduced after export was started. If true
         * such records wont get exported
         */
        if (topTable == null) {
            return -1;
        }

        TableImpl tableToUse = topTable;

        /*
         * Iterate over the key to get hold of the table representing this
         * record to be exported.
         */
        while (!keyIter.atEndOfKey()) {

            int numComponentsToSkip = tableToUse.getPrimaryKeySize();

            if (tableToUse.getParent() != null) {
                TableImpl parentTable = (TableImpl) tableToUse.getParent();
                numComponentsToSkip -= parentTable.getPrimaryKeySize();
            }

            for (int i = 0; i < numComponentsToSkip; i++) {
                keyIter.next();
            }

            if (keyIter.atEndOfKey()) {
                break;
            }

            /*
             * Get the next level child tableId in this key
             */
            final String tableId = keyIter.next();

            /*
             * Get hold of the child table whose table id is tableId
             */
            for (Map.Entry<String, Table> entry : tableToUse.getChildTables()
                    .entrySet()) {

                TableImpl childTable = (TableImpl)entry.getValue();

                if (childTable.getIdString().equals(tableId)) {
                    tableToUse = childTable;
                    break;
                }
            }

            if (tableToUse.equals(topTable)) {
                return -1;
            }
        }

        if (tableToUse.dontExport()) {
            return 0;
        }

        /*
         * Induce schema changes when the store iterator is importing data to
         * export store
         */
        if (testFlag == true) {
            if (tableToUse.getFullName().equals(testTable)) {
                assert TestHookExecute.doHookIfSet(CHANGE_HOOK, "TABLE,3");
                testFlag = false;
            }
        }

        String tableName = tableToUse.getFullName();

        byte[] keyBytes = keySerializer.toByteArray(key);
        byte[] valueBytes = vv.getValue().getValue();
        Value.Format valueFormat = vv.getValue().getFormat();
        int exportTableVersion = exportedTableVersion.get(tableName);

        /*
         * Empty values still need to be used because the current schema
         * may have added fields which need to be defaulted.
         */
        if (valueBytes.length == 0 || (valueBytes[0] != exportTableVersion)) {
            RowImpl row = tableToUse.createRowFromKeyBytes(keyBytes);
            createExportRowFromValueVersion(tableToUse,
                                            row,
                                            vv,
                                            exportTableVersion);

            Key rKey = row.getPrimaryKey(false);
            Value rValue = row.createValue();

            keyBytes = keySerializer.toByteArray(rKey);
            valueBytes = rValue.getValue();
            valueFormat = rValue.getFormat();
        }

        String keyLength = keyBytes.length + " ";
        byte[] keyLengthBytes = keyLength.getBytes(StandardCharsets.UTF_8);

        long recordLength = 0;
        recordLength += keyLengthBytes.length + keyBytes.length;

        String valueLength = valueBytes.length + " ";
        byte[] valueLengthBytes = valueLength.getBytes(StandardCharsets.UTF_8);

        recordLength += valueLengthBytes.length + valueBytes.length;

        /* Value format */
        byte[] valueFormatBytes = new byte[] {(byte)valueFormat.ordinal()};
        recordLength += 1;

        /*
         * Convert row expiration time in milliseconds to hours
         */
        int expiryTimeInHours = (int)(expirationTime/(60 * 60 * 1000));
        byte[] expiryTimePacked = null;
        byte[] expiryBufLengthBytes = null;
        byte[] zeroExpiryTimeLengthBytes = null;

        if (expiryTimeInHours > 0) {
            /*
             * Convert row expiration time in hours to binary using
             * PackedInteger. This allows the expiration time of record to be
             * stored more compactly in the export package.
             */
            expiryTimePacked =
                new byte[PackedInteger.getWriteIntLength(expiryTimeInHours)];
            PackedInteger.writeInt(expiryTimePacked, 0, expiryTimeInHours);

            String expiryBufLength = expiryTimePacked.length + " ";
            expiryBufLengthBytes =
                expiryBufLength.getBytes(StandardCharsets.UTF_8);


            recordLength += expiryBufLengthBytes.length +
                            expiryTimePacked.length;
        } else {
            String zeroExpirationEntry = expiryTimeInHours + " ";
            zeroExpiryTimeLengthBytes =
                zeroExpirationEntry.getBytes(StandardCharsets.UTF_8);
            recordLength += zeroExpiryTimeLengthBytes.length;
        }

        /*
         * Calculate the checksum of the record bytes
         */
        long checkSum = Utilities.getChecksum(keyBytes, valueBytes);

        /*
         * Convert the checksum in long format to binary using PackedInteger
         */
        byte[] checkSumPacked =
            new byte[PackedInteger.getWriteLongLength(checkSum)];
        PackedInteger.writeLong(checkSumPacked, 0, checkSum);

        String checksumLength = checkSumPacked.length + " ";
        byte[] checksumLengthBytes =
            checksumLength.getBytes(StandardCharsets.UTF_8);

        recordLength += checksumLengthBytes.length + checkSumPacked.length;

        /*
         * Check if the record bytes can be transferred to RecordStream
         */
        recordStreamMap.canAddRecord(tableName, recordLength);

        /*
         * Stream the record bytes to the external export store.
         */
        recordStreamMap.insert(tableName, keyLengthBytes);
        recordStreamMap.insert(tableName, keyBytes);
        recordStreamMap.insert(tableName, valueLengthBytes);
        recordStreamMap.insert(tableName, valueBytes);
        recordStreamMap.insert(tableName, valueFormatBytes);
        if (expiryTimeInHours > 0) {
            recordStreamMap.insert(tableName, expiryBufLengthBytes);
            recordStreamMap.insert(tableName, expiryTimePacked);
        } else {
            recordStreamMap.insert(tableName, zeroExpiryTimeLengthBytes);
        }
        recordStreamMap.insert(tableName, checksumLengthBytes);
        recordStreamMap.insert(tableName, checkSumPacked);

        return 0;
    }

    /**
     * Performs a table metadata diff. This method checks if the any of the
     * exported table metadata has been deleted or altered at the end of
     * export.
     */
    public void doTableMetadataDiff() {

        logger.info("Generating Metadata Difference.");

        /*
         * Holds all the deleted tables at the end of export
         */
        List<String> deletedTables = new ArrayList<String>();

        /*
         * Holds all the tables that have been altered.
         */
        List<String> evolvedTables = new ArrayList<String>();

        for (Map.Entry<String, Integer> entry :
                exportedTableVersion.entrySet()) {

            String tableName = entry.getKey();
            Table table = tableAPI.getTable(tableName);

            /*
             * Exported table no longer present in the KVStore
             */
            if (table == null) {
                deletedTables.add(tableName);
                continue;
            }

            int exportTableVersion = entry.getValue();
            int postExportTableVersion = table.numTableVersions();

            /*
             * Table version mismatch during export and at the end of export.
             */
            if (exportTableVersion != postExportTableVersion) {
                evolvedTables.add(tableName);
            }
        }

        if (deletedTables.isEmpty() && evolvedTables.isEmpty()) {

            logger.info("Exported Table Metadata did " +
                        "not change during export!");
            return;
        }

        StringBuilder message = new StringBuilder();
        message.append("Metadata change found!\n");

        if (!deletedTables.isEmpty()) {
            message.append("Table deleted:\n");

            for (String tableName : deletedTables) {
                message.append(tableName + " ");
            }
            message.append("\n");
        }

        if (!evolvedTables.isEmpty()) {
            message.append("Table evolved:\n");

            for (String tableName : evolvedTables) {
                message.append(tableName + " ");
            }
            message.append("\n");
        }

        logger.info(message.toString());
    }

    private void doChildTableMetadataDiff(TableImpl parent,
                                          List<String> addedTables) {

        for (Map.Entry<String, Table> entry :
                parent.getChildTables().entrySet()) {

            TableImpl tableImpl = (TableImpl)entry.getValue();
            String tableName = tableImpl.getFullName();

            if (!exportedTableVersion.containsKey(tableName)) {
                addedTables.add(tableName);
            }

            doChildTableMetadataDiff(tableImpl, addedTables);
        }
    }

    /**
     * Performs table and avro schema metadata diff. This method checks if any
     * of the avro and/or table schemas have been evolved during or at the end
     * of export. It also checks if any new avro and/or table schemas have been
     * added. For tables it checks if any of the exported tables have been
     * deleted.
     */
    public void doMetadataDiff() {

        logger.info("Generating Metadata Difference.");

        /*
         * Holds all the deleted tables at the end of export
         */
        List<String> deletedTables = new ArrayList<String>();

        /*
         * Holds all the tables that have been altered since export started
         */
        List<String> evolvedTables = new ArrayList<String>();

        /*
         * Holds all the tables that have been added since export started
         */
        List<String> addedTables = new ArrayList<String>();

        /*
         * Holds all the avro schemas that have evolved since export started
         */
        List<String> evolvedAvroSchemas = new ArrayList<String>();

        /*
         * Holds all the avro schemas that have been added since export started
         */
        List<String> addedAvroSchemas = new ArrayList<String>();

        SortedMap<Integer, SchemaData> schemas = avroDdl.getAllSchemas(true);

        /*
         * Find all the tables that have been deleted and/or evolved since
         * export started
         */
        for (Map.Entry<String, Integer> entry :
                exportedTableVersion.entrySet()) {

            String tableName = entry.getKey();

            Table table = tableAPI.getTable(tableName);

            if (table == null) {
                deletedTables.add(tableName);
                continue;
            }

            int exportTableVersion = entry.getValue();
            int postExportTableVersion = table.numTableVersions();

            if (exportTableVersion != postExportTableVersion) {
                evolvedTables.add(tableName);
            }
        }

        /*
         * Find all the tables that have been added since export started
         */
        for (Map.Entry<String, Table> entry : tableAPI.getTables().entrySet()) {

            String tableName = entry.getKey();
            TableImpl tableImpl = (TableImpl)entry.getValue();

            if (tableImpl.dontExport()) {
                continue;
            }

            if (!exportedTableVersion.containsKey(tableName)) {
                addedTables.add(tableName);
            }

            /*
             * Find all the child tables that have been added since export
             */
            doChildTableMetadataDiff(tableImpl, addedTables);
        }

        /*
         * Find all the avro schemas that have been added and/or evolved
         * since export started
         */
        for (Map.Entry<Integer, SchemaData> entry : schemas.entrySet()) {

            String schemaName = entry.getValue().getSchema().getFullName();

            Integer exportedSchemaId = schemaNameToLatestIdMap.get(schemaName);

            if (exportedSchemaId == null) {
                addedAvroSchemas.add(schemaName);
                continue;
            }

            Integer postExportSchemaId = entry.getKey();

            if (exportedSchemaId != postExportSchemaId &&
                    postExportSchemaId > exportedSchemaId) {

                evolvedAvroSchemas.add(schemaName);
            }
        }

        if (deletedTables.isEmpty() && evolvedTables.isEmpty() &&
            addedTables.isEmpty() && evolvedAvroSchemas.isEmpty() &&
            addedAvroSchemas.isEmpty()) {

            logger.info("Metadata did not change during export!");
            return;
        }

        StringBuilder message = new StringBuilder();

        message.append("Metadata change found!\n");

        if (!deletedTables.isEmpty()) {

            message.append("Table deleted:\n");

            for (String tableName : deletedTables) {
                message.append(tableName + " ");
            }
            message.append("\n");
        }

        if (!addedTables.isEmpty()) {

            message.append("New tables added:\n");

            for (String tableName : addedTables) {
                message.append(tableName + " ");
            }
            message.append("\n");
        }

        if (!evolvedTables.isEmpty()) {

            message.append("Table evolved:\n");

            for (String tableName : evolvedTables) {
                message.append(tableName + " ");
            }
            message.append("\n");
        }

        if (!addedAvroSchemas.isEmpty()) {

            message.append("New avro schemas added:\n");

            for (String avro : addedAvroSchemas) {
                message.append(avro + " ");
            }
            message.append("\n");
        }

        if (!evolvedAvroSchemas.isEmpty()) {

            message.append("Avro schemas evolved:\n");

            for (String avro : evolvedAvroSchemas) {
                message.append(avro + " ");
            }
            message.append("\n");
        }

        logger.info(message.toString());
    }

    /**
     * This class holds all the RecordStreams. It contains methods which
     * transfer the record bytes to the RecordStream.
     */
    class RecordStreamMap {

        /*
         * Provides mapping between the name of file segment being exported and
         * the RecordStream which transfers the file bytes.
         */
        private final Map<String, RecordEntityInfo> recEntityMap;

        /*
         * A given file is transferred in chunks of 1GB to the export store.
         * Chunk class is used to keep track of the file chunk sequences that
         * are being exported. Mapping of the file and its chunk sequences
         * are captured in this map.
         *
         * TODO: Include chunks inside recEntityMap
         */
        private final Map<String, Chunk> chunks;

        public RecordStreamMap() {
            recEntityMap = new ConcurrentHashMap<String, RecordEntityInfo>();
            chunks = new ConcurrentHashMap<String, Chunk>();
        }

        /**
         * Size of file segment stored in external export store is fixated to
         * 1GB. Check if the entire record bytes with size recordLength can be
         * accommodated in this file segment. If not, flush the stream.
         */
        void canAddRecord(String fileName, long recordLength) {

            RecordEntityInfo recEntityInfo = recEntityMap.get(fileName);

            if (recEntityInfo == null) {
                return;
            }

            recEntityInfo.canAddRecord(fileName, recordLength);
        }

        /**
         * Transfer recordBytes into the file RecordStream
         */
        void insert(String fileName, byte[] recordBytes) {

            RecordEntityInfo recEntityInfo = recEntityMap.get(fileName);

            if (recEntityInfo == null) {
                recEntityInfo = new RecordEntityInfo();
                recEntityMap.put(fileName, recEntityInfo);
            }

            recEntityInfo.insert(fileName, recordBytes);
        }

        /**
         * Transfer LOB recordBytes into the LOB RecordStream
         */
        void insert(String lobName, ByteBuffer buffer) {

            RecordEntityInfo recEntityInfo = recEntityMap.get(lobName);

            if (recEntityInfo == null) {
                recEntityInfo = new RecordEntityInfo();
                recEntityMap.put(lobName, recEntityInfo);
            }

            RecordStream stream = recEntityInfo.getRecordStream();

            /*
             * If no stream has been created yet, create one and start streaming
             * the LOB record bytes.
             */
            if (stream == null ||
                stream.getFileSize() > getMaxLobFileSize()) {

                /*
                 * Adding this record will exceed the file segment size limit
                 * (1GB). Flush this stream and create a new RecordStream.
                 * Continue transferring the LOB record bytes in this new
                 * RecordStream.
                 */
                if (stream != null) {

                    logger.info("Chunk size for lob " + lobName + " exceeded."
                                + " Flushing the stream.");

                    recEntityInfo.flushStream();
                }

                CustomOutputStream out = new CustomOutputStream();
                WritableByteChannel outChannel = Channels.newChannel(out);
                CustomInputStream in = new CustomInputStream(out);

                /*
                 * Spawns the worker thread
                 */
                createTaskFromList(in, lobName);

                stream = new RecordStream(lobName, outChannel, out);
                recEntityInfo.setRecordStream(stream);
            }

            stream.addByteBuffer(buffer);
        }

        /**
         * Flush all the current running streams
         */
        public void flushAllStreams() {

            logger.info("Flushing all the RecordStreams.");

            for (Map.Entry<String, RecordEntityInfo> entry :
                    recEntityMap.entrySet()) {

                RecordEntityInfo recEntityInfo = entry.getValue();
                recEntityInfo.flushStream();
            }
        }

        /**
         * Spawns a worker thread which transfers the data bytes to the export
         * store for a given file
         */
        public void createTaskFromList(CustomInputStream in, String fileName) {

            Chunk chunk = chunks.get(fileName);

            if (chunk == null) {
                chunk = new Chunk();
                chunks.put(fileName, chunk);
            }

            String chunkSequence = chunk.next();

            logger.info("Creating a new RecordStream for " + fileName +
                        ". File segment number: " + chunk.getNumChunks() +
                        ". Chunk sequence: " + chunk.get());

            Future<Boolean> future = threadPool
               .submit(new WriteTask(in, fileName, chunkSequence));

            taskFutureMap.put(fileName, new FutureHolder(future));
        }

        /**
         * Perform any work that needs to be performed post export. The derived
         * classes provide implementation for this method.
         */
        public void performPostExportWork() {

            logger.info("Performing post export work.");

            doPostExportWork(chunks);
        }

        /**
         * Wrapper on top of RecordStream which creates a synchronization point
         * so you can safely switch over to a new RecordStream.
         * There is one per logical export stream (per table, per lob, etc) for
         * the lifetime of the export.
         */
        class RecordEntityInfo {

            private RecordStream recStream;

            void setRecordStream(RecordStream recStream) {
                this.recStream = recStream;
            }

            RecordStream getRecordStream() {
                return recStream;
            }

            /**
             * Flush the stream
             */
            synchronized void flushStream() {

                if (recStream == null) {
                    return;
                }

                CustomOutputStream out = recStream.out;
                String fileName = recStream.fileName;

                out.customFlush();
                recStream = null;

                try {
                    taskWaitQueue.put(taskFutureMap.get(fileName));
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE,
                               "Exception populating TaskWaiterQueue "
                               + "with " + fileName + " future instance.", e);
                }

                taskFutureMap.remove(fileName);
            }

            /**
             * The maximum size of a file segment stored in external export
             * store is 1GB. Check if the entire record bytes with size
             * recordLength can be accommodated in this file segment. If not,
             * flush the stream.
             */
            synchronized void canAddRecord(String fileName, long recordLength) {

                if (recStream == null) {
                    return;
                }

                if (recStream.getFileSize() + recordLength >
                    maxFileSegmentSize) {

                    logger.info("Chunk size for file " + fileName +
                                " exceeded." +
                                " Flushing the stream.");

                    flushStream();
                }
            }

            /**
             * Transfer recordBytes into the file RecordStream
             */
            synchronized void insert(String fileName, byte[] recordBytes) {

                if (recStream != null) {
                    recStream.addRecord(recordBytes);
                    return;
                }

                CustomOutputStream out = new CustomOutputStream();
                CustomInputStream in = new CustomInputStream(out);
                out.setRecordEntityInfo(this);

                /*
                 * Spawns the worker thread
                 */
                createTaskFromList(in, fileName);

                recStream = new RecordStream(fileName, out);
                recStream.addRecord(recordBytes);
            }
        }

        /**
         * Represents a channel for bytes of a given file segment. Contains
         * methods to add bytes to the stream. The worker threads read
         * bytes from this stream and transfer it to the export store.
         */
        class RecordStream {

            private final String fileName;
            private CustomOutputStream out;
            private WritableByteChannel outChannel;
            private long fileSize = 0;

            RecordStream(String fileName,
                         CustomOutputStream out) {
                this.fileName = fileName;
                this.out = out;
            }

            RecordStream(String lobName,
                         WritableByteChannel outChannel,
                         CustomOutputStream out) {
                this.fileName = lobName;
                this.outChannel = outChannel;
                this.out = out;
            }

            void setOutputStream(CustomOutputStream out) {
                this.out = out;
            }

            /**
             * Add the record bytes to the stream
             */
            void addRecord(byte[] record) {

                out.customWrite(record);
                fileSize += record.length;
            }

            /**
             * Add the LOB record bytes to the stream
             */
            void addByteBuffer(ByteBuffer byteBuffer) {

                try {
                    fileSize += outChannel.write(byteBuffer);
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Exception transferring LOB " +
                               fileName + " bytes to export store.", e);
                }
            }

            public String getFileName() {
                return fileName;
            }

            public long getFileSize() {
                return fileSize;
            }

            public CustomOutputStream getStream() {
                return out;
            }

            @Override
            public String toString() {
                return "FileName: " + fileName +
                       ". Size: " + fileSize;
            }
        }
    }

    /**
     * Thread  responsible for inserting the given LOB data bytes into the
     * RecordStream.
     */
    private class WriteLobTask implements Callable<Boolean> {

        String lobName;
        InputStream in;

        public WriteLobTask(String lobName,
                            InputStream in) {
            this.lobName = lobName;
            this.in = in;
        }

        @Override
        public Boolean call() throws Exception {

            logger.info("WriteLobTask thread spawned for " + lobName);

            ReadableByteChannel inChannel = Channels.newChannel(in);
            ByteBuffer buffer = ByteBuffer.allocateDirect(1024*100);

            while (inChannel.read(buffer) >= 0 || buffer.position() > 0) {
                buffer.flip();
                recordStreamMap.insert(lobName, buffer);
                buffer.compact();
            }

            return true;
        }
    }

    /**
     * Worker thread responsible for reading the bytes from recordStreamMap and
     * transferring them to the export store.
     */
    private class WriteTask implements Callable<Boolean> {

        String fileName;
        String chunkSequence;
        CustomInputStream in;

        public WriteTask(CustomInputStream in,
                         String fileName,
                         String chunkSequence) {
            this.in = in;
            this.fileName = fileName;
            this.chunkSequence = chunkSequence;
        }

        @Override
        public Boolean call() {

            logger.info("WriteTask worker thread spawned for " + fileName);

            doExport(fileName, chunkSequence, in);
            return true;
        }
    }

    /**
     * This class holds a Future<Boolean> and exists so that a
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
     * A Callable that reaps all of the worker thread tasks on the queue and
     * aggregates their results. It returns when an empty Future or
     * FutureHolder is found in the queue or one of the Futures fails with an
     * exception. In this case the main thread needs to occasionally check if
     * this Future is done.
     */
    private class TaskWaiter implements Callable<Boolean> {

        @Override
        public Boolean call()
            throws Exception {

            logger.info("TaskWaiter thread spawned.");

            while (true) {

                FutureHolder holder;

                try {
                    holder = taskWaitQueue.take();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }

                Future<Boolean> future;
                if (holder == null || holder.getFuture() == null) {
                    return true;
                }

                future = holder.getFuture();

                try {
                    future.get();
                } catch (ExecutionException e) {
                    logger.log(Level.SEVERE, "Exception in TaskWaiter.", e);
                } catch (InterruptedException ie) {
                    logger.log(Level.SEVERE, "Exception in TaskWaiter.", ie);
                }
            }
        }
    }

    /**
     * Wait for all Worker Thread tasks that have been scheduled.
     */
    private Boolean waitForTasks()
        throws Exception {

        logger.info("Waiting for WriteTasks threads to " +
                    "complete execution.");

        try {

            taskWaitQueue.put(new FutureHolder(null));
            taskWait.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Exception waiting for WriteLobTask " +
                       "thread to complete execution", e);

            return false;
        } catch (InterruptedException ie) {
            logger.log(Level.SEVERE, "Exception waiting for WriteLobTask " +
                       "thread to complete execution", ie);

            return false;
        }

        return true;
    }

    /**
     * Wait for all the scheduled Threads transferring the LOB data bytes into
     * the RecordStream.
     */
    private Boolean waitForLobTasks()
            throws Exception {

        logger.info("Waiting for WriteLobTasks threads to " +
                    "complete execution.");

        for (FutureHolder holder : lobTaskWaitList) {
            Future<Boolean> future;

            if (holder == null || holder.getFuture() == null) {
                continue;
            }

            future = holder.getFuture();

            try {

                future.get();
            } catch (ExecutionException e) {

                logger.log(Level.SEVERE, "Exception waiting for WriteLobTask " +
                           "thread to complete execution", e);

                return false;
            } catch (InterruptedException ie) {

                logger.log(Level.SEVERE, "Exception waiting for WriteLobTask " +
                           "thread to complete execution", ie);

                return false;
            }
        }

        return true;
    }

    /**
     * Deserialize a record encoded in avro. This API is used by the export
     * utility to deserialize a record using the version of the table that was
     * exported and NOT the latest evolved version of the table.
     *
     * @param vv record that needs to be deserialized
     * @param exportTableVersion export table version used to deserialize the
     *        record in avro
     * @param row row containing only the key portion
     */
    public void createExportRowFromValueVersion
        (TableImpl table,
         RowImpl row,
         ValueVersion vv,
         int exportTableVersion) {

        final String name = table.getName();

        byte[] data = vv.getValue().getValue();

        if (data.length == 0) {
            return;
        }

        int offset = 0;
        int tableVersion = data[offset];

        /*
         * Get the writer schema for the record
         */
        Schema writerSchema = schemaCache.getWriterSchema(name, tableVersion);

        if (writerSchema == null) {
            String writerSchemaString =
                table.generateAvroSchema(tableVersion, false);
            writerSchema = new Schema.Parser().parse(writerSchemaString);
            schemaCache.putWriterSchema(name, tableVersion, writerSchema);
        }

        /*
         * Get the reader schema for the record using exportTableVersion
         */
        Schema expReaderSchema = schemaCache.getReaderSchema(name);

        if (expReaderSchema == null) {
            String readerSchemaString =
                table.generateAvroSchema(exportTableVersion, false);
            if (readerSchemaString == null) {
                /*
                 * no reader schema, table is key-only, all non-key fields
                 * are null
                 */
                return;
            }
            expReaderSchema = new Schema.Parser().parse(readerSchemaString);
            schemaCache.putReaderSchema(name, expReaderSchema);
        }

        table.createExportRowFromValueSchema(writerSchema,
                                             expReaderSchema,
                                             row,
                                             data,
                                             offset,
                                             exportTableVersion,
                                             vv.getValue().getFormat());
    }

    /******************* Abstract Methods **************************/

    /**
     * Streams all the bytes for a given file segment from the RecordStream
     * to the export store.
     */
    abstract boolean doExport(String fileName,
                              String chunkSequence,
                              CustomInputStream in);

    /**
     * Perform any work that needs to be done post export.
     */
    abstract void doPostExportWork(Map<String, Chunk> chunks);

    /**
     * Get the max LOB file segment size that will be stored in the export
     * store.
     */
    abstract long getMaxLobFileSize();

    /**
     * Sets the logger handler
     */
    abstract void setLoggerHandler(Logger logger);

    /**
     * Flush all the log streams
     */
    abstract void flushLogs();

    /**
     * Write the export stats - store name, helper hosts, export start time
     * and export end time to the export store
     */
    abstract void generateExportStats(String exportStats);
}

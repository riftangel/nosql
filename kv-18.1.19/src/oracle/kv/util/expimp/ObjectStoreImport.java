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
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import oracle.cloud.storage.CloudStorage;
import oracle.cloud.storage.CloudStorageConfig;
import oracle.cloud.storage.CloudStorageFactory;
import oracle.cloud.storage.exception.AuthenticationException;
import oracle.cloud.storage.exception.SystemException;
import oracle.cloud.storage.model.Container;
import oracle.cloud.storage.model.StorageInputStream;
import oracle.kv.util.expimp.ExitHandler.ExitCode;

import com.sun.jersey.api.client.ClientHandlerException;

/**
 * An implementation class for AbstractStoreImport to import contents from
 * Oracle Storage Cloud Service to Oracle NoSql Store
 */
public class ObjectStoreImport extends AbstractStoreImport {

    /*
     * Name of the container in the Oracle Storage Cloud Service where all the
     * data/metadata from kvstore will be exported. Container is analogous to
     * export package in local file system
     */
    private String containerName;

    /*
     * Handle to the Oracle Storage Cloud Service instance
     */
    private CloudStorage storeConnection;

    /*
     * Key for the object holding name of all the file segments already exported
     */
    private String statusKey;

    /* kvstore name */
    private String storeName;

    private String[] helperHosts;

    /*
     * Stream for logging data
     */
    private ByteArrayOutputStream loggerOutput;

    private static final String importLogKey = "importLogKey";
    private Logger logger;

    public ObjectStoreImport(String storeName,
                             String[] helperHosts,
                             String kvUsername,
                             String kvSecurityFile,
                             String containerName,
                             String serviceName,
                             String userName,
                             String password,
                             String serviceURL,
                             String status,
                             boolean json) {

        super(storeName, helperHosts, kvUsername, kvSecurityFile, json);

        this.storeName = storeName;
        this.helperHosts = helperHosts;
        this.containerName = containerName;

        loggerOutput = new ByteArrayOutputStream();

        /*
         * Set handle to the logger
         */
        setLoggerHandler();

        /*
         * Oracle Storage Cloud Service client configuration builder
         */
        CloudStorageConfig myConfig = new CloudStorageConfig();

        try {
            /*
             * Use CloudStorageConfig interface to specify options for
             * connecting to a Service instance.
             */
            myConfig.setServiceName(serviceName)
                    .setUsername(userName)
                    .setPassword(password.toCharArray())
                    .setServiceUrl(serviceURL);

            /*
             * Get a handle to a Service Instance of the Oracle Storage
             * Cloud Service
             */
            storeConnection = CloudStorageFactory.getStorage(myConfig);
            statusKey = ((status != null) ? status + "key" : null);

            for (Container container : storeConnection.listContainers()) {

                /*
                 * Container with export package found
                 */
                if (container.getName().equals(containerName)) {
                    return;
                }
            }

            logger.severe("Container " + containerName + " not present " +
                          "in Oracle Storage Cloud Service.");
            exit(true, ExitCode.EXIT_NOEXPPACKAGE, System.err, null);

        } catch (MalformedURLException mue) {

            logger.log(Level.SEVERE, "MalformedURLException ", mue);
            exit(true, ExitCode.EXIT_EXPSTR_NOCONNECT, System.err, null);
        }  catch (AuthenticationException ae) {

            logger.log(Level.SEVERE, "AuthenticationException ", ae);
            exit(true, ExitCode.EXIT_NOPERM, System.err, null);
        } catch (ClientHandlerException che) {

            logger.log(Level.SEVERE, "Client handler exception ", che);
            exit(true, ExitCode.EXIT_EXPSTR_NOCONNECT, System.err, null);
        }
    }

    /**
     * Loads all the file segment names. The names in the status file represents
     * all the file segments that are already present in the kvstore.
     * The import of all file segments present in the status file will be
     * skipped.
     */
    private void loadStatusFile() {

        StorageInputStream statusStream = null;

        if (statusKey != null) {

            logger.info("Loading status file: " + statusKey);
            statusStream = retrieveObject(statusKey);
        }

        if (statusStream != null) {

            readStream(new ImportProcess(){

                @Override
                public void doProcessing(String line) {
                    loadStatusFile(line);
                }

            }, statusStream, null);
        }
    }

    /**
     * Imports all the schema definitions and data/metadata from the Oracle
     * Storage Cloud Service container to kvstore
     */
    @Override
    void doImport() {

        StringBuilder hhosts = new StringBuilder();

        for (int i = 0; i < helperHosts.length - 1; i++) {
            hhosts.append(helperHosts[i] + ",");
        }

        hhosts.append(helperHosts[helperHosts.length - 1]);

        logger.info("Starting import of export package " +
            "to store " + storeName + ", helperHosts=" + hhosts.toString());

        /*
         * Imports all the schema definitions to kvstore
         */
        importSchemaDefinition(null);

        /*
         * Loads the status file
         */
        loadStatusFile();

        /*
         * Retrieve the custom metadata map. It provides a mapping
         * between the file name and the number of segments of the file
         */
        String chunksKey = "chunksKey";
        Map<String, String> customMetadata = new HashMap<String, String>();

        StorageInputStream in = retrieveObject(chunksKey);

        /*
         * Return if custom metadata map not found
         */
        if (in == null) {

            logger.severe("Custom metadata map not available in the " +
                          "container. Import cant proceed without this map.");

            return;
        }

        customMetadata = in.getCustomMetadata();

        logger.info("Importing data into KVStore.");

        String otherDataFile = "OtherData";
        String otherDataPrefix = otherDataFile + "-" +
                                 otherDataFile.length();

        BufferedInputStream bin = null;

        /*
         * Retrieve the input streams for all the data files from Oracle
         * Storage Cloud Service. Custom metadata map is used to retrieve the
         * names of all the data files
         */
        for (Map.Entry<String, String> entry : customMetadata.entrySet()) {

            String[] fileNameAndNumChunks = entry.getValue().split(",");
            String fileName = fileNameAndNumChunks[0].trim();
            String numChunksString = fileNameAndNumChunks[1].trim();
            Integer numChunks = Integer.parseInt(numChunksString);

            String filePrefix = fileName + "-" + fileName.length();

            Chunk chunk = new Chunk();

            boolean isTableData = !filePrefix.equals(otherDataPrefix);

            /*
             * Retrieve the input stream for every segment of the file
             */
            for (int i = 0; i < numChunks; i++) {

                String nextChunkSequence = chunk.next();
                String fileSegmentName = filePrefix + "-" + nextChunkSequence;

                logger.info("Importing file segment: " + fileName +
                            " . Chunk sequence: " + nextChunkSequence);

                in = retrieveObject(fileSegmentName);

                if (in == null) {
                    continue;
                }

                bin = new BufferedInputStream(in);

                populateEntryStreams(bin,
                                     fileName,
                                     nextChunkSequence,
                                     isTableData);
            }
        }

        /*
         * Perform bulk put. This does the actual work of importing data
         * from the file segment entry streams to kvstore
         */
        performBulkPut();
        doPostImportWork();

        try {
            if (bin != null) {
                bin.close();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Exception closing stream.", e);
        }

        try {
            if (in != null) {
                in.close();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Exception closing stream.", e);
        }

        String impCompleteMsg = "Completed importing export package to store " +
            storeName + ", helperHosts=" + hhosts.toString();
        logger.info(impCompleteMsg);

        exit(true, ExitCode.EXIT_OK, System.out, impCompleteMsg);
    }

    /**
     * Import the schema definitions and the data of the requested tables.
     *
     * @param tableNames names of all the tables whose schemas and data needs
     *        to be imported from Oracle Storage Cloud Service to kvstore
     */
    @Override
    void doTableImport(String[] tableNames) {

        StringBuilder hhosts = new StringBuilder();

        for (int i = 0; i < helperHosts.length - 1; i++) {
            hhosts.append(helperHosts[i] + ",");
        }

        hhosts.append(helperHosts[helperHosts.length - 1]);

        logger.info("Starting import of tables from export package to store " +
            storeName + ", helperHosts=" + hhosts.toString());

        List<String> tablesToImport =
            new LinkedList<String>(Arrays.asList(tableNames));

        List<String> tablesListClone = new ArrayList<String>();
        tablesListClone.addAll(tablesToImport);

        /*
         * Import the requested tables schema definitions
         */
        importSchemaDefinition(tablesListClone);

        /*
         * Ignore all the table names which are not found in the Oracle Storage
         * Cloud Service. Log the message.
         */
        for (String table : tablesListClone) {
            logger.warning("Schema definition for Table: " + table +
                           " not found in the export package.");

            tablesToImport.remove(table);
        }

        /*
         * Load the status file
         */
        loadStatusFile();

        logger.info("Importing table data into KVStore.");

        /*
         * Retrieve the custom metadata map. It provides a mapping
         * between the file name and the number of segments of the file
         */
        String chunksKey = "chunksKey";
        Map<String, String> customMetadata = new HashMap<String, String>();

        StorageInputStream in = retrieveObject(chunksKey);

        if (in == null) {

            logger.severe("Custom metadata map not available in the " +
                          "container. Import cant proceed without this map.");

            return;
        }

        customMetadata = in.getCustomMetadata();

        String otherDataFile = "OtherData";

        BufferedInputStream bin = null;

        /*
         * Retrieve the input streams for all the table data files from Oracle
         * Storage Cloud Service. Custom metadata map is used to retrieve the
         * names of all the table data files
         */
        for (Map.Entry<String, String> entry : customMetadata.entrySet()) {

            String[] fileNameAndNumChunks = entry.getValue().split(",");
            String fileName = fileNameAndNumChunks[0].trim();

            if (fileName.equals(otherDataFile) ||
                    !tablesToImport.contains(fileName)) {

                continue;
            }

            String numChunksString = fileNameAndNumChunks[1].trim();
            Integer numChunks = Integer.parseInt(numChunksString);

            String filePrefix = fileName + "-" + fileName.length();

            Chunk chunk = new Chunk();

            /*
             * Retrieve the input stream for every segment of the file
             */
            for (int i = 0; i < numChunks; i++) {

                String nextChunkSequence = chunk.next();
                String fileSegmentName = filePrefix + "-" + nextChunkSequence;

                logger.info("Importing table file segment: " + fileName +
                            " . Chunk sequence: " + nextChunkSequence);

                in = retrieveObject(fileSegmentName);

                if (in == null) {
                    continue;
                }

                bin = new BufferedInputStream(in);

                populateEntryStreams(bin,
                                     fileName,
                                     nextChunkSequence,
                                     true);
            }
        }

        /*
         * Perform bulk put. This does the actual work of importing data
         * from the file segment entry streams to kvstore
         */
        performBulkPut();
        doPostImportWork();

        try {
            if (bin != null) {
                bin.close();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Exception closing stream.", e);
        }

        try {
            if (in != null) {
                in.close();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Exception closing stream.", e);
        }

        String impCompleteMsg = "Completed import of tables from export " +
            "package to store " + storeName + ", helperHosts=" +
            hhosts.toString();
        logger.info(impCompleteMsg);

        exit(true, ExitCode.EXIT_OK, System.out, impCompleteMsg);
    }

    /**
     * Gets the given LOB input stream from Oracle Storage Cloud Service
     *
     * @param lobFileName name of the lob file in Oracle Storage Cloud Service
     */
    @Override
    InputStream getLobInputStream(String lobFileName) {

        StorageInputStream stream = null;
        String prefixKey = lobFileName + "-" + lobFileName.length();
        String objectKey = prefixKey + "-ManifestKey";

        stream = retrieveObject(objectKey);

        return stream;
    }

    /**
     * Flushes all the logging data
     */
    @Override
    void flushLogs() {

        InputStream stream =
            new ByteArrayInputStream(loggerOutput.toByteArray());

        storeObject(importLogKey, stream);
    }

    /**
     * Sets the log handler
     */
    @Override
    void setLoggerHandler(Logger logger) {

        this.logger = logger;

        StreamHandler streamHandler =
             new StreamHandler(loggerOutput, new SimpleFormatter() {

                 @Override
                 public synchronized String format(LogRecord record) {
                     return Utilities.format(record);
                 }
             }) {

            @Override
            public synchronized void publish(final LogRecord record) {
                super.publish(record);
                flush();
            }
        };

        logger.addHandler(streamHandler);
        logger.setLevel(Level.ALL);
    }

    /**
     * Imports schema definitions from Oracle Storage Cloud Service to kvstore
     *
     * @param tableNames names of all the tables whose schema definitions
     *        needs to be imported.
     *        If null, imports all the schema definitions (table and avro)
     *        available in Oracle Storage Cloud Service
     */
    void importSchemaDefinition(final List<String> tableNames) {

        String schemaFile = "SchemaDefinition";
        String prefixKey = schemaFile + "-" + schemaFile.length();

        /*
         * Key holding schema definition in Oracle Storage Cloud Service
         */
        String schemaDefinitionKey = prefixKey + "-ManifestKey";

        StorageInputStream stream = retrieveObject(schemaDefinitionKey);

        if (stream != null) {

            readStream(new ImportProcess(){

                @Override
                public void doProcessing(String line) {

                    /*
                     * Import the schema (table or avro schema)
                     */
                    if (tableNames == null) {
                        importSchema(line);
                    }

                    /*
                     * Import the table schema if the table is in
                     * tableNames list
                     */
                    else {
                        importTableSchema(line, tableNames);
                    }
                }
            }, stream, tableNames);
        }
    }

    /**
     * Read and process the StorageInputStream line by line
     *
     * @param ip interface that defines how to process the line from the file
     * @param stream input stream for data from oracle storage cloud service
     * @param list if null process all the lines. Else process only if the line
     *        represents a table and is present in the list
     */
    private void readStream(ImportProcess ip,
                            StorageInputStream stream,
                            List<String> list) {

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(stream));
            String line = reader.readLine();

            while (line != null) {

                ip.doProcessing(line);
                line = reader.readLine();

                /*
                 * Break from the loop after the list has been exhausted
                 */
                if (list != null && list.isEmpty()) {
                    break;
                }
            }

        } catch (IOException ioe) {

            logger.log(Level.SEVERE, "IOException: ", ioe);
        } finally {

            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {

                logger.log(Level.SEVERE, "IOException: ", e);
            }

            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (IOException e) {

                logger.log(Level.SEVERE, "IOException: ", e);
            }
        }
    }

    /**
     * Interface that defines how to process the line from StorageInputStream
     */
    interface ImportProcess {
        public void doProcessing(String line);
    }

    /**
     * Write all the file segments that have been successfully imported to
     * Oracle Storage Cloud Service reference by the status key
     */
    @Override
    void writeStatusFile(Set<String> fileSegments) {

        if (statusKey != null && fileSegments != null &&
                !fileSegments.isEmpty()) {

            logger.info("Writing the status file to oracle object store");

            StringBuffer sb = new StringBuffer();

            for (String segment : fileSegments) {
                sb.append(segment).append("\n");
            }

            InputStream stream =
                new ByteArrayInputStream(sb.toString()
                    .getBytes(StandardCharsets.UTF_8));

            storeObject(statusKey, stream);
        }
    }

    /**
     * Store an object to Oracle Storage Cloud Service
     *
     * @param key referencing key for the object
     * @param stream holds the object data
     */
    private void storeObject(String key, InputStream stream) {

        try {

            storeConnection.storeObject(containerName,
                                        key,
                                        "text/plain",
                                        stream);
        } catch (SystemException se) {

            logger.log(Level.SEVERE, "Exception storing object " +
                       key + " from Oracle Storage Cloud " +
                       "Service", se);
        } catch (Exception e) {

            logger.log(Level.SEVERE, "Exception storing object " +
                       key + " from Oracle Storage Cloud " +
                       "Service", e);
        }
    }

    /**
     * Retrieve the object from Oracle Storage Cloud Service referenced by the
     * object key
     *
     * @param key referencing key for the object
     * @return input stream for the object data
     */
    private StorageInputStream retrieveObject(String key) {

        try {
            StorageInputStream stream =
                storeConnection.retrieveObject(containerName, key);

            return stream;
        } catch (SystemException se) {

            logger.log(Level.SEVERE, "Exception retrieving object " +
                       key + " from Oracle Storage Cloud " +
                       "Service", se);
            return null;
        } catch (Exception e) {

            logger.log(Level.SEVERE, "Exception retrieving object " +
                       key + " from Oracle Storage Cloud " +
                       "Service", e);
            return null;
        }
    }
}

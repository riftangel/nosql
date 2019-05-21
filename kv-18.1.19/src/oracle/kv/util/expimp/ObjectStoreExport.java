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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
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
import oracle.kv.util.expimp.CustomStream.CustomInputStream;
import oracle.kv.util.expimp.ExitHandler.ExitCode;

import com.sun.jersey.api.client.ClientHandlerException;

/**
 * An implementation class for AbstractStoreExport used to export the data from
 * Oracle NoSql store to Oracle Storage Cloud Service.
 */
public class ObjectStoreExport extends AbstractStoreExport {

    /*
     * Name of the container in the Oracle Storage Cloud Service where all the
     * data/metadata from kvstore will be exported. Container is analogous to
     * export package in local file system
     */
    private String containerName;

    /*
     * Handle to Oracle Storage Cloud Service instance
     */
    private CloudStorage storeConnection;

    /*
     * Size of exported LOB file segment = 1GB
     */
    private static final long fileSize = 1000 * 1000 * 1000;

    /*
     * Stream for logging data
     */
    private final ByteArrayOutputStream loggerOutput;

    private Logger logger;

    private static final String chunksKey = "chunksKey";
    private static final String exportStatsKey = "exportStats";
    private static final String exportLogKey = "exportLogKey";

    /**
     * Constructor that connects to the Oracle storage cloud service and
     * creates the container if it does not exists.
     *
     * @param storeName kvstore name
     * @param helperHosts kvstore helper hosts
     * @param containerName name of container in Oracle storage cloud service
     * @param serviceName Oracle storage cloud service instance name
     * @param userName Oracle storage cloud service username
     * @param password Oracle storage cloud service password
     * @param serviceURL Oracle storage cloud service URL
     */
    public ObjectStoreExport(String storeName,
                             String[] helperHosts,
                             String kvUsername,
                             String kvSecurityFile,
                             String containerName,
                             String serviceName,
                             String userName,
                             String password,
                             String serviceURL,
                             boolean json) {

        super(storeName, helperHosts, kvUsername, kvSecurityFile, json);

        loggerOutput = new ByteArrayOutputStream();

        /*
         * Set handle to the logger
         */
        setLoggerHandler();
        this.containerName = containerName;

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

            try {
                /*
                 * Create a new storage Container to store the exported data/
                 * metadata
                 */
                storeConnection.createContainer(containerName);
            } catch (ClientHandlerException | SystemException e) {

                logger.warning("Container " + containerName + " already " +
                               "exists. Stopping Export.");
                exit(true, ExitCode.EXIT_CONTAINER_EXISTS, System.err, null);
            }
        } catch (MalformedURLException mue) {

            logger.log(Level.SEVERE, "MalformedURLException ", mue);
            exit(true, ExitCode.EXIT_EXPSTR_NOCONNECT, System.err, null);
        } catch (AuthenticationException ae) {

            logger.log(Level.SEVERE, "AuthenticationException ", ae);
            exit(true, ExitCode.EXIT_NOPERM, System.err, null);
        } catch (ClientHandlerException che) {

            logger.log(Level.SEVERE, "Client handler exception ", che);
            exit(true, ExitCode.EXIT_EXPSTR_NOCONNECT, System.err, null);
        }
    }

    /**
     * Exports the file segment bytes to the container in Oracle Storage
     * Cloud Service.
     *
     * @param fileName file being exported
     * @param chunkSequence identifier for the file segment being exported
     * @param stream input stream reading bytes from kvstore into export store
     */
    @Override
    boolean doExport(String fileName,
                     String chunkSequence,
                     CustomInputStream stream) {

        String filePrefix = fileName + "-" + fileName.length();
        String fileKey = filePrefix + "-" + chunkSequence;

        return storeObject(fileKey, stream);
    }

    /**
     * Work done post export. A hash map with file name as the key and the
     * number of segments of the file as the value is stored in the container
     * in Oracle Storage Cloud Service. This map is used during import to
     * determine the number of segments for a given file and to retrieve all
     * the file segments.
     */
    @Override
    void doPostExportWork(Map<String, Chunk> chunks) {

        /*
         * Map holding all the exported filenames and the number of file
         * segments
         */
        Map<String, String> customMetadata = new HashMap<String, String>();

        for (Map.Entry<String, Chunk> entry : chunks.entrySet()) {

            String fileName = entry.getKey();
            Chunk chunk = entry.getValue();

            String filePrefix = fileName + "-" + fileName.length();
            String objectStoreRecordKey = filePrefix + "-" + "ManifestKey";

            try {
                /*
                 * Store the manifest object for this file. The manifest object
                 * is used to retrieve all the file segments belonging to a
                 * given file.
                 */
                storeConnection
                    .storeObjectManifest(containerName,
                                         objectStoreRecordKey,
                                         "text/plain",
                                         containerName,
                                         filePrefix);
            } catch (SystemException se) {

                logger.log(Level.SEVERE, "Exception storing manifest object " +
                           objectStoreRecordKey + " in Oracle Storage " +
                           "Cloud Service", se);

            } catch (Exception e) {

                logger.log(Level.SEVERE, "Exception storing manifest object " +
                           objectStoreRecordKey + " in Oracle Storage " +
                           "Cloud Service", e);

            }

            /*
             * CustomMetadata map will hold entries only for table data and
             * other data (Avro and none format data). SchemaDefinition and
             * LobFile segments are skipped.
             */
            if (fileName.equals("SchemaDefinition") ||
                    fileName.contains("LOBFile")) {
                continue;
            }

            customMetadata.put(fileName, fileName + ", " +
                               chunk.getNumChunks().toString());
        }

        InputStream stream = new ByteArrayInputStream(chunksKey
                             .getBytes(StandardCharsets.UTF_8));

        try {
            /*
             * Store the customMetadata hashMap into Oracle Storage Cloud
             * Service
             */
            storeConnection.storeObject(containerName,
                                        chunksKey,
                                        "text/plain",
                                        customMetadata,
                                        stream);
        } catch (SystemException se) {

            logger.log(Level.SEVERE, "Exception storing object " +
                       chunksKey + " in Oracle Storage " +
                       "Cloud Service", se);

        } catch (Exception e) {

            logger.log(Level.SEVERE, "Exception storing object " +
                       chunksKey + " in Oracle Storage " +
                       "Cloud Service", e);

        }
    }

    /**
     * Flushes all the logging data
     */
    @Override
    void flushLogs() {

        InputStream stream =
            new ByteArrayInputStream(loggerOutput.toByteArray());

        storeObject(exportLogKey, stream);
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
     * Returns the maximum size of lob file segment that will be exported
     */
    @Override
    long getMaxLobFileSize() {
        return fileSize;
    }

    /**
     * Stores an object in Oracle Storage Cloud Service.
     *
     * @param key key for the object
     * @param stream stream holding the object data
     * @return true if the object successfully stored. False otherwise.
     */
    private boolean storeObject(String key, InputStream stream) {

        try {

            storeConnection.storeObject(containerName,
                                        key,
                                        "text/plain",
                                        stream);

            return true;
        } catch (SystemException se) {

            logger.log(Level.SEVERE, "Exception storing object " +
                       key + " in Oracle Storage Cloud " +
                       "Service", se);

            return false;
        } catch (Exception e) {

            logger.log(Level.SEVERE, "Exception storing object " +
                       key + " in Oracle Storage Cloud " +
                       "Service", e);

            return false;
        }
    }

    @Override
    void generateExportStats(String exportStats) {
        try {

            InputStream stream = new ByteArrayInputStream(exportStats
                .getBytes(StandardCharsets.UTF_8));

            /*
             * Store the export stats into Oracle Storage Cloud
             * Service
             */
            storeConnection.storeObject(containerName,
                                        exportStatsKey,
                                        "text/plain",
                                        stream);
        } catch (SystemException se) {

            logger.log(Level.SEVERE, "Exception storing object " +
                       chunksKey + " in Oracle Storage " +
                       "Cloud Service", se);

        } catch (Exception e) {

            logger.log(Level.SEVERE, "Exception storing object " +
                       chunksKey + " in Oracle Storage " +
                       "Cloud Service", e);

        }
    }
}

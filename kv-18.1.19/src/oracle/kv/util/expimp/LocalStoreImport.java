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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import oracle.kv.util.expimp.ExitHandler.ExitCode;

/**
 * An implementation class for AbstractStoreImport to import contents from local
 * file system export package to Oracle NoSql Store
 */
public class LocalStoreImport extends AbstractStoreImport {

    /*
     * Directory in the file system holding the entire export package
     */
    private File exportFolder;

    /*
     * File holding all the file segment names that have been successfully
     * imported into the kvstore
     */
    private File statusFile;

    /*
     * Name of the kvstore
     */
    private String storeName;

    private String[] helperHosts;

    /*
     * Import log file location inside the export package
     */
    private File importLogFile;
    private Logger logger;

    /**
     * Constructor that creates the export package directory structure
     *
     * @param storeName kvstore name
     * @param helperHosts kvstore helper hosts
     * @param exportFolderPath path in local file system for export package
     * @param statusFile status filename
     */
    public LocalStoreImport(String storeName,
                            String[] helperHosts,
                            String userName,
                            String securityFile,
                            String exportFolderPath,
                            String statusFile,
                            boolean json) {

        super(storeName, helperHosts, userName, securityFile, json);

        this.storeName = storeName;
        this.helperHosts = helperHosts;

        this.exportFolder = new File(exportFolderPath);
        this.statusFile = (statusFile != null ?
            new File(exportFolder, statusFile) : null);

        if (!exportFolder.exists() || !exportFolder.isDirectory()) {
            exit(false, ExitCode.EXIT_NOEXPPACKAGE, System.err, null);
        }

        if (!Files.isReadable(exportFolder.toPath())) {
            exit(false, ExitCode.EXIT_NOREAD, System.err, null);
        }

        this.importLogFile = new File(exportFolder, "Import.log");

        /*
         * Set handle to the logger
         */
        setLoggerHandler();
    }

    /**
     * Imports schema definitions from the export package to kvstore
     *
     * @param tableNames names of all the tables whose schema definitions
     *        needs to be imported.
     *        If null, imports all the schema definitions (table and avro)
     *        available in the export package
     */
    void importSchemaDefinition(final List<String> tableNames) {

        File schemaFolder = new File(exportFolder, "SchemaDefinition");

        if (!schemaFolder.exists() || !schemaFolder.isDirectory()) {

            logger.log(Level.SEVERE, "Schema definition directory missing.");
            return;
        }

        for (File schemaFile : schemaFolder.listFiles()) {

            if (schemaFile.isFile() &&
                schemaFile.getName().contains("SchemaDefinition") &&
                Files.isReadable(schemaFile.toPath())) {

                try {
                    InputStream stream = new FileInputStream(schemaFile);

                    readFile(new ImportProcess(){

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
                } catch (FileNotFoundException e) {

                    logger.log(Level.SEVERE, "File segment " + schemaFile +
                               " not found in the export package.", e);
                }

                /*
                 * Break from the loop after all the requested table schema
                 * definitions have been imported
                 */
                if (tableNames != null && tableNames.isEmpty()) {
                    break;
                }
            }
        }
    }

    /**
     * Read and process the file stream line by line
     *
     * @param ip interface that defines how to process the line from the file
     * @param stream input stream for the file
     * @param list if null process all the lines. Else process only if the line
     *        represents a table and is present in the list
     */
    private void readFile(ImportProcess ip,
                          InputStream stream,
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
     * Interface that defines how to process a line from the file stream
     */
    interface ImportProcess {
        public void doProcessing(String line);
    }

    /**
     * Loads all the file segment names that are present in the status file.
     * The names in the status file represents all the file segments that are
     * already present in the kvstore. The import of all file segments present
     * in the status file will be skipped.
     */
    private void loadStatusFile() {

        if (statusFile != null) {

            logger.info("Loading status file: " + statusFile);

            try {
                InputStream stream = new FileInputStream(statusFile);

                readFile(new ImportProcess(){

                    @Override
                    public void doProcessing(String line) {
                        loadStatusFile(line);
                    }

                }, stream, null);
            } catch (FileNotFoundException e) {

                logger.info("Status file " + statusFile + " not found in the " +
                            "export package.");
            }
        }
    }

    /**
     * Imports all the schema definitions and data/metadata from the export
     * package to kvstore
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

        /* For testing purpose. Induce schema changes before schema import */
        induceImportChange("BEFORE");

        /*
         * Imports all the schema definitions to kvstore
         */
        importSchemaDefinition(null);

        /* For testing purpose. Induce schema changes after schema import */
        induceImportChange("AFTER");

        /*
         * Loads the status file
         */
        loadStatusFile();

        logger.info("Importing OtherData (Avro/None format) into kvstore.");

        File dataFolder = new File(exportFolder, "Data");
        File otherDataFolder = new File(dataFolder, "Other");

        /*
         * Import avro/none format data
         */
        if (otherDataFolder.exists() && otherDataFolder.isDirectory()) {
            for (File dataFile : otherDataFolder.listFiles()) {
                if (dataFile.isFile() &&
                    Files.isReadable(dataFile.toPath())) {

                    try {
                        InputStream stream =
                            new FileInputStream(dataFile);

                        BufferedInputStream in =
                            new BufferedInputStream(stream);

                        String fileSegmentName = dataFile.getName();

                        String fileName =
                            fileSegmentName
                            .substring(0, fileSegmentName.lastIndexOf("-"));

                        String chunkSequence =
                            fileSegmentName
                            .substring(fileSegmentName.lastIndexOf("-") + 1,
                                       fileSegmentName.length());

                        logger.info("Importing file segment: " + fileName +
                                    " .Chunk sequence: " + chunkSequence);

                        populateEntryStreams(in,
                                             fileName,
                                             chunkSequence,
                                             false          // not table data
                                             );

                    } catch (FileNotFoundException e) {
                        logger.log(Level.SEVERE, "File segment " + dataFile +
                                   " not found in the export package.", e);
                    }
                }
            }
        }

        logger.info("Importing the table data into kvstore.");

        File tableFolder = new File(dataFolder, "Table");

        /*
         * Import the table data
         */
        if (tableFolder.exists() && tableFolder.isDirectory()) {

            for (File dataFile : tableFolder.listFiles()) {

                if (dataFile.isDirectory()) {
                    importTableSegments(dataFile);
                }
            }
        }

        /*
         * Perform bulk put. This does the actual work of importing data
         * from the file segment entry streams to kvstore
         */
        performBulkPut();
        doPostImportWork();

        String impCompleteMsg = "Completed importing export package to store " +
            storeName + ", helperHosts=" + hhosts.toString();
        logger.info(impCompleteMsg);

        exit(true, ExitCode.EXIT_OK, System.out, impCompleteMsg);
    }

    /**
     * Import the schema definitions and the data of the requested tables.
     *
     * @param tableNames names of all the tables whose schemas and data needs
     *        to be imported from export package to kvstore
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
         * Ignore all the table names which are not found in the export package.
         * Log the message.
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

        logger.info("Importing the table data into kvstore.");

        File dataFolder = new File(exportFolder, "Data");
        File tableFolder = new File(dataFolder, "Table");

        /*
         * Import the table data
         */
        if (tableFolder.exists() && tableFolder.isDirectory()) {
            for (String table : tablesToImport) {
                File importTableFolder = new File(tableFolder, table);

                if (importTableFolder.exists() &&
                        importTableFolder.isDirectory()) {

                    importTableSegments(importTableFolder);
                }
            }
        }

        /*
         * Perform bulk put. This does the actual work of importing data
         * from the file segment entry streams to kvstore
         */
        performBulkPut();
        doPostImportWork();


        String impCompleteMsg = "Completed import of tables from export " +
            "package to store " + storeName + ", helperHosts=" +
            hhosts.toString();
        logger.info(impCompleteMsg);

        exit(true, ExitCode.EXIT_OK, System.out, impCompleteMsg);
    }

    /**
     * Sets the log handler
     */
    @Override
    void setLoggerHandler(Logger logger) {

        this.logger = logger;

        try {

            FileHandler fileHandler =
                new FileHandler(importLogFile.getAbsolutePath(), false);
            fileHandler.setFormatter(new SimpleFormatter() {

                    @Override
                    public synchronized String format(LogRecord record) {
                        return Utilities.format(record);
                    }
                });

            logger.addHandler(fileHandler);
            logger.setLevel(Level.ALL);
        } catch (FileNotFoundException e) {
            exit(false, ExitCode.EXIT_UNEXPECTED, System.err, null);
        } catch (SecurityException se) {
            exit(false, ExitCode.EXIT_UNEXPECTED, System.err, null);
        } catch (IOException ioe) {
            exit(false, ExitCode.EXIT_UNEXPECTED, System.err, null);
        }
    }

    /**
     * Imports the requested tables data only if this tables schema is present
     * in the kvstore
     *
     * @param dataFile Table file segment name. Data from this file will
     *        get imported to kvstore
     */
    private void importTableSegments(File dataFile) {
        String tableName = dataFile.getName();

        /*
         * Ignore the table data for tables whose schema is not present in the
         * kvstore
         */
        if(!isTableSchemaImported(tableName)) {

            logger.warning("Schema for table: " + tableName +
                " not present. Import of this table data skipped.");

            return;
        }

        for (File tableFile : dataFile.listFiles()) {

            if (tableFile.isFile() &&
                    Files.isReadable(tableFile.toPath())) {

                try {
                    InputStream stream =
                        new FileInputStream(tableFile);

                    BufferedInputStream in =
                        new BufferedInputStream(stream);

                    String fileSegmentName = tableFile.getName();
                    String fileName =
                        fileSegmentName.substring(0,
                        fileSegmentName.lastIndexOf("-"));

                    String chunkSequence =
                        fileSegmentName.substring(
                        fileSegmentName.lastIndexOf("-") + 1,
                        fileSegmentName.length());

                    logger.info("Importing table file segment: " + fileName +
                                " . Chunk sequence: " + chunkSequence);

                    populateEntryStreams(in,
                                         fileName,
                                         chunkSequence,
                                         true   // is table data
                                         );
                } catch (FileNotFoundException e) {
                    logger.log(Level.SEVERE, "Table File segment " + tableFile +
                               " not found in the export package.", e);
                }
            }
        }
    }

    /**
     * Gets the given LOB input stream from the export package
     *
     * @param lobFileName name of the lob file in export package
     */
    @Override
    InputStream getLobInputStream(String lobFileName) {

        File dataFolder = new File(exportFolder, "Data");
        File lobFolder = new File(dataFolder, "LOB");

        lobFolder = new File(lobFolder, lobFileName);

        if (!lobFolder.exists() || !lobFolder.isDirectory()) {

            logger.warning("LOB folder not found in export package.");
            return null;
        }

        /*
         * Batch all the lob file segment input streams
         */
        List<InputStream> inputStream = new ArrayList<InputStream>();

        for (File lobFile : lobFolder.listFiles()) {

            if (lobFile.isFile()) {
                try {
                    inputStream.add(new FileInputStream(lobFile));
                } catch (FileNotFoundException e) {
                    logger.log(Level.SEVERE, "File segment for LOB: " + lobFile
                               + "not found.", e);
                }
            }
        }

        /*
         * Return the entire batch of lob file segment input streams as a
         * single input stream using SequenceInputStream
         */
        Enumeration<InputStream> enumeration =
            new IteratorEnumeration<InputStream>(inputStream.iterator());
        SequenceInputStream stream = new SequenceInputStream(enumeration);

        return stream;
    }

    class IteratorEnumeration<E> implements Enumeration<E> {
        private final Iterator<E> iterator;

        public IteratorEnumeration(Iterator<E> iterator) {
            this.iterator = iterator;
        }

        @Override
        public E nextElement() {
            return iterator.next();
        }

        @Override
        public boolean hasMoreElements() {
            return iterator.hasNext();
        }

    }

    /**
     * Write all the file segments that have been successfully imported into
     * a status file
     */
    @Override
    void writeStatusFile(Set<String> fileSegments) {

        if (statusFile != null && fileSegments != null &&
                !fileSegments.isEmpty()) {

            logger.info("Writing the status log file.");

            PrintWriter writer = null;

            try {
                FileOutputStream fos = new FileOutputStream(statusFile);
                writer = new PrintWriter(fos);

                for (String dbname : fileSegments) {
                    writer.printf("%s\n", dbname);
                }

            } catch (Exception e) {

                logger.log(Level.SEVERE, "Exception saving status file " +
                           statusFile, e);

            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    /**
     * Nothing to do here for local file system
     */
    @Override
    void flushLogs() {

    }
}

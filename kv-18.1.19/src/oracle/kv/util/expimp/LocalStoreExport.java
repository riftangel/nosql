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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import oracle.kv.util.expimp.CustomStream.CustomInputStream;
import oracle.kv.util.expimp.ExitHandler.ExitCode;

/**
 * An implementation class for AbstractStoreExport used to export the data from
 * Oracle NoSql store to Local file system.
 */
public class LocalStoreExport extends AbstractStoreExport {

    /*
     * Directory in the file system holding the entire export package
     */
    private File exportFolder;

    /*
     * Directory path inside the export package holding all the table data files
     */
    private File tableFolder;

    /*
     * Directory path inside the export package holding all the LOB data files
     */
    private File lobFolder;

    /*
     * Directory path inside the export package holding Avro, None format
     * data files
     */
    private File otherFolder;

    /*
     * Export log file location inside the export package
     */
    private File exportLogFile;

    /*
     * Directory inside the export package holding the schema definition files
     */
    private File schemaFolder;
    private Logger logger;

    /*
     * Export folder directory names
     */
    private static final String logFileName = "Export.log";
    private static final String schemaFolderName = "SchemaDefinition";
    private static final String dataFolderName = "Data";
    private static final String lobFolderName = "LOB";
    private static final String tableFolderName = "Table";
    private static final String otherFolderName = "Other";
    private static final String exportStatsFileName = "Export.stat";

    /*
     * Size of exported LOB file segment = 1GB
     */
    private static final long fileSize = 1000 * 1000 * 1000;

    private static final int EXPORT_BUFFER_SIZE = 4096;

    /**
     * Constructor that creates the export package directory structure
     *
     * @param storeName kvstore name
     * @param helperHosts kvstore helper hosts
     * @param exportPackagePath path in local file system for export package
     * @param exportTable true if exporting subset of tables in the kvsotre.
     *                    false if exporting the entire kvstore
     */
    public LocalStoreExport(String storeName,
                            String[] helperHosts,
                            String userName,
                            String securityFile,
                            String exportPackagePath,
                            boolean exportTable,
                            boolean json) {

        super(storeName, helperHosts, userName, securityFile, json);

        exportFolder = new File(exportPackagePath);

        if (!exportFolder.exists() || !exportFolder.isDirectory()) {
            exit(false, ExitCode.EXIT_NO_EXPORT_FOLDER, System.err, null);
        }

        if (!Files.isWritable(exportFolder.toPath())) {
            exit(false, ExitCode.EXIT_NOWRITE, System.err, null);
        }

        exportLogFile = new File(exportFolder, logFileName);

        /*
         * Set handle to the logger
         */
        setLoggerHandler();

        schemaFolder = new File(exportFolder, schemaFolderName);
        schemaFolder.mkdir();

        File dataFolder = new File(exportFolder, dataFolderName);
        dataFolder.mkdir();

        tableFolder = new File(dataFolder, tableFolderName);
        tableFolder.mkdir();

        if (!exportTable) {

            lobFolder = new File(dataFolder, lobFolderName);
            lobFolder.mkdir();

            otherFolder = new File(dataFolder, otherFolderName);
            otherFolder.mkdir();
        }
    }

    /**
     * Export the file segment to the export package in local file system
     *
     * @param fileName file being exported
     * @param chunkSequence identifier for the file segment being exported
     * @param stream input stream reading bytes from kvstore into export store
     */
    @Override
    boolean doExport(String fileName,
                     String chunkSequence,
                     CustomInputStream stream) {

        OutputStream output = null;
        File file;

        if (fileName.equals(schemaFolderName)) {

            /*
             * Exported entity is a schema definition file segment
             */
            file = new File(schemaFolder,
                            fileName + "-" + chunkSequence +  ".txt");

        } else if (fileName.contains(otherFolderName)) {

            /*
             * Exported entity is OtherData file segment
             */
            file = new File(otherFolder,
                            fileName + "-" + chunkSequence + ".data");

        } else if (fileName.contains(lobFolderName)) {

            /*
             * Exported entity is a LOB file segment
             */
            file = new File(lobFolder, fileName);

            if (!file.exists()) {
                file.mkdir();
            }

            file = new File(file, fileName + "-" + chunkSequence + ".data");

        } else {

            /*
             * Exported entity is a table file segment
             */
            file = new File(tableFolder, fileName);

            if (!file.exists()) {
                file.mkdir();
            }

            file = new File(file, fileName + "-" + chunkSequence + ".data");
        }

        try {

            /*
             * Export the file segment into the export package
             */
            output = new FileOutputStream(file);
            exportDataStream(stream, output);
        } catch (Exception e) {

            logger.log(Level.SEVERE, "Exception exporting " + fileName +
                       ". Chunk sequence: " + chunkSequence, e);

            return false;
        } finally {

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {

                    logger.log(Level.SEVERE, "Exception exporting " + fileName +
                               ". Chunk sequence: " + chunkSequence, e);

                    return false;
                }
            }

            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {

                    logger.log(Level.SEVERE, "Exception exporting " + fileName +
                               ". Chunk sequence: " + chunkSequence, e);

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Export the stream of data from kvstore to the local filesystem
     *
     * @param input the InputStream to read from
     * @param output the OuputStream to write to
     * @throws IOException
     */
    private void exportDataStream(final InputStream input,
                                  final OutputStream output)
        throws IOException {

        int bytesRead = 0;
        byte[] buffer = new byte[EXPORT_BUFFER_SIZE];

        while ((bytesRead = input.read(buffer)) != -1) {
            output.write(buffer, 0, bytesRead);
        }
    }

    /**
     * No work needs to be done post export in case of local file system
     */
    @Override
    void doPostExportWork(Map<String, Chunk> chunks) {

    }

    /**
     * Returns the maximum size of lob file segment that will be exported
     */
    @Override
    long getMaxLobFileSize() {
        return fileSize;
    }

    /**
     * Sets the log handler
     */
    @Override
    void setLoggerHandler(Logger logger) {

        this.logger = logger;

        try {

            FileHandler fileHandler =
                new FileHandler(exportLogFile.getAbsolutePath(), false);
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
     * Nothing to do here for local file system
     */
    @Override
    void flushLogs() {

    }

    /**
     * Write the export stats - store name, helper hosts, export start time
     * and export end time to the export store - local file system
     */
    @Override
    void generateExportStats(String exportStats) {

        File exportStatsFile = new File(exportFolder, exportStatsFileName);
        PrintWriter out = null;
        try {
            out = new PrintWriter(exportStatsFile);
            out.write(exportStats);
        } catch (FileNotFoundException e) {
            logger.log(Level.WARNING, "Export stats file not found", e);
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }
}

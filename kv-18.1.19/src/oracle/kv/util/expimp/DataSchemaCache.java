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

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

/**
 * Cache of all the table schemas held during the export/import process.
 * Instead of recreating the schemas for all the reconstructing export/import,
 * the schemas are cached to improve the performance
 */
public class DataSchemaCache {

    /*
     * Map holding all the writer schemas for a given table, keyed first by
     * table name (in the outermost Map), and then by table version in the
     * inner Map.
     */
    private final Map<String, Map<Integer, Schema>> expWriterSchemas;

    /*
     * Map holding the reader schema for a given table, keyed by table name.
     */
    private final Map<String, Schema> expReaderSchemas;

    public DataSchemaCache() {
        expWriterSchemas = new HashMap<String, Map<Integer, Schema>>();
        expReaderSchemas = new HashMap<String, Schema>();
    }

    /**
     * Return the writer schema for a given table name and version
     */
    public Schema getWriterSchema(String tableName, Integer tableVersion) {

        Map<Integer, Schema> expWriterSchema = expWriterSchemas.get(tableName);

        if (expWriterSchema == null) {
            expWriterSchema = new HashMap<Integer, Schema>();
            expWriterSchemas.put(tableName, expWriterSchema);
        }

        return expWriterSchema.get(tableVersion);
    }

    /**
     * Store the writer schema for a given table name and version
     */
    public void putWriterSchema(String tableName,
                                Integer tableVersion,
                                Schema writerSchema) {

        Map<Integer, Schema> expWriterSchema = expWriterSchemas.get(tableName);
        expWriterSchema.put(tableVersion, writerSchema);
    }

    /**
     * Return the reader schema for a given table name
     */
    public Schema getReaderSchema(String tableName) {
        return expReaderSchemas.get(tableName);
    }

    /**
     * Store the reader schema for a given table name
     */
    public void putReaderSchema(String tableName, Schema readerSchema) {
        expReaderSchemas.put(tableName, readerSchema);
    }
}

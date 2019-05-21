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

package oracle.kv.hadoop;

import oracle.kv.KeyValueVersion;
import oracle.kv.KVStore;

import org.apache.avro.generic.IndexedRecord;

/**
 * Avro Formatter is an interface implemented by user-specified classes that
 * can format NoSQL Database records into AvroRecords to be returned by a
 * KVAvroRecordReader. {@link #toAvroRecord} is called once for each record
 * retrieved by a KVAvroInputFormat.
 *
 * @since 2.0
 *
 * @deprecated as of 4.0, use the table API instead.
 */
@Deprecated
public interface AvroFormatter {

    /**
     * Convert a KeyValueVersion into a String.
     *
     * @param kvv the Key and Value to be formatted.
     *
     * @param kvstore the KV Store object related to this record so that the
     * Formatter may retrieve (e.g.) Avro bindings.
     *
     * @return The Avro record suitable for interpretation by the caller
     * of the KVAvroInputFormat.
     */
    public IndexedRecord toAvroRecord(KeyValueVersion kvv, KVStore kvstore);
}

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

package oracle.kv.avro;

import java.util.Arrays;

import org.apache.avro.Schema;

/**
 * A RawRecord represents an Avro object as a Schema along with the raw Avro
 * serialized data. It is used with a {@link RawAvroBinding}.
 *
 * @see RawAvroBinding
 * @see AvroCatalog#getRawBinding getRawBinding
 *
 * @since 2.0
 *
 * @deprecated as of 4.0, use the table API instead.
 */
@Deprecated
public class RawRecord {
    private final byte[] rawData;
    private final Schema schema;

    /**
     * Creates a RawRecord from a Schema and Avro serialized data.
     */
    public RawRecord(byte[] rawData, Schema schema) {
        this.rawData = rawData;
        this.schema = schema;
    }

    /**
     * Returns the Avro serialized data for this RawRecord.
     */
    public byte[] getRawData() {
        return rawData;
    }

    /**
     * Returns the Avro Schema for this RawRecord.
     */
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RawRecord)) {
            return false;
        }
        final RawRecord o = (RawRecord) other;
        return Arrays.equals(rawData, o.rawData) && schema.equals(o.schema);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rawData);
    }

    @Override
    public String toString() {
        return Arrays.toString(rawData) + "\nSchema: " + schema.toString();
    }
}

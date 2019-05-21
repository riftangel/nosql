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

package oracle.kv.impl.api.avro;

import org.apache.avro.Schema;

import com.sleepycat.util.PackedInteger;

import oracle.kv.Value;
import oracle.kv.avro.RawAvroBinding;
import oracle.kv.avro.RawRecord;
import oracle.kv.avro.UndefinedSchemaException;

/**
 * This class forms the basis for other binding implementations in that it is
 * responsible for packaging a schema ID and Avro binary data into a Value byte
 * array, and unpackaging it.  It also ensures that the toObject method throws
 * IllegalArgumentException when the schema ID is invalid, and that the toValue
 * method throws UndefinedSchemaException when the schema is unknown.
 */
@SuppressWarnings("deprecation")
class RawBinding implements RawAvroBinding {

    private final SchemaCache schemaCache;

    RawBinding(SchemaCache schemaCache) {
        this.schemaCache = schemaCache;
    }

    @Override
    public RawRecord toObject(Value value)
        throws IllegalArgumentException {

        /* Derive schema from schema ID in value byte array. */
        final Schema schema = getValueSchema(value, schemaCache);

        /* Copy raw data out of value byte array. */
        final byte[] buf = value.getValue();
        final int dataOffset = getValueRawDataOffset(value);
        final byte[] rawData = new byte[buf.length - dataOffset];
        System.arraycopy(buf, dataOffset, rawData, 0, rawData.length);

        return new RawRecord(rawData, schema);
    }

    public RawRecord toObjectForImport(Value value, Schema schema)
        throws IllegalArgumentException {

        /* Copy raw data out of value byte array. */
        final byte[] buf = value.getValue();
        final int dataOffset = getValueRawDataOffset(value);
        final byte[] rawData = new byte[buf.length - dataOffset];
        System.arraycopy(buf, dataOffset, rawData, 0, rawData.length);

        return new RawRecord(rawData, schema);
    }

    /**
     * Shared by toObject and SharedCache.CBindingBridgeImpl.
     * See CBindingBridge.getValueRawDataOffset.
     */
    static int getValueRawDataOffset(Value value) {
        return PackedInteger.getReadSortedIntLength(value.getValue(), 0);
    }

    /**
     * Shared by toObject and SharedCache.CBindingBridgeImpl.
     * See CBindingBridge.getValueSchema.
     */
    static Schema getValueSchema(Value value, SchemaCache schemaCache)
        throws IllegalArgumentException {

        /* Check value format. */
        if (value.getFormat() != Value.Format.AVRO) {
            throw new IllegalArgumentException("Value.Format is not AVRO");
        }

        /* Unpackage schema ID and raw data (Avro binary data). */
        final byte[] buf = value.getValue();
        final int schemaId;
        try {
            schemaId = PackedInteger.readSortedInt(buf, 0);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException
                ("Internal schema ID in Value is invalid, possibly due to" +
                 " incorrect binding usage", e);
        }

        /* Get schema and check for invalid schema ID. */
        final SchemaInfo info = schemaCache.getSchemaInfoById(schemaId);
        if (info == null) {
            throw new IllegalArgumentException
                ("Internal schema ID in Value is unknown, possibly due to" +
                 " incorrect binding usage; schemaId: " + schemaId);
        }

        return info.getSchema();
    }

    @Override
    public Value toValue(RawRecord object)
        throws UndefinedSchemaException {

        final Schema schema = object.getSchema();
        final byte[] rawData = object.getRawData();
        final int rawDataSize = rawData.length;

        /* Allocate value byte array and copy in schema ID. */
        final Value value = allocateValue(schema, rawDataSize, schemaCache);

        /* Copy in raw data. */
        final int dataOffset = getValueRawDataOffset(value);
        final byte[] buf = value.getValue();
        System.arraycopy(rawData, 0, buf, dataOffset, rawDataSize);

        return value;
    }

    /**
     * Shared by toValue and SharedCache.CBindingBridgeImpl.
     * See CBindingBridge.allocateValue.
     */
    static Value allocateValue(Schema schema,
                               int rawDataSize,
                               SchemaCache schemaCache)
        throws UndefinedSchemaException {

        /* Get schema ID and check for undefined schema. */
        final SchemaInfo info = schemaCache.getSchemaInfoByValue(schema);
        if (info == null) {
            throw AvroCatalogImpl.newUndefinedSchemaException(schema);
        }
        final int schemaId = info.getId();

        /* Allocate room for schema ID and raw data (Avro binary data). */
        final int dataOffset = PackedInteger.getWriteSortedIntLength(schemaId);
        final byte[] buf = new byte[dataOffset + rawDataSize];

        /* Copy in the schema ID. */
        PackedInteger.writeSortedInt(buf, 0, schemaId);

        return Value.internalCreateValue(buf, Value.Format.AVRO);
    }
}

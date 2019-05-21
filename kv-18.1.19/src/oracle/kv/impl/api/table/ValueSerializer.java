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

package oracle.kv.impl.api.table;

import java.util.Iterator;
import java.util.Map.Entry;

import oracle.kv.table.ArrayDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.MapDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.FieldDef.Type;

/**
 * This class encapsulates 5 Serializer interfaces thats read information of
 * Row, RecordValue, MapValue, ArrayValue and FieldValue used to serialize to
 * Key/Value.
 */
public class ValueSerializer {

    /**
     * A Serializer interface to read information of Row.
     */
    public interface RowSerializer extends RecordValueSerializer {
        /**
         * Returns the table of Row instance.
         */
        Table getTable();

        /**
         * Returns the time to live (TTL) value for this row
         */
        TimeToLive getTTL();

        /**
         * Returns true if the RowSerializer is for a Primary Key.
         */
        boolean isPrimaryKey();

        /**
         * Returns a name that represents the class, it is used in the error
         * message.
         */
        String getClassNameForError();
    }

    /**
     * A Serializer interface to read information of RecordValue.
     */
    public interface RecordValueSerializer {
        /**
         * Returns the number of the fields in this record.
         */
        int size();

        /**
         * Returns the serializer instance of field value at the given position.
         */
        FieldValueSerializer get(int pos);

        /**
         * Returns the record type that this record conforms to.
         */
        RecordDef getDefinition();
    }

    /**
     * A Serializer interface to read information of MapValue.
     */
    public interface MapValueSerializer {

        /**
         * Returns the MapDef that defines the content of this map.
         */
        MapDef getDefinition();

        /**
         * Returns the size of the map.
         */
        int size();

        /**
         * Returns the iterator of MapValue entries, the entry is String and
         * FieldValueSerialzier pair.
         */
        Iterator<Entry<String, FieldValueSerializer>> iterator();
    }

    /**
     * A Serializer interface to read information of ArrayValue.
     */
    public interface ArrayValueSerializer {
        /**
         * Returns the ArrayDef that defines the content of this array.
         */
         ArrayDef getDefinition();

        /**
         * Returns the size of the array.
         */
        int size();

        /**
         * Returns the iterator of ArrayValue elements.
         */
        Iterator<FieldValueSerializer> iterator();
    }

    /**
     * A Serializer interface to read information of FieldValue.
     */
    public interface FieldValueSerializer {
        /**
         * Returns the type associated with this value.
         */
        FieldDef getDefinition();

        /**
         * Returns the kind of the type associated with this value.
         */
        Type getType();

        /**
         * Returns true if the value is NULL value.
         */
        boolean isNull();

        /**
         * Returns true if the value is NULL JSON value.
         */
        boolean isJsonNull();

        /**
         * Returns true if the value is EMPTY value.
         */
        boolean isEMPTY();

        /**
         * Returns the int value of this object.
         */
        int getInt();

        /**
         * Returns the string value of this object.
         */
        String getString();

        /**
         * Returns the long value of this object.
         */
        long getLong();

        /**
         * Returns the double value of this object.
         */
        double getDouble();

        /**
         * Returns the float value of this object.
         */
        float getFloat();

        /**
         * Returns the bytes value of this object.
         */
        byte[] getBytes();

        /**
         * Returns the boolean value of this object.
         */
        boolean getBoolean();

        /**
         * Returns the string of the ENUM value object.
         */
        String getEnumString();

        /**
         * Returns the bytes value of this Fixed binary object.
         */
        byte[] getFixedBytes();

        /**
         * Returns the bytes value of this Number value object.
         */
        byte[] getNumberBytes();

        /**
         * Returns the bytes value of this Timestamp value object.
         */
        byte[] getTimestampBytes();

        /**
         * Casts to RecordValueSerializer
         *
         * @throws ClassCastException if the current value is not a RecordValue
         */
        RecordValueSerializer asRecordValueSerializer();

        /**
         * Casts to MapValueSerializer
         *
         * @throws ClassCastException if the current value is not a MapValue
         */
        MapValueSerializer asMapValueSerializer();

        /**
         * Casts to ArrayValueSerializer
         *
         * @throws ClassCastException if the current value is not an ArrayValue
         */
        ArrayValueSerializer asArrayValueSerializer();
    }
}

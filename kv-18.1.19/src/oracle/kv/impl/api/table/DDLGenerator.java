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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.table.ArrayDef;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FixedBinaryDef;
import oracle.kv.table.Index;
import oracle.kv.table.MapDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.TimestampDef;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;

/**
 * Generates the Table Definition and Index Definition DDLs from Table
 */
public class DDLGenerator {

    private final Table table;
    private final StringBuilder tableSb;
    private final List<String> indexDDLs;

    public DDLGenerator(Table table) {
        this.table = table;

        tableSb = new StringBuilder();
        generateDDL();
        indexDDLs = new ArrayList<String>();
        generateAllIndexDDL();
    }

    /**
     * Create a generator from a JSON description.
     * First, create the Table, then use it.
     * TODO: remove this when it's clear there are no users.
     * The *old* cloud proxy still references this but that will
     * change.
     */
    public DDLGenerator(final String jsonTable) {
        table = TableJsonUtils.fromJsonString(jsonTable, null);
        tableSb = new StringBuilder();
        generateDDL();
        indexDDLs = new ArrayList<String>();
        generateAllIndexDDL();
    }

    /** Retrieve the DDL which describes the table */
    public String getDDL() {
        return tableSb.toString();
    }


    /** Retrieve the index DDLs */
    public List<String> getAllIndexDDL() {
        return indexDDLs;
    }

    /**
     * Generate the Table DDL definition
     */
    private void generateDDL() {

        /* Append the table preamble */
        tableSb.append("CREATE TABLE ").append(table.getFullName())
            .append(" (");

        /*
         * List that contains all the parent key fields which needs to be
         * ignored while constructing the DDL. For the top most level table,
         * this list is empty
         */
        List<String> rejectKeyList = new ArrayList<String>();
        Table parentTable = table.getParent();

        if (parentTable != null) {
            rejectKeyList = parentTable.getPrimaryKey();
        }
        TableImpl tableImpl = (TableImpl)table;
        FieldMap fieldMap = tableImpl.getFieldMap();

        /*
         * Generate the DDL for all the fields of this table
         */
        getAllFieldDDL(tableImpl.getPrimaryKeyInternal(),
                       fieldMap, rejectKeyList);

        tableSb.append(", ");

        /*
         * Generate the DDL of the keys
         */
        if (parentTable != null) {
            getChildTableKeyDDL(rejectKeyList);
        } else {
            getParentTableKeyDDL();
        }

        tableSb.append(")");

        /*
         * Generate the DDL statement for the TTL if it exists
         */
        TimeToLive defaultTTL = table.getDefaultTTL();

        if (defaultTTL != null) {
            tableSb.append(" USING TTL ").append(defaultTTL.getValue())
                .append(" ").append(defaultTTL.getUnit().name());
        }
    }


    /**
     * Generate the Index definition DDLs for the table
     */
    private void generateAllIndexDDL() {

        Map<String, Index> indexes = table.getIndexes();

        if (indexes.size() != 0) {
            for (Map.Entry<String, Index> indexEntry : indexes.entrySet()) {
                Index index = indexEntry.getValue();
                indexDDLs.add(getIndexDDL(index));
            }
        }
    }

    /**
     * Generate the DDL for all the fields of this table
     *
     * @param fieldMap
     * @param rejectKeyList empty for top most level table. For the child tables
     *                      this list contains the primary keys fields of its
     *                      top level parent tables which needs to be ignored
     *                      while building the DDL
     */
    private void getAllFieldDDL(List<String> primaryKeyFields,
                                FieldMap fieldMap, List<String> rejectKeyList) {

        int numFields = fieldMap.size();

        for (int i = 0; i < numFields; i++) {

            FieldMapEntry entry = fieldMap.getFieldMapEntry(i);
            String fname = entry.getFieldName();

            if (rejectKeyList.contains(fname)) {
                continue;
            }

            FieldDef field = entry.getFieldDef();

            getFieldDDL(field, fname);

            if (!entry.isNullable() && (primaryKeyFields == null ||
                                        !primaryKeyFields.contains(fname))) {
                tableSb.append(" NOT NULL");
            }

            FieldValueImpl fieldValue = entry.getDefaultValue();

            if (!fieldValue.isNull()) {
                String defValue = fieldValue.toString();

                tableSb.append(" DEFAULT ").append(defValue);
            }

            String description = field.getDescription();

            if (description != null) {
                tableSb.append(" COMMENT \"").append(description)
                    .append("\"");
            }

            if (i == numFields - 1) {
                break;
            }

            tableSb.append(", ");
        }
    }

    /**
     * Generate the DDL for the generic table field.
     *
     * @param field generic FieldDef instance
     * @param fname name of the field
     */
    private void getFieldDDL(FieldDef field, String fname) {

        switch (field.getType()) {
            case STRING:
            case BINARY:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case NUMBER:
            case JSON:
                /*
                 * Generate the ddl for String, Binary, Boolean, Double
                 * Float, Number and Json type fields
                 */
                getDDL(field, fname);
                break;

            case ENUM:
                /*
                 * Generate the ddl for Enum type fields
                 */
                EnumDef enumField = (EnumDef)field;
                getEnumDDL(enumField, fname);
                break;

            case FIXED_BINARY:
                /*
                 * Generate the ddl for fixed binary type fields
                 */
                FixedBinaryDef fixedBinaryField = (FixedBinaryDef)field;
                getFixedBinaryDDL(fixedBinaryField, fname);
                break;

            case INTEGER:
            case LONG:
                /*
                 * Generate the ddl for Integer and Long type fields
                 */
                getNumericDDL(field, fname);
                break;

            case ARRAY:
                /*
                 * Generate the ddl for Array type fields
                 */
                ArrayDef arrayField = (ArrayDef)field;
                getArrayDDL(arrayField, fname);
                break;

            case MAP:
                /*
                 * Generate the ddl for Map type fields
                 */
                MapDef mapField = (MapDef)field;
                getMapDDL(mapField, fname);
                break;

            case RECORD:
                /*
                 * Generate the ddl for Record type fields
                 */
                RecordDef recField = (RecordDef)field;
                getRecordDDL(recField, fname);
                break;

            case TIMESTAMP:
                /*
                 * Generate the ddl for Timestamp type fields
                 */
                TimestampDef tsField = (TimestampDef)field;
                getTimestampDDL(tsField, fname);
                break;

            case ANY:
            case ANY_ATOMIC:
            case ANY_RECORD:
            case ANY_JSON_ATOMIC:
            case EMPTY:
                break;
        }
    }

    /**
     * @param index Index instance
     * @return DDL representing the Index
     */
    private String getIndexDDL(Index index) {

        StringBuilder sb = new StringBuilder();

        /* Append the index preamble */
        sb.append("CREATE");

        if (index.getType() == Index.IndexType.TEXT) {
            sb.append(" FULLTEXT");
        }

        sb.append(" INDEX ").append(index.getName())
            .append(" ON ").append(table.getFullName()).append("(");

        /* Append the fields */
        List<String> fields = ((IndexImpl)index).getFields();
        int numFields = fields.size();

        for (int i = 0; i < numFields; i++) {

            String field = fields.get(i);
            sb.append(field);

            FieldDef.Type type = ((IndexImpl)index).getFieldType(i);
            if (type != null) {
                sb.append(" as " + type);
            }

            /*
             * Append the text index field annotation
             */
            if (index.getType() == Index.IndexType.TEXT) {

                String annotationField = index.getAnnotationForField(field);
                if (annotationField != null) {
                    sb.append(" ").append(annotationField);
                }
            }

            if (i == numFields - 1) {
                break;
            }
            sb.append(",");
        }

        sb.append(")");

        /*
         * Append the text index properties
         */
        if (index.getType() == Index.IndexType.TEXT) {

            IndexImpl indexImpl = (IndexImpl) index;
            Map<String, String> properties = indexImpl.getProperties();

            for (Map.Entry<String, String> entry : properties.entrySet()) {
                sb.append(" ").append(entry.getKey()).append(" = ")
                    .append(entry.getValue());
            }
        }

        /*
         * Append the index description
         */
        String description = index.getDescription();

        if (description != null) {
            sb.append(" COMMENT \"").append(description)
                .append("\"");
        }

        return sb.toString();
    }

    /**
     * Generates the ddl for String, Binary, Boolean, Double, Float
     * Number and Json type fields
     */
    private void getDDL(FieldDef field, String fname) {

        String type = field.getType().toString();

        if (fname == null) {
            tableSb.append(type);
            return;
        }

        tableSb.append(fname).append(" ").append(type);
    }

    /**
     * Generates the ddl for Enum type fields
     */
    private void getEnumDDL(EnumDef field, String fname) {

        String type = field.getType().toString();

        if (fname == null) {
            tableSb.append(type);
            return;
        }

        tableSb.append(fname).append(" ").append(type).append("(");

        String symbol = null;
        String[] allSymbols = field.getValues();
        int numSymbols = allSymbols.length;

        for (int i = 0; i < numSymbols; i++) {
            symbol = allSymbols[i];

            if (i == numSymbols - 1) {
                break;
            }

            tableSb.append(symbol).append(", ");
        }

        if (symbol != null) {
            tableSb.append(symbol);
        }

        tableSb.append(")");
    }

    /**
     * Generate the ddl for fixed binary type fields
     */
    private void getFixedBinaryDDL(FixedBinaryDef field, String fname) {

        if (fname == null) {
            tableSb.append("BINARY(").append(field.getSize()).append(")");
            return;
        }

        tableSb.append(fname).append(" ").append("BINARY(")
            .append(field.getSize()).append(")");
    }

    /**
     * Generates the ddl for Integer and Long type fields
     */
    private void getNumericDDL(FieldDef field, String fname) {

        String type = field.getType().toString();

        if (fname == null) {
            tableSb.append(type);
            return;
        }

        tableSb.append(fname).append(" ").append(type);
    }

    /**
     * Generate the ddl for Array type fields
     */
    private void getArrayDDL(ArrayDef field, String fname) {

        FieldDef element = field.getElement();

        if (fname == null) {
            tableSb.append(field.getType().toString()).append("(");

            /*
             * Recursively generate the field DDL of the Array element field
             */
            getFieldDDL(element, null);
            tableSb.append(")");
            return;
        }

        tableSb.append(fname).append(" ").append(field.getType().toString())
            .append("(");

        /*
         * Recursively generate the field DDL of the Array element field
         */
        getFieldDDL(element, null);
        tableSb.append(")");
    }

    /**
     * Generate the ddl for Map type fields
     */
    private void getMapDDL(MapDef field, String fname) {

        FieldDef element = field.getElement();

        if (fname == null) {
            tableSb.append(field.getType().toString()).append("(");

            /*
             * Recursively generate the field DDL of the Array element field
             */
            getFieldDDL(element, null);
            tableSb.append(")");
            return;
        }

        tableSb.append(fname).append(" ").append(field.getType().toString())
            .append("(");

        /*
         * Recursively generate the field DDL of the Array element field
         */
        getFieldDDL(element, null);
        tableSb.append(")");
    }

    /**
     * Generate the ddl for Record type fields
     */
    private void getRecordDDL(RecordDef field, String fname) {

        RecordDefImpl fieldImpl = (RecordDefImpl)field;
        FieldMap fieldMap = fieldImpl.getFieldMap();

        if (fname == null) {
            tableSb.append(field.getType().toString()).append("(");

            /*
             * Recursively generate the field DDLs of the Record fields
             */
            getAllFieldDDL(null, fieldMap, new ArrayList<String>());

            tableSb.append(")");
            return;
        }

        tableSb.append(fname).append(" ").append(field.getType().toString())
            .append("(");

        /*
         * Recursively generate the field DDLs of the Record fields
         */
        getAllFieldDDL(null, fieldMap, new ArrayList<String>());
        tableSb.append(")");
    }

    /**
     * Generate the ddl for timestamp type fields
     */
    private void getTimestampDDL(TimestampDef field, String fname) {

        if (fname == null) {
            tableSb.append("TIMESTAMP(").append(field.getPrecision()).append(")");
            return;
        }

        tableSb.append(fname).append(" ").append("TIMESTAMP(")
            .append(field.getPrecision()).append(")");
    }

    /**
     * Generate the DDL statement for the Child Table key
     */
    private void getChildTableKeyDDL(List<String> rejectKeyList) {

        tableSb.append("PRIMARY KEY(");
        String nextKey = "";

        /*
         * Table primary key fields
         */
        List<String> primaryKey = table.getPrimaryKey();

        TableImpl tabImpl = (TableImpl)table;
        List<Integer> primaryKeySizes = tabImpl.getPrimaryKeySizes();
        int keyIndex = rejectKeyList.size();

        int pKeySize = primaryKey.size();

        for (int i = 0; i < pKeySize; i++) {

            nextKey = primaryKey.get(i);

            /*
             * If the field is a part of parent table primary key, do not
             * include it in the child table DDL statement
             */
            if (rejectKeyList.contains(nextKey)) {
                continue;
            }

            if (i == pKeySize - 1) {
                break;
            }

            tableSb.append(nextKey);
            appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
            tableSb.append(", ");
        }

        tableSb.append(nextKey);
        appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
        tableSb.append(")");
    }

    /**
     * Generate the DDL statement for the Parent Table key
     */
    private void getParentTableKeyDDL() {

        /*
         * Table primary key fields
         */
        List<String> primaryKey = table.getPrimaryKey();

        TableImpl tabImpl = (TableImpl)table;
        List<Integer> primaryKeySizes = tabImpl.getPrimaryKeySizes();
        int keyIndex = 0;

        /*
         * Table shard key fields
         */
        List<String> shardKey = table.getShardKey();
        int sKeySize = shardKey.size();
        Iterator<String> pKeyIterator = primaryKey.iterator();

        tableSb.append("PRIMARY KEY(");

        /*
         * Retrieve the shard fields
         */
        if (sKeySize != 0) {

            tableSb.append("SHARD(");
            String nextKey = "";

            for (int i = 0; i < sKeySize; i++) {

                nextKey = shardKey.get(i);
                pKeyIterator.next();

                if (i == sKeySize - 1) {
                    break;
                }

                tableSb.append(nextKey);
                appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
                tableSb.append(", ");
            }

            tableSb.append(nextKey);
            appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
            tableSb.append(")");

            if (pKeyIterator.hasNext()) {
                tableSb.append(", ");
            }
        }

        String nextKey = "";

        /*
         * Retrieve the primary key fields
         */
        while (pKeyIterator.hasNext()) {

            nextKey = pKeyIterator.next();

            if (!pKeyIterator.hasNext()) {
                break;
            }

            tableSb.append(nextKey);
            appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
            tableSb.append(", ");
        }

        tableSb.append(nextKey);
        appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
        tableSb.append(")");
    }

    /**
     * Append the primary key sizes
     */
    private void appendPrimaryKeySizes(List<Integer> primaryKeySizes,
                                       int keyIndex) {

        if (primaryKeySizes != null && keyIndex != primaryKeySizes.size()) {
            Integer pKeySize = primaryKeySizes.get(keyIndex);

            if (pKeySize != 0) {
                tableSb.append("(")
                       .append(pKeySize.toString())
                       .append(")");
            }
        }
    }
}

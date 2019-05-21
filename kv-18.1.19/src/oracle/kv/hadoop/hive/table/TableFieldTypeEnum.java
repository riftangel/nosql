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

package oracle.kv.hadoop.hive.table;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Enum class that defines values corresponding to each of the enum
 * values defined in FieldDef.Type; which represent the possible field
 * types of a KV Store table. The methods of this enum provide a
 * mechanism for mapping a table defined in a given KV Store and a
 * table created in Hive.
 */
public enum TableFieldTypeEnum {


    TABLE_FIELD_STRING {
        @Override
        public String toString() {
            return FieldDef.Type.STRING.toString();
        }
    },
    TABLE_FIELD_BOOLEAN {
        @Override
        public String toString() {
            return FieldDef.Type.BOOLEAN.toString();
        }
    },
    TABLE_FIELD_INTEGER {
        @Override
        public String toString() {
            return FieldDef.Type.INTEGER.toString();
        }
    },
    TABLE_FIELD_LONG {
        @Override
        public String toString() {
            return FieldDef.Type.LONG.toString();
        }
    },
    TABLE_FIELD_FLOAT {
        @Override
        public String toString() {
            return FieldDef.Type.FLOAT.toString();
        }
    },
    TABLE_FIELD_DOUBLE {
        @Override
        public String toString() {
            return FieldDef.Type.DOUBLE.toString();
        }
    },
    TABLE_FIELD_ENUM {
        @Override
        public String toString() {
            return FieldDef.Type.ENUM.toString();
        }
    },
    TABLE_FIELD_BINARY {
        @Override
        public String toString() {
            return FieldDef.Type.BINARY.toString();
        }
    },
    TABLE_FIELD_FIXED_BINARY {
        @Override
        public String toString() {
            return FieldDef.Type.FIXED_BINARY.toString();
        }
    },
    TABLE_FIELD_MAP {
        @Override
        public String toString() {
            return FieldDef.Type.MAP.toString();
        }
    },
    TABLE_FIELD_RECORD {
        @Override
        public String toString() {
            return FieldDef.Type.RECORD.toString();
        }
    },
    TABLE_FIELD_ARRAY {
        @Override
        public String toString() {
            return FieldDef.Type.ARRAY.toString();
        }
    },
    TABLE_FIELD_NULL {
        @Override
        public String toString() {
            return "NULL kv table field";
        }
    },
    TABLE_FIELD_UNKNOWN_TYPE {
        @Override
        public String toString() {
            return "unknown kv table field type";
        }
    };

    /**
     * Maps the given field type of a KV Store table to the corresponding
     * enum value defined in this class; corresponding to a field type of
     * a KV Store table.
     */
    public static TableFieldTypeEnum fromKvType(FieldDef.Type kvType) {
        if (kvType == null) {
            return null;
        }
        return stringToEnumValue(kvType.toString());
    }

    /**
     * Maps the given Hive column type to the corresponding enum value
     * defined in this class; corresponding to a field type of a KV Store
     * table. Note that some of the Hive types have no corresponding type;
     * in which case, TABLE_FIELD_UNKNOWN_TYPE is returned.
     */
    public static TableFieldTypeEnum fromHiveType(TypeInfo hiveType) {
        return fromHiveType(hiveType, null);
    }

    public static TableFieldTypeEnum fromHiveType(
                                     TypeInfo hiveType, FieldDef.Type kvType) {

        /* Check for Primitive types. */

        final String typeName = hiveType.getTypeName();

        if (serdeConstants.BOOLEAN_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_BOOLEAN;
        }
        if (serdeConstants.INT_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_INTEGER;
        }
        if (serdeConstants.BIGINT_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_LONG;
        }
        if (serdeConstants.FLOAT_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_FLOAT;
        }
        if (serdeConstants.DECIMAL_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_FLOAT;
        }
        if (serdeConstants.DOUBLE_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_DOUBLE;
        }
        if (serdeConstants.STRING_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_STRING;
        }
        if (serdeConstants.BINARY_TYPE_NAME.equals(typeName)) {
            if (TABLE_FIELD_FIXED_BINARY.equals(fromKvType(kvType))) {
                return TABLE_FIELD_FIXED_BINARY;
            }
            return TABLE_FIELD_BINARY;
        }

        /* The following Hive types have no match in KV table types. */

        if (serdeConstants.TINYINT_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.SMALLINT_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.DATE_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.DATETIME_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.TIMESTAMP_TYPE_NAME.equals(
                                                   typeName)) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }
        if (serdeConstants.UNION_TYPE_NAME.equals(typeName)) {
            return TABLE_FIELD_UNKNOWN_TYPE;
        }

        /* Check for complex types. */
        final ObjectInspector.Category category = hiveType.getCategory();

        if (ObjectInspector.Category.LIST.equals(category)) {
            return TABLE_FIELD_ARRAY;
        }
        if (ObjectInspector.Category.MAP.equals(category)) {
            if (TABLE_FIELD_ENUM.equals(fromKvType(kvType))) {
                return TABLE_FIELD_ENUM;
            }
            return TABLE_FIELD_MAP;
        }
        if (ObjectInspector.Category.STRUCT.equals(category)) {
            return TABLE_FIELD_RECORD;
        }

        return TABLE_FIELD_UNKNOWN_TYPE;
    }

    public static TableFieldTypeEnum stringToEnumValue(String str) {
        for (TableFieldTypeEnum enumVal : TableFieldTypeEnum.values()) {
            if (enumVal.toString().equals(str)) {
                return enumVal;
            }
        }
        final String msg = "no enum value " + TableFieldTypeEnum.class +
                           "." + str;
        throw new IllegalArgumentException(msg);
    }

    public static boolean kvHiveTypesMatch(
                              FieldDef kvFieldDef, TypeInfo hiveColumnType) {

        final Log LOG = LogFactory.getLog(TableFieldTypeEnum.class.getName());

        /* Compare top-level types. */
        final FieldDef.Type kvFieldType = kvFieldDef.getType();

        if (!fromKvType(kvFieldType).equals(
                                fromHiveType(hiveColumnType, kvFieldType))) {

            /* Special case: KV type ENUM & Hive type STRING handled below. */
            if (!(TABLE_FIELD_ENUM.equals(fromKvType(kvFieldType)) &&
                  TABLE_FIELD_STRING.equals(fromHiveType(hiveColumnType)))) {

                LOG.error("Field type MISMATCH: " + fromKvType(kvFieldType) +
                           " != " + fromHiveType(hiveColumnType, kvFieldType));
                return false;
            }
        }

        /* If top-level types are primitive and match, then it's a match. */
        if (isPrimitive(kvFieldType) && isPrimitive(hiveColumnType)) {
            return true;
        }

        /* Top-level types match, but neither are primitive; deep dive. */
        switch (kvFieldType) {

            /* If kvType is ENUM, then Hive type must be STRING */
            case ENUM:

                if (!TABLE_FIELD_STRING.equals(
                                            fromHiveType(hiveColumnType))) {

                    LOG.error("Field type MISMATCH: for KV ENUM field type, " +
                              "expected Hive STRING column type, but Hive " +
                              "column type is " +
                              hiveColumnType.getTypeName());
                    return false;
                }
                return true;

            /*
             * If kvType is ARRAY, then Hive type must be LIST, and must have
             * matching element type.
             */
            case ARRAY:

                if (!Category.LIST.equals(hiveColumnType.getCategory())) {

                    LOG.error("Field type MISMATCH: for KV ARRAY field " +
                              "type, expected Hive LIST column type, but " +
                              "Hive column type is " +
                              hiveColumnType.getCategory());
                    return false;
                }
                final TypeInfo hiveElementType =
                    ((ListTypeInfo) hiveColumnType).getListElementTypeInfo();

                final ArrayValue kvArrayValue = kvFieldDef.createArray();
                final ArrayDef kvArrayDef = kvArrayValue.getDefinition();
                final FieldDef kvElementDef = kvArrayDef.getElement();

                LOG.debug("KV ARRAY field type and Hive LIST column type: " +
                          "comparing KV ARRAY element type [" +
                          kvElementDef.getType() + "] with Hive LIST " +
                          "element type [" + hiveElementType + "]");

                return kvHiveTypesMatch(kvElementDef, hiveElementType);

            /*
             * If kvType is MAP, then Hive type must be MAP<STRING, type>, and
             * must have matching value types.
             */
            case MAP:

                if (!Category.MAP.equals(hiveColumnType.getCategory())) {

                    LOG.error("Field type MISMATCH: for KV MAP field type, " +
                              "expected Hive MAP column type, but Hive " +
                              "column type is " +
                              hiveColumnType.getCategory());
                    return false;
                }

                final TypeInfo hiveMapKeyType =
                    ((MapTypeInfo) hiveColumnType).getMapKeyTypeInfo();
                final TypeInfo hiveMapValType =
                    ((MapTypeInfo) hiveColumnType).getMapValueTypeInfo();

                /* Hive key type must be STRING. */
                if (!TABLE_FIELD_STRING.equals(fromHiveType(hiveMapKeyType))) {

                    LOG.error("Field type MISMATCH: for KV MAP field type " +
                              "and Hive MAP column type, expected STRING " +
                              "key type, but Hive MAP column's key type is " +
                              fromHiveType(hiveMapKeyType));
                    return false;
                }

                /* Hive value type must match kv value type. */
                final MapValue kvMapValue = kvFieldDef.createMap();
                final MapDef kvMapDef = kvMapValue.getDefinition();
                final FieldDef kvMapValueDef = kvMapDef.getElement();

                LOG.debug("KV MAP field type and Hive MAP column type: " +
                          "comparing KV MAP value type [" +
                          kvMapValueDef.getType() + "] with Hive MAP " +
                          "value type [" + hiveMapValType + "]");

                return kvHiveTypesMatch(kvMapValueDef, hiveMapValType);

            /*
             * If kvType is RECORD, then Hive type must be STRUCT, and must
             * have same element types.
             */
            case RECORD:

                if (!Category.STRUCT.equals(hiveColumnType.getCategory())) {

                    LOG.error("Field type MISMATCH: for KV RECORD field " +
                              "type, expected Hive STRUCT column type, but " +
                              "Hive column type is " +
                              hiveColumnType.getCategory());
                    return false;
                }

                /*
                 * Hive STRUCT field names and corresponding field types must
                 * match KV RECORD field names.
                 *
                 * -- NOTE --
                 *
                 * KV field names (and table and index names), as well as
                 * the names of the elements of a RECORD field, are case
                 * INSENSITIVE, but case PRESERVING. For example, if a
                 * KV table is created with a RECORD field having two elements
                 * named "MY_ELEMENT_1" and "MY_ELEMENT_2", those elements
                 * can be referenced using the Strings "my_element_1" and
                 * "my_element_2" (or "mY_eLEment_1" and "MY_element_2", etc.).
                 * But when the element names are retrieved (via
                 * RecordDef.getFields() for example), the names returned
                 * for the desired elements will always be in the case
                 * used when the RECORD was originally created; that is,
                 * "MY_ELEMENT_1" and "MY_ELEMENT_2". Compare this with how
                 * Hive handles case in its STRUCT data type.
                 *
                 * Recall that the Hive SerDeParameters.getColumnNames()
                 * method returns the names of a Hive table's columns (which
                 * correspond to a KV table's top-level fields) all in
                 * LOWER CASE. Unfortunately, Hive seems to handle case for the
                 * names of the elements of a Hive STRUCT differently. When the
                 * names of the elements of a Hive STRUCT are retrieved using
                 * the Hive StructTypeInfo.getAllStructFieldNames() method,
                 * the case of those names appears to be PRESERVED, rather
                 * than changed to all lower case; as is done for the column
                 * names. That is, if the element names of a Hive STRUCT
                 * are, for example, "mY_eLEment_1" and "MY_element_2", then
                 * StructTypeInfo.getAllStructFieldNames() will return
                 * "mY_eLEment_1" and "MY_element_2"; rather than
                 * "my_element_1" and "my_element_2" (or "MY_ELEMENT_1" and
                 * "MY_ELEMENT_2"). As a result, when validating the element
                 * names of the KV RECORD with the corresponding Hive STRUCT
                 * element names below, the retrieved element names for both
                 * the KV RECORD and the Hive STRUCT are all converted to
                 * lower case before performing the comparison.
                 */
                final List<String> hiveRecFieldNames =
                    ((StructTypeInfo) hiveColumnType).getAllStructFieldNames();

                final RecordValue kvRecValue = kvFieldDef.createRecord();
                final RecordDef kvRecDef = kvRecValue.getDefinition();
                final List<String> kvRecFieldNames = kvRecDef.getFieldNames();

                /* Validate number of RECORD elements & STRUCT elements. */
                if (hiveRecFieldNames.size() != kvRecFieldNames.size()) {

                    LOG.error("Field type MISMATCH: for KV RECORD field " +
                              "type and Hive STRUCT column type, number of " +
                              "KV RECORD elements [" +
                              kvRecFieldNames.size() + "] != number of " +
                              "Hive STRUCT elements [" +
                              hiveRecFieldNames.size() + "]. " +
                              "\nKV RECORD element names = " +
                              kvRecFieldNames +
                              "\nHive STRUCT element names = " +
                              hiveRecFieldNames);
                    return false;
                }

                /* Validate RECORD & STRUCT element NAMES (use lower case). */
                final List<String> hiveNamesLower = new ArrayList<String>();
                for (String name : hiveRecFieldNames) {
                    hiveNamesLower.add(name.toLowerCase());
                }

                for (String kvRecFieldName : kvRecFieldNames) {

                    /* Validate the current KV and Hive Record field names. */
                    final String kvFieldLower = kvRecFieldName.toLowerCase();

                    if (!hiveNamesLower.contains(kvFieldLower)) {

                        LOG.error("Field type MISMATCH: for KV RECORD field " +
                                  "type and Hive STRUCT column type, " +
                                  "KV RECORD element name [" + kvFieldLower +
                                  "] does NOT MATCH any Hive STRUCT element " +
                                  "names " + hiveNamesLower);
                        return false;
                    }

                    /*
                     * The current KV element name matches one of the element
                     * names in the Hive STRUCT. Get the corresponding KV and
                     * Hive data types and compare them.
                     *
                     * Note that the method
                     * StructTypeInfo.getStructFieldTypeInfo(<fieldname>)
                     * appears to have a bug (see below). Therefore, a local
                     * version of that method is defined in this class and
                     * used here.
                     *
                     * Note also that because FieldDef.getField(<fieldname>)
                     * is NOT case sensitive, the corresponding values of
                     * hiveRecFieldNames can be used when retrieving the
                     * element types of the KV RECORD.
                     */
                    final TypeInfo hiveRecFieldType =
                        getStructFieldTypeInfo(
                            kvRecFieldName,
                            (StructTypeInfo) hiveColumnType);

                    final FieldDef kvRecFieldDef =
                                       kvRecDef.getFieldDef(kvRecFieldName);

                    if (!kvHiveTypesMatch(kvRecFieldDef, hiveRecFieldType)) {

                        LOG.error("Field type MISMATCH: for KV RECORD field " +
                                  "type and Hive STRUCT column type, " +
                                  "KV RECORD element type [" +
                                  kvRecFieldDef.getType() + "] does " +
                                  "NOT MATCH the corresponding Hive STRUCT " +
                                  "element type [" + hiveRecFieldType + "]");
                        return false;
                    }
                }
                return true;

            default:
                LOG.error("Field type MISMATCH: UNKNOWN KV field type " +
                          "[" + kvFieldType + "]");
                return false;
        }
    }

    private static boolean isPrimitive(final FieldDef.Type kvType) {
        switch (kvType) {
            case ENUM:
                return false;
            case ARRAY:
                return false;
            case MAP:
                return false;
            case RECORD:
                return false;
            default:
                return true;
        }
    }

    private static boolean isPrimitive(final TypeInfo hiveType) {
        if (ObjectInspector.Category.PRIMITIVE.equals(
                                                   hiveType.getCategory())) {
            return true;
        }
        return false;
    }

    /**
     * Replaces the <code>getStructFieldTypeInfo</code> method from the
     * <code>org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo</code>
     * class. This is necessary because it appears that there may be a
     * bug in that method from the Hive package. In that method, the value
     * of the element name input to the method's <code>field</code> parameter
     * is always converted to lower case and the resulting lower case
     * String is used to search through the List of field names maintained
     * in the class. Unfortunately, the elements in that List of field names
     * are NOT converted to lower case. This means that if a Hive table is
     * created containing a STRUCT with element (field) names that are either
     * upper or mixed case, then a match will never be found because a lower
     * String will always be compared to Strings that are not lower case.
     *
     * This method addresses that issue by comparing both to lower case; and
     * returns the <code>StructFieldTypeInfo</code> from the set of all
     * types maintained by the class that corresponds to the given element
     * name.
     */
    private static TypeInfo getStructFieldTypeInfo(
                                final String elementName,
                                final StructTypeInfo structTypeInfo) {

        final String elementLower = elementName.toLowerCase();
        final List<String> allStructFieldNames =
                               structTypeInfo.getAllStructFieldNames();
        final List<TypeInfo> allStructFieldTypeInfos =
                                 structTypeInfo.getAllStructFieldTypeInfos();

        for (int i = 0; i < allStructFieldNames.size(); i++) {
            final String fieldLower =
                             (allStructFieldNames.get(i)).toLowerCase();
            if (fieldLower.equals(elementLower)) {
                return allStructFieldTypeInfos.get(i);
            }
        }
        throw new RuntimeException("cannot find STRUCT element [name=" +
                                   elementName + "] in " +
                                   allStructFieldNames);
    }
}

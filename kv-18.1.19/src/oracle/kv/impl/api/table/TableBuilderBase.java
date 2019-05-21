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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

import oracle.kv.table.FieldDef;
import oracle.kv.table.TimeToLive;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import static oracle.kv.impl.api.table.TableJsonUtils.DEFAULT;
import static oracle.kv.impl.api.table.TableJsonUtils.NULLABLE;

/**
 * TableBuilderBase is a base class for TableBuilder and TableEvolver that
 * has shared code to add/construct instances of FieldDef.  It has several
 * table-specific methods which are used as interfaces to allow consistent
 * return values (TableBuilderBase) on usage.
 */
public class TableBuilderBase {

    protected FieldMap fields;
    protected TimeToLive ttl;

    private boolean skipNullableDefaultValidation = false;

    /**
     * A constructor for a new empty object.
     */
    TableBuilderBase() {
        fields = new FieldMap();
    }

    TableBuilderBase(FieldMap map) {
        fields = map;
    }

    /**
     * Returns the string form of the type of this builder.  This can be
     * useful for error messages to avoid instanceof.  This defaults to
     * "Table"
     */
    public String getBuilderType() {
        return "Table";
    }

    /**
     * Returns true if this builder is a collection builder, such as
     * ArrayBuild or MapBuilder.
     */
    public boolean isCollectionBuilder() {
        return false;
    }

    public int size() {
        return fields.size();
    }

    public FieldMap getFieldMap() {
        return fields;
    }

    /**
     * This method accepts paths in dot notation to address nested fields.
     */
    public FieldDef getField(String name) {
        return TableImpl.findTableField(new TablePath(fields, name));
    }

    public FieldDef getField(TablePath tableField) {
        return TableImpl.findTableField(tableField);
    }

    /**
     * These must be overridden by TableBuilder
     */
    @SuppressWarnings("unused")
    public TableBuilderBase primaryKey(String ... key) {
        throw new IllegalArgumentException("primaryKey not supported");
    }

    public void validatePrimaryKeyFields() {
        throw new IllegalArgumentException(
            "validatePrimaryKeyFields not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase shardKey(String ... key) {
        throw new IllegalArgumentException("shardKey not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase primaryKey(List<String> key) {
        throw new IllegalArgumentException("primaryKey not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase shardKey(List<String> key) {
        throw new IllegalArgumentException("shardKey not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase primaryKeySize(String keyField, int size) {
        throw new IllegalArgumentException("primaryKeySize not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase setR2compat(boolean r2compat) {
        throw new IllegalArgumentException("setR2compat not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase setSchemaId(int id) {
        throw new IllegalArgumentException("setSchemaId not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase addSchema(String avroSchema) {
        throw new IllegalArgumentException("addSchema not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase setDescription(String description) {
        throw new IllegalArgumentException("setDescription not supported");
    }

    public TableImpl buildTable() {
        throw new IllegalArgumentException("buildTable must be overridden");
    }

    public FieldDef build() {
        throw new IllegalArgumentException("build must be overridden");
    }

    /**
     * Validate the object by building it.  This may be overridden if
     * necessary.
     */
    public TableBuilderBase validate() {
        build();
        return this;
    }

    /*
     * Integer
     */
    public TableBuilderBase addInteger(String name) {
        return addInteger(name, null, null, null);
    }

    public TableBuilderBase addInteger(
        String name,
        String description,
        Boolean nullable,
        Integer defaultValue) {

        IntegerDefImpl def = new IntegerDefImpl(description);

        if (isCollectionBuilder()) {
            checkDefaultNotAllowed(defaultValue);
            return addField(def);
        }

        IntegerValueImpl value = (defaultValue != null ?
                                  def.createInteger(defaultValue) : null);

        return addField(name, def, nullable, value);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addInteger() {
        return addInteger(null, null, null, null);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addJson() {
        return addJson(null, null);
    }

    /*
     * Long
     */
    public TableBuilderBase addLong(String name) {
        return addLong(name, null, null, null);
    }

    public TableBuilderBase addLong(String name, String description,
                                    Boolean nullable, Long defaultValue) {
        LongDefImpl def = new LongDefImpl(description);
        if (isCollectionBuilder()) {
            checkDefaultNotAllowed(defaultValue);
            return addField(def);
        }
        LongValueImpl value = (defaultValue != null ?
                               def.createLong(defaultValue) : null);
        return addField(name, def, nullable, value);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addLong() {
        return addLong(null, null, null, null);
    }

    /*
     * Double
     */
    public TableBuilderBase addDouble(String name) {
        return addDouble(name, null, null, null);
    }

    public TableBuilderBase addDouble(String name, String description,
                                      Boolean nullable,
                                      Double defaultValue) {
        DoubleDefImpl def = new DoubleDefImpl(description);
        if (isCollectionBuilder()) {
            checkDefaultNotAllowed(defaultValue);
            return addField(def);
        }
        DoubleValueImpl value = (defaultValue != null ?
                                 def.createDouble(defaultValue) : null);
        return addField(name, def, nullable, value);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addDouble() {
        return addDouble(null, null, null, null);
    }

    /*
     * Float
     */
    public TableBuilderBase addFloat(String name) {
        return addFloat(name, null, null, null);
    }

    public TableBuilderBase addFloat(String name, String description,
                                     Boolean nullable,
                                     Float defaultValue) {
        FloatDefImpl def = new FloatDefImpl(description);
        if (isCollectionBuilder()) {
            checkDefaultNotAllowed(defaultValue);
            return addField(def);
        }
        FloatValueImpl value = (defaultValue != null ?
                                def.createFloat(defaultValue) : null);
        return addField(name, def, nullable, value);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addFloat() {
        return addFloat(null, null, null, null);
    }

    /*
     * Number
     */
    public TableBuilderBase addNumber(String name) {
        return addNumber(name, null, null, null);
    }

    public TableBuilderBase addNumber(String name, String description,
                                      Boolean nullable,
                                      BigDecimal defaultValue) {
        NumberDefImpl def = new NumberDefImpl(description);
        if (isCollectionBuilder()) {
            checkDefaultNotAllowed(defaultValue);
            return addField(def);
        }
        NumberValueImpl value = (defaultValue != null ?
                                     def.createNumber(defaultValue) : null);
        return addField(name, def, nullable, value);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addNumber() {
        return addNumber(null, null, null, null);
    }

    /*
     * Boolean
     */
    public TableBuilderBase addBoolean(String name) {
        return addBoolean(name, null, null, null);
    }

    public TableBuilderBase addBoolean(String name, String description) {
        return addBoolean(name, description, null, null);
    }

    public TableBuilderBase addBoolean(String name, String description,
                                       Boolean nullable,
                                       Boolean defaultValue) {
        BooleanDefImpl def = new BooleanDefImpl(description);
        if (isCollectionBuilder()) {
            checkDefaultNotAllowed(defaultValue);
            return addField(def);
        }
        BooleanValueImpl value = (defaultValue != null ?
                                  def.createBoolean(defaultValue) : null);
        return addField(name, def, nullable, value);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addBoolean() {
        return addBoolean(null, null, null, null);
    }

    /*
     * String
     */
    public TableBuilderBase addString(String name) {
        return addString(name, null, null, null);
    }

    public TableBuilderBase addString(String name, String description,
                                      Boolean nullable,
                                      String defaultValue) {
        StringDefImpl def = new StringDefImpl(description);
        if (isCollectionBuilder()) {
            checkDefaultNotAllowed(defaultValue);
            return addField(def);
        }
        StringValueImpl value = (defaultValue != null ?
                                 def.createString(defaultValue) : null);
        return addField(name, def, nullable, value);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addString() {
        return addString(null, null, null, null);
    }

    /*
     * Enum
     */
    public TableBuilderBase addEnum(String name, String[] values,
                                    String description,
                                    Boolean nullable, String defaultValue) {
        EnumDefImpl def = new EnumDefImpl(name, values, description);
        if (isCollectionBuilder()) {
            checkDefaultNotAllowed(defaultValue);
            return addField(def);
        }
        EnumValueImpl value = (defaultValue != null ?
                               def.createEnum(defaultValue) : null);
        return addField(name, def, nullable, value);
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addEnum(String name, String[] values,
                                    String description) {
        return addEnum(name, values, description, null, null);
    }

    /*
     * Binary
     */
    public TableBuilderBase addBinary() {
        return addBinary(null);
    }

    public TableBuilderBase addBinary(String name,
                                      String description) {
        return addBinary(name, description, null, null);
    }

    public TableBuilderBase addBinary(String name) {
        return addBinary(name, null);
    }

    /*
     * This is a special case for where there may be a union and null
     * default value coming from a schema.  It should never happen
     * when creating a table from an R2 schema.  It is useful for testing.
     */
    public TableBuilderBase addBinary(String name,
                                      String description,
                                      Boolean nullable,
                                      String defaultValue,
                                      boolean base64Encoded) {
        byte[] bytes = null;
        if (defaultValue != null) {
            bytes = base64Encoded ?
                    TableJsonUtils.decodeBase64(defaultValue) :
                    defaultValue.getBytes();
        }
        return addBinary(name, description, nullable, bytes);
    }

    public TableBuilderBase addBinary(String name,
                                      String description,
                                      Boolean nullable,
                                      byte[] defaultValue) {
        BinaryDefImpl def = new BinaryDefImpl(description);
        if (isCollectionBuilder()) {
            return addField(def);
        }

        BinaryValueImpl binaryValue = null;
        if (defaultValue != null) {
            binaryValue = def.createBinary(defaultValue);
        }
        return addField(name, def, nullable, binaryValue);
    }

    /*
     * FixedBinary
     */
    public TableBuilderBase addFixedBinary(String name, int size) {
        return addFixedBinary(name, size, null);
    }

    /*
     * FixedBinary requires a name whether it's in a record or being
     * added to a collection.  When being added to a record, pass
     * true as the isRecord parameter.
     */
    public TableBuilderBase addFixedBinary(String name, int size,
                                           String description) {
        FixedBinaryDefImpl def =
            new FixedBinaryDefImpl(name, size, description);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        return addField(name, def, null, null);
    }

    public TableBuilderBase addFixedBinary(String name, int size,
                                           String description,
                                           Boolean nullable,
                                           String defaultValue,
                                           boolean base64Encoded) {
        byte[] bytes = null;
        if (defaultValue != null) {
            bytes = base64Encoded ?
                    TableJsonUtils.decodeBase64(defaultValue) :
                    defaultValue.getBytes();
        }
        return addFixedBinary(name, size, description, nullable, bytes);
    }

    /*
     * This is a special case for where there may be a union and null
     * default value coming from a schema.  It should never happen
     * when creating a table from an R2 schema.  It is useful for testing.
     */
    public TableBuilderBase addFixedBinary(String name, int size,
                                           String description,
                                           Boolean nullable,
                                           byte[] defaultValue) {
        FixedBinaryDefImpl def =
            new FixedBinaryDefImpl(name, size, description);
        if (isCollectionBuilder()) {
            return addField(def);
        }

        FixedBinaryValueImpl fixedValue = null;
        if (defaultValue != null) {
            fixedValue = def.createFixedBinary(defaultValue);
        }
        return addField(name, def, nullable, fixedValue);
    }

    /**
     * Timestamp
     */
    public TableBuilderBase addTimestamp(int precision) {

        return addTimestamp(null, precision);
    }

    public TableBuilderBase addTimestamp(String name, int precision) {

        return addTimestamp(name, precision, null, null, null);
    }

    public TableBuilderBase addTimestamp(String name,
                                         int precision,
                                         String description,
                                         Boolean nullable,
                                         Timestamp defaultValue) {

        TimestampDefImpl def = new TimestampDefImpl(precision, description);
        TimestampValueImpl defaultVal = null;
        if (defaultValue != null) {
            defaultVal = def.createTimestamp(defaultValue);
        }
        if (isCollectionBuilder()) {
            return addField(def);
        }
        return addField(name, def, nullable, defaultVal);
    }

    /**
     * Add field to map or array. These fields do not have names.
     */
    @SuppressWarnings("unused")
    public TableBuilderBase addField(FieldDef field) {

        throw new IllegalArgumentException(
            "addField(FieldDef) can only be used for maps and arrays");
    }

    /**
     * Adds a new field to the table or record being built.
     * @param tablePath a path to the field to add.
     * @param def the FieldDef for the new field
     * @param nullable true if the field is to be nullable
     * @param defaultValue the default value for the field, null if no default
     */
    public TableBuilderBase addField(
        TablePath tablePath,
        FieldDef def,
        Boolean nullable,
        FieldValueImpl defaultValue) {

        assert(!isCollectionBuilder());
        assert tablePath.getPathName() != null;

        int numSteps = tablePath.numSteps();
        assert(numSteps != 0);

        String pathName = tablePath.getPathName();
        String newFieldName = tablePath.remove(numSteps - 1);

        FieldMapEntry fme =
            new FieldMapEntry(newFieldName,
                              (FieldDefImpl)def,
                              (nullable != null ? nullable : true),
                              defaultValue);

        validateFieldAddition(newFieldName, pathName, fme);

        if (numSteps == 1) {

            /* this is a single-component, top-level field */
            if (fields.getFieldDef(newFieldName) != null) {
                throw new IllegalArgumentException(
                    "Column already exists: " + newFieldName);
            }

            fields.put(fme);
            return this;
        }

        def = TableImpl.findTableField(tablePath);

        if (def == null) {
            throw new IllegalArgumentException(
                "Can not add field " + newFieldName + " to path " +
                tablePath.getPathName() + " because that path does not exist");
        }

        if (!def.isRecord()) {
            throw new IllegalArgumentException(
                "Can not add field " + newFieldName + " to path " +
                tablePath.getPathName() +
                " because that path does not have a record type");
        }

        RecordDefImpl recDef = (RecordDefImpl)def.asRecord();
        recDef.getFieldMap().put(fme);

        return this;
    }

    /**
     * Adds a new field to the table or record being built.
     * @param name the name of the field. This may be a dot-separated path
     * into a complex type.
     * @param def the FieldDef for the new field
     * @param nullable true if the field is to be nullable
     * @param defaultValue the default value for the field, null if no default
     */
    public TableBuilderBase addField(
        String name,
        FieldDef def,
        Boolean nullable,
        FieldValueImpl defaultValue) {

        assert(!isCollectionBuilder());
        assert name != null;

        return addField(new TablePath(fields, name),
                        def, nullable, defaultValue);
    }

    /**
     * Adds to a record using default values.
     */
    public TableBuilderBase addField(String name, FieldDef def) {

        if (isCollectionBuilder()) {
            return addField(def);
        }

        return addField(name, def, null, null);
    }

    /**
     * Validate the addition of a field to the map.  At this time it ensures
     * that the field name uses allowed characters.  Sub-classes involved with
     * schema evolution will override it.
     * Add JSON
     *
     * For now, this is always nullable, so no default value.
     */
    public TableBuilderBase addJson(String name,
                                    String description) {
        return addField(name, new JsonDefImpl(description));
    }


    /**
     * Validates a single-component of a field name (the last component of a
     * path) to be sure it does not violate the rules for field names.
     *
     * This also validates that, if an attempt is made to set a complex type
     * not-nullable and/or a non-null default value, an exception is thrown.
     * Complex and binary types may not have default values. These include
     *  map, array, record, JSON, binary and fixed_binary.
     */
    @SuppressWarnings("unused")
    void validateFieldAddition(final String fieldName,
                               final String pathName,
                               final FieldMapEntry fme) {
        if (fieldName != null) {
            TableImpl.validateIdentifier(fieldName,
                                         TableImpl.MAX_NAME_LENGTH,
                                         "Field names");
        }
        /*
         * if generating schema from Avro, don't do this check because that path
         * allows default values for complex types for compatibility.
         */
        FieldDef def = fme.getFieldDef();
        if (skipNullableDefaultValidation || def.isAtomic()) {
            return;
        }
        if (!fme.isNullable() || fme.getDefaultValueInternal() != null) {
            throw new IllegalArgumentException(
                "Fields of type: " + def.getType() + " must be nullable " +
                "and may not have default values");
        }
    }

    /**
     * Removes a field.
     *
     * @param tablePath a path to the field to be removed. It may be deeply
     * nested.
     */
    public void removeField(TablePath tablePath) {

        FieldDef toBeRemoved = getField(tablePath);
        if (toBeRemoved == null) {
            throw new IllegalArgumentException
                ("Field does not exist: " + tablePath.getPathName());
        }
        validateFieldRemoval(tablePath);

        fields.removeField(tablePath);
    }

    public void removeField(String fieldName) {

        removeField(new TablePath(fields, fieldName));
    }

    /**
     * Default implementation of field removal validation.  This is
     * overridden by classes that need to perform actual validation.
     */
    @SuppressWarnings("unused")
    void validateFieldRemoval(TablePath tablePath) {
    }

    TableBuilderBase generateAvroSchemaFields(
        Schema schema,
        String name,
        JsonNode defaultValue,
        String desc) {

        skipNullableDefaultValidation = true;

        return generateAvroSchemaFields(schema, name, defaultValue,
                                        desc, false);
    }

    /*
     * NOTE: newer types have not been added here because this is a test-only
     * method.
     */
    private TableBuilderBase generateAvroSchemaFields(
        Schema schema,
        String name,
        JsonNode defaultValue,
        String desc,
        boolean isUnion) {

        Schema.Type ftype = schema.getType();

        switch (ftype) {
        case BOOLEAN:
            if (isCollectionBuilder()) {
                addBoolean();
            } else {
                addBoolean(name, desc,
                           isUnion, /* nullable */
                           (defaultValue != null && !defaultValue.isNull() ?
                            defaultValue.getBooleanValue() : null));
            }
            break;
        case BYTES:
            if (isCollectionBuilder()) {
                addBinary();
            } else {
                addBinary(name, desc, isUnion /* nullable */,
                          (defaultValue != null && !defaultValue.isNull() ?
                          defaultValue.getTextValue() : null), false);
            }
            break;
        case FIXED:
            if (isCollectionBuilder()) {
                addFixedBinary(name, schema.getFixedSize(), desc);
            } else {
                addFixedBinary(name, schema.getFixedSize(), desc,
                               isUnion, /* nullable */
                               (defaultValue != null && !defaultValue.isNull() ?
                                defaultValue.getTextValue() : null), false);
            }
            break;
        case DOUBLE:
            if (isCollectionBuilder()) {
                addDouble();
            } else {
                addDouble(name, desc,
                          isUnion, /* nullable */
                          (defaultValue != null && !defaultValue.isNull() ?
                           defaultValue.asDouble() :
                           null));
            }
            break;
        case FLOAT:
            if (isCollectionBuilder()) {
                addFloat();
            } else {
                addFloat(name, desc,
                         isUnion, /* nullable */
                         (defaultValue != null && !defaultValue.isNull() ?
                          (float) defaultValue.asDouble() :
                          null));
            }
            break;
        case ENUM:
            List<String> symbols = schema.getEnumSymbols();
            String[] enumValues = new String[symbols.size()];
            for (int i = 0; i < enumValues.length; i++) {
                enumValues[i] = symbols.get(i);
            }
            if (isCollectionBuilder()) {
                addEnum(name, enumValues, null);
            } else {
                addEnum(name, enumValues, desc,
                        isUnion, /* nullable */
                        (defaultValue != null && !defaultValue.isNull() ?
                         defaultValue.asText() : null));
            }
            break;
        case INT:
            if (isCollectionBuilder()) {
                addInteger();
            } else {
                addInteger(name, desc,
                           isUnion, /* nullable */
                           (defaultValue != null && !defaultValue.isNull() ?
                            defaultValue.getIntValue() : null));
            }
            break;
        case LONG:
            if (isCollectionBuilder()) {
                addLong();
            } else {
                addLong(name, desc,
                        isUnion, /* nullable */
                        (defaultValue != null && !defaultValue.isNull() ?
                         defaultValue.getLongValue() : null));
            }
            break;
        case ARRAY:
            ArrayDefImpl arrayDef = (ArrayDefImpl)
                TableBuilder.createArrayBuilder(desc)
                .generateAvroSchemaFields(schema,
                                          null, /* name */
                                          null, /* default */
                                          desc).build();
            if (isCollectionBuilder()) {
                addField(arrayDef);
            } else {
                FieldValueImpl defaultVal = arrayDef.createValue(defaultValue);
                addField(name, arrayDef, isUnion, defaultVal);
            }
            break;
        case MAP:
            MapDefImpl mapDef = (MapDefImpl)
                TableBuilder.createMapBuilder(desc)
                .generateAvroSchemaFields(schema,
                                          null, /* name */
                                          null, /* default */
                                          desc).build();
            if (isCollectionBuilder()) {
                addField(mapDef);
            } else {
                FieldValueImpl defaultVal = mapDef.createValue(defaultValue);
                addField(name, mapDef, isUnion, defaultVal);
            }
            break;
        case RECORD:
            RecordDefImpl recordDef = (RecordDefImpl)
                TableBuilder.createRecordBuilder(schema.getName(),
                                                 desc)
                .generateAvroSchemaFields(schema,
                                          null, /* name */
                                          null, /* default */
                                          desc).build();
            if (isCollectionBuilder()) {
                addField(recordDef);
            } else {
                FieldValueImpl defaultVal =
                    recordDef.createValue(defaultValue);
                addField(name, recordDef, isUnion, defaultVal);
            }
            break;
        case STRING:
            if (isCollectionBuilder()) {
                addString();
            } else {
                addString(name, desc,
                          isUnion, /* nullable */
                          (defaultValue != null && !defaultValue.isNull() ?
                           defaultValue.getTextValue() : null));
            }
            break;
        case UNION:
            unionNotAllowed(isUnion, Schema.Type.UNION);
            handleUnion(schema, name, defaultValue, desc);
            break;
        case NULL:
            throw new IllegalArgumentException
                ("Unsupported Avro type: " + ftype);
        default:
            throw new IllegalStateException
                ("Unknown type: " + ftype);
        }
        return this;
    }

    /*
     * Unions are handled under constrained conditions:
     * 1.  there may be only 2 schemas in the union
     * 2.  one of the schemas must be "null"
     * 3.  the non-null schema must be a simple type such as integer or string
     * Under these conditions they become a single nullable field in the table.
     *
     * Avro is a bit finicky about unions and default values.  The default value
     * must be the first member of the union (see comments in FieldDefImpl).
     */
    private void handleUnion(
        Schema schema,
        String name,
        JsonNode defaultValue,
        String desc) {

        List<Schema> unionSchemas = schema.getTypes();
        if (unionSchemas.size() != 2) {
            throw new IllegalArgumentException
                ("Avro unions must contain only 2 members");
        }
        boolean foundNull = false;
        Schema nonNullSchema = null;
        for (Schema s : unionSchemas) {
            if (s.getType() == Schema.Type.NULL) {
                foundNull = true;
            } else {
                nonNullSchema = s;
            }
        }
        if (!foundNull) {
            throw new IllegalArgumentException
                ("Avro union must include null");
        }
        generateAvroSchemaFields(nonNullSchema,
                                 name,
                                 defaultValue,
                                 desc,
                                 true);
    }

    private void unionNotAllowed(boolean isUnion, Schema.Type type) {
        if (isUnion) {
            throw new IllegalArgumentException
                ("Avro union with type is not supported: " + type);
        }
    }

    void fromJson(String fieldName, ObjectNode node) {

        /*
         * allow nullable/default complex types in this path. It means an R2
         * Avro schema has been used for a table and is round-tripping.
         */
        skipNullableDefaultValidation = true;

        JsonNode defaultNode = node.get(DEFAULT);
        Boolean nullable = TableJsonUtils.getBoolean(node, NULLABLE);

        FieldDefImpl def = TableJsonUtils.fromJson(node);

        /*
         * Default node of "null" and no default are equivalent.
         */
        FieldValueImpl value = (defaultNode == null || defaultNode.isNull()) ?
            null : def.createValue(defaultNode);
        addField(fieldName, def, nullable, value);
    }

    /**
     * When defining fields inside maps and arrays default values are not
     * allowed.
     */
    private void checkDefaultNotAllowed(Object o) {
        assert isCollectionBuilder();
        if (o !=  null) {
            throw new IllegalArgumentException
                ("Default values are not allowed for fields " +
                 "in maps and arrays");
        }
    }

    /**
     * Gets default Time-to-Live which is may apply to records without a
     * TTL specification.
     * @return can be null.
     */
    public TimeToLive getDefaultTTL() {
        return ttl;
    }

    /**
     * Sets default Time-To-Live.
     *
     * No validation is required at this time as TimeToLive cannot be constructed
     * with a unit other than hours or days.
     *
     * @param ttl can be null.
     */
    public void setDefaultTTL(TimeToLive ttl) {
        this.ttl = ttl;
    }


}

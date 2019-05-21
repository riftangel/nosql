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

import java.io.Serializable;

import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import static oracle.kv.impl.api.table.TableJsonUtils.DEFAULT;
import static oracle.kv.impl.api.table.TableJsonUtils.NULL;
import static oracle.kv.impl.api.table.TableJsonUtils.NULLABLE;
import static oracle.kv.impl.api.table.TableJsonUtils.TYPE;

/**
 * FieldMapEntry encapsulates the properties of FieldDef instances that are
 * specific to record types (TableImpl, RecordDefImpl) -- nullable and default
 * values.
 */
@Persistent(version=2)
class FieldMapEntry implements Cloneable, Serializable {

    private static final long serialVersionUID = 1L;

    private transient final String fieldName;

    private final FieldDefImpl field;

    /*
     * These are not final to allow resetting as primary keys, which are
     * implicitly not nullable, but also don't have default values.
     */
    private boolean nullable;

    private FieldValueImpl defaultValue;

    FieldMapEntry(
        String name,
        FieldDefImpl type,
        boolean nullable,
        FieldValueImpl defaultValue) {

        this.fieldName = name;
        this.field = type;
        this.nullable = nullable;
        this.defaultValue = defaultValue;

        /*
         * NOTE: this code used to do this validation, but primary key fields
         * are now forced into this state, so this invariants are enforced in
         * the table creation paths above (DDL, CLI).
        if (!nullable && defaultValue == null) {
            throw new IllegalArgumentException(
                "Not nullable field " + name + " must have a default value");
        }
        */
    }

    /**
     * Creates a default entry -- nullable, no default
     */
    FieldMapEntry(String name, FieldDefImpl type) {
        this(name, type, true, null);
    }

    private FieldMapEntry(FieldMapEntry other) {
        this.fieldName = other.fieldName;
        this.field = other.field.clone();
        this.nullable = other.nullable;
        this.defaultValue = (other.defaultValue != null ?
                             other.defaultValue.clone() : null);
    }

    @Override
    public FieldMapEntry clone() {
        return new FieldMapEntry(this);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private FieldMapEntry() {
        fieldName = null;
        field = null;
        defaultValue = null;
        nullable = false;
    }

    String getFieldName() {
        return fieldName;
    }

    FieldDefImpl getFieldDef() {
        return field;
    }

    boolean isNullable() {
        return nullable;
    }

    boolean isPrecise() {
        return field.isPrecise();
    }

    boolean isSubtype(FieldMapEntry sup) {

        if (!fieldName.equals(sup.fieldName)) {
            return false;
        }

        if (nullable != sup.nullable) {
            if (nullable) {
                return false;
            }
        }

        return field.isSubtype(sup.field);
    }

    FieldValueImpl getDefaultValueInternal() {
        return defaultValue;
    }

    FieldValueImpl getDefaultValue() {
        return (defaultValue != null ? defaultValue :
                NullValueImpl.getInstance());
    }

    boolean hasDefaultValue() {
        return defaultValue != null && !defaultValue.isNull();
    }

    /*
     * Primary keys are not nullable, but also do not have default values.
     */
    void setAsPrimaryKey() {
        nullable = false;
        defaultValue = null;
    }

    /*
     * Set the entry as nullable without a default
     */
    void setNullable() {
        nullable = true;
        defaultValue = null;
    }

    /**
     * Compare equality.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FieldMapEntry) {
            FieldMapEntry other = (FieldMapEntry) obj;
            return (fieldName.equalsIgnoreCase(other.fieldName) &&
                    field.equals(other.field) &&
                    nullable == other.nullable &&
                    getDefaultValue().equals(other.getDefaultValue()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (fieldName.hashCode() +
                field.hashCode() +
                ((Boolean) nullable).hashCode() +
                getDefaultValue().hashCode());
    }

    @Override
    public String toString() {
        return "FieldMapEntry[" + fieldName + ", " + field + ", " +
               nullable + "]";
    }

    /**
     * This function creates the "type" definition required by Avro for the
     * instance.  It is called from TableImpl and RecordDefImpl when generating
     * Avro schemas for named field types.  It abstracts out handling of
     * nullable fields and fields with default values.
     *
     * Fields that are nullable are implemented as an Avro union with the
     * "null" Avro value.  The problem is that default values for unions
     * require the type of the defaulted field to come first in the union
     * declaration. This means that the order of the fields in the union depend
     * on the presence and type of the default.
     *
     * All record fields in the Avro schema get default values for schema
     * evolution.  This is either user-defined or null.
     */
    final JsonNode createAvroTypeAndDefault(ObjectNode node) {
        if (isNullable()) {
            ArrayNode arrayNode = node.putArray(TYPE);
            if (getDefaultValue().isNull()) {
                arrayNode.add(NULL);
                arrayNode.add(field.mapTypeToAvroJsonNode());
            } else {
                arrayNode.add(field.mapTypeToAvroJsonNode());
                arrayNode.add(NULL);
            }
        } else {
            node.put(TYPE, field.mapTypeToAvroJsonNode());
        }
        node.put(DEFAULT, getDefaultValue().toJsonNode());
        return node;
    }

    /**
     * Outputs the state of the entry to the ObjectNode for display
     * as JSON.  This is called indirectly by toJsonString() methods.
     * First, output the information from the FieldDefImpl, then output
     * default and nullable state.
     */
    void toJson(ObjectNode node) {
        field.toJson(node);
        node.put(NULLABLE, nullable);
        node.put(DEFAULT, getDefaultValue().toJsonNode());
    }
}

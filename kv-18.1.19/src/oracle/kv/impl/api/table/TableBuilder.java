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
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;

import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Table;

/**
 * TableBuilder is a class used to construct Tables and complex data type
 * instances.  The instances themselves are immutable.  The pattern used
 * is
 * 1.  create TableBuilder
 * 2.  add state in terms of data types
 * 3.  build the desired object
 *
 * When constructing a child table the parent table's Table object is required.
 * This requirement makes it easy to add the necessary parent information to
 * the child table.  It is also less error prone than requiring the caller to
 * add the parent's key fields by hand.
 *
 * There is a special case is where the builder is created from a JSON string
 * which may already have the parent information encoded.  That area is a
 * TODO until a final decision on what JSON string format(s) are allowed for
 * this if it is even publicly supported.
 */
public class TableBuilder extends TableBuilderBase {

    private final String name;

    private String description;

    /* These apply only to tables */

    private final TableImpl parent;

    private final String namespace;

    private List<String> primaryKey;

    private List<String> shardKey;

    private HashMap<String,Integer> primaryKeySizes;

    private boolean r2compat;

    private boolean schemaAdded;

    private int schemaId;

    private ResourceOwner owner;

    private boolean sysTable;

    private TableBuilder(
        String namespace,
        String name,
        String description,
        Table parent,
        boolean copyParentInfo,
        boolean isSysTable) {

        this.name = name;
        this.namespace = namespace;
        this.description = description;
        schemaAdded = false;
        r2compat = false;
        this.parent = (parent != null ? (TableImpl) parent : null);
        this.sysTable = isSysTable;
        primaryKey = new ArrayList<String>();
        shardKey = new ArrayList<String>();

        /* Add the key columns of the parent table to this TableBuilder. */
        if (parent != null && copyParentInfo) {
            addParentInfo();
        }

        TableImpl.validateTableName(name, sysTable);
        if (namespace != null) {
            TableImpl.validateNamespace(namespace);
        }
    }

    /**
     * There is no need to go more than one level because the primary key of
     * each table includes the fields of its ancestors.
     */
    private void addParentInfo() {
        for (String fieldName : parent.getPrimaryKey()) {
            fields.put(parent.getFieldMapEntry(fieldName, true));
            primaryKey.add(fieldName);
        }

        /*
         * Copy the primary key sizes of the parent to this object's map
         * of key sizes. There's a translation from array of int to the
         * map.
         */
        if (parent.getPrimaryKeySizes() != null) {
            primaryKeySizes =
                new HashMap<String, Integer>();
            final List<String> parentKeys = parent.getPrimaryKey();
            for (int i = 0; i < parent.getPrimaryKeySizes().size(); i++) {
                int index = parent.getPrimaryKeySizes().get(i);
                if (index != 0) {
                    primaryKeySizes.put(parentKeys.get(i), index);
                }
            }
        }
    }

    /**
     * Getters/Setters
     */
    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public TableBuilderBase setDescription(String description) {
        this.description = description;
        return this;
    }

    public TableImpl getParent() {
        return parent;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getShardKey() {
        return shardKey;
    }

    public boolean isR2compatible() {
        return r2compat;
    }

    @Override
    public TableBuilderBase setR2compat(boolean value) {
        this.r2compat = value;
        return this;
    }

    public int getSchemaId() {
        return schemaId;
    }

    /**
     * This implicitly sets r2compat to true as well.
     */
    @Override
    public TableBuilderBase setSchemaId(int id) {
        schemaId = id;
        r2compat = true;
        return this;
    }

    public ResourceOwner getOwner() {
        return owner;
    }

    public TableBuilderBase setOwner(ResourceOwner newOwner) {
        this.owner = newOwner;
        return this;
    }

    public boolean isSysTable() {
        return sysTable;
    }

    /**
     * Table-only methods
     */
    @Override
    public TableBuilderBase primaryKey(String ... key) {
        for (String field : key) {
            if (primaryKey.contains(field)) {
                throw new IllegalArgumentException
                    ("The primary key field already exists: " + field);
            }
            primaryKey.add(field);
        }
        return this;
    }

    @Override
    public void validatePrimaryKeyFields() {
        if (primaryKey == null) {
            return;
        }
        for (String key : primaryKey) {
            FieldMapEntry fme = fields.getFieldMapEntry(key);
            if (fme == null) {
                throw new IllegalArgumentException
                    ("Field does not exist: " + key);
            }
            if (fme.getDefaultValueInternal() != null) {
                throw new IllegalArgumentException
                    ("Primary key fields can not have default values");
            }
        }

        /*
         * Validate regular fields as well. This used to be done directly
         * in FieldMapEntry
         */
        for (FieldMapEntry fme : fields.getFieldProperties()) {
            if (!primaryKey.contains(fme.getFieldName())) {
                if (!fme.isNullable() &&
                    (fme.getDefaultValueInternal() == null)) {
                    throw new IllegalArgumentException(
                        "Not nullable field " + fme.getFieldName() +
                        " must have a default value");
                }
            }
        }
    }

    @Override
    public TableBuilderBase shardKey(String ... key) {
        if (parent != null) {
            throw new IllegalArgumentException
                ("Child tables cannot have a shard key.");
        }
        for (String field : key) {
            if (shardKey.contains(field)) {
                throw new IllegalArgumentException
                    ("The shard key field already exists: " + field);
            }
            shardKey.add(field);
        }
        return this;
    }

    @Override
    public TableBuilderBase primaryKey(final List<String> pKey) {
        this.primaryKey = pKey;
        return this;
    }

    @Override
    public TableBuilderBase shardKey(final List<String> mKey) {
        if (parent != null) {
            throw new IllegalArgumentException
                ("Child tables cannot have a shard key.");
        }
        this.shardKey = mKey;
        return this;
    }

    @Override
    public TableBuilderBase primaryKeySize(String keyField, int size) {
        if (primaryKey == null) {
            throw new IllegalArgumentException
                ("primaryKeySize() cannot be called before primaryKey()");
        }

        int index = primaryKey.indexOf(keyField);
        if (index < 0) {
            throw new IllegalArgumentException
                ("Field is not part of primary key: " + keyField);
        }

        /*
         * If the field is present (it may not be), make sure it's an integer.
         * If not present, the type validation will happen on table creation.
         */
        FieldDef field = getField(keyField);
        if (field != null && !field.isInteger()) {
            throw new IllegalArgumentException
                ("primaryKeySize() requires an INTEGER field type: " +
                 keyField);
        }

        if (size <= 0 || size > 5) {
            throw new IllegalArgumentException("Invalid primary key size: " +
                                               size + ". Size must be 1-5.");
        }
        /*
         * Ignore a size of 5, which is the largest size possible
         * for an integer of any value.
         */
        if (size < 5) {
            if (primaryKeySizes == null) {
                primaryKeySizes = new HashMap<String, Integer>();
            }
            primaryKeySizes.put(keyField, size);
        }
        return this;
    }

    /**
     * Build the actual TableImpl
     */
    @Override
    public TableImpl buildTable() {
        /*
         * If the shard key is not provided it defaults to:
         * o the primary key if this is a top-level table
         * o the parent's shard key if this is a child table.
         */
        if (shardKey == null || shardKey.isEmpty()) {
            if (parent != null) {
                shardKey = new ArrayList<String>(parent.getShardKey());
            } else {
                shardKey = primaryKey;
            }
        }

        return TableImpl.createTable(namespace,
                                     getName(),
                                     parent,
                                     getPrimaryKey(),
                                     getPrimaryKeySizes(),
                                     getShardKey(),
                                     fields,
                                     r2compat,
                                     schemaId,
                                     getDescription(),
                                     true,
                                     owner,
                                     ttl,
                                     null, // limits
                                     sysTable);
    }

    @Override
    void validateFieldAddition(final String fieldName,
                               final String pathName,
                               final FieldMapEntry fme) {

        super.validateFieldAddition(fieldName, pathName, fme);

        /*
         * Cannot add a field that has the same name as a primary key field.
         */
        if (parent != null) {
            if (parent.isKeyComponent(fieldName)) {
                throw new IllegalArgumentException
                    ("Cannot add field, it already exists in primary key " +
                        "fields of its parent table: " + fieldName);
            }
        }
    }

    /*
     * Used to validate the state of the builder to ensure that it can be used
     * to build a table.  The simplest way to do this is to actually build one
     * and let TableImpl do the validation.  This method is used by tests and
     * the CLI.
     */
    @Override
    public TableBuilderBase validate() {
        buildTable();
        return this;
    }

    /*
     * Create a table schema out of the given AVRO schema.
     */
    @Override
    public TableBuilderBase addSchema(String avroSchema) {

        if (schemaAdded) {
            throw new IllegalArgumentException
                ("Only one schema may be added to a table");
        }

        Schema schema = new Schema.Parser().parse(avroSchema);

        List<Schema.Field> schemaFields = schema.getFields();

        for (Schema.Field field : schemaFields) {
            generateAvroSchemaFields(field.schema(),
                                     field.name(),
                                     field.defaultValue(),
                                     field.doc());
        }
        validatePrimaryKeyFields();
        schemaAdded = true;
        return this;
    }

    /*
     * Show the current state of the table.  The simplest way is to create a
     * not-validated table and display it.
     */
    public String toJsonString(boolean pretty) {
        TableImpl t = TableImpl.createTable(namespace,
                                            getName(),
                                            parent,
                                            getPrimaryKey(),
                                            getPrimaryKeySizes(),
                                            getShardKey(),
                                            fields,
                                            r2compat,
                                            schemaId,
                                            getDescription(),
                                            false,
                                            owner,
                                            ttl,
                                            null, // limits
                                            sysTable);
        return t.toJsonString(pretty);
    }

    public List<Integer> getPrimaryKeySizes() {
        if (primaryKeySizes == null) {
            return null;
        }
        ArrayList<Integer> list = new ArrayList<Integer>(primaryKey.size());
        for (String key : primaryKey) {
            Integer size = primaryKeySizes.get(key);
            if (size == null) {
                size = 0;
            }
            list.add(size);
        }
        return list;
    }

    /**
     * Build a Table from its JSON format.
     */
    public static TableImpl fromJsonString(
        String jsonString,
        Table parent) {
        return TableJsonUtils.fromJsonString(jsonString, (TableImpl) parent);
    }

    /**
     * Create a table builder
     */
    public static TableBuilder createTableBuilder(
        String namespace,
        String name,
        String description,
        Table parent,
        boolean copyParentInfo) {
        return new TableBuilder(namespace, name, description,
                                parent, copyParentInfo, false);
    }

    public static TableBuilder createTableBuilder(
        String namespace,
        String name,
        String description,
        Table parent) {
        return createTableBuilder(namespace, name, description, parent, true);
    }

    public static TableBuilder createTableBuilder(String namespace,
                                                  String name) {
        return createTableBuilder(namespace, name, null, null, false);
    }

    /*
     * Compatibility for lots of test code
     */
    public static TableBuilder createTableBuilder(
        String name,
        String description,
        Table parent) {
        return createTableBuilder(null, name, description, parent, true);
    }

    /*
     * Compatibility for test code
     */
    public static TableBuilder createTableBuilder(String name) {
        return createTableBuilder(null, name, null, null, false);
    }

    public static TableBuilder createSystemTableBuilder(String name) {
        return new TableBuilder(null, name, null, null, false, true);
    }

    /**
     * Creates an ArrayBuilder.
     */
    public static ArrayBuilder createArrayBuilder(String description) {
        return new ArrayBuilder(description);
    }

    public static ArrayBuilder createArrayBuilder() {
        return new ArrayBuilder();
    }

    /**
     * Creates an MapBuilder.
     */
    public static MapBuilder createMapBuilder(String description) {
        return new MapBuilder(description);
    }

    public static MapBuilder createMapBuilder() {
        return new MapBuilder();
    }

    /**
     * Creates a RecordBuilder.
     */
    public static RecordBuilder createRecordBuilder(
        String name,
        String description) {
        return new RecordBuilder(name, description);
    }

    public static RecordBuilder createRecordBuilder(String name) {
        return new RecordBuilder(name);
    }
}

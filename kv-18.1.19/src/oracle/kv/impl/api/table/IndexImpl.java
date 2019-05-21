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

import static oracle.kv.impl.api.table.TableJsonUtils.DESC;
import static oracle.kv.impl.api.table.TableJsonUtils.FIELDS;
import static oracle.kv.impl.api.table.TableJsonUtils.NAME;
import static oracle.kv.impl.api.table.TableJsonUtils.TABLE;
import static oracle.kv.impl.api.table.TableJsonUtils.TYPE;
import static oracle.kv.impl.api.table.TableJsonUtils.TYPES;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.TablePath.StepInfo;
import oracle.kv.impl.api.table.TablePath.StepKind;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Table;

import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Implementation of the Index interface.  Instances of this class are created
 * and associated with a table when an index is defined.  It contains the index
 * metdata as well as many utility functions used in serializing and
 * deserializing index keys.
 *
 * An index can be viewed as a sorted table of N + 1 columns. Each of the first
 * N columns has an indexable atomic type (one of the numeric types or string
 * or enum). The last column stores serialized primary keys "pointing" to rows
 * in the undelying base table.
 *
 * The rows of the index are computed as follows:
 *
 * - Each index column C (other than the last one) is associated with a path
 *   expr Pc, which when evaluated on a base-table row R produces one or more
 *   indexable atomic values. Let Pc(R) be the *set* of values produced by Pc
 *   on a row R (Pc(R) may produce duplicate values, but the duplicates do not
 *   participate in index creation). If Pc is a path expr that may produce
 *   multiple values from a row, we say that C is a "multi-key" column, and
 *   the whole index is a "multi-key" index.
 *
 * - Each Pc may have at most one step, call it MK, that may produce multiple
 *   values. MK is a [] or _key step whose input is an array or map value from
 *   a row. We denote with MK-Pc the path expr that contains the steps from the
 *   start of Pc up to (and including) MK, and with R-Pc the remaining steps in
 *   Pc.
 *
 * - An index may contain more than one multi-key column, but the path exprs
 *   for all of these columns must all have the same MK-Pc.
 *
 * - Then, conceptually, the index rows are computed by a query like this:
 *
 *   select a.Pc1 as C1, c.R-Pc2 as C2, c.R-Pc3 as C3, primary_key(a) as PK
 *   from A as a, a.MK-Pc as c
 *   order by a.Pc1, c.R-Pc2, c.R-Pc3
 *
 *   In the above query, we assumed the index has 4 columns (N = 3), two of
 *   which (C2 and C3) are multi-key columns sharing the MK-Pc path. If there
 *   are no multi-key columns, the query is simpler:
 *
 *   select a.Pc1 as C1, a.Pc2 as C2, a.Pc3 as C3, primary_key(a) as PK
 *   from A as a,
 *   order by a.Pc1, a.Pc2, a.Pc3
 */
public class IndexImpl implements Index, Serializable {

    private static final String KEY_TAG = "_key";

    private static final String DOT_BRACKETS = ".[]";

    private static final long serialVersionUID = 1L;

    /* The serial version for v4.3 and before */
    static final int INDEX_VERSION_V0 = 0;

    /*
     * Index serial v1:
     * - Add EMTPY_INDICATOR
     * - NULL_INDICATOR is changed to 0x7f
     */
    static final int INDEX_VERSION_V1 = 1;

    /* The current serial version used in the index */
    static final int INDEX_VERSION_CURRENT = INDEX_VERSION_V1;

    /*
     * The indicator used in key serialization for values that are not
     * one of the special values (SQL NULL, json null, or EMPTY)
     */
    static final byte NORMAL_VALUE_INDICATOR = 0x00;

    /*
     * The indicator representing the EMPTY value (added since v1):
     * The EMPTY value is used if an index path, when evaluated on a
     * table as a query path expr, returns an empty result. For example:
     *  - The index field is an empty array or map.
     *  - The index field is map.<specific-key>, the specific-key is missing
     *    from the map value.
     *  - If the index field is a sub field of json field, the sub field is
     *    missing from the json value and the json value is not sql null or
     *    json null.
     */
    static final byte EMPTY_INDICATOR = 0x7D;

    /* The indicator for JSON null used in key serialization (added since v1) */
    static final byte JSON_NULL_INDICATOR = 0x7E;

    /* The indicator for null value used in key serialization (v1) */
    static final byte NULL_INDICATOR_V1 = 0x7F;

    /* The indicator for null value used in key serialization */
    static final byte NULL_INDICATOR_V0 = 0x01;

    /* the index name */
    private final String name;

    /* the (optional) index description, user-provided */
    private final String description;

    /* the associated table */
    private final TableImpl table;

    /*
     * The stringified path exprs that define the index columns. In the case of
     * map indexes a path expr may contain the special steps "._key" and ".[]"
     * to distinguish between the 3 possible ways of indexing a map: (a) all
     * the keys (using a _key step), (b) all the values (using a [] step), or
     * (c) the value of a specific map entry (using the specific key of the
     * entry we want indexed). In case of array indexes, the ".[]" is used.
     *
     * The string format described above is the "old" format.  The newFields
     * list below stores the same strings in the "new" foramt, which is the
     * one that is used in the DDL systax. Specifically, for maps the new format
     * uses ".keys()" and ".values()" instead of "._key", ".[]", and for arrays
     * it uses "[]" instead of ".[]".
     *
     * The new format was introduced in 4.3 and all the data model code was
     * modified to use this format. However, the old format is still here
     * to satisfy the backwards compatimity requirement. The difficult case
     * is when a new server must serialize (via java) and send an IndexImpl
     * to an old client, which of course, expects and undestands the old
     * format only. For this reason, during (de)serialization the old format
     * is used always, and newFields is declared transient.
     *
     * In 4.4, newFields (and newAnnotations) were made persistent. this.fields
     * and this.annotations remained peristent in order to handle the old-client
     * problem mentioned above, as well as existing indexes created by older
     * server versions. newFields was made persistent because in 4.4 steps may
     * be quoted, and the quotes must be preserved when an IndexImpl is
     * serialized to disk (putting the quotes into this.fields would not work
     * because the index would then not work at an old client, since the old
     * client would not be removing the quotes). Notice also that with the
     * introduction of json indexes in 4.4, this.fields alone is not enough
     * to correctly initialize the transient state of IndexImpl, because in
     * the case of json index paths there is no schema to determine whether
     * a .[] step appearing in this.fields should be converted to .values()
     * or to [].
     */
    private final List<String> fields;


    /* status is used when an index is being populated to indicate readiness */
    private IndexStatus status;

    private final Map<String, String> annotations;

    /*
     * properties of the index; used by text indexes only; can be null.
     */
    private final Map<String, String> properties;

    /*
     * Indicates whether this index includes an indicator byte in each field
     * of its binary keys. Indicator bytes were added in 4.2. For older,
     * existing indexes this will be false. For new indexes it is true, unless
     * test-specific state is set to turn this feature off.
     */
    private final boolean isNullSupported;

    /*
     * The version of the index. This data member was introduced in 4.4.
     */
    private final int indexVersion;

    /*
     * This data member was made persistent in 4.4.
     */
    private List<String> newFields;

    /*
     * The declared types for the json index paths (if an index path is not
     * json, its corresponding entry in the list will be null).
     * This data member was introduced in 4.4.
     */
    private final List<FieldDef.Type> types;

    /*
     * This data member was made persistent in 4.4
     */
    private Map<String, String> newAnnotations;

    /*
     * transient version of the index column definitions, materialized as
     * IndexField for efficiency. It is technically final but is not because it
     * needs to be initialized in readObject after deserialization.
     */
    private transient List<IndexField> indexFields;

    /*
     * transient indication of whether this is a multiKeyMapIndex.  This is
     * used for serialization/deserialization of map indexes.  It is
     * technically final but is not because it needs to be initialized in
     * readObject after deserialization.
     */
    private transient boolean isMultiKeyMapIndex;

    /*
     * transient RecordDefImpl representing the definition of IndexKeyImpl
     * instances for this index.
     */
    private transient RecordDefImpl indexKeyDef;

    /*
     * transient RecordDefImpl representing a full index entry, including the
     * primary key fields
     */
    private transient RecordDefImpl indexEntryDef;

    public enum IndexStatus {
        /** Index is transient */
        TRANSIENT() {
            @Override
            public boolean isTransient() {
                return true;
            }
        },

        /** Index is being populated */
        POPULATING() {
            @Override
            public boolean isPopulating() {
                return true;
            }
        },

        /** Index is populated and ready for use */
        READY() {
            @Override
            public boolean isReady() {
                return true;
            }
        };

        /**
         * Returns true if this is the {@link #TRANSIENT} type.
         * @return true if this is the {@link #TRANSIENT} type
         */
        public boolean isTransient() {
            return false;
        }

        /**
         * Returns true if this is the {@link #POPULATING} type.
         * @return true if this is the {@link #POPULATING} type
         */
        public boolean isPopulating() {
            return false;
        }

        /**
         * Returns true if this is the {@link #READY} type.
         * @return true if this is the {@link #READY} type
         */
        public boolean isReady() {
            return false;
        }
    }

    public IndexImpl(String name,
                     TableImpl table,
                     List<String> fields,
                     List<FieldDef.Type> types,
                     String description) {
    	this(name, table, fields, types, null, null, description);
    }

    public IndexImpl(String name, TableImpl table, List<String> fields,
                     String description) {
    	this(name, table, fields, null, null, null, description);
    }

    /* Constructor for Full Text Indexes. */
    public IndexImpl(String name,
                     TableImpl table,
                     List<String> fields,
                     List<FieldDef.Type> types,
                     Map<String, String> annotations,
                     Map<String, String> properties,
                     String description) {
    	this.name = name;
    	this.table = table;
    	this.newFields = fields;
    	this.types = types;
    	this.newAnnotations = annotations;
    	this.properties = properties;
    	this.description = description;
    	status = IndexStatus.TRANSIENT;
    	isNullSupported = areIndicatorBytesEnabled();
    	indexVersion = getIndexVersion();

    	/* validate initializes indexFields and the other transient state */
    	validate();

    	this.fields = new ArrayList<String>(newFields.size());

    	if (newAnnotations != null) {
    	    this.annotations = new HashMap<String, String>();
    	} else {
    	    this.annotations = null;
    	}

    	translateToOldFields();
    }

    private void translateToOldFields() {

        StringBuilder sb = new StringBuilder();

        for (IndexField field : indexFields) {

            for (int s = 0; s < field.numSteps(); ++s) {

                StepInfo si = field.getStepInfo(s);
                String step = si.step;

                if (si.kind == StepKind.BRACKETS) {
                    sb.append(TableImpl.BRACKETS);

                } else if (si.kind == StepKind.VALUES) {
                    sb.append(TableImpl.BRACKETS);

                } else if (si.kind == StepKind.KEYS) {
                    sb.append(KEY_TAG);

                } else {
                    /*
                     * If the step was a quoted one, the quotes are removed
                     * in the old path format. Leaving the quotes there would
                     * definitely make the index not work at an old client.
                     * Without the quotes, the index may or may not work at
                     * an old client, depending on what is inside the quotes.
                     */
                    sb.append(step);
                }

                if (s < field.numSteps() - 1) {
                    sb.append(TableImpl.SEPARATOR);
                }
            }

            String oldField = sb.toString();
            sb.delete(0, sb.length());

            fields.add(oldField);

            if (newAnnotations != null) {
                String annotation = newAnnotations.get(field);
                if (annotation != null) {
                    annotations.put(oldField, annotation);
                }
            }
        }
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public String getName()  {
        return name;
    }

    /**
     * Returns true if this index indexes all the entries (both keys and
     * associated data) of a map, with the key field appearing before any
     * of the data fields. This is info needed by the query optimizer.
     */
    public boolean isMapBothIndex() {

        List<IndexField> ipaths = getIndexFields();
        boolean haveMapKey = false;
        boolean haveMapValue = false;

        if (!isMultiKeyMapIndex) {
            return false;
        }

        for (IndexField ipath : ipaths) {

            if (ipath.isMapKeys()) {
                haveMapKey = true;
                if (haveMapValue) {
                    return false;
                }
            } else if (ipath.isMapValues()) {
                haveMapValue = true;
                if (haveMapKey) {
                    break;
                }
            }
        }

        return (haveMapKey && haveMapValue);
    }

    /*
     * Returns a potentially modified list of the string paths for the
     * indexed fields.
     */
    @Override
    public List<String> getFields() {
        if (newFields == null) {
            initTransientState();
        }
        return newFields;
    }

    public IndexField getIndexPath(int pos) {
        return getIndexFields().get(pos);
    }

    public IndexField getIndexPath(String fieldName) {
        return getIndexFields().get(getIndexKeyDef().getFieldPos(fieldName));
    }

    boolean fieldMayHaveSpecialValue(int pos) {
        return getIndexPath(pos).mayHaveSpecialValue();
    }

    /**
     * Returns an list of the fields that define a text index.
     * These are in order of declaration which is significant.
     *
     * @return the field names
     */
    public List<AnnotatedField> getFieldsWithAnnotations() {

        if (! isTextIndex()) {
            throw new IllegalStateException
                ("getFieldsWithAnnotations called on non-text index");
        }

        final List<AnnotatedField> fieldsWithAnnotations =
            new ArrayList<AnnotatedField>(getFields().size());

        Map<String, String> anns = getAnnotationsInternal();

        for (String field : getFields()) {
            fieldsWithAnnotations.add(
               new AnnotatedField(field, anns.get(field)));
        }
        return fieldsWithAnnotations;
    }

    Map<String, String> getAnnotations() {
        if (isTextIndex()) {
            if (annotations != null && newAnnotations == null) {
                initTransientState();
            }
            return Collections.unmodifiableMap(newAnnotations);
        }
        return Collections.emptyMap();
    }

    Map<String, String> getAnnotationsInternal() {
        if (annotations != null && newAnnotations == null) {
            initTransientState();
        }
        return newAnnotations;
    }

    public Map<String, String> getProperties() {
        if (properties != null) {
            return properties;
        }
        return Collections.emptyMap();
    }

    @Override
    public String getDescription()  {
        return description;
    }

    @Override
    public IndexKeyImpl createIndexKey() {
        return new IndexKeyImpl(this, getIndexKeyDef());
    }

    /*
     * Creates an IndexKeyImpl from a RecordValue that is known to be
     * a flattened IndexKey. This is used by the query engine where IndexKeys
     * are serialized and deserialized as plain RecordValue instances.
     *
     * Unlike the method below, this assumes a flattened structure in value
     * ("value" must have the same type def as the IndexKey).
     */
    public IndexKeyImpl createIndexKeyFromFlattenedRecord(RecordValue value) {
        IndexKeyImpl ikey = createIndexKey();
        ikey.copyFrom(value);
        return ikey;
    }

    @Override
    public IndexKeyImpl createIndexKey(RecordValue value) {
        if (value instanceof IndexKey) {
            throw new IllegalArgumentException(
                "Cannot call createIndexKey with IndexKey argument");
        }
        IndexKeyImpl ikey = createIndexKey();
        populateIndexRecord(ikey, (RecordValueImpl) value);
        return ikey;
    }

    @Override
    public IndexKey createIndexKeyFromJson(String jsonInput, boolean exact) {
        return createIndexKeyFromJson
            (new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public IndexKey createIndexKeyFromJson(InputStream jsonInput,
                                           boolean exact) {
        IndexKeyImpl key = createIndexKey();

        /*
         * Using addMissingFields false to not add missing fields, if Json
         * contains a subset of index fields, then build partial index key.
         */
        ComplexValueImpl.createFromJson(key, jsonInput, exact,
                                        false /*addMissingFields*/);
        return key;
    }

    @Override
    public FieldRange createFieldRange(String path) {

        FieldDef ifieldDef = getIndexKeyDef().getField(path);

        if (ifieldDef == null) {
            throw new IllegalArgumentException(
                "Field does not exist in index: " + path);
        }
        return new FieldRange(path, ifieldDef, 0);
    }

    /**
     * Populates the IndexKey from the record, handling complex values.
     */
    private void populateIndexRecord(IndexKeyImpl indexKey,
                                     RecordValueImpl value) {
        assert !(value instanceof IndexKey);
        int i = 0;
        for (IndexField field : getIndexFields()) {
            FieldValueImpl v = value.findFieldValue(field, -1, null);
            if (v != null) {
                indexKey.put(i, v);
            }
            i++;
        }
        indexKey.validate();
    }

    public int numFields() {
        return getFields().size();
    }

    /**
     * Returns true if the index comprises only fields from the table's primary
     * key.  Nested types can't be key components so there is no need to handle
     * a complex path.
     */
    public boolean isKeyOnly() {
        for (IndexField ifield : getIndexFields()) {
            if (ifield.isComplex()) {
                return false;
            }
            if (!table.isKeyComponent(ifield.getStep(0))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return true if this index has multiple keys per record.  This can happen
     * if there is an array or map in the index.  An index can only contain one
     * array or map.
     */
    public boolean isMultiKey() {
    	if (! isTextIndex()) {
            for (IndexField field : getIndexFields()) {
                if (field.isMultiKey()) {
                    return true;
                }
            }
    	}
      return false;
    }

    public IndexStatus getStatus() {
        return status;
    }

    public void setStatus(IndexStatus status) {
        this.status = status;
    }

    public TableImpl getTableImpl() {
        return table;
    }

    public List<FieldDef.Type> getTypes() {
        return types;
    }

    /**
     * Returns the list of IndexField objects defining the index.  It is
     * transient, and if not yet initialized, initialize it.
     */
    public List<IndexField> getIndexFields() {
        if (indexFields == null) {
            initTransientState();
        }
        return indexFields;
    }

    public RecordDefImpl getIndexKeyDef() {
        if (indexKeyDef == null) {
            initTransientState();
        }
        return indexKeyDef;
    }

    public RecordDefImpl getIndexEntryDef() {
        if (indexEntryDef == null) {
            initTransientState();
        }
        return indexEntryDef;
    }

    public String getFieldName(int i) {
        return getIndexKeyDef().getFieldName(i);
    }

    public FieldDefImpl getFieldDef(int i) {
        return getIndexKeyDef().getFieldDef(i);
    }

    /**
     * Compares if the specified index fields are equals to that of this index,
     * return true if they are equal, otherwise return false.
     */
    public boolean compareIndexFields(List<String> fieldNames) {

        if (fieldNames == null || fieldNames.size() != getFields().size()) {
            return false;
        }

        for (int i = 0; i < fieldNames.size(); i++) {
            String field = fieldNames.get(i);
            IndexField ifield = new IndexField(table, field,
                                               getFieldType(i), i);
            validateIndexField(ifield, true);
            if (!ifield.equals(getIndexFields().get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Initializes the transient list of index fields.  This is used when
     * the IndexImpl was constructed via deserialization and the constructor
     * and validate() were not called.
     *
     * TODO: figure out how to do transient initialization in the
     * deserialization case.  It is not as simple as implementing readObject()
     * because an intact Table is required.  Calling validate() from TableImpl's
     * readObject() does not work either (generates an NPE).
     */
    private synchronized void initTransientState() {

        /*
         * Since the indexKeyDef is the last thing to initialize in this
         * method, so check this depends on the setting of indexKeyDef.
         */
        if (indexKeyDef != null) {
            return;
        }

        int numFields;
        boolean convertOldPathFormat = false;

        if (newFields == null) {
            convertOldPathFormat = true;
            numFields = fields.size();
            newFields = new ArrayList<String>(numFields);

            if (annotations != null) {
                newAnnotations = new HashMap<String, String>();
            }
        } else {
            numFields = newFields.size();
        }

        indexFields = new ArrayList<IndexField>(numFields);

        for (int i = 0; i < numFields; ++i) {

            String newField;

            if (convertOldPathFormat) {
                String field = fields.get(i);
                newField = convertOldIndexPath(field);
            } else {
                newField = newFields.get(i);
            }

            IndexField indexField = new IndexField(table, newField,
                                                   getFieldType(i), i);

            try {
                validateIndexField(indexField, false);
            } catch (RuntimeException ex) {
                if (convertOldPathFormat) {
                    newFields = null;
                    if (newAnnotations != null) {
                        newAnnotations = null;
                    }
                }
                throw ex;
            }

            if (convertOldPathFormat) {
                newFields.add(newField);

                if (annotations != null) {
                    String ann = annotations.get(fields.get(i));
                    if (ann != null) {
                        newAnnotations.put(newField, ann);
                    }
                }
            }

            indexFields.add(indexField);
        }

        indexKeyDef = createIndexKeyDef();
        indexEntryDef = createIndexEntryDef();
    }

    /*
     * The field path may be in the old format, that uses ._key and .[] steps.
     * If so, it must be converted to the new format.
     *
     * ._key steps are easy: just replace them with .keys().
     *
     * .[] steps may apply to either arrays or maps, and must be converted to
     * [] or .values(), respectively. To do so, we have to know the type of
     * the path up to the .[] step.
     */
    private String convertOldIndexPath(String field) {

        if (field.contains(KEY_TAG)) {
            return field.replace(KEY_TAG, TableImpl.KEYS);
        }

        if (field.contains(DOT_BRACKETS)) {

            int bracketsIdx = field.indexOf(DOT_BRACKETS);

            String mapOrArrayPath = field.substring(0, bracketsIdx);

            /*
             * mapOrArrayPath should contain only dot-separated identifiers,
             * so it is ok to call table.findTableField() on it.
             */
            FieldDefImpl def = table.findTableField(mapOrArrayPath);

            if (def.isArray()) {
                return field.replace(DOT_BRACKETS, TableImpl.BRACKETS);
            }

            if (!def.isMap()) {
                /*
                 * Maybe it's a json index that is being deserialized at an
                 * old client.
                 */
                throw new IllegalArgumentException(
                    "Multikey index path does not contain an array or map. " +
                    "mapOrArrayPath = " + mapOrArrayPath);
            }

            return field.replace(TableImpl.BRACKETS, TableImpl.VALUES);
        }

        /*
         * The path contains dot-separated identifiers only. However, it may
         * still lead to or cross an array. This is possible if the index was
         * created with a KVS version prior to 4.2, because at that itme []
         * was optional for arrays. In this case, we MUST find the array step
         * and add the [] after it. This will be done in validateIndexField().
         */
        return field;
    }

    /**
     * If there's a multi-key field in the index return a new IndexField
     * based on the the path to the complex instance.
     */
    private IndexField findMultiKeyField() {
        for (IndexField field : getIndexFields()) {
            if (field.isMultiKey()) {
                return field.getMultiKeyField();
            }
        }

        throw new IllegalStateException
            ("Could not find any multiKeyField in index " + name);
    }

    public boolean isMultiKeyMapIndex() {
        return isMultiKeyMapIndex;
    }

    /**
     * If an index path of this index contains a .keys() step, return the
     * position of associated index field. Otherwise return -1. It is used
     * in query/compiler/IndexAnalyzer.java
     */
    public int getPosForKeysField() {

        for (IndexField field : getIndexFields()) {
            if (field.isMapKeys()) {
                return field.getPosition();
            }
        }

        return -1;
    }

    /**
     * Extracts an index key from the key and data for this
     * index.  The key has already matched this index.
     *
     * @param key the key bytes
     *
     * @param data the row's data bytes
     *
     * @param keyOnly true if the index only uses key fields.  This
     * optimizes deserialization.
     *
     * @return the byte[] serialization of an index key or null if there
     * is no entry associated with the row, or the row does not match a
     * table record.
     *
     * While not likely it is possible that the record is not actually  a
     * table record and the key pattern happens to match.  Such records
     * will fail to be deserialized and throw an exception.  Rather than
     * treating this as an error, silently ignore it.
     *
     * TODO: maybe make this faster.  Right now it turns the key and data
     * into a Row and extracts from that object which is a relatively
     * expensive operation, including full Avro deserialization.
     */
    public byte[] extractIndexKey(byte[] key,
                                  byte[] data,
                                  boolean keyOnly) {
        RowImpl row = table.createRowFromBytes(key, data, keyOnly);
        if (row != null) {
            return serializeIndexKey(row, -1);
        }
        return null;
    }

    /**
     * Extracts multiple index keys from a single record.  This is used if
     * one of the indexed fields is an array.  Only one array is allowed
     * in an index.
     *
     * @param key the key bytes
     *
     * @param data the row's data bytes
     *
     * @param keyOnly true if the index only uses key fields.  This
     * optimizes deserialization.
     *
     * @return a List of byte[] serializations of index keys or null if there
     * is no entry associated with the row, or the row does not match a
     * table record.  This list may contain duplicate values.  The caller is
     * responsible for handling duplicates (and it does).
     *
     * While not likely it is possible that the record is not actually  a
     * table record and the key pattern happens to match.  Such records
     * will fail to be deserialized and throw an exception.  Rather than
     * treating this as an error, silently ignore it.
     *
     * TODO: can this be done without reserializing to Row?  It'd be
     * faster but more complex.
     *
     * 1.  Deserialize to RowImpl
     * 2.  Find the map or array value and get its size
     * 3.  for each map or array entry, serialize a key using that entry
     */
    public List<byte[]> extractIndexKeys(byte[] key,
                                         byte[] data,
                                         boolean keyOnly) {

        RowImpl row = table.createRowFromBytes(key, data, keyOnly);
        return extractIndexKeys(row);
    }

    public List<byte[]> extractIndexKeys(RowImpl row) {

        if (row == null) {
            return null;
        }

        ArrayList<byte[]> returnList;
        final FieldValueImpl empty = EmptyValueImpl.getInstance();

        IndexField arrayOrMapPath = findMultiKeyField();

        /*
         * Look for the map/array that is the source of the multiple index
         * entries from the current row.
         */
        FieldValueImpl mapOrArrayVal = row.findFieldValue(arrayOrMapPath,
                                                          -1,/*arrayIndex*/
                                                          null/*mapkey*/);

        if (isMultiKeyMapIndex) {

            if (mapOrArrayVal.isMap()) {
                MapValueImpl map = (MapValueImpl)mapOrArrayVal;

                if (map.size() == 0) {
                    byte[] serKey = serializeIndexKey(row, -1, empty);
                    returnList = new ArrayList<byte[]>(1);
                    if (serKey != null) {
                        returnList.add(serKey);
                    }
                } else {
                    returnList = new ArrayList<byte[]>(map.size());

                    for (String mapKey : map.getFieldsInternal().keySet()) {
                        byte[] serKey = serializeIndexKey(row, mapKey);
                        if (serKey != null) {
                            returnList.add(serKey);
                        }
                    }
                }
            } else if (mapOrArrayVal.isNull() || mapOrArrayVal.isAtomic()) {

                /*
                 * If the map is NULL, we put NULL in the index entry,
                 * If the map is atomic (which includes json null or EMPTY),
                 * we put EMPTY.
                 */
                if (!mapOrArrayVal.isNull()) {
                    mapOrArrayVal = empty;
                }

                byte[] serKey = serializeIndexKey(row, -1, mapOrArrayVal);
                returnList = new ArrayList<byte[]>(1);
                if (serKey != null) {
                    returnList.add(serKey);
                }
            } else {
                throw new IllegalArgumentException(
                    "Cannot create index entry for index " + getName() +
                    " on row\n" + row + "\nThe row does not contain a map " +
                    "at path " + findMultiKeyField());
            }

        } else {
            if (mapOrArrayVal.isArray()) {
                ArrayValueImpl array = (ArrayValueImpl)mapOrArrayVal;

                if (array.size() == 0) {
                    byte[] serKey = serializeIndexKey(row, -1, empty);
                    returnList = new ArrayList<byte[]>(1);
                    if (serKey != null) {
                        returnList.add(serKey);
                    }
                } else {
                    int size = array.size();

                    returnList = new ArrayList<byte[]>(size);

                    for (int i = 0; i < size; i++) {
                        byte[] serKey = serializeIndexKey(row, i, null);
                        if (serKey != null) {
                            returnList.add(serKey);
                        }
                    }
                }
            } else {
                /*
                 * If mapOrArrayVal is not an array, there should not be any
                 * array at all in the path. We call serializeIndexKey() passing
                 * -1 as the array index to make sure that this is the case.
                 * serializeIndexKey() will do the right thing in all cases.
                 * Specifically:
                 * - If mapOrArrayVal is NULL, it will put NULL in the index.
                 * - If mapOrArrayVal is EMPTY, it will put EMPTY in the index.
                 * - Else, the index path should be evaluated as if
                 *   mapOrArrayVal was a single-element array containing the
                 *   mapOrArrayVal. In this case what gets into the index
                 *   depends on the kind of mapOrArrayVal and where the [] are
                 *   in the index path. For example, if mapOrArrayVal is an
                 *   atomic value, the if the [] is the last step of the index
                 *   path, mapOrArrayVal is put into the index (if it belongs
                 *   to the declared type of the path); otherwise EMPTY is put
                 *   into the index.
                 */
                returnList = new ArrayList<byte[]>(1);

                byte[] serKey = serializeIndexKey(row, -1, null);
                if (serKey != null) {
                    returnList.add(serKey);
                }
            }
        }

        return returnList;
    }

    /*
     * Generates a JSON ObjectNode representing the index. The input side
     * of this representation is handled in JsonTableUtils.indexFromJsonNode().
     */
    public void toJsonNode(ObjectNode node) {

        node.put(NAME, name);

        node.put(TABLE, table.getFullName());

        node.put(TYPE, getType().toString().toLowerCase());

        if (description != null) {
            node.put(DESC, description);
        }

        if (isMultiKey()) {
            node.put("multi_key", "true");
        }

        ArrayNode fieldArray = node.putArray(FIELDS);
        for (String field : getFields()) {
            fieldArray.add(field);
        }

        if (types != null && types.size() != 0) {
            ArrayNode typesArray = node.putArray(TYPES);
            for (FieldDef.Type type : types) {
                if (type == null) {
                    typesArray.addNull();
                } else {
                    typesArray.add(type.toString());
                }
            }
        }

        if (annotations != null) {
            putMapAsJson(node, "annotations", getAnnotationsInternal());
        }
        if (properties != null) {
            putMapAsJson(node, "properties", properties);
        }
    }

    private static void putMapAsJson(ObjectNode node,
                                     String mapName,
                                     Map<String, String> map) {
        ObjectNode mapNode = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            mapNode.put(entry.getKey(), entry.getValue());
        }
        node.put(mapName, mapNode);
    }

    public FieldDef.Type getFieldType(int position) {
        if (types == null) {
            return null;
        }
        return types.get(position);
    }

    /**
     * Validate that the name, fields, and types of the index match
     * the table.  This also initializes the (transient) list of index fields in
     * indexFields, so that member must not be used in validate() itself.
     *
     * This method must only be called from the constructor.  It is not
     * synchronized and changes internal state.
     */
    private void validate() {

        TableImpl.validateIdentifier(name,
                                     TableImpl.MAX_NAME_LENGTH,
                                     "Index names");

        IndexField multiKeyField = null;

        if (getFields().isEmpty()) {
            throw new IllegalCommandException(
                "Index requires at least one field");
        }

        assert indexFields == null;

        indexFields = new ArrayList<IndexField>(getFields().size());

        int position = 0;
        for (String field : getFields()) {

            if (field == null || field.length() == 0) {
                throw new IllegalCommandException(
                    "Invalid (null or empty) index field name");
            }

            IndexField ifield = new IndexField(table, field,
                                               getFieldType(position),
                                               position++);

            /*
             * The check for multiKey needs to consider all fields as well as
             * fields that reference into complex types.  A multiKey field may
             * occur at any point in the navigation path (first, interior, leaf).
             *
             * The call to isMultiKey() will set the multiKey state in
             * the IndexField.
             *
             * Allow more than one multiKey field in a single index IFF they are
             * in the same object (map or array).
             */
            validateIndexField(ifield, true);

            /* Don't restrict number of multi-key fields for text indexes. */
            if (ifield.isMultiKey() && !isTextIndex()) {
                IndexField mkey = ifield.getMultiKeyField();
                if (multiKeyField != null && !mkey.equals(multiKeyField)) {
                    throw new IllegalCommandException(
                        "Indexes may index only one array or only one map (" +
                        "this implies that all multi-key paths in an index " +
                        "definition must use the same path before their " +
                        "multi-key step)");
                }
                multiKeyField = mkey;
            }

            if (indexFields.contains(ifield)) {
                throw new IllegalCommandException(
                    "Index already contains the field: " + field);
            }

            indexFields.add(ifield);
        }

        assert newFields.size() == indexFields.size();

        /*
         * initialize transient RecordDef representing the IndexKeyImpl
         * definition.
         */
        indexKeyDef = createIndexKeyDef();
        indexEntryDef = createIndexEntryDef();

        table.checkForDuplicateIndex(this);
    }

    /**
     * Validates the given index path expression (ipath) and returns its data
     * type (which must be one of the indexable atomic types).
     *
     * This call has a side effect of setting the multiKey state in the
     * IndexField so that the lookup need not be done twice.
     */
    private void validateIndexField(IndexField ipath, boolean isNewIndex) {

        int numSteps = ipath.numSteps();

        int stepIdx = 0;
        String step = ipath.getStep(stepIdx);
        FieldDef stepDef = ipath.getFirstDef();

        if (stepDef == null) {
            throw new IllegalCommandException(
                "Invalid index field definition : " + ipath + "\n" +
                "There is no field named " + step);
        }

        while (stepIdx < numSteps) {

            /*
             * TODO: Prevent any path through these types from
             * participating in a text index, until the text index
             * implementation supports them correctly.
             */
            if (isTextIndex() &&
                (stepDef.isBinary() ||
                 stepDef.isFixedBinary() ||
                 stepDef.isEnum())) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "Fields of type " + stepDef.getType() +
                        " cannot participate in a FULLTEXT index.");
            }

            if (stepDef.isJson()) {

                ipath.setIsJson();
                stepIdx++;

                if (ipath.getJsonFieldPath() == null) {
                    ipath.setJsonFieldPath(stepIdx);
                }

                if (stepIdx >= numSteps) {
                    break;
                }

                if (ipath.isBracketsStep(stepIdx) ||
                    ipath.isKeysStep(stepIdx) ||
                    ipath.isValuesStep(stepIdx)) {

                    ipath.setIsJsonMultiKey();
                    ipath.setMultiKeyPath(stepIdx);

                    if (ipath.isKeysStep(stepIdx)) {
                        ipath.setIsMapKeys();
                        ipath.declaredType = FieldDefImpl.stringDef;
                        isMultiKeyMapIndex = true;
                    } else if (ipath.isValuesStep(stepIdx)){
                        ipath.setIsMapValues();
                        isMultiKeyMapIndex = true;
                    }
                } else {
                    ipath.setIsMapKeyStep(stepIdx);
                }

            } else if (stepDef.isRecord()) {

                ++stepIdx;
                if (stepIdx >= numSteps) {
                    break;
                }

                step = ipath.getStep(stepIdx);
                stepDef = stepDef.asRecord().getFieldDef(step);

                if (stepDef == null) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "There is no field named \"" + step + "\" after " +
                        "path " + ipath.getPathName(stepIdx - 1));
                }

            } else if (stepDef.isArray()) {

                if (ipath.isMultiKey()) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "The definition contains more than one multi-key " +
                        "fields. The second multi-key field is " + step);
                }

                /*
                 * If there is a next step and it is [], consume it.
                 *
                 * Else, if we are creating a new index, throw an exception,
                 * because as of version 4.2 the use of [] is mandatory for
                 * array indexes.
                 *
                 * Otherwise, we must be reading from a store created prior
                 * tp v4.2. In this case, the [] may be missing. We must add
                 * it to the index path, because it is expected to be there
                 * in other parts of the code (for example, the index matching
                 * done by query processor).
                 */
                if (stepIdx + 1 < numSteps &&
                    ipath.getStep(stepIdx + 1).equals(TableImpl.BRACKETS)) {
                    ++stepIdx;

                } else if (isNewIndex) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "Can not index an array as a whole; use " + step +
                        "[]  to index the elements of the array");

                } else {
                    ++stepIdx;
                    ++numSteps;
                    ipath.add(stepIdx, TableImpl.BRACKETS, false);
                }

                ipath.setMultiKeyPath(stepIdx);

                step = TableImpl.BRACKETS;
                stepDef = stepDef.asArray().getElement();

            } else if (stepDef.isMap()) {

                ++stepIdx;
                if (stepIdx >= numSteps) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "Can not index a map as a whole; use " +
                        ".values() to index the elements of the map or " +
                        ".keys() to index the keys of the map");
                }

                step = ipath.getStep(stepIdx);

                if (ipath.isValuesStep(stepIdx) ||
                    ipath.isBracketsStep(stepIdx)) {

                    if (ipath.isMultiKey()) {
                        throw new IllegalCommandException(
                            "Invalid index field definition : " + ipath + "\n" +
                            "The definition contains more than one multi-key " +
                            "fields. The second multi-key field is " + step);
                    }

                    if (ipath.isBracketsStep(stepIdx)) {
                        ipath.setIsValuesStep(stepIdx);
                    }

                    ipath.setMultiKeyPath(stepIdx);
                    ipath.setIsMapValues();
                    isMultiKeyMapIndex = true;

                    /* Consume the .values() step */
                    stepDef = stepDef.asMap().getElement();

                } else if (ipath.isKeysStep(stepIdx)) {

                    if (ipath.isMultiKey()) {
                        throw new IllegalCommandException(
                            "Invalid index field definition : " + ipath + "\n" +
                            "The definition contains more than one multi-key " +
                            "fields. The second multi-key field is " + step);
                    }

                    ipath.setMultiKeyPath(stepIdx);
                    ipath.setIsMapKeys();
                    isMultiKeyMapIndex = true;

                    /* Consume the .keys() step */
                    stepDef = FieldDefImpl.stringDef;

                } else {
                    ipath.setIsMapKeyStep(stepIdx);
                    stepDef = stepDef.asMap().getElement();
                }

            } else {

                ++stepIdx;
                if (stepIdx >= numSteps) {
                    break;
                }

                step = ipath.getStep(stepIdx);
                throw new IllegalCommandException(
                    "Invalid index field definition : " + ipath + "\n" +
                    "There is no field named \"" + step + "\" after " +
                    "path " + ipath.getPathName(stepIdx - 1));
            }
        }

        if (!stepDef.isValidIndexField()) {
            throw new IllegalCommandException(
                "Invalid index field definition : " + ipath + "\n" +
                "Cannot index values of type " + stepDef);
        }

        /*
         * If NULLs are allowed in index key, the nullablity of 2 kinds of
         * field below is true:
         *  1. The complex field or nested field of complex type.
         *  2. The simple field is nullable and not a primary key field.
         */
        boolean nullable = (ipath.isComplex() ||
                            (!table.isKeyComponent(ipath.getStep(0)) &&
                             ipath.getFieldMap()
                                 .getFieldMapEntry(ipath.getStep(0))
                                     .isNullable())) &&
                           this.supportsSpecialValues();
        ipath.setNullable(nullable);

        /*
         * Specific types in index declarations are only allowed for JSON, and
         * in this path, the IndexField's typeDef becomes that of the specified
         * type, and not JSON.
         */
        if (ipath.getDeclaredType() != null) {

            if (!stepDef.isJson()) {
                throw new IllegalCommandException(
                    "Invalid index field definition: " + ipath + "\n" +
                    "Specific types are only allowed for JSON data types.");
            }

            ipath.type = ipath.getDeclaredType();
            return;
        }

        if (stepDef.isJson()) {
            throw new IllegalCommandException
                ("Invalid index field definition: " + ipath + "\n" +
                 "Please specify data type for JSON index field.");
        }

        ipath.type = (FieldDefImpl) stepDef;
    }

    @Override
    public String toString() {
        return "Index[" + name + ", " + table.getId() + ", " + status + "]";
    }

    /**
     * Creates a binary index key from an IndexKey. In this case
     * the IndexKey may be partially filled.
     *
     * This is the version used by most client-based callers.
     *
     * @return the serialized index key or null if the IndexKey cannot
     * be serialized (e.g. it has null values).
     */
    public byte[] serializeIndexKey(IndexKeyImpl indexKey) {
        return serializeIndexKey(indexKey, SerialVersion.CURRENT);
    }

    private byte[] serializeIndexKey(IndexKeyImpl indexKey,
                                     short opSerialVersion) {
        TupleOutput out = null;

        try {
            out = new TupleOutput();

            int numFields = getIndexKeyDef().getNumFields();

            boolean keyHasIndicators = (opSerialVersion >= SerialVersion.V12);

            for (int i = 0; i < numFields; ++i) {

                FieldValueImpl val = indexKey.get(i);

                if (val == null) {
                    /* A partial key, done with fields */
                    break;
                }

                /*
                 * If any values are NULL it is not possible to serialize the
                 * index key, even partially. NULL values cannot be indexed
                 * so this row has no entry for this index.
                 */
                if (FieldValueImpl.isSpecialValue(val) &&
                    (!this.supportsSpecialValues() || !keyHasIndicators)) {
                    return null;
                }

                serializeValue(out, val, indexFields.get(i), opSerialVersion);
            }

            return (out.size() != 0 ? out.toByteArray() : null);

        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ioe) {
            }
        }
    }

    /**
     * Create a binary index key from a row. This method is used for simple
     * (non-multi-key) indexes and for indexes on arrays. It is also used
     * for multi-key map indexes if the map is null or empty inside the given
     * row.
     *
     * @param record the record to extract a binary index key from. Actually,
     * this is RowImpl. The caller can vouch for the validity of the object.
     *
     * @param arrayIndex will be 0 if not doing an array lookup, or if the
     * desired array index is actually 0.  For known array lookups it may be
     * &gt;0.
     *
     * @return the serialized index key or null if the record cannot
     * be serialized.
     *
     * These are conditions that will cause serialization to fail:
     * 1.  The record has a null values in one of the index keys
     * 2.  An index key field contains a map and the record does not
     * have a value for the indexed map key value
     *
     * TODO: consider sharing more code with the other serializeIndexKey()
     * method.
     *
     * This is public so it can be used by TableSizeCommand. Otherwise it'd be
     * private.
     */
    public byte[] serializeIndexKey(RecordValueImpl record, int arrayIndex) {
        return serializeIndexKey(record, arrayIndex, null);
    }

    private byte[] serializeIndexKey(RecordValueImpl record,
                                     int arrayIndex,
                                     FieldValueImpl nullOrEmptyMapArray) {

        if (nullOrEmptyMapArray != null && !this.supportsSpecialValues()) {
            return null;
        }

        if (isMultiKeyMapIndex()) {
            if (nullOrEmptyMapArray == null) {
                throw new IllegalStateException("Wrong serializer for " +
                    "map index");
            }
        }

        TupleOutput out = null;

        try {
            out = new TupleOutput();

            for (IndexField field : getIndexFields()) {

                FieldValueImpl val =
                    ((field.isMultiKey() && nullOrEmptyMapArray != null) ?
                     nullOrEmptyMapArray :
                     record.findFieldValue(field, arrayIndex, null/*mapKey*/));

                /*
                 * In the case of a JSON index it is possible to resolve to
                 * a map or array (JSON object, JSON array). For now, if this
                 * happens, serialize a null (SQL).
                 *
                 * NOTE: in the near future it is likely that if a JSON array is
                 * encountered it will be indexed as a multi-value index, unless
                 * otherwise specified. JSON objects found as targets for
                 * indexing are not valid.
                 *
                 * Issue: JSON indexes may be multi-key and may not be. This
                 * will affect IndexField and how it handles multi-key indexes.
                 * At this time that state is static and not dependent on the
                 * actual data. Generic JSON indexes will always be potentially
                 * multi-key.
                 */

                /*
                 * If the value is NULL or EMPTY, and the index does not
                 * support indexing of NULL/EMPTY, it is not possible to
                 * create a binary index key. In this case, this row has
                 * no entry for this index.
                 */
                if (FieldValueImpl.isSpecialValue(val) &&
                    !this.supportsSpecialValues()) {
                    return null;
                }

                serializeValue(out, val, field);
            }

            return (out.size() != 0 ? out.toByteArray() : null);

        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ioe) {
            }
        }
    }

    /**
     * Create a binary index key from a row. This method is used for multi-key
     * map indexes.
     *
     * @param record the record to extract a binary index key from. Actually,
     * this is RowImpl. The caller can vouch for the validity of the object.
     *
     * @param mapKey will be null if not doing a map lookup.
     *
     * @return the serialized index key or null if the record cannot
     * be serialized.
     *
     * These are conditions that will cause serialization to fail:
     * 1. The record has a null value in one of the index keys
     * 2. An index key field contains a map and the record does not
     *    have a value for the indexed map key value
     *
     * TODO: consider sharing more code with the other serializeIndexKey()
     * method.
     *
     * This method is package protected vs private because it's used by test
     * code.
     */
    byte[] serializeIndexKey(RecordValueImpl record, String mapKey) {

        assert isMultiKeyMapIndex();
        TupleOutput out = null;
        try {
            out = new TupleOutput();

            for (IndexField field : getIndexFields()) {

                FieldValueImpl val = record.findFieldValue(field, -1, mapKey);

                /*
                 * If the value is NULL or EMPTY, and the index does not
                 * support indexing of NULL/EMPTY, it is not possible to
                 * create a binary index key. In this case, this row has
                 * no entry for this index.
                 */
                if (FieldValueImpl.isSpecialValue(val) &&
                    !this.supportsSpecialValues()) {
                    return null;
                }

                serializeValue(out, val, field);
            }

            return (out.size() != 0 ? out.toByteArray() : null);

        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ioe) {
            }
        }
    }

    /**
     * Serializes a specific scalar FieldValue that is a component of an index.
     */
    private void serializeValue(
        TupleOutput out,
        FieldValueImpl val,
        IndexField indexField) {

        serializeValue(out, val, indexField, SerialVersion.CURRENT);
    }

    private void serializeValue(
        TupleOutput out,
        FieldValueImpl val,
        IndexField indexField,
        short opSerialVersion) {

        /*
         * Handle various null forms and field separator
         */
        if (indexField.isNullable() && opSerialVersion >= SerialVersion.V12) {
            byte indicator = getSerializeIndicator(val, opSerialVersion);
            out.writeByte(indicator);
            if (indicator != NORMAL_VALUE_INDICATOR) {
                return;
            }
        }

        if (indexField.isJson()) {

            /*
             * Validate the type of the field if the index is defined with a
             * constrained type. This only works for exact matches of specific
             * scalar types. Testing for a scalar vs array index field, when
             * supported, must be done in callers.
             */
            FieldDefImpl declaredType = indexField.getDeclaredType();

            if (declaredType != null && declaredType.isPrecise()) {

                /*
                 * Check that the value belongs to a subtype of the
                 * declared type, and cast to the declared type if needed.
                 */
                FieldDefImpl valDef = val.getDefinition();
                FieldValueImpl castVal = null;
                if (valDef.isSubtype(declaredType)) {
                    castVal = (FieldValueImpl)val.castToSuperType(declaredType);
                }

                if (castVal != null) {
                    val = castVal;
                } else {
                    /* castVal == null means conversion above failed */
                    throw new IllegalArgumentException(
                        "Invalid type for JSON index field: " +
                        indexField + ". Type is " +
                        val.getType() + ", expected type is " +
                        declaredType.getType() + "\nvalue = " + val);
                }
            } else {
                /*
                 * Multiple types may be mapped to a single index type to
                 * simplify searches. E.g. generic JSON indexes may map
                 * all numeric types to Number in an index.
                 */
                val = convertToJsonValue(val);
                out.writeByte(val.getType().ordinal());
            }
        }

        switch (val.getType()) {
        case INTEGER:
            out.writeSortedPackedInt(val.asInteger().get());
            break;
        case STRING:
            out.writeString(val.asString().get());
            break;
        case LONG:
            out.writeSortedPackedLong(val.asLong().get());
            break;
        case DOUBLE:
            out.writeSortedDouble(val.asDouble().get());
            break;
        case FLOAT:
            out.writeSortedFloat(val.asFloat().get());
            break;
        case NUMBER:
            out.write(((NumberValueImpl)val).getBytes());
            break;
        case ENUM:
            /* enumerations are sorted by declaration order */
            out.writeSortedPackedInt(val.asEnum().getIndex());
            break;
        case BOOLEAN:
            out.writeBoolean(val.asBoolean().get());
            break;
        case TIMESTAMP:
            out.write(((TimestampValueImpl)val).getBytes(true));
            break;
        default:
            throw new IllegalStateException(
                "Type not supported in indexes: " + val.getType());
        }
    }

    public boolean supportsSpecialValues() {
        return isNullSupported;
    }

    private byte getSerializeIndicator(FieldValue val,
                                       short opSerialVersion) {
        if (val.isNull()) {
            return getNullIndicator(opSerialVersion);
        } else if (((FieldValueImpl)val).isEMPTY()) {
            return (opSerialVersion < SerialVersion.V14 ?
                    getNullIndicator(opSerialVersion) :
                    getEmptyIndicator());
        } else if (val.isJsonNull()) {
            return JSON_NULL_INDICATOR;
        }
        return NORMAL_VALUE_INDICATOR;
    }

    private FieldValueImpl getSpecialValue(byte indicator,
                                           short opSerialVersion) {
        return (indicator == getNullIndicator(opSerialVersion) ?
                NullValueImpl.getInstance() :
                (indicator == EMPTY_INDICATOR ?
                 EmptyValueImpl.getInstance() :
                 (indicator == JSON_NULL_INDICATOR ?
                  NullJsonValueImpl.getInstance() : null)));
    }

    private byte getNullIndicator(short opSerialVersion) {
        return (indexVersion >= INDEX_VERSION_V1 &&
                opSerialVersion >= SerialVersion.V14) ?
                NULL_INDICATOR_V1 : NULL_INDICATOR_V0;
    }

    private byte getEmptyIndicator() {
        return (indexVersion >= INDEX_VERSION_V1) ?
                EMPTY_INDICATOR : NULL_INDICATOR_V0;
    }

    /**
     * This method is used only to support queries compiled with clients older
     * than verion 18.1
     *
     * Deserialize the serialized format of an index key directly into a Row.
     *
     * Arrays -- if there is an array index the index key returned will
     * be the serialized value of a single array entry and not the array
     * itself. This value needs to be deserialized back into a single-value
     * array.
     *
     * Maps -- if there is a map index the index key returned will
     * be the serialized value of a single map entry.  It may be key-only or
     * it may be key + value. In both cases the map and the appropriate key
     * need to be created.
     *
     * In this path, partial population is allowed.
     *
     * @param data the bytes
     * @param row the Row to use.
     */
    public void rowFromIndexKey(byte[] data, RowImpl row) {

        TupleInput input = null;

        try {
            input = new TupleInput(data);

            /*
             * If the index indexes a .keys() path, keyForKeysField is the
             * value of this path in the current index entry. This is needed
             * by the putComplex method.
             */
            String keyForKeysField = null;

            for (IndexField ifield : getIndexFields()) {

                if (input.available() <= 0) {
                    break;
                }

                int jsonArrayPathPos = ifield.isJsonAsArray() ?
                        (ifield.getMultiKeyField().numSteps() - 1) : -1;

                if (ifield.mayHaveSpecialValue()) {
                    byte in = input.readByte();

                    FieldValueImpl specialVal =
                        getSpecialValue(in, SerialVersion.CURRENT);

                    if (specialVal != null) {
                        row.putComplex(ifield, specialVal, keyForKeysField,
                                       jsonArrayPathPos);
                        continue;
                    }
                }

                FieldDefImpl def = ifield.getType();
                FieldDef.Type type = (ifield.isJson() ?
                                      ifield.getDeclaredType().getType() :
                                      def.getType());

                switch (type) {
                case INTEGER:
                case STRING:
                case LONG:
                case DOUBLE:
                case BOOLEAN:
                case FLOAT:
                case NUMBER:
                case ENUM:
                case TIMESTAMP:
                    FieldValueImpl val = (FieldValueImpl)def.createValue(
                        type, FieldValueImpl.readTuple(type, def, input));

                    if (ifield.isMapKeys()) {
                       keyForKeysField = ((StringValueImpl)val).get();
                    }

                    row.putComplex(ifield, val, keyForKeysField,
                                   jsonArrayPathPos);
                    break;

                 default:
                    throw new IllegalStateException
                        ("Type not supported in indexes: " + type);
                }
            }
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException ioe) {
                /* ignore IOE on close */
            }
        }
    }

    /**
     * Deserialize the binary format of an index entry into a (flat)
     * RecordValue that contains both the index key values and the
     * associated primary key values.
     */
    public void rowFromIndexEntry(RecordValueImpl row,
                                  byte[] primKey,
                                  byte[] indexKey) {

        TupleInput input = null;

        try {
            input = new TupleInput(indexKey);

            /*
             * Deserialize the index key (not including the associated
             * primary key).
             */
            for (int pos = 0; pos < numFields(); ++pos) {

                if (input.available() <= 0) {
                    throw new IllegalStateException(
                        "Index key desrialization error: the index key " +
                        "is too short");
                }

                IndexField ifield = getIndexPath(pos);

                if (ifield.mayHaveSpecialValue()) {
                    byte in = input.readByte();

                    FieldValueImpl specialVal =
                        getSpecialValue(in, SerialVersion.CURRENT);

                    if (specialVal != null) {
                        row.putInternal(pos, specialVal);
                        continue;
                    }
                }

                FieldDefImpl fdef = indexKeyDef.getFieldDef(pos);
                FieldDef.Type ftype = fdef.getType();

                switch (ftype) {
                case INTEGER:
                case STRING:
                case LONG:
                case DOUBLE:
                case BOOLEAN:
                case FLOAT:
                case NUMBER:
                case ENUM:
                case TIMESTAMP:
                    FieldValueImpl val = (FieldValueImpl)fdef.createValue(
                        ftype, FieldValueImpl.readTuple(ftype, fdef, input));
                    row.putInternal(pos, val);
                    break;

                 default:
                    throw new IllegalStateException(
                        "Type not supported in indexes: " + ftype);
                }
            }
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException ioe) {
                /* ignore IOE on close */
            }
        }

        /*
         * Deserialize the values of the associated primary key.
         */
        if (primKey != null) {
            if (!table.initRowFromKeyBytes(primKey, numFields(), row)) {
                throw new IllegalStateException(
                    "Failed to deserialize primary key retrieved from index " +
                    getName());
            }
        }
    }

    public IndexKeyImpl deserializeIndexKey(byte[] data, boolean partialOK) {
        return deserializeIndexKey(data, partialOK, SerialVersion.CURRENT);
    }

    /**
     * Deserializes binary index keys into IndexKeyImpl. This method is used
     * both at the server and the client:
     *
     * (a) At the client it deserializes binary index keys that are sent
     * from the server during an IndexKeysIterate operation. These keys are
     * "full"index keys that have been extracted from an index at the server
     * partialOK is false in this case.
     *
     * (b) At the server it deserializes binary search index keys that are
     * sent from a client as the start/stop keys of an IndexOperation.
     * partialOK is true in this case.
     *
     * @param data the bytes
     * @param partialOK true if not all fields must be in the data stream.
     * @param opSerialVersion In case (b) above, this is the serial version
     *        used by the IndexOperation.
     * @return an instance of IndexKeyImpl
     */
    IndexKeyImpl deserializeIndexKey(byte[] data,
                                     boolean partialOK,
                                     short opSerialVersion) {
        TupleInput input = null;
        RecordDefImpl idxKeyDef = getIndexKeyDef();
        IndexKeyImpl indexKey = new IndexKeyImpl(this, idxKeyDef);

        /*
         * For versions >= 4.2, each index field includes an "indicator" byte.
         * For 4.2 and 4.3 the byte is a NULL indicator. For 4.4+ it is a
         * "special value" indicator (NULL, json null, or empty). Furthermore,
         * the value for the NULL indicator has changed between 4.3 and 4.4. In
         * case (b) above, we need to check if the client key contains indicators
         * and if so, map the value of each indicator to the correct special
         * value. In case (a) we don't need to do this, because the server
         * makes sure that the binary keys sent to the client have the format
         * that is expected by the client In this case, opSerialVersion will
         * be SerialVersion.CURRENT.
         */
        boolean keyHasIndicators = (opSerialVersion >= SerialVersion.V12);

        try {
            input = new TupleInput(data);

            int numFields = idxKeyDef.getNumFields();

            for (int i = 0; i < numFields; ++i) {

                if (input.available() <= 0) {
                    break;
                }

                IndexField ifield = getIndexPath(i);

                if (keyHasIndicators && ifield.mayHaveSpecialValue()) {

                    byte in = input.readByte();

                    FieldValueImpl specialVal =
                        getSpecialValue(in, opSerialVersion);

                    if (specialVal != null) {
                        indexKey.put(i, specialVal);
                        continue;
                    }
                }

                FieldDefImpl def = idxKeyDef.getFieldDef(i);
                FieldDef.Type type = def.getType();

                switch (type) {
                case INTEGER:
                case STRING:
                case LONG:
                case DOUBLE:
                case FLOAT:
                case NUMBER:
                case ENUM:
                case BOOLEAN:
                case TIMESTAMP:
                    FieldValue val = def.createValue(
                        type, FieldValueImpl.readTuple(type, def, input));
                    indexKey.put(i, val);
                    break;

                 default:
                     throw new IllegalStateException(
                         "Type not supported in indexes: " + type);
                }
            }

            if (!partialOK && !indexKey.isComplete()) {
                throw new IllegalStateException(
                    "Missing fields from index data for index " +
                    getName() + ", expected " + numFields +
                    ", received " +   indexKey.size());
            }
            return indexKey;
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException ioe) {
                /* ignore IOE on close */
            }
        }
    }

    /*
     * See javadoc for IndexOperationHandler.reserializeOldKeys().
     */
    byte[] reserializeOldKey(byte[] key, short opVersion) {

        IndexKeyImpl ikey = deserializeIndexKey(key,
                                                true, /*partialOk*/
                                                opVersion);
        return serializeIndexKey(ikey);
    }

    /*
     * See javadoc for IndexKeysIterateHandler.reserializeToOldKeys()
     */
    public byte[] reserializeToOldKey(byte[] indexKey, short opVersion) {

        IndexKeyImpl ikey = deserializeIndexKey(indexKey,
                                                false, /* partialOK */
                                                SerialVersion.CURRENT);

        return serializeIndexKey(ikey, opVersion);
    }

    /**
     * Checks to see if the index contains the *single* named field.
     * For simple types this is a simple contains operation.
     *
     * For complex types this needs to validate for a put of a complex
     * type that *may* contain an indexed field.
     * Validation of such fields must be done later.
     *
     * In the case of a nested field name with dot-separated names,
     * this code simply checks that fieldName is one of the components of
     * the complex field (using String.contains()).
     */
    boolean isIndexPath(TablePath tablePath) {

        for (IndexField indexField : getIndexFields()) {
            if (indexField.equals(tablePath)) {
                    return true;
            }
        }
        return false;
    }

    /**
     * Creates a RecordDef for the flattened key fields. The rules for naming
     * the fields of the new RecordDef are:
     * 1. top-level atomic fields are left as-is (the field name, e.g. "a")
     * 2. paths into nested Records that do not involve arrays or maps are
     * left intact (e.g. "a.b.c")
     * 3. array elements turn into <i>path-to-array</i>[]
     * 4. map elements turn into <i>path-to-map</i>.values()
     * 5. map keys turn into <i>path-to-map</i>.keys()
     *
     * Index fields cannot contain more than one array or map which simplifies
     * the translations.
     */
    private RecordDefImpl createIndexKeyDef() {

        FieldMap fieldMap = createFieldMapForIndexKey();
        return new RecordDefImpl(fieldMap, null);
    }

    private RecordDefImpl createIndexEntryDef() {

        FieldMap fieldMap = createFieldMapForIndexKey();

        RecordDefImpl pkDef = table.getPrimKeyDef();
        int primKeySize = pkDef.getNumFields();

        for (int i = 0; i < primKeySize; ++i) {

            /*
             * Note: A '#' is placed in front of each prim key column name
             * because a prim key column may also be part of the index key,
             * so without the '#' we could end up with 2 record fields having
             * the same name. Other code that uses this indexEntryDef is
             * careful to access these fields positionally, rather than by name.
             */
            FieldDefImpl fdef = pkDef.getFieldDef(i);
            String fname = "#" + pkDef.getFieldName(i);
            final FieldMapEntry fme = new FieldMapEntry(fname, fdef);

            fieldMap.put(fme);
        }

        return new RecordDefImpl(fieldMap, null);
    }

    private FieldMap createFieldMapForIndexKey() {

        FieldMap fieldMap = new FieldMap();

        /*
         * Use the list of IndexField because it's already got complex
         * paths translated to a normalized form with use of [] in the
         * appropriate places.
         */
        for (int i = 0; i < numFields(); ++i) {

            IndexField indexField = getIndexPath(i);
            FieldDefImpl fdef = indexField.getType();
            String fname = newFields.get(i);
            final FieldMapEntry fme = new FieldMapEntry(fname, fdef);

            fieldMap.put(fme);
        }

        return fieldMap;
    }

    /**
     * Encapsulates a single field in an index, which may be simple or
     * complex.  Simple fields (e.g. "name") have a single component. Fields
     * that navigate into nested fields (e.g. "address.city") have multiple
     * components.  The state of whether a field is simple or complex is kept
     * by TablePath.
     *
     * IndexField adds this state:
     *   multiKeyField -- if this field results in a multi-key index this holds
     *     the portion of the field's path that leads to the FieldValue that
     *     makes it multi-key -- an array or map.  This is used as a cache to
     *     make navigation to that field easier.
     *   multiKeyType -- if multiKeyPath is set, this indicates if the field
     *     is a map key or map value field.
     * Arrays don't need additional state.
     *
     * Field names are case-insensitive, so strings are stored lower-case to
     * simplify case-insensitive comparisons.
     */
    public static class IndexField extends TablePath {

        /* the path to a multi-key field (map or array) */
        private IndexField multiKeyField;

        /*
         * The position of the multikey step (if any) within the multikey
         * index paths of this index.
         */
        private int multiKeyStepPos = -1;

        private MultiKeyType multiKeyType;

        /*
         * true if any part of the path is JSON. When this is true, if there
         * are remaining steps after the JSON field they are ignored because
         * they may or may not exist in any given JSON document
         */
        private boolean isJson;

        /*
         * true if the multi key field is the JSON field or sub field
         * of JSON field
         */
        private boolean isJsonMultiKey;

        private TablePath jsonFieldPath;

        /* the position in the key */
        private final int position;

        private FieldDefImpl declaredType;

        /*
         * The data type of this field. If the field is a json field with
         * a declared type, then this.type and this.declaredType are the
         * same type.
         */
        private FieldDefImpl type;

        /* the nullability of the field */
        private boolean nullable;

        /* ARRAY is not included because no callers need that information */
        private enum MultiKeyType { NONE, MAPKEY, MAPVALUE }

        /* public access for use by the query compiler */
        public IndexField(TableImpl table,
                          String field,
                          FieldDef.Type typecode,
                          int position) {
            super(table, field);
            multiKeyType = MultiKeyType.NONE;
            this.position = position;
            this.declaredType = (typecode == null ?
                                 null :
                                 getFieldDefForTypecode(typecode));
            this.jsonFieldPath = null;
        }

        private IndexField(FieldMap fieldMap, String field, int position) {
            super(fieldMap, field);
            multiKeyType = MultiKeyType.NONE;
            this.position = position;
            this.declaredType = null;
            this.jsonFieldPath = null;
        }

        IndexField getMultiKeyField() {
            return multiKeyField;
        }

        public int getMultiKeyStepPos() {
            return multiKeyStepPos;
        }

        public boolean isMultiKey() {
            return multiKeyField != null;
        }

        private void setIsJson() {
            isJson = true;
        }

        public boolean isJson() {
            return isJson;
        }

        void setIsJsonMultiKey() {
            isJsonMultiKey = true;
        }

        boolean isJsonMultiKey() {
            return isJsonMultiKey;
        }

        boolean isJsonAsArray() {
            if (isJsonMultiKey()) {
                return !isMapKeys() && !isMapValues();
            }
            return false;
        }

        public int getPosition() {
            return position;
        }

        public FieldDefImpl getType() {
            return type;
        }

        public void setType(FieldDefImpl def) {
            type = def;
        }

        public FieldDefImpl getDeclaredType() {
            return declaredType;
        }

        boolean hasPreciseType() {
            return type.isPrecise();
        }

        boolean isNullable() {
            return nullable;
        }

        boolean mayHaveSpecialValue() {
            /*
             * Only top-level atomic columns may be non-nullable. Such columns
             * are non-EMPTY and non-json-null as well. So, this method is just
             * returns whether the index field is nullable.
             */
            return nullable;
        }

        private void setMultiKeyPath(int pos) {
            multiKeyField = new IndexField(getFieldMap(), null, position);
            for (int i = 0; i < pos; ++i) {
                StepInfo si = getStepInfo(i);
                multiKeyField.addStepInfo(new StepInfo(si));
            }
            multiKeyStepPos = pos;
        }

        public boolean isMapKeys() {
            return multiKeyType == MultiKeyType.MAPKEY;
        }

        private void setIsMapKeys() {
            multiKeyType = MultiKeyType.MAPKEY;
        }

        public boolean isMapValues() {
            return multiKeyType == MultiKeyType.MAPVALUE;
        }

        private void setIsMapValues() {
            multiKeyType = MultiKeyType.MAPVALUE;
        }

        public void setNullable(boolean nullable) {
            this.nullable = nullable;
        }

        private void setJsonFieldPath(int pos) {
            jsonFieldPath = new TablePath(getFieldMap(), (String)null);
            for (int i = 0; i < pos; ++i) {
                StepInfo si = getStepInfo(i);
                jsonFieldPath.addStepInfo(new StepInfo(si));
            }
        }

        TablePath getJsonFieldPath() {
            return jsonFieldPath;
        }

        private static FieldDefImpl getFieldDefForTypecode(FieldDef.Type code) {
            switch (code) {
            case INTEGER:
                return FieldDefImpl.integerDef;
            case LONG:
                return FieldDefImpl.longDef;
            case DOUBLE:
                return FieldDefImpl.doubleDef;
            case BOOLEAN:
                return FieldDefImpl.booleanDef;
            case STRING:
                return FieldDefImpl.stringDef;
            case NUMBER:
                return FieldDefImpl.numberDef;
            default:
                throw new IllegalArgumentException(
                    "Invalid type for JSON index field: " + code);
            }
        }
    }

    @Override
    public Index.IndexType getType() {
        if (getAnnotationsInternal() == null) {
            return Index.IndexType.SECONDARY;
        }
        return Index.IndexType.TEXT;
    }

    private boolean isTextIndex() {
        return getType() == Index.IndexType.TEXT;
    }

    public static void populateMapFromAnnotatedFields(
         List<AnnotatedField> fields,
         List<String> fieldNames,
         Map<String, String> annotations) {

    	for (AnnotatedField f : fields) {
            String fieldName = f.getFieldName();
            fieldNames.add(fieldName);
            annotations.put(fieldName, f.getAnnotation());
    	}
    }

    /**
     * This lightweight class stores an index field, along with
     * an annotation.  Not all index types require annotations;
     * It is used for the mapping specifier in full-text indexes.
     */
    public static class AnnotatedField implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String fieldName;

        private final String annotation;

        public AnnotatedField(String fieldName, String annotation) {
            assert(fieldName != null);
            this.fieldName = fieldName;
            this.annotation = annotation;
        }

        /**
         * The name of the indexed field.
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         *  The field's annotation.  In Text indexes, this is the ES mapping
         *  specification, which is a JSON string and may be null.
         */
        public String getAnnotation() {
            return annotation;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            AnnotatedField other = (AnnotatedField) obj;

            if (! fieldName.equals(other.fieldName)) {
                return false;
            }

            return (annotation == null ?
                    other.annotation == null :
                    JsonUtils.jsonStringsEqual(annotation, other.annotation));
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + fieldName.hashCode();
            if (annotation != null) {
                result = prime * result + annotation.hashCode();
            }
            return result;
        }
    }

    @Override
    public String getAnnotationForField(String fieldName) {
        if (isTextIndex() == false) {
            return null;
        }
        return getAnnotationsInternal().get(fieldName);
    }

    public RowImpl deserializeRow(byte[] keyBytes, byte[] valueBytes) {
        return table.createRowFromBytes(keyBytes, valueBytes, false);
    }

    /**
     * Formats the index.
     * @param asJson true if output should be JSON, otherwise tabular.
     */
    public String formatIndex(boolean asJson) {
        if (asJson) {
            ObjectWriter writer = JsonUtils.createWriter(true);
            ObjectNode o = JsonUtils.createObjectNode();
            toJsonNode(o);
            /*
             * Format the JSON into a string
             */
            try {
                return writer.writeValueAsString(o);
            } catch (IOException ioe) {
                throw new IllegalArgumentException
                    ("Failed to serialize index description: " +
                     ioe.getMessage());
            }
        }
        return TabularFormatter.formatIndex(this);
    }

    /* For testing purpose */
    final static String INDEX_NULL_DISABLE = "test.index.null.disable";

    private boolean areIndicatorBytesEnabled() {
        if (Boolean.getBoolean(INDEX_NULL_DISABLE)) {
            return false;
        }
        return true;
    }

    final static String INDEX_SERIAL_VERSION = "test.index.serial.version";

    public int getIndexVersion() {
        return Integer.getInteger(INDEX_SERIAL_VERSION, INDEX_VERSION_CURRENT);
    }

    /**
     * For generic JSON indexes numerics, at least, will likely be all
     * converted to Number in indexes. This is TODO.
     */
    private static FieldValueImpl convertToJsonValue(FieldValueImpl val) {
        return val;
    }

    /**
     * This is directly from JE's com.sleepycat.je.tree.Key class and is the
     * default byte comparator for JE's btree.
     *
     * Compare using a default unsigned byte comparison.
     */
    static int compareUnsignedBytes(byte[] key1,
                                    int off1,
                                    int len1,
                                    byte[] key2,
                                    int off2,
                                    int len2) {
        int limit = Math.min(len1, len2);

        for (int i = 0; i < limit; i++) {
            byte b1 = key1[i + off1];
            byte b2 = key2[i + off2];
            if (b1 == b2) {
                continue;
            }
            /*
             * Remember, bytes are signed, so convert to shorts so that we
             * effectively do an unsigned byte comparison.
             */
            return (b1 & 0xff) - (b2 & 0xff);
        }

        return (len1 - len2);
    }

    static int compareUnsignedBytes(byte[] key1, byte[] key2) {
        return compareUnsignedBytes(key1, 0, key1.length, key2, 0, key2.length);
    }
}

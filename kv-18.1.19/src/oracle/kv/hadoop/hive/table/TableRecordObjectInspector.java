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
import java.util.Arrays;
import java.util.List;

import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordValue;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.RECORD to Hive column type STRUCT.
 */
public class TableRecordObjectInspector extends StandardStructObjectInspector {

    /*
     * Note: with respect to the 'get' methods defined by this ObjectInspector,
     * when the Hive infrastructure invokes those methods during a given
     * query, the data Object input to those methods may be an instance of
     * FieldValue (specifically, a RecordValue) or may be an instance of the
     * corresponding Java class (that is, a List or Object[] representing a
     * Hive STRUCT type). As a result, each such method must be prepared to
     * handle both cases.
     *
     * With respect to the 'create/set' methods, this class defaults to the
     * 'create/set' methods provided by the StandardStructObjectInspector
     * parent class; which always return a Java List or Object[] (representing
     * a Hive STRUCT type), instead of a RecordValue.
     *
     * Finally, note that the logger used in this class is inherited from
     * the parent class.
     */

    protected static final String DEFAULT_FIELD_NAME = "";

    private static final List<String> DEFAULT_FIELD_NAME_LIST =
        new ArrayList<String>();

    private static final List<ObjectInspector> DEFAULT_OI_LIST =
        new ArrayList<ObjectInspector>();

    static {
        DEFAULT_FIELD_NAME_LIST.add(DEFAULT_FIELD_NAME);
        DEFAULT_OI_LIST.add(null);
    }

    TableRecordObjectInspector() {
        super(DEFAULT_FIELD_NAME_LIST, DEFAULT_OI_LIST);
    }

    TableRecordObjectInspector(
        List<String> structFieldNames,
        List<ObjectInspector> structFieldObjectInspectors) {

        super(structFieldNames, structFieldObjectInspectors);
    }

    TableRecordObjectInspector(
        List<String> structFieldNames,
        List<ObjectInspector> structFieldObjectInspectors,
        List<String> structFieldComments) {

        super(
           structFieldNames, structFieldObjectInspectors, structFieldComments);
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {

        if (fieldName == null) {
            return null;
        }
        return super.getStructFieldRef(fieldName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getStructFieldData(Object data, StructField fieldRef) {

        if (data == null || fieldRef == null) {
            return null;
        }

        final MyField f = (MyField) fieldRef;
        final int fieldID = f.getFieldID();
        assert (fieldID >= 0 && fieldID < fields.size());

        if (data instanceof FieldValue) {

            if (((FieldValue) data).isNull()) {
                return null;
            }

            final RecordValue recordValue = ((FieldValue) data).asRecord();
            final int nFields = recordValue.size();

            if (fields.size() != nFields) {
                LOG.warn("Trying to access " + fields.size() +
                         " fields inside a RECORD of " + nFields +
                         " elements: " + recordValue.getFieldNames());
            }

            if (fieldID >= nFields) {
                return null;
            }

            final List<String> recFields = recordValue.getFieldNames();
            int i = 0;
            for (String recFieldName : recFields) {
                if (i == fieldID) {
                    return recordValue.get(recFieldName);
                }
                i++;
            }

        } else if (data instanceof List) {

            final List<Object> list = (List<Object>) data;
            final int nFields = list.size();
            if (fields.size() != nFields) {
                LOG.warn("Trying to access " + fields.size() +
                         " fields inside a LIST of " + nFields +
                         " elements: " + list);
            }

            if (fieldID >= nFields) {
                return null;
            }
            return list.get(fieldID);

        } else if (data instanceof Object[]) {

            final Object[] arr = (Object[]) data;
            final int nFields = arr.length;
            if (fields.size() != nFields) {
                LOG.warn("Trying to access " + fields.size() +
                         " fields inside an ARRAY of " + nFields +
                         " elements: " + Arrays.asList(arr));
            }

            if (fieldID >= nFields) {
                return null;
            }
            return arr[fieldID];
        }
        throw new IllegalArgumentException(
                      "invalid input object: must be Object[], " +
                      "List, or RecordValue");
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> getStructFieldsDataAsList(Object data) {

        if (data == null) {
            return null;
        }

        if (data instanceof FieldValue) {

            if (((FieldValue) data).isNull()) {
                return null;
            }

            final RecordValue recordValue = ((FieldValue) data).asRecord();
            assert (recordValue.size() == fields.size());
            final List<Object> list = new ArrayList<Object>();
            for (String recFieldName : recordValue.getFieldNames()) {
                list.add(recordValue.get(recFieldName));
            }
            return list;
        } else if (data instanceof List) {
            final List<Object> list = (List<Object>) data;
            assert (list.size() == fields.size());
            return list;
        } else if (data instanceof Object[]) {
            final Object[] arr = (Object[]) data;
            assert (arr.length == fields.size());
            return Arrays.asList(arr);
        }
        throw new IllegalArgumentException(
                      "invalid input object: must be Object[], " +
                      "List, or RecordValue");
    }

    /**
     * Special class provided to support testing the
     * <code>getStructFieldData</code> method; which casts the given
     * <code>StructField</code> parameter to the <code>MyField</code>
     * nested class of the <code>StandardStructObjectInspector</code>
     * class. Tests can create an instance or subclass of this class
     * to be input to that method, and the cast that is performed will
     * be allowed.
     */
    protected static class TableStructField extends MyField {
        public TableStructField() {
            this(0, "", null);
        }

        public TableStructField(int fieldId,
                               String fieldName,
                               ObjectInspector fieldObjectInspector) {
            super(fieldId, fieldName, fieldObjectInspector);
        }

        public TableStructField(int fieldId,
                               String fieldName,
                               ObjectInspector fieldObjectInspector,
                               String fieldComment) {
            super(fieldId, fieldName, fieldObjectInspector, fieldComment);
        }
    }
}

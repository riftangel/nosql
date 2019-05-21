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

import oracle.kv.Key;
import oracle.kv.table.FieldRange;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.util.ArrayPosition;

/**
 * A class to encapsulate a Key created from a Row or a PrimaryKey in a Table.
 * The algorithm is to iterate the target table's primary key adding fields and
 * static table ids as they are encountered, "left to right."  Addition of key
 * components ends when either (1) the entire primary key is added or (2)
 * a primary key field is missing from the Row.  In the latter case the
 * boolean allowPartial must be true allowing a partial primary key. If either
 * (1) or (2) is true, this.done is set to true.
 *
 * In the case of partial keys it is important to get as many key components
 * as possible to get the most specific parent key for the iteration possible.
 */
public class TableKey {
    final private ArrayList<String> major;
    final private ArrayList<String> minor;
    final private ArrayPosition pkIterator;
    final private Iterator<String> majorIterator;
    final private boolean allowPartial;
    final private RowSerializer row;
    final private TableImpl table;
    private Key key;
    private boolean majorComplete;
    private boolean keyComplete;
    private boolean done;
    private ArrayList<String> current;

    private TableKey(TableImpl table,
                     RowSerializer row,
                     boolean allowPartial) {
        this.row = row;
        this.allowPartial = allowPartial;
        major = new ArrayList<String>();
        minor = new ArrayList<String>();
        pkIterator = new ArrayPosition(table.getPrimaryKey().size());
        majorIterator = table.getShardKey().iterator();
        current = major;
        keyComplete = true;
        this.table = table;
    }

    /**
     * Create a Key for a record based on its value
     * (tableId, primaryKey)
     *
     * If record is empty this is a table iteration on a top-level
     * table -- validate that.
     */
    public static TableKey createKey(Table table, Row row,
                                     boolean allowPartial) {
        return createKeyInternal(table, (RowSerializer)row, allowPartial);
    }

    static TableKey createKeyInternal(Table table,
                                      RowSerializer row,
                                      boolean allowPartial) {
        if (row.size() == 0) {
            if (!allowPartial) {
                throw new IllegalArgumentException("Primary key is empty");
            }
            return new TableKey((TableImpl) table, row, allowPartial).create();
        }
        return new TableKey((TableImpl) table, row, allowPartial).create();
    }

    public TableKey create() {
        createPrimaryKey(table);
        key = Key.createKey(major, minor);
        return this;
    }

    TableImpl getTable() {
        return table;
    }

    public boolean getKeyComplete() {
        return keyComplete;
    }

    public boolean getMajorKeyComplete() {
        return majorComplete;
    }

    public Key getKey() {
        return key;
    }

    RowImpl getRow() {
        return (row instanceof RowImpl) ? (RowImpl)row : null;
    }

    public byte[] getKeyBytes() {
        return key.toByteArray();
    }

    /*
     * If the major key has been consumed, move to minor array.
     */
    private void incrementMajor() {
        if (majorIterator.hasNext()) {
            majorIterator.next();
        } else {
            current = minor;
            majorComplete = true;
        }
    }

    /*
     * Initialize the state of this object's major and minor lists
     * so that a Key can be created from them for use in table and
     * store operations.
     *
     * The key may be complete or partial (if allowPartial is true).
     * If partial the key fields are added "in order" left to right,
     * stopping when a missing field is encountered.
     */
    private void createPrimaryKey(TableImpl currentTable) {

        if (currentTable.getParent() != null) {
            createPrimaryKey((TableImpl) currentTable.getParent());
        }

        /*
         * If not done yet, add the table's static id and then continue
         * processing the pri key columns that apply to currentTable only.
         * Even if done is true, we must still get into the while-loop below
         * to make sure that there are not other prim key columns set into
         * this.row.
         */
        if (!done){
            current.add(currentTable.getIdString());
        }

        int lastPrimKeyCol = currentTable.getPrimaryKeySize() - 1;

        while (pkIterator.hasNext()) {

            int pos = pkIterator.next();

            /* The position within "row" of the current pk column */
            int pkFieldPos = (row.isPrimaryKey() ?
                              pos :
                              table.getPrimKeyPositions()[pos]);

            if (!done) {
                incrementMajor();
            }

            /*
             * Add the field to the appropriate key array.
             */
            FieldValueSerializer value = row.get(pkFieldPos);

            if (value != null) {

                if (!keyComplete) {
                    throw new IllegalArgumentException(
                        "A required field is missing from the Primary Key");
                }

                String keyStr = formatToKey(value,
                    currentTable.getPrimaryKeySize(pos));
                current.add(keyStr);
            } else {
                keyComplete = false;

                if (!allowPartial) {
                    String fname = table.getRowDef().getFieldName(pkFieldPos);
                    throw new IllegalArgumentException(
                        "Missing primary key field: " + fname);
                }

                /* TODO: check for complete keys for parent tables */
                done = true;

                /*
                 * Continue iterating over the remaining prim key columns to
                 * check that none of them is set.
                 */
            }

            /*
             * In the case of a parent table the parent's primary key will
             * run out before that of the target table.  Just consume the
             * fields that belong to the parent.
             */
            if (pos == lastPrimKeyCol) {
                if (!done) {
                    incrementMajor();
                }
                break;
            }
        }
    }

    /*
     * Return a String representation of the value suitable for use as part of
     * a primary key for the specified value.
     */
    private String formatToKey(FieldValueSerializer value, int keyLen) {

        FieldDefImpl def = (FieldDefImpl)value.getDefinition();
        switch (def.getType()) {
        case BOOLEAN:
            return BooleanValueImpl.toKeyString(value.getBoolean());
        case INTEGER:
            return IntegerValueImpl.toKeyString(value.getInt(), def, keyLen);
        case LONG:
            return LongValueImpl.toKeyString(value.getLong(), def);
        case STRING:
            return StringValueImpl.toKeyString(value.getString());
        case ENUM:
            return EnumValueImpl.toKeyString((EnumDefImpl)def,
                ((EnumDefImpl)def).indexOf(value.getEnumString()));
        case DOUBLE:
            return DoubleValueImpl.toKeyString(value.getDouble());
        case FLOAT:
            return FloatValueImpl.toKeyString(value.getFloat());
        case NUMBER:
            return NumberValueImpl.toKeyString(value.getNumberBytes());
        case TIMESTAMP:
            return TimestampValueImpl.toKeyString(value.getTimestampBytes());
        default:
            throw new IllegalArgumentException
            ("Invalid type for primary key " + def.getType());
        }
    }

    /**
     * Ensure that if the FieldRange is non-null that the field is
     * the "next" one after the last field specified in the primary key.
     *
     * The size of the PrimaryKey (row) should be identical to the index
     * of the field in the primary key list.  That is, if the field is
     * the 3rd field of the key, the first two should be present making
     * the size of the row 2 -- the same as the index of the 3rd field.
     */
    void validateFieldOrder(FieldRange range) {

        if (range != null) {
            List<String> primaryKey = table.getPrimaryKey();
            int index = primaryKey.indexOf(range.getFieldName());
            if (index < 0) {
                throw new IllegalArgumentException
                    ("Field is not part of primary key: " +
                     range.getFieldName());
            }
            if (row.size() < index) {
                throw new IllegalArgumentException
                    ("PrimaryKey is missing fields more significant than" +
                     " field: " + range.getFieldName());
            }
            if (row.size() > index) {
                throw new IllegalArgumentException
                    ("PrimaryKey has extra fields beyond" +
                     " field: " + range.getFieldName());
            }
        }
    }

    void validateFields() {
        if (row.isPrimaryKey()) {
            RecordValueImpl.validIndexFields(row, row.getClassNameForError());
        }
    }
}

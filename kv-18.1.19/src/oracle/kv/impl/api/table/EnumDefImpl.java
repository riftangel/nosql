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

import static oracle.kv.impl.api.table.TableJsonUtils.ENUM_NAME;
import static oracle.kv.impl.api.table.TableJsonUtils.ENUM_VALS;
import static oracle.kv.impl.api.table.TableJsonUtils.NAME;
import static oracle.kv.impl.api.table.TableJsonUtils.TYPE;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

@Persistent(version=1)
public class EnumDefImpl extends FieldDefImpl implements EnumDef {

    private static final long serialVersionUID = 1L;

    private final String[] values;

    /* AVRO requires names for records. */
    private String name;

    private transient int encodingLen;

    EnumDefImpl(final String[] values, final String description) {

        super(FieldDef.Type.ENUM, description);
        this.values = values;

        validate();
        init();
    }

    EnumDefImpl(
        final String name,
        final String[] values,
        final String description) {

        this(values, description);

        if (name == null) {
            throw new IllegalArgumentException
                ("Enumerations require a name");
        }

        this.name = name;
    }

    EnumDefImpl(final String name, String[] values) {
        this(name, values, null);
    }

    @SuppressWarnings("unused")
    private EnumDefImpl() {
        name = null;
        values = null;
    }

    private EnumDefImpl(EnumDefImpl impl) {
        super(impl);
        this.name = impl.name;
        values = Arrays.copyOf(impl.values, impl.values.length);
        encodingLen = impl.encodingLen;
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public EnumDefImpl clone() {
        return new EnumDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof EnumDefImpl) {
            EnumDefImpl otherDef = (EnumDefImpl) other;
            return Arrays.equals(values, otherDef.getValues());
        }
        return false;
    }

    @Override
    public boolean isValidKeyField() {
        return true;
    }

    @Override
    public boolean isValidIndexField() {
        return true;
    }

    @Override
    public EnumDef asEnum() {
        return this;
    }

    @Override
    public EnumValueImpl createEnum(String value) {
        return new EnumValueImpl(this, value);
    }

    /*
     * Public api methods from EnumDef
     */

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String[] getValues() {
        return values;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isEnum()) {
            return this.equals(superType);
        }

        if (superType.isAny() || superType.isAnyAtomic()) {
            return true;
        }

        return false;
    }

    @Override
    protected void toJson(ObjectNode node) {
        super.toJson(node);
        node.put(ENUM_NAME, name);
        ArrayNode enumVals = node.putArray(ENUM_VALS);
        for (String val : getValues()) {
            enumVals.add(val);
        }
    }

    /**
     * {
     *  "name": "xxx",
     *  "type": {
     *    "name" : "xxx",
     *    "type" : "enum",
     *    "symbols" " : [ "a", "b", ... , "z" ]
     *  }
     * }
     */
    @Override
    public JsonNode mapTypeToAvro(ObjectNode node) {
        if (node == null) {
            node = JsonUtils.createObjectNode();
        }
        node.put(NAME, name);
        node.put(TYPE, "enum");
        ArrayNode enumVals = node.putArray("symbols");
        for (String val : getValues()) {
            enumVals.add(val);
        }
        return node;
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isTextual()) {
            throw new IllegalArgumentException
                ("Default value for type ENUM is not a string");
        }
        return createEnum(node.asText());
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.TABLE_API_VERSION;
    }

    /*
     * local methods
     */

    public void setName(String n) {
        name = n;

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                "Enumeration types require a name");
        }
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        init();
    }

    private void init() {
        encodingLen = SortableString.encodingLength(values.length);
        if (encodingLen < 2) {
            encodingLen = 2;
        }
    }

    int indexOf(String enumValue) {
        for (int i = 0; i < values.length; i++) {
            if (enumValue.equals(values[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException
            ("Value is not valid for the enumeration: " + enumValue);
    }

    /**
     * TODO
     */
    int getEncodingLen() {
        return encodingLen;
    }

    /*
     * Simple value comparison function, used to avoid circular calls
     * between equals() and EnumValueImpl.equals().
     */
    public boolean valuesEqual(EnumDefImpl other) {
        return Arrays.equals(values, other.getValues());
    }

    /*
     * Make sure that the type definition is valid: Check for duplicate values
     * and validate the values of the enumeration strings themselves.
     */
    private void validate() {
        if (values == null || values.length < 1) {
            throw new IllegalArgumentException
                ("Enumerations requires one or more values");
        }

        HashSet<String> set = new HashSet<String>();
        for (String value: values) {
            validateStringValue(value);
            if (set.contains(value)) {
                throw new IllegalArgumentException
                    ("Duplicated enumeration value: " + value);
            }
            set.add(value);
        }
    }

    /**
     * Validates the value of the enumeration string.  The strings must
     * work for Avro schema, which means avoiding special characters.
     */
    private void  validateStringValue(String value) {
        if (!value.matches(TableImpl.VALID_NAME_CHAR_REGEX)) {
            throw new IllegalArgumentException
                ("Enumeration string names may contain only " +
                 "alphanumeric values plus the character \"_\": " + value);
        }
    }

    /*
     * Used when creating a value of this enum type, to check that the value
     * is one of the allowed ones.
     */
    void validateValue(String value) {
        for (String val : values) {
            if (val.equals(value)) {
                return;
            }
        }
        throw new IllegalArgumentException
            ("Invalid enumeration value '" + value +
             "', must be in values: " + Arrays.toString(values));
    }

    /**
     * Create the value represented by this index in the declaration
     */
    public EnumValueImpl createEnum(int index) {
        if (!isValidIndex(index)) {
            throw new IllegalArgumentException
                ("Index is out of range for enumeration: " + index);
        }
        return new EnumValueImpl(this, values[index]);
    }

    boolean isValidIndex(int index) {
        return (index < values.length);
    }

    /*
     * NOTE: this code is left here for future support of limited schema
     * evolution to add enumeration values.
     *
     * Enumeration values can only be added, and only at the end of the array.
     * This function ensures that this is done correctly.
    void addValue(String newValue) {
        if (SortableString.encodingLength(values.length + 1) >
            encodingLen) {
            throw new IllegalArgumentException
                ("Cannot add another enumeration value, too large: " +
                 values.length + 1);
        }
        String[] newArray = new String[values.length + 1];
        int i = 0;
        while (i < values.length) {
            newArray[i] = values[i++];
        }
        newArray[i] = newValue;
        values = newArray;
    }
    */
}

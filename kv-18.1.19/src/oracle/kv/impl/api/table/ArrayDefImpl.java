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

import static oracle.kv.impl.api.table.TableJsonUtils.COLLECTION;

import oracle.kv.impl.util.JsonUtils;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.FieldDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * ArrayDefImpl implements the ArrayDef interface.
 */
@Persistent(version=1)
public class ArrayDefImpl extends FieldDefImpl implements ArrayDef {

    private static final long serialVersionUID = 1L;

    private final FieldDefImpl element;

    ArrayDefImpl(FieldDefImpl element, String description) {

        super(FieldDef.Type.ARRAY, description);
        if (element == null) {
            throw new IllegalArgumentException
                ("Array has no field and cannot be built");
        }

        this.element = element;
    }

    /**
     * This constructor is only used by test code.
     */
    ArrayDefImpl(FieldDefImpl element) {
        this(element, null);
    }

    private ArrayDefImpl(ArrayDefImpl impl) {
        super(impl);
        element = impl.element.clone();
    }

    @SuppressWarnings("unused")
    private ArrayDefImpl() {
        element = null;
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public ArrayDefImpl clone() {

        if (this == FieldDefImpl.arrayAnyDef ||
            this == FieldDefImpl.arrayJsonDef) {
            return this;
        }

        return new ArrayDefImpl(this);
    }

    @Override
    public int hashCode() {
        return element.hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other instanceof ArrayDefImpl) {
            return element.equals(((ArrayDefImpl)other).getElement());
        }
        return false;
    }

    @Override
    public ArrayDef asArray() {
        return this;
    }

    @Override
    public ArrayValueImpl createArray() {
        return new ArrayValueImpl(this);
    }

    /*
     * Public api methods from ArrayDef
     */

    @Override
    public FieldDefImpl getElement() {
        return element;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return element.isPrecise();
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (this == superType) {
            return true;
        }

        if (superType.isArray()) {
            ArrayDefImpl supArray = (ArrayDefImpl)superType;
            return element.isSubtype(supArray.element);
        }

        if (superType.isJson()) {
            return element.isSubtype(jsonDef);
        }

        if (superType.isAny()) {
             return true;
        }

        return false;
    }

    /**
     * If called for an array the fieldName applies to a field in the array's
     * element, so pass it on.
     */
    @Override
    FieldDefImpl findField(String fieldName) {
        return element.findField(fieldName);
    }

    @Override
    void toJson(ObjectNode node) {
        super.toJson(node);
        ObjectNode collNode = node.putObject(COLLECTION);
        if (element != null) {
            element.toJson(collNode);
        }
    }

    /**
     * {
     *  "type": {
     *    "type" : "array",
     *    "items" : "simpleType"  or for a complex type
     *    "items" : {
     *        "type" : ...
     *        ...
     *     }
     *  }
     * }
     */
    @Override
    public JsonNode mapTypeToAvro(ObjectNode node) {
        if (node == null) {
            node = JsonUtils.createObjectNode();
        }
        node.put("type", "array");
        node.put("items", element.mapTypeToAvroJsonNode());
        return node;
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isArray()) {
            throw new IllegalArgumentException
                ("Default value for type ARRAY is not an array");
        }
        if (node.size() != 0) {
            throw new IllegalArgumentException
                ("Default value for array must be null or an empty array");
        }
        return createArray();
    }

    @Override
    public short getRequiredSerialVersion() {
        return element.getRequiredSerialVersion();
    }

    @Override
    int countTypes() {
        return element.countTypes() + 1; /* +1 for this field */
    }
}

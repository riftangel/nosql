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

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.BooleanDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;

/**
 * BooleanDefImpl implements the BooleanDef interface.
 */
@Persistent(version=1)
public class BooleanDefImpl extends FieldDefImpl implements BooleanDef {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor requiring all fields.
     */
    BooleanDefImpl(final String description) {
        super(Type.BOOLEAN, description);
    }

    /**
     * This constructor defaults most fields.
     */
    BooleanDefImpl() {
        super(Type.BOOLEAN);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public BooleanDefImpl clone() {
        return FieldDefImpl.booleanDef;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof BooleanDefImpl;
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
    public BooleanDef asBoolean() {
        return this;
    }

    @Override
    public BooleanValueImpl createBoolean(boolean value) {
        return BooleanValueImpl.create(value);
    }

    @Override
    BooleanValueImpl createBoolean(String value) {
        return BooleanValueImpl.create(value);
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.TABLE_API_VERSION;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isBoolean() ||
            superType.isAny() ||
            superType.isAnyAtomic() ||
            superType.isAnyJsonAtomic() ||
            superType.isJson()) {
            return true;
        }
        return false;
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isBoolean()) {
            throw new IllegalArgumentException
                ("Default value for type BOOLEAN is not boolean");
        }
        return createBoolean(node.getBooleanValue());
    }
}

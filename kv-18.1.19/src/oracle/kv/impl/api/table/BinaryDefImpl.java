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

import java.io.IOException;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.BinaryDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

/**
 * BinaryDefImpl implements the BinaryDef interface.
 */
@Persistent(version=1)
public class BinaryDefImpl extends FieldDefImpl implements BinaryDef {

    private static final long serialVersionUID = 1L;

    BinaryDefImpl(String description) {
        super(Type.BINARY, description);
    }

    BinaryDefImpl() {
        super(Type.BINARY);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public BinaryDefImpl clone() {
        return FieldDefImpl.binaryDef;
    }

    @Override
    public boolean equals(Object other) {

        return (other instanceof BinaryDefImpl);
    }

    @Override
    public BinaryDef asBinary() {
        return this;
    }

    @Override
    public BinaryValueImpl createBinary(byte[] value) {
        return new BinaryValueImpl(value);
    }

    /*
     * Public api methods from BinaryDef
     */

    @Override
    public BinaryValueImpl fromString(String base64) {
        TextNode n = new TextNode(base64);
        try {
            return createBinary(n.getBinaryValue());
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                ("Cannot create binary from string: " + base64, ioe);
        }
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isBinary() ||
            superType.isAny() ||
            superType.isAnyAtomic()) {
            return true;
        }
        return false;
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isBinary() && !node.isTextual()) {
            throw new IllegalArgumentException
                ("Default value for type BINARY is not binary or text");
        }
        try {
            final byte[] bytes;
            if (node.isBinary()) {
                bytes = node.getBinaryValue();
            } else {
                assert (node.isTextual());
                String str = node.getTextValue();
                bytes = TableJsonUtils.decodeBase64(str);
            }
            return createBinary(bytes);
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                ("IOException creating binary value: " + ioe, ioe);
        }
    }

    @Override
    public short getRequiredSerialVersion() {
        return SerialVersion.TABLE_API_VERSION;
    }
}

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

import static oracle.kv.impl.api.table.TableJsonUtils.FIXED;
import static oracle.kv.impl.api.table.TableJsonUtils.FIXED_SIZE;
import static oracle.kv.impl.api.table.TableJsonUtils.NAME;
import static oracle.kv.impl.api.table.TableJsonUtils.TYPE;

import java.io.IOException;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.FixedBinaryDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

/**
 * FixedBinaryDefImpl implements the FixedBinaryDef interface.
 */
@Persistent(version=1)
public class FixedBinaryDefImpl extends FieldDefImpl implements FixedBinaryDef {

    private static final long serialVersionUID = 1L;

    /* AVRO requires names for records. */
    private String name;

    private final int size;

    FixedBinaryDefImpl(int size, String description) {

        super(Type.FIXED_BINARY, description);
        this.size = size;

        validate();
    }

    FixedBinaryDefImpl(String name, int size, String description) {

        this(size, description);

        if (name == null) {
            throw new IllegalArgumentException
                ("FixedBinaryDef requires a name");
        }

        this.name = name;
    }

    FixedBinaryDefImpl(String name, int size) {
        this(name, size, null);
    }

    /* for persistence */
    @SuppressWarnings("unused")
    private FixedBinaryDefImpl() {
        super(Type.BINARY);
        size = 0;
        name = null;
    }

    private FixedBinaryDefImpl(FixedBinaryDefImpl impl) {
        super(impl);
        this.name = impl.name;
        this.size = impl.size;
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public FixedBinaryDefImpl clone() {
        return new FixedBinaryDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + size + name.hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof FixedBinaryDefImpl) {
            FixedBinaryDefImpl otherDef = (FixedBinaryDefImpl) other;
            return (size == otherDef.size);
        }
        return false;
    }

    @Override
    public FixedBinaryDef asFixedBinary() {
        return this;
    }

    @Override
    public FixedBinaryValueImpl createFixedBinary(byte[] value) {
        validateValue(value);
        return new FixedBinaryValueImpl(value, this);
    }

    /*
     * Public api methods from FixedBinaryDef
     */

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FixedBinaryValueImpl fromString(String base64) {
        TextNode n = new TextNode(base64);
        try {
            return createFixedBinary(n.getBinaryValue());
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

        if (superType.isFixedBinary()) {
            return this.equals(superType);
        }

        if (superType.isBinary() ||
            superType.isAny() ||
            superType.isAnyAtomic()) {
            return true;
        }

        return false;
    }

    @Override
    void toJson(ObjectNode node) {
        super.toJson(node);
        node.put(FIXED_SIZE, size);
        node.put(NAME, name);
    }

    /*
     * This method needs to be overridden because this calls can generate
     * either BYTES or FIXED for the Avro type.
     */
    @Override
    public JsonNode mapTypeToAvro(ObjectNode node) {
        if (node == null) { /* can this happen ? */
            node = JsonUtils.createObjectNode();
        }
        node.put(TYPE, FIXED);
        node.put(NAME, name);
        node.put(FIXED_SIZE, size);
        return node;
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isBinary() && !node.isTextual()) {
            throw new IllegalArgumentException
                ("Default value for type FIXED_BINARY is not binary or text");
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
            if (bytes.length != size) {
                throw new IllegalArgumentException
                    ("Illegal size for FIXED_BINARY: " + bytes.length +
                     ", must be " + size);
            }
            return createFixedBinary(bytes);
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                ("IOException creating fixed binary value: " + ioe, ioe);
        }
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
                "Fixed binary types require a name");
        }
    }

    private void validate() {
        if (size <= 0) {
            throw new IllegalArgumentException
                ("FixedBinaryDef size limit must be a positive integer");
        }
    }

    private void validateValue(byte[] value) {
        if (value.length != size) {
            throw new IllegalArgumentException
                ("Invalid length for FixedBinary array, it must be " + size +
                 ", and it is " + value.length);
        }
    }
}

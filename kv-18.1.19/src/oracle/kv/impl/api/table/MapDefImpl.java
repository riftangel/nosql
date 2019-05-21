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
import oracle.kv.table.FieldDef;
import oracle.kv.table.MapDef;
import com.sleepycat.persist.model.Persistent;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * MapDefImpl implements the MapDef interface.
 */
@Persistent(version=1)
public class MapDefImpl extends FieldDefImpl implements MapDef {

    private static final long serialVersionUID = 1L;

    private final FieldDefImpl element;

    MapDefImpl(FieldDefImpl element, String description) {

        super(FieldDef.Type.MAP, description);
        if (element == null) {
            throw new IllegalArgumentException
                ("Map has no field and cannot be built");
        }
        this.element = element;
    }

   MapDefImpl(FieldDefImpl element) {
       this(element, null);
   }

    private MapDefImpl(MapDefImpl impl) {
        super(impl);
        element = impl.element.clone();
    }

    /**
     * For DPL
     */
    @SuppressWarnings("unused")
    private MapDefImpl() {
        element = null;
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public MapDefImpl clone() {

        if (this == FieldDefImpl.mapAnyDef ||
            this == FieldDefImpl.mapJsonDef) {
            return this;
        }

        return new MapDefImpl(this);
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

        if (other instanceof MapDefImpl) {
            return element.equals(((MapDefImpl)other).getElement());
        }
        return false;
    }

    @Override
    public MapDef asMap() {
        return this;
    }

    @Override
    public MapValueImpl createMap() {
        return new MapValueImpl(this);
    }

    /*
     * Public api methods from MapDef
     */

    @Override
    public FieldDefImpl getElement() {
        return element;
    }

    @Override
    public FieldDef getKeyDefinition() {
        return FieldDefImpl.stringDef;
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

        if (superType.isMap()) {
            MapDefImpl supMap = (MapDefImpl)superType;
            return element.isSubtype(supMap.element);
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
     * If called for a map the fieldName applies to the key that is being
     * indexed in the map, so the target is the map's element.
     */
    @Override
    FieldDefImpl findField(String fieldName) {
        return element;
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
     *    "type" : "map",
     *    "values" : "simpleType"  or for a complex type
     *    "values" : {
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

        node.put("type", "map");
        node.put("values", element.mapTypeToAvroJsonNode());
        return node;
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isObject()) {
            throw new IllegalArgumentException
                ("Default value for type MAP is not a map");
        }
        if (node.size() != 0) {
            throw new IllegalArgumentException
                ("Default value for map must be null or an empty map");
        }
        return createMap();
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

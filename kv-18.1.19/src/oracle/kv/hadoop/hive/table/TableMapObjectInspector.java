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

import java.util.Map;

import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;

/**
 * The Hive ObjectInspector that is used to translate KVStore row fields
 * of type FieldDef.Type.MAP to Hive column type MAP.
 */
public class TableMapObjectInspector extends StandardMapObjectInspector {

    /*
     * Note: with respect to the 'get' methods defined by this ObjectInspector,
     * when the Hive infrastructure invokes those methods during a given
     * query, the data Object input to those methods may be an instance of
     * FieldValue (specifically, a MapValue) or may be an instance of the
     * corresponding Java class (that is, a Map). As a result, each such
     * method must be prepared to handle both cases.
     *
     * With respect to the 'create/set' methods, this class defaults to
     * the 'create/set' methods provided by the StandardMapObjectInspector
     * parent class; which always return a Java Map, instead of a MapValue.
     */

    TableMapObjectInspector(ObjectInspector mapKeyObjectInspector,
                            ObjectInspector mapValueObjectInspector) {

        super(mapKeyObjectInspector, mapValueObjectInspector);
    }

    @Override
    public Object getMapValueElement(Object data, Object key) {

        if (data == null || key == null) {
            return null;
        }

        if (data instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) data;
            return map.get(key);
        } else if (data instanceof FieldValue) {
            if (((FieldValue) data).isNull()) {
                return null;
            }
            final MapValue mapValue = ((FieldValue) data).asMap();
            return mapValue.get((String) key);
        }
        throw new IllegalArgumentException(
                      "object is not of type Map or type FieldValue");
    }

    @Override
    public int getMapSize(Object data) {

        if (data == null) {
            return -1;
        }

        if (data instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) data;
            return map.size();
        } else if (data instanceof FieldValue) {
            if (((FieldValue) data).isNull()) {
                return 0;
            }
            final MapValue mapValue = ((FieldValue) data).asMap();
            return mapValue.size();
        }
        throw new IllegalArgumentException(
                      "object is not of type Map or type FieldValue");
    }

    @Override
    public Map<?, ?> getMap(Object data) {

        if (data == null) {
            return null;
        }

        if (data instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) data;
            return map;
        } else if (data instanceof FieldValue) {
            if (((FieldValue) data).isNull()) {
                return null;
            }
            final MapValue mapValue = ((FieldValue) data).asMap();
            return mapValue.getFields();
        }
        throw new IllegalArgumentException(
                      "object is not of type Map or type FieldValue");
    }
}

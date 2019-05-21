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

import java.util.List;
import java.util.Map;

import oracle.kv.table.FieldDef;

/**
 * A TableChange to add an index.
 */
class AddIndex extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String description;
    private final String tableName;
    private final String namespace;
    private final List<String> fields;
    private final List<FieldDef.Type> types;
    private final Map<String,String> annotations;
    private final Map<String,String> properties;

    AddIndex(String namespace,
             String indexName,
             String tableName,
             List<String> fields,
             List<FieldDef.Type> types,
             String description,
             int seqNum) {

        this(namespace, indexName, tableName, fields, types,
             null, null, description, seqNum);
    }

    AddIndex(String namespace,
             String indexName,
             String tableName,
             List<String> fields,
             Map<String,String> annotations,
             Map<String,String> properties,
             String description,
             int seqNum) {

        this(namespace, indexName, tableName, fields, null,
             annotations, properties, description, seqNum);
    }

    private AddIndex(String namespace,
                     String indexName,
                     String tableName,
                     List<String> fields,
                     List<FieldDef.Type> types,
                     Map<String,String> annotations,
                     Map<String,String> properties,
                     String description,
                     int seqNum) {

        super(seqNum);
        name = indexName;
        this.description = description;
        this.tableName = tableName;
        this.namespace = namespace;
        this.fields = fields;
        this.types = types;
        this.annotations = annotations;
        this.properties = properties;
    }

    @Override
    boolean apply(TableMetadata md) {
        if (annotations == null) {
            md.insertIndex(namespace, name, tableName,
                           fields, types, description);
        } else {
            md.insertTextIndex
                (namespace, name, tableName, fields,
                 annotations, properties, description);
        }
        return true;
    }
}

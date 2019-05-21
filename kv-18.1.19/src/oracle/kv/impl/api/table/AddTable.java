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

import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.table.TimeToLive;

/**
 * A TableChange to create/add a new table
 */
class AddTable extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String parentName;
    private final String namespace;
    private final List<String> primaryKey;
    private final List<Integer> primaryKeySizes;
    private final List<String> shardKey;
    private final FieldMap fields;
    private final TimeToLive ttl;
    private final boolean r2compat;
    private final int schemaId;
    private final String description;
    private final ResourceOwner owner;
    private final boolean sysTable;
    private final TableLimits limits;

    AddTable(TableImpl table, int seqNum) {
        super(seqNum);
        name = table.getName();
        namespace = table.getNamespace();
        final TableImpl parent = (TableImpl) table.getParent();
        parentName = (parent == null) ? null : parent.getFullName();
        primaryKey = table.getPrimaryKey();
        primaryKeySizes = table.getPrimaryKeySizes();
        shardKey = table.getShardKey();
        fields = table.getFieldMap();
        ttl = table.getDefaultTTL();
        r2compat = table.isR2compatible();
        schemaId = table.getSchemaId();
        description = table.getDescription();
        owner = table.getOwner();
        sysTable = table.isSystemTable();
        limits = (parent == null) ? table.getTableLimits() : null;
    }

    @Override
    public boolean apply(TableMetadata md) {
        md.insertTable(namespace, name, parentName,
                       primaryKey, primaryKeySizes, shardKey, fields,
                       ttl, limits, r2compat, schemaId, description, owner,
                       sysTable);
        return true;
    }
}

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

import oracle.kv.table.TimeToLive;


/**
 * A TableChange that evolves an existing table.
 */
class EvolveTable extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final String namespace;
    private final FieldMap fields;
    private final TimeToLive ttl;
    private final String description;

    EvolveTable(TableImpl table, int seqNum) {
        super(seqNum);
        tableName = table.getFullName();
        namespace = table.getNamespace();
        fields = table.getFieldMap();
        ttl = table.getDefaultTTL();
        description = table.getDescription();
    }

    @Override
    public boolean apply(TableMetadata md) {
        md.evolveTable(namespace, tableName, fields, ttl, description);
        return true;
    }
}

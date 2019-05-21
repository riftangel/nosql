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

/**
 * A change to table limits.
 */
class TableLimit extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String tableName;
    private final TableLimits limits;

    TableLimit(TableImpl table, int seqNum) {
        super(seqNum);
        assert table.isTop();
        tableName = table.getFullName();
        namespace = table.getNamespace();
        limits = table.getTableLimits();
    }

    @Override
    public boolean apply(TableMetadata md) {
        final TableImpl table = md.getTable(namespace, tableName, true);
        if (table == null) {
            return false;
        }
        table.setTableLimits(limits);
        return true;
    }
}

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
 * Removes a table (or marks it for deletion).
 */
class DropTable extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final String namespace;
    private final boolean markForDelete;

    DropTable(String namespace, String tableName,
              boolean markForDelete, int seqNum) {
        super(seqNum);
        this.tableName = tableName;
        this.namespace = namespace;
        this.markForDelete = markForDelete;
    }

    @Override
    public boolean apply(TableMetadata md) {
        md.removeTable(namespace, tableName, markForDelete);
        return true;
    }
}

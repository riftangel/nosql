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
 *
 */
class UpdateIndexStatus extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String indexName;
    private final String tableName;
    private final String namespace;
    private final IndexImpl.IndexStatus status;

    UpdateIndexStatus(IndexImpl index, int seqNum) {
        super(seqNum);
        indexName = index.getName();
        tableName = index.getTable().getFullName();
        namespace = index.getTable().getNamespace();
        status = index.getStatus();
    }

    @Override
    public boolean apply(TableMetadata md) {
        md.changeIndexStatus(namespace, indexName, tableName, status);
        return true;
    }
}

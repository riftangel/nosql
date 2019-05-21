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

package oracle.kv.impl.admin.plan;

import java.util.List;
import java.util.Set;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.ResourceOwner;

/**
 * Remove table plan supporting table privileges automatic cleanup.
 */
public class RemoveTablePlanV2 extends MultiMetadataPlan {

    private static final long serialVersionUID = 1L;

    private final ResourceOwner tableOwner;
    private final long tableId;
    private final String tableFullName;
    private final boolean toRemoveIndex;

    protected RemoveTablePlanV2(String planName,
                                String namespace,
                                String tableName,
                                Planner planner) {
        super(planName, planner);

        final TableImpl table = TablePlanGenerator.
            getAndCheckTable(namespace, tableName, getTableMetadata());

        tableOwner = table.getOwner();
        tableId = table.getId();
        tableFullName = table.getFullName();
        toRemoveIndex = !table.getIndexes().isEmpty();
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableFullName() {
        return tableFullName;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        return TablePlanGenerator.
            getRemoveTableRequiredPrivs(tableOwner, toRemoveIndex, tableId);
    }

    @Override
    protected Set<MetadataType> getMetadataTypes() {
        return TABLE_SECURITY_TYPES;
    }
}

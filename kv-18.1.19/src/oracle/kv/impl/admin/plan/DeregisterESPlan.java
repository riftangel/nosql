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

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.UpdateESConnectionInfo;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;

/**
 * A plan for informing the store of the existence of an Elasticsearch node.
  */
public class DeregisterESPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    public DeregisterESPlan(String name, Planner planner) {
        super(name, planner);

        final Admin admin = getAdmin();
        final Parameters p = admin.getCurrentParameters();

        verifyNoTextIndexes(admin);

        /*
         *  update the ES connection info list on each SNA.
         */
        int taskCount = 0;
        for (StorageNodeParams snaParams : p.getStorageNodeParams()) {
            if (! ("" == snaParams.getSearchClusterName())) {
                taskCount++;
                addTask(new UpdateESConnectionInfo
                        (this, snaParams.getStorageNodeId(), "", "",false));
            }
        }
        if (taskCount == 0) {
            throw new IllegalCommandException
                ("No ES cluster is currently registered with the store.");
        }
    }

    private void verifyNoTextIndexes(Admin admin) {
        final TableMetadata tableMd = admin.getMetadata(TableMetadata.class,
                                                        MetadataType.TABLE);
        if (tableMd == null) {
            return;
        }

        final Set<String> indexnames = tableMd.getTextIndexNames();
        if (indexnames.isEmpty()) {
            return;
        }

        final StringBuilder sb = new StringBuilder
            ("Cannot deregister ES because these text indexes exist:");
        String eol = System.getProperty("line.separator");
        for (String s : indexnames) {
            sb.append(eol);
            sb.append(s);
        }

        throw new IllegalCommandException(sb.toString());
    }

    @Override
    public void preExecutionSave() {
    }

    @Override
    public boolean isExclusive() {
        return true;
    }

    @Override
    public String getDefaultName() {
        return "Deregister Elasticsearch cluster";
    }

    @Override
	public void stripForDisplay() {
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}

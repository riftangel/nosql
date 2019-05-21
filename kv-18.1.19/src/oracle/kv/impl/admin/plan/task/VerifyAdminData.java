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

package oracle.kv.impl.admin.plan.task;

import java.util.Set;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.VerifyDataPlan;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task that verify the btree and log files of databases
 * for admins.
 *
 */
public class VerifyAdminData extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    private VerifyDataPlan plan;
    private Set<StorageNodeId> targetSNIds;
    private boolean verifyIndex;
    private boolean verifyRecord;
    private long btreeDelay;
    private long logDelay;
    private boolean verifyBtree;
    private boolean verifyLog;

    public VerifyAdminData(VerifyDataPlan plan,
                           Set<StorageNodeId> targetSNIds,
                           boolean verifyBtree,
                           boolean verifyLog,
                           boolean verifyIndex,
                           boolean verifyRecord,
                           long btreeDelay,
                           long logDelay) {
        super();
        this.plan = plan;
        this.targetSNIds = targetSNIds;
        this.verifyIndex = verifyIndex;
        this.verifyRecord = verifyRecord;
        this.btreeDelay = btreeDelay;
        this.logDelay = logDelay;
        this.verifyBtree = verifyBtree;
        this.verifyLog = verifyLog;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public State doWork() throws Exception {
        Admin admin = plan.getAdmin();
        for (StorageNodeId targetSNId : targetSNIds) {
            StorageNodeParams snParam = admin.getStorageNodeParams(targetSNId);
            CommandServiceAPI cs = RegistryUtils.
                getAdmin(snParam.getHostname(), snParam.getRegistryPort(),
                         plan.getLoginManager());

            cs.verifyData(verifyBtree, verifyLog, verifyIndex, verifyRecord,
                          btreeDelay, logDelay);

        }

        return State.SUCCEEDED;

    }

    @Override
    protected Plan getPlan() {
        return plan;
    }

}

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

import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.VerifyDataPlan;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task that verify the btree and log files of databases
 * for RNs.
 *
 */
public class VerifyRNData extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private VerifyDataPlan plan;
    private Set<RepNodeId> targetRnIds;
    private boolean verifyIndex;
    private boolean verifyRecord;
    private long btreeDelay;
    private long logDelay;
    private boolean verifyBtree;
    private boolean verifyLog;

    public VerifyRNData(VerifyDataPlan plan,
                        Set<RepNodeId> targetRnIds,
                        boolean verifyBtree,
                        boolean verifyLog,
                        boolean verifyIndex,
                        boolean verifyRecord,
                        long btreeDelay,
                        long logDelay) {
        super();
        this.plan = plan;
        this.targetRnIds = targetRnIds;
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
        RegistryUtils registry =
            new RegistryUtils(plan.getAdmin().getCurrentTopology(),
                              plan.getAdmin().getLoginManager());
        for (RepNodeId targetRnId : targetRnIds) {
            RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(targetRnId);

            rnAdmin.verifyData(verifyBtree, verifyLog, verifyIndex,
                               verifyRecord, btreeDelay, logDelay);

        }

        return State.SUCCEEDED;
    }

    @Override
    protected Plan getPlan() {
        return plan;
    }

}

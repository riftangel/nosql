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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.VerifyAdminData;
import oracle.kv.impl.admin.plan.task.VerifyRNData;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.SerialVersion;

/**
 * A plan that verifies either the btree, or log files, or both of databases.
 * For btree verification, it can verify primary tables and indexes. Also, it
 * can verify data records in disk. The verification can be ran on selected
 * admins or/and rns.
 */
public class VerifyDataPlan extends AbstractPlan {
    private static final long serialVersionUID = 1L;
    public volatile static TestHook<Object[]> VERIFY_HOOK;

    public VerifyDataPlan(String name,
                          Planner planner,
                          Set<AdminId> allAdmins,
                          Map<Integer, Set<RepNodeId>> allRns,
                          boolean verifyBtree,
                          boolean verifyLog,
                          boolean verifyIndex,
                          boolean verifyRecord,
                          long btreeDelay,
                          long logDelay) {
        super(name, planner);
        Admin admin = planner.getAdmin();
        KVVersion storeVersion = admin.getStoreVersion();
        KVVersion expectVersion = SerialVersion.
            getKVVersion(SerialVersion.VERIFY_DATA_VERSION);
        if (storeVersion.compareTo(expectVersion) < 0) {
            throw new IllegalCommandException(
                "Store version is not capable " +
                "of executing plan. Required version is " +
                expectVersion.getNumericVersionString() +
                ", store version is " +
                storeVersion.getNumericVersionString());
        }
        ParallelBundle parallelTasks = new ParallelBundle();

        if (allAdmins != null) {
            Parameters parameters = admin.getCurrentParameters();
            Set<StorageNodeId> allSnIds = new HashSet<StorageNodeId>();
            for (AdminId aid : allAdmins) {
                AdminParams current = parameters.get(aid);
                StorageNodeId snid = current.getStorageNodeId();
                allSnIds.add(snid);
            }
            /* run verifications on all admins serially. */
            parallelTasks.addTask(new VerifyAdminData(this, allSnIds,
                                                      verifyBtree, verifyLog,
                                                      verifyIndex, verifyRecord,
                                                      btreeDelay, logDelay));

        }

        if (allRns != null) {
            for (Set<RepNodeId> rnids : allRns.values()) {
                if (rnids != null) {
                    /*
                     * run verifications on RNs within the same shard serially
                     */
                    parallelTasks.addTask(new VerifyRNData(this, rnids,
                                                           verifyBtree,
                                                           verifyLog,
                                                           verifyIndex,
                                                           verifyRecord,
                                                           btreeDelay,
                                                           logDelay));
                }
            }
        }

        addTask(parallelTasks);
    }

    @Override
    public String getDefaultName() {
        return "Verify Data";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    void preExecutionSave() {
        /* nothing to do before execution */
    }

}

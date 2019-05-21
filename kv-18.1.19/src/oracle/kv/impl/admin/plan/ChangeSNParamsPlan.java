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

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.StartNode;
import oracle.kv.impl.admin.plan.task.StopNode;
import oracle.kv.impl.admin.plan.task.WaitForNodeState;
import oracle.kv.impl.admin.plan.task.WriteNewParams;
import oracle.kv.impl.admin.plan.task.WriteNewSNParams;
import oracle.kv.impl.mgmt.MgmtUtil;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class ChangeSNParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    public static final KVVersion STORAGE_DIR_SIZE_SUPPORT = KVVersion.R4_2;

    protected ParameterMap newParams;

    public ChangeSNParamsPlan(String name,
                              Planner planner,
                              StorageNodeId snId,
                              ParameterMap newParams) {

        super(name, planner);

        this.newParams = newParams;

        /**
         * Set correct storage node id because this is going to be stored.
         */
        newParams.setParameter(ParameterState.COMMON_SN_ID,
                               Integer.toString(snId.getStorageNodeId()));

        validateParams(planner, snId);
        addTask(new WriteNewSNParams(this, snId, newParams, false));

        /*
         * If we have changed the capacity, file system percentage, memory
         * setting or numCPUS of this SN, we may have to change the params
         * for any RNs on this SN.  Also check for storage directory size
         * changes.
         */
        if (newParams.exists(ParameterState.COMMON_MEMORY_MB) ||
            newParams.exists(ParameterState.SN_RN_HEAP_PERCENT) ||
            newParams.exists(ParameterState.COMMON_CAPACITY) ||
            newParams.exists(ParameterState.COMMON_NUMCPUS) ||
            newParams.exists(ParameterState.SN_ROOT_DIR_SIZE)) {
            updateRNParams(snId, newParams);
        } else if (newParams.getName().
                      equals(ParameterState.BOOTSTRAP_MOUNT_POINTS)) {
            updateRNStorageDirs(snId, newParams);
            /*
             * TODO : Yet to add support for changing
             * BOOTSTRAP_ADMIN_MOUNT POINTS and BOOTSTRAP_RNLOG_MOUNT_POINTS
             */
        }

        /*
         * This is a no-restart plan at this time, we are done.
         */
    }

    /* DPL */
    protected ChangeSNParamsPlan() {
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    void preExecutionSave() {
       /* Nothing to save before execution. */
    }

    @Override
    public String getDefaultName() {
        return "Change Storage Node Params";
    }

    private void validateParams(Planner p, StorageNodeId snId) {
        if (newParams.getName().
            equals(ParameterState.BOOTSTRAP_MOUNT_POINTS)) {
            Admin admin = p.getAdmin();
            Parameters parameters = admin.getCurrentParameters();
            /*
             * Will throw an IllegalCommandException if a storage directory
             * is in use.
             */
            StorageNodeParams.validateStorageDirMap(newParams, parameters,snId);
        }
        if (newParams.getName().equals(ParameterState.SNA_TYPE)) {
            String error = validateMgmtParams(newParams);
            if (error != null) {
                throw new IllegalCommandException(error);
            }
            /* Let the StorageNodeParams class validate */
            new StorageNodeParams(newParams).validate();
        }
    }

    /**
     * Return a non-null error message if incorrect mgmt param values are
     * present.
     */
    public static String validateMgmtParams(ParameterMap aParams) {
        if (!aParams.exists(ParameterState.COMMON_MGMT_CLASS)) {
            return null;
        }
        Parameter mgmtClass
            = aParams.get(ParameterState.COMMON_MGMT_CLASS);
        if (! MgmtUtil.verifyImplClassName(mgmtClass.asString())) {
            return
                ("The given value " + mgmtClass.asString() +
                 " is not allowed for the parameter " +
                 mgmtClass.getName());
        }
        return null;
    }

    public StorageNodeParams getNewParams() {
        return new StorageNodeParams(newParams);
    }

    /**
     * Generate tasks to update the JE cache size or JVM args for any RNS on
     * this SN.
     */
    private void updateRNParams(StorageNodeId snId,
                                ParameterMap newMap) {

        Admin admin = planner.getAdmin();
        StorageNodeParams snp = admin.getStorageNodeParams(snId);
        ParameterMap policyMap = admin.getCurrentParameters().copyPolicies();

        /* Find the capacity value to use */
        int capacity = snp.getCapacity();
        if (newMap.exists(ParameterState.COMMON_CAPACITY)) {
            capacity = newMap.get(ParameterState.COMMON_CAPACITY).asInt();
        }

        /*
         * Find the number of RNs hosted on this SN; that affects whether we
         * modify the heap value.
         */
        final int numHostedRNs =
            admin.getCurrentTopology().getHostedRepNodeIds(snId).size();

        final int numHostedANs =
                admin.getCurrentTopology().getHostedArbNodeIds(snId).size();

        /* Find the RN heap memory percent to use */
        int rnHeapPercent = snp.getRNHeapPercent();
        if (newMap.exists(ParameterState.SN_RN_HEAP_PERCENT)) {
            rnHeapPercent =
                newMap.get(ParameterState.SN_RN_HEAP_PERCENT).asInt();
        }

        /* Find the memory mb value to use */
        int memoryMB = snp.getMemoryMB();
        if (newMap.exists(ParameterState.COMMON_MEMORY_MB)) {
            memoryMB = newMap.get(ParameterState.COMMON_MEMORY_MB).asInt();
        }

        /* Find the numCPUs value to use */
        int numCPUs = snp.getNumCPUs();
        if (newMap.exists(ParameterState.COMMON_NUMCPUS)) {
            numCPUs = newMap.get(ParameterState.COMMON_NUMCPUS).asInt();
        }

        Parameter newRootSize = snp.getMap().
                                    get(ParameterState.SN_ROOT_DIR_SIZE);
        if (newParams.exists(ParameterState.SN_ROOT_DIR_SIZE)) {
            newRootSize = newMap.get(ParameterState.SN_ROOT_DIR_SIZE);
        }

        /* Find the -XX:ParallelGCThread flag to use */
        int gcThreads = StorageNodeParams.calcGCThreads
            (numCPUs, capacity, snp.getGCThreadFloor(),
             snp.getGCThreadThreshold(), snp.getGCThreadPercent());

        for (RepNodeParams rnp :
                 admin.getCurrentParameters().getRepNodeParams()) {
            if (!rnp.getStorageNodeId().equals(snId)) {
                continue;
            }

            RNHeapAndCacheSize heapAndCache =
                StorageNodeParams.calculateRNHeapAndCache
                (policyMap, capacity, numHostedRNs, memoryMB, rnHeapPercent,
                 rnp.getRNCachePercent(), numHostedANs);
            ParameterMap rnMap = new ParameterMap(ParameterState.REPNODE_TYPE,
                                                  ParameterState.REPNODE_TYPE);

            /*
             * Hang onto the current JVM params in a local variable. We may
             * be making multiple changes to them, if we change both heap and
             * parallel gc threads.
             */
            String currentJavaMisc = rnp.getJavaMiscParams();
            if (rnp.getMaxHeapMB() != heapAndCache.getHeapMB()) {
                /* Set both the -Xms and -Xmx flags */
                currentJavaMisc = rnp.replaceOrRemoveJVMArg
                    (currentJavaMisc, RepNodeParams.XMS_FLAG,
                     heapAndCache.getHeapValAndUnit());
                currentJavaMisc = rnp.replaceOrRemoveJVMArg
                    (currentJavaMisc, RepNodeParams.XMX_FLAG,
                     heapAndCache.getHeapValAndUnit());
                rnMap.setParameter(ParameterState.JVM_MISC, currentJavaMisc);
            }

            if (rnp.getRNCachePercent() != heapAndCache.getCachePercent()) {
                rnMap.setParameter(ParameterState.RN_CACHE_PERCENT,
                                Long.toString (heapAndCache.getCachePercent()));
            }

            if (rnp.getJECacheSize() != heapAndCache.getCacheBytes()) {
                rnMap.setParameter(ParameterState.JE_CACHE_SIZE,
                                   Long.toString(heapAndCache.getCacheBytes()));
            }

            if (gcThreads != 0) {
                /* change only if old and new values don't match */
                String oldGc = RepNodeParams.parseJVMArgsForPrefix
                    (RepNodeParams.PARALLEL_GC_FLAG, currentJavaMisc);
                if (oldGc != null) {
                    if (Integer.parseInt(oldGc) != gcThreads) {
                        currentJavaMisc =
                            rnp.replaceOrRemoveJVMArg(currentJavaMisc,
                                              RepNodeParams.PARALLEL_GC_FLAG,
                                              Integer.toString(gcThreads));
                        rnMap.setParameter
                            (ParameterState.JVM_MISC, currentJavaMisc);
                    }
                }
            }

            /* If the RN mount point is null, it is on the root */
            if ((rnp.getStorageDirectoryPath() == null) && (newRootSize != null)) {
                rnMap.setParameter(ParameterState.RN_MOUNT_POINT_SIZE,
                                   newRootSize.asString());
            }
            generateRNUpdateTasks(snId, rnp, rnMap);
        }
    }

    /**
     * Generate tasks to update storage directory sizes for RNs on the specified
     * SN. An update is done if the RN has a defined storage directory and the
     * size has changed.
     */
    private void updateRNStorageDirs(StorageNodeId snId, ParameterMap newMap) {
        final Admin admin = planner.getAdmin();
        for (RepNodeParams rnp :
                admin.getCurrentParameters().getRepNodeParams()) {
            if (!rnp.getStorageNodeId().equals(snId)) {
                continue;
            }

            /* If the current storage dir is null, this RN is in the root dir */
            final String rnPath = rnp.getStorageDirectoryPath();
            if (rnPath == null) {
                continue;
            }

            /*
             * If the RN's storage dir is not in the parameters, something is
             * wrong.
             */
            final Parameter updatedSD = newMap.get(rnPath);
            if (updatedSD == null) {
                throw new IllegalCommandException(
                             "The storage directory for " + rnp.getRepNodeId() +
                             " is not defined in the parameters for " + snId);
            }

            /* Update the RN only if the size has changed */
            final long updatedSize = SizeParameter.getSize(updatedSD);
            if (updatedSize == rnp.getStorageDirectorySize()) {
                continue;
            }

            /*
             * Don't allow non-zero storage directory sizes until all of the
             * Admins are upgraded. Otherwise, sizes may be lost if an older
             * Admin becomes the master and reads then writes storage directory
             * information.
             */
            if ((updatedSize > 0) &&
                !admin.checkAdminGroupVersion(STORAGE_DIR_SIZE_SUPPORT)) {
                throw new IllegalCommandException(
                           "Cannot set storage directory size until all Admin" +
                           " nodes are at or above software version " +
                           STORAGE_DIR_SIZE_SUPPORT.getNumericVersionString());
            }
            ParameterMap rnMap = new ParameterMap(ParameterState.REPNODE_TYPE,
                                                  ParameterState.REPNODE_TYPE);
            rnMap.setParameter(ParameterState.RN_MOUNT_POINT_SIZE,
                               updatedSD.asString());
            generateRNUpdateTasks(snId, rnp, rnMap);
        }
    }

    /**
     * Generates tasks to update the RN parameters, and restart the RN
     * if needed.
     */
    private void generateRNUpdateTasks(StorageNodeId snId,
                                       RepNodeParams rnp,
                                       ParameterMap rnMap) {
        if (rnMap.isEmpty()) {
            return;
        }
        final RepNodeId rnId = rnp.getRepNodeId();
        addTask(new WriteNewParams(this,
                                   rnMap,
                                   rnId,
                                   snId,
                                   true));

        if (rnMap.hasRestartRequired()) {
            addTask(new StopNode(this, snId, rnId, false));
            addTask(new StartNode(this, snId, rnId, false));
            addTask(new WaitForNodeState(this,
                                         rnId,
                                         ServiceStatus.RUNNING));
        } else {
            addTask(new NewRepNodeParameters(this, rnId));
        }
    }

    @Override
    public void stripForDisplay() {
        newParams = null;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}

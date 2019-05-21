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

import org.codehaus.jackson.node.ObjectNode;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.plan.task.DeployDatacenter;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.VersionUtil;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class DeployDatacenterPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /** The first version that supports secondary data centers. */
    public static final KVVersion SECONDARY_DC_VERSION =
        KVVersion.R3_0;    /* R3.0 Q1/2014 */

    /** The first version that supports arbiters. */
    public static final KVVersion ARBITER_DC_VERSION =
        KVVersion.R4_0;

    /** The first version that supports zone affinity. */
    public static final KVVersion AFFINITY_DC_VERSION =
        KVVersion.R18_1;

    /* The original parameters. */
    private String datacenterName;

    /* Deprecated as of R2, no longer supplied through the CLI */
    @SuppressWarnings("unused")
    @Deprecated
    private String comment;

    private DatacenterId datacenterId;
    private Datacenter preexistingDC;

    /* Results */
    private DatacenterParams newDCParams;

    // TODO the bulk of the deployment should be moved to the task, rather
    // than plan construction, so it happens dynamically.
    DeployDatacenterPlan(String planName,
                         Planner planner,
                         Topology topology,
                         String datacenterName,
                         int repFactor,
                         DatacenterType datacenterType,
                         boolean allowArbiters,
                         boolean masterAffinity) {
        super(planName, planner, topology);

        this.datacenterName = datacenterName;

        /* Error Checking */
        Rules.validateReplicationFactor(repFactor);
        Rules.validateArbiter(allowArbiters, datacenterType);
        Rules.validateMasterAffinity(masterAffinity, datacenterType);

        preexistingDC = getPreexistingDatacenter();
        checkVersionRequirements(datacenterType, allowArbiters, masterAffinity);
        if (preexistingDC != null) {
            checkPreexistingDCParams(repFactor, datacenterType, allowArbiters,
                                     masterAffinity);
            datacenterId = preexistingDC.getResourceId();
            newDCParams =
                planner.getAdmin().getCurrentParameters().get(datacenterId);
        } else {

            if (allowArbiters && datacenterType == DatacenterType.SECONDARY) {
                throw new IllegalArgumentException(
                    "Secondary Zones do not allow Arbiters.");
            }

            if (masterAffinity && datacenterType == DatacenterType.SECONDARY) {
                throw new IllegalArgumentException(
                    "Secondary Zones do not allow master affinity.");
            }

            /* Add new data center to topology and create DataCenterParams */
            final Datacenter newDc = Datacenter.newInstance(datacenterName,
                                                            repFactor,
                                                            datacenterType,
                                                            allowArbiters,
                                                            masterAffinity);
            datacenterId = topology.add(newDc).getResourceId();
            newDCParams = new DatacenterParams(datacenterId, datacenterName);
        }

        /*
         * Create a deploy task, even if the topology and params are unchanged.
         * Right now, that deploy task will be meaningless, but in the future,
         * we may need to do some form of work even on a plan retry. Also,
         * the actual creation of the datacenter should be moved to task
         * run time, rather than being done at plan construction time.
         */
        addTask(new DeployDatacenter(this));
    }

    /** Returns the datacenter named by the plan, or null */
    private Datacenter getPreexistingDatacenter() {
        for (final Datacenter dc : getTopology().getDatacenterMap().getAll()) {
            if (dc.getName().equals(datacenterName)) {
                return dc;
            }
        }
        return null;
    }

    /**
     * Guard against a datacenter of the same name, but with different
     * attributes.
     *
     * @throw IllegalCommandException if params for this Datacenter already
     * exist, and are different from the new ones proposed
     */
    private void checkPreexistingDCParams(
        final int repFactor,
        final DatacenterType datacenterType,
        final boolean allowArbiters,
        final boolean masterAffinity) {

        if (preexistingDC.getRepFactor() != repFactor) {
            throw new IllegalCommandException(
                "Zone " + datacenterName +
                " already exists but has a repFactor of " +
                preexistingDC.getRepFactor() +
                " rather than the requested repFactor of " +
                repFactor);
        }
        if (preexistingDC.getDatacenterType() != datacenterType) {
            throw new IllegalCommandException(
                "Zone " + datacenterName +
                " already exists but has type " +
                preexistingDC.getDatacenterType() +
                " rather than the requested type " + datacenterType);
        }
        if (preexistingDC.getAllowArbiters() != allowArbiters) {
            throw new IllegalCommandException(
                "Zone " + datacenterName +
                " already exists but has allowArbiters " +
                preexistingDC.getAllowArbiters() +
                " rather than the requested allowArbiters of " + allowArbiters);
        }
        if (preexistingDC.getMasterAffinity() != masterAffinity) {
            throw new IllegalCommandException(
                "Zone " + datacenterName +
                " already exists but has masterAffinity " +
                preexistingDC.getAllowArbiters() +
                " rather than the requested masterAffinity of " +
                masterAffinity);
        }
    }

    /**
     * Check that the attempt to deploy a datacenter of the specified type is
     * supported by the store version.
     */
    private void checkVersionRequirements(
        final DatacenterType datacenterType,
        final boolean allowArbiters,
        final boolean masterAffinity) {

        final Admin admin = planner.getAdmin();

        if (datacenterType.isSecondary() ||
            allowArbiters || masterAffinity) {
            final KVVersion storeVersion;
            try {
                storeVersion = admin.getStoreVersion();
            } catch (AdminFaultException e) {
                if (datacenterType.isSecondary()) {
                    throw new IllegalCommandException(
                        "Cannot create zone " + datacenterName +
                        " as a secondary zone when unable to confirm" +
                        " that all nodes in the store support secondary" +
                        " zones, which require version " +
                        SECONDARY_DC_VERSION.getNumericVersionString() +
                        " or later.",
                        e);
                } else if (allowArbiters) {
                    throw new IllegalCommandException(
                        "Cannot create zone " + datacenterName +
                        " zone allowing Arbiters when unable to confirm" +
                        " that all nodes in the store support" +
                        " zones allowing Arbiters, which require version " +
                        ARBITER_DC_VERSION.getNumericVersionString() +
                        " or later.",
                        e);
                }
                throw new IllegalCommandException(
                    "Cannot create zone " + datacenterName +
                    " allowing master affinity when unable to" +
                    " confirm that all nodes in the store support zones" +
                    " allowing master affinity, which requires version " +
                    AFFINITY_DC_VERSION.getNumericVersionString() +
                    " or later.",
                    e);
            }

            if (datacenterType.isSecondary()) {
                if (VersionUtil.compareMinorVersion(
                         storeVersion, SECONDARY_DC_VERSION) < 0) {
                    throw new IllegalCommandException(
                        "Cannot create zone " + datacenterName +
                        " as a secondary zone when not all nodes in the" +
                        " store support secondary zones." +
                        " The highest version supported by all nodes is " +
                        storeVersion.getNumericVersionString() +
                        ", but secondary zones require version "
                        + SECONDARY_DC_VERSION.getNumericVersionString() +
                        " or later.");
                }
            }

            if (allowArbiters) {
                if (VersionUtil.compareMinorVersion(
                        storeVersion, ARBITER_DC_VERSION) < 0) {
                   throw new IllegalCommandException(
                       "Cannot create zone " + datacenterName +
                       " allowing Arbiters when not all nodes in the" +
                       " store support zones allowing Arbiters." +
                       " The highest version supported by all nodes is " +
                       storeVersion.getNumericVersionString() +
                       ", but zones allowing Arbiters require version "
                       + ARBITER_DC_VERSION.getNumericVersionString() +
                       " or later.");
               }
            }

            if (masterAffinity) {
                if (VersionUtil.compareMinorVersion(
                        storeVersion, AFFINITY_DC_VERSION) < 0) {
                    throw new IllegalCommandException(
                        "Cannot create zone " + datacenterName +
                        " allowing master affinity when not all nodes in the" +
                        " store support zones allowing master affinity," +
                        " which requires version " +
                        AFFINITY_DC_VERSION.getNumericVersionString() +
                        " or later.");
               }
            }
        }
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private DeployDatacenterPlan() {
    }

    @Override
    void preExecutionSave() {

        /*
         * We only need to save the topology and params if this is the first
         * time the datacenter is created. It may be that we are retrying the
         * plan and that the topology and params are unchanged from what is
         * in the Admin db.
         */
        if (isFirstExecutionAttempt()) {
           getAdmin().saveTopoAndParams(getTopology(), getDeployedInfo(),
                                        newDCParams, this);
        }
    }

    @Override
    public String getDefaultName() {
        return "Deploy Zone";
    }

    public String getDatacenterName() {
        return datacenterName;
    }

    public int getRepFactor() {
        return getTopology().get(datacenterId).getRepFactor();
    }

    private DatacenterType getDatacenterType() {
        return getTopology().get(datacenterId).getDatacenterType();
    }

    private boolean getAllowArbiters() {
        return getTopology().get(datacenterId).getAllowArbiters();
    }

    private boolean getMasterAffinity() {
        return getTopology().get(datacenterId).getMasterAffinity();
    }

    @Override
    public String getOperation() {
        String arbiterOption =
            getAllowArbiters() ? " -arbiters" : " -no-arbiters";
        String masterAffinityOption =
            getMasterAffinity() ? " -master-affinity" : " -no-master-affinity";
        return "plan deploy-zone -name " + datacenterName +
               " -rf " + getRepFactor() +
               " -type " + getDatacenterType().name() +
               arbiterOption + masterAffinityOption;
    }

    /*
     * TODO: replace field names with constants held in a json/command output
     * utility class.
     */
    @Override
    public ObjectNode getPlanJson() {
        ObjectNode jsonTop = JsonUtils.createObjectNode();
        jsonTop.put("plan_id", getId());
        jsonTop.put("zone_name", datacenterName);
        jsonTop.put("zone_id", datacenterId.toString());
        jsonTop.put("type", getDatacenterType().name());
        jsonTop.put("rf", getRepFactor());
        jsonTop.put("allow_arbiters", getAllowArbiters());
        jsonTop.put("master_affinity", getMasterAffinity());
        return jsonTop;
    }
}

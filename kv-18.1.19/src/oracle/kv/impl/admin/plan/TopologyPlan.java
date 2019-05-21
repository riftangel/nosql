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

import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * An abstract class representing a plan that changes the topology of the
 * store.  Topology plans are exclusive - no other plans can run while a
 * topology plan is being run.
 *
 * Topologies are known to the client, repNodes, and admin. Of the three, the
 * admin database contains the authoritative, latest version. Clients, repNodes,
 * and admin replicas may have earlier, less up to date versions, but over time
 * the topology will circulate and all services should become up to date.
 *
 * A Topology plan first creates the new, desired topology, and then begins to
 * execute the appropriate actions to change the store to conform to that
 * topology. Since the admin database is the authoritative copy, the target
 * topology must be stored before being distributed to a client or repNode.
 * Because of this, before the plan successfully finishes, this latest topology
 * will contain services that may not yet exist.
 */
@Persistent
public abstract class TopologyPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    /**
     * The plan topology describes the desired final state of the kvstore after
     * the plan has been executed.
     */
    private Topology topology;

    private transient DeploymentInfo deploymentInfo;

    /**
     */
    TopologyPlan(String name,
                 Planner planner,
                 Topology topo) {
        super(name, planner);
        this.topology = topo;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    protected TopologyPlan() {
    }

    @Override
    public boolean isExclusive() {
        return true;
    }

    public Topology getTopology() {
        return topology;
    }

    protected void setTopology(Topology t) {
        topology = t;
    }

    /**
     * A topology plan should only save its topology on the first plan
     * execution attempt. On subsequent attempts, the topology it is using
     * should be the same version as what is stored. No other topology should
     * have gotten in ahead of it, because topology plans are supposed to
     * be exclusive.
     */
    public boolean isFirstExecutionAttempt() {
        Topology current = getAdmin().getCurrentTopology();
        Topology createdByPlan = getTopology();
        if (current.getSequenceNumber() > createdByPlan.getSequenceNumber()) {
            throw new NonfatalAssertionException
                ("Unexpected error: the current topology version (" +
                 current.getSequenceNumber() + ") is greater than the " +
                 "topology version (" + createdByPlan.getSequenceNumber() +
                 ") used by " +  getName());
        }

        return (current.getSequenceNumber() <
                createdByPlan.getSequenceNumber());

    }

    @Override
    public DeploymentInfo getDeployedInfo() {
        return deploymentInfo;
    }

    @Override
    synchronized PlanRun startNewRun() {
        deploymentInfo = DeploymentInfo.makeDeploymentInfo
                (this, TopologyCandidate.NO_NAME);
        return super.startNewRun();
    }

    @Override
    public void stripForDisplay() {
        topology = null;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}

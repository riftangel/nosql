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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Send a simple newGlobalParameters call to the RepNodeAdminAPI to refresh its
 * global parameters without a restart.
 */
@Persistent
public class NewRNGlobalParameters extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private RepNodeId targetNodeId;
    private AbstractPlan plan;

    public NewRNGlobalParameters(AbstractPlan plan,
                                 RepNodeId targetNodeId) {
        this.plan = plan;
        this.targetNodeId = targetNodeId;
    }

    /* For DPL */
    @SuppressWarnings("unused")
    private NewRNGlobalParameters() {
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public State doWork() throws Exception {
        plan.getLogger().log(Level.FINE,
                             "{0} sending newGlobalParameters", this);

        final RegistryUtils registry =
            new RegistryUtils(plan.getAdmin().getCurrentTopology(),
                              plan.getAdmin().getLoginManager());
        try {
            final RepNodeAdminAPI rnAdmin =
                registry.getRepNodeAdmin(targetNodeId);
            rnAdmin.newGlobalParameters();
        } catch (RemoteException | NotBoundException e) {
            plan.getLogger().log(Level.WARNING,
                                 "Rep Node Admin for {0} cannot be " +
                                 "contacted when updating " +
                                 "its global parameters due to " +
                                 "exception {1}.",
                                  new Object[]{targetNodeId, e});
            return State.ERROR;
        }
        return State.SUCCEEDED;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" refresh ").append(targetNodeId)
                          .append(" global parameter state without restarting");
    }
}

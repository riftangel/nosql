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

import java.util.logging.Level;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Send a simple newParameters call to the Admin to refresh its parameters
 * without a restart.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class NewAdminParameters extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AdminId targetAdminId;
    private AbstractPlan plan;
    private String hostname;
    private int registryPort;

    NewAdminParameters() {
    }

    public NewAdminParameters(AbstractPlan plan,
                              String hostname,
                              int registryPort,
                              AdminId targetAdminId) {
        this.plan = plan;
        this.hostname = hostname;
        this.registryPort = registryPort;
        this.targetAdminId = targetAdminId;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        plan.getLogger().log(Level.FINE,
                             "{0} sending newParameters to Admin", this);

        final CommandServiceAPI cs = ServiceUtils.waitForAdmin
            (hostname, registryPort, plan.getLoginManager(),
             40, ServiceStatus.RUNNING);

        cs.newParameters();
        return State.SUCCEEDED;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" refresh ").append(targetAdminId)
                               .append(" parameter state without restarting");
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}

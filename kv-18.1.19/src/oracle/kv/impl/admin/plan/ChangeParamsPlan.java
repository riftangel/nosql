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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.StartNode;
import oracle.kv.impl.admin.plan.task.StopNode;
import oracle.kv.impl.admin.plan.task.WaitForNodeState;
import oracle.kv.impl.admin.plan.task.WriteNewParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class ChangeParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    protected ParameterMap newParams;
    public ChangeParamsPlan(String name,
                            Planner planner,
                            Topology topology,
                            Set<RepNodeId> rnids,
                            ParameterMap map) {

        super(name, planner);

        this.newParams = map;
        /* Do as much error checking as possible, before the plan is executed.*/
        validateParams(newParams);
        Set<RepNodeId> restartIds = new HashSet<>();

        /*
         * First write the new params on all nodes.
         */
        for (RepNodeId rnid : rnids) {
            StorageNodeId snid = topology.get(rnid).getStorageNodeId();
            ParameterMap filtered = newParams.readOnlyFilter();
            addTask(new WriteNewParams(this, filtered, rnid, snid, true));
            RepNodeParams current = planner.getAdmin().getRepNodeParams(rnid);

            /*
             * If restart is required put the Rep Node in a new set to be
             * handled below, otherwise, add a task to refresh parameters.
             */
            if (filtered.hasRestartRequiredDiff(current.getMap())) {
                restartIds.add(rnid);
            } else {
                addTask(new NewRepNodeParameters(this, rnid));
            }
        }

        if (!restartIds.isEmpty()) {
            List<RepNodeId> restart = sort(restartIds, topology);
            for (RepNodeId rnid : restart) {
                StorageNodeId snid = topology.get(rnid).getStorageNodeId();

                addTask(new StopNode(this, snid, rnid, true));
                addTask(new StartNode(this, snid, rnid, true));
                addTask(new WaitForNodeState(this,
                                             rnid,
                                             ServiceStatus.RUNNING));
            }
        }
    }

    protected List<RepNodeId> sort(Set<RepNodeId> ids,
    		@SuppressWarnings("unused") Topology topology) {
        List<RepNodeId> list = new ArrayList<>();
        for (RepNodeId id : ids) {
            list.add(id);
        }
        return list;
    }

    protected void validateParams(ParameterMap map) {

        /* Check for incorrect JE params. */
        ParameterUtils.validateParams(map);
    }

    /* DPL */
    protected ChangeParamsPlan() {
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
        return "Change RepNode Params";
    }

    public RepNodeParams getNewParams() {
        return new RepNodeParams(newParams);
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

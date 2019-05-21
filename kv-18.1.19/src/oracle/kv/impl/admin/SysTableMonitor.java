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

package oracle.kv.impl.admin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.systables.SysTableDescriptor;
import oracle.kv.impl.systables.SysTableRegistry;

import com.sleepycat.je.utilint.StoppableThread;


/**
 * A task that monitors system tables. It will create the tables if they don't
 * exist and will upgrade existing tables if needed. This task should be run
 * when the Admin becomes the master. If any action is taken (creating or
 * evolving a table) the task will reschedule itself so that the actions can
 * be retried in case they fail.
 *
 * This task is designed so it is not necessary to monitor plan failures. If
 * the plan fails a new one will be created the next go around. An attempt is
 * made to cancel plans that end in non-terminal state. However, if they are
 * missed the plan pruning mechanism will eventually remove them.
 */
class SysTableMonitor extends StoppableThread {

    private static final int THREAD_SOFT_SHUTDOWN_MS = 10000;

    private final Admin admin;

    /**
     * Map of descriptors to plan IDs. The map is initialized to the
     * set of table descriptors with the plan ID set to NO_PLAN. If the table
     * needs attention the resulting plan ID is added. If no action is taken,
     * the descriptor is removed. The task will retry (up to 10 times) as
     * long as the map is not empty.
     */
    private final Map<SysTableDescriptor, Integer> descriptors;

    /**
     * Value in descriptors map to indicate there is no plan associated with
     * the table.
     */
    private static final int NO_PLAN = -1;

    private int attempts = 0;

    private volatile boolean isShutdown = false;

    SysTableMonitor(Admin admin) {
        super("System table monitor");
        this.admin = admin;
        descriptors = new HashMap<>(SysTableRegistry.descriptors.length);
        for (SysTableDescriptor desc : SysTableRegistry.descriptors) {
            descriptors.put(desc, NO_PLAN);
        }
    }

    /**
     * Checks to see if the system tables in the descriptors map are up to
     * the correct version. If they are not, a plan is created to upgrade them.
     */
    @Override
    public void run() {
        if (isShutdown) {
            return;
        }
        getLogger().log(Level.INFO, "Starting {0}", this);

        while (!isShutdown && !descriptors.isEmpty()) {
            checkTables();

            if (isShutdown || descriptors.isEmpty()) {
                break;
            }
            attempts++;
            if (attempts > 10) {
                getLogger().log(Level.SEVERE,
                                "Failed to create or upgrade system " +
                                "tables after {0} attempts.", attempts);
                break;
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ie) {
                /* The sleep may be interrupted by shutdown */
                if (!isShutdown) {
                    getLogger().log(Level.SEVERE,
                                    "Unexpected exception in {0}: {1}",
                                    new Object[]{this, ie});
                }
                break;
            }
        }
        getLogger().log(Level.INFO, "Exit {0}", this);
    }

    private void checkTables() {
        getLogger().log(Level.FINE, "Checking status of system tables");

        final TableMetadata md = admin.getMetadata(TableMetadata.class,
                                                   Metadata.MetadataType.TABLE);

        final Iterator<Entry<SysTableDescriptor, Integer>> itr =
                descriptors.entrySet().iterator();

        while (!isShutdown && itr.hasNext()) {
            final Entry<SysTableDescriptor, Integer> entry = itr.next();
            final SysTableDescriptor desc = entry.getKey();
            int planId = entry.getValue();

            /*
             * If there is already a plan in place to handle this table, check
             * on its state.
             */
            if (planId != NO_PLAN) {
                if (checkPlanState(planId,  desc)) {
                    /* Plan succeeded, remove the descriptor */
                    itr.remove();
                }
                continue;
            }
            getLogger().log(Level.FINE,
                            "Checking status of system table {0}",
                            desc.getTableName());

            /*
             * If the table does not exist, or the metadata does not exist
             * create the table (the plan will also create the metadata if
             * needed).
             */
            final TableImpl table =
                    (md == null) ? null : md.getTable(desc.getTableName());
            if (table == null) {
                final TableImpl newTable = desc.buildTable();
                try {
                    planId = admin.getPlanner().
                                    createAddTablePlan("Create system table",
                                                       newTable,
                                                       null,
                                                       true /* systemTable */);
                    admin.approvePlan(planId);
                    admin.executePlanOrFindMatch(planId);
                    descriptors.put(desc, planId);
                } catch (Exception e) {
                    if (!isShutdown) {
                        getLogger().log(Level.INFO,
                                        "Exception creating system" +
                                        " table {0} {1}",
                                        new Object[]{newTable.getName(),
                                                     e.getMessage()});
                    }
                }
                continue;
            }

            /*
             * See if the table needs to be upgraded.
             */
            final TableImpl newTable = desc.evolveTable(table, getLogger());
            if (newTable != null) {
                try {
                    planId = admin.getPlanner().
                               createEvolveTablePlan("Upgrade system table",
                                                     newTable.getNamespace(),
                                                     newTable.getFullName(),
                                                     table.getTableVersion(),
                                                     newTable.getFieldMap(),
                                                     newTable.getDefaultTTL(),
                                                     newTable.getDescription(),
                                                     true /* systemTable */);
                    admin.approvePlan(planId);
                    admin.executePlanOrFindMatch(planId);
                    descriptors.put(desc, planId);
                } catch (Exception e) {
                    if (!isShutdown) {
                        getLogger().log(Level.INFO,
                                        "Exception evolving system" +
                                        " table {0} {1}",
                                        new Object[]{table.getName(),
                                                     e.getMessage()});
                    }
                }
                continue;
            }
            assert planId == NO_PLAN;
            /* Nothing needed to be done, remove the descriptor. */
            itr.remove();
        }
    }

    /**
     * Checks on the state of the plan handling the descriptor. Returns true
     * if the plan has completed with success and its descriptor can be
     * removed from the descriptors map.
     */
    private boolean checkPlanState(int planId, SysTableDescriptor desc) {
        final Plan plan = admin.getPlanById(planId);

        /*
         * In theory the plan could be pruned. In this case we don't know
         * what state it ended in, so retry just in case.
         */
        if (plan == null) {
            getLogger().log(Level.FINE,
                            "Plan {0} for {1} is missing, retrying",
                            new Object[]{planId, desc});
            descriptors.put(desc, NO_PLAN);
            return false;
        }

        switch (plan.getState()) {
        case PENDING:
        case APPROVED:
        case RUNNING:
            /* Plan is still running. */
            return false;

        case SUCCEEDED:
            getLogger().log(Level.FINE,
                            "Plan {0} for {1} completed",
                            new Object[]{planId, desc});
            return true;

        case INTERRUPTED:
        case ERROR:
        case INTERRUPT_REQUESTED:
            /* Cancel old plan */
            admin.cancelPlan(planId);

            //$FALL-THROUGH$
        case CANCELED:
             /* Plan failed or was canceled. Retry. */
            getLogger().log(Level.FINE,
                            "Plan {0} for {1} did not complete, retrying",
                            new Object[]{planId, desc});
            descriptors.put(desc, NO_PLAN);
            return false;
        }
        throw new IllegalStateException("Unknown plan state: " +
                                        plan.getState());
    }

    @Override
    protected int initiateSoftShutdown() {
        isShutdown = true;
        return THREAD_SOFT_SHUTDOWN_MS;
    }

    @Override
    protected Logger getLogger() {
        return admin.getLogger();
    }
}

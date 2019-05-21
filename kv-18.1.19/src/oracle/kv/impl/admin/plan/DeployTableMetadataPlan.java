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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.JsonUtils;

import com.sleepycat.persist.model.Persistent;

import org.codehaus.jackson.node.ObjectNode;

/**
 * A general purpose plan for executing changes to table metadata. By default
 * this plan will lock out other table metadata plans and elasticity plans.
 */
@Persistent
public class DeployTableMetadataPlan extends MetadataPlan<TableMetadata> {
    private static final long serialVersionUID = 1L;

    /** The first version that supports table operations. */
    private static final KVVersion TABLE_SUPPORT_VERSION =
        KVVersion.R3_0; /* R3.0 Q1/2014 */

    /** The first version that supports table authorization. */
    private final static KVVersion TABLE_AUTH_VERSION =
        KVVersion.R3_3; /* R3.3 Q1/2015 */

    DeployTableMetadataPlan(String planName,
                            Planner planner) {
        this(planName, planner, false);
    }

    DeployTableMetadataPlan(String planName,
                            Planner planner,
                            boolean systemPlan) {
        super(planName, planner, systemPlan);
        /* Ensure all nodes in the store support table operations */
        checkVersion(planner.getAdmin(), TABLE_SUPPORT_VERSION,
                     "Cannot perform plan " + planName + " when not all" +
                     " nodes in the store support table feature.");
    }

    /*
     * No-arg ctor for use by DPL.
     */
    DeployTableMetadataPlan() {
    }

    @Override
    protected Class<TableMetadata> getMetadataClass() {
        return TableMetadata.class;
    }

    @Override
    public MetadataType getMetadataType() {
        return MetadataType.TABLE;
    }

    @Override
    public void preExecutionSave() {
        /* Nothing to save before execution. */
    }

    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        /*
         * Override the default preExecuteCheck since we do not want the
         * basis sequence number checked to allow concurrent table
         * metadata plan execution.
         */
    }

    @Override
    public String getDefaultName() {
        return "Deploy Table Metadata";
    }

    @Override
    public boolean isExclusive() {
      return false;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSDBA for table operations */
        return SystemPrivilege.sysdbaPrivList;
    }

    /*
     * A series of new version table plans since R3.3 with table authorization,
     * since they are set with finer-grain table-level privileges now rather
     * than the blanket SYSDBA privilege.
     */

    /**
     * AddTablePlan requires CREATE_ANY_TABLE privilege.
     */
    static class AddTablePlan extends DeployTableMetadataPlan {
        private static final long serialVersionUID = 1L;

        AddTablePlan(String planName,
                     Planner planner,
                     boolean systemPlan) {
            super(planName, planner, systemPlan);
            checkVersion(planner.getAdmin(), TABLE_AUTH_VERSION,
                         "Cannot add new tables until all nodes in the store" +
                         " have been upgraded to " + TABLE_AUTH_VERSION);
        }
        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            return SystemPrivilege.tableCreatePrivList;
        }
    }

    /**
     * RemoveTablePlan requires DROP_ANY_TABLE privilege.
     */
    static class RemoveTablePlan extends DeployTableMetadataPlan {
        private static final long serialVersionUID = 1L;
        private final ResourceOwner tableOwner;
        private final long tableId;
        private final String tableName;
        private final boolean toRemoveIndex;

        RemoveTablePlan(String planName,
                        String namespace,
                        String tableName,
                        Planner planner) {
            super(planName, planner);

            final TableImpl table = TablePlanGenerator.
                getAndCheckTable(namespace, tableName, getMetadata());

            tableOwner = table.getOwner();
            tableId = table.getId();
            toRemoveIndex = !table.getIndexes().isEmpty();
            this.tableName = tableName;
        }

        @Override
        public void preExecuteCheck(boolean force, Logger plannerlogger) {

            /*
             * Checks before each execution to ensure no new privilege defined
             * on this table has been granted, before the drop operation ends
             * successfully.
             */
            checkPrivilegesOnTable();
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            return TablePlanGenerator.
                getRemoveTableRequiredPrivs(tableOwner, toRemoveIndex, tableId);
        }

        /**
         * Checks whether any privileges defined on the dropping table exists
         * in any role. If yes, the drop will fail. This check prevents any
         * privilege defined on a removed table seen by users.
         */
        void checkPrivilegesOnTable() {
            final SecurityMetadata secMd =
                planner.getAdmin().getMetadata(SecurityMetadata.class,
                                               MetadataType.SECURITY);
            final Set<String> involvedRoles =
                TablePlanGenerator.getInvolvedRoles(tableId, secMd);

            if (!involvedRoles.isEmpty()) {
                throw new IllegalCommandException(
                    "Cannot drop table " + tableName + " since there are " +
                    "privileges defined on it in roles " + involvedRoles +
                    ". Please revoke the privileges explicitly and then try" +
                    " dropping the table again.");
            }
        }
    }

    /**
     * EvolveTablePlan requires EVOLVE_TABLE(tableId) privilege.
     */
    static class EvolveTablePlan extends DeployTableMetadataPlan {
        private static final long serialVersionUID = 1L;
        private final ResourceOwner tableOwner;
        private final long tableId;

        EvolveTablePlan(String planName,
                        String namespace,
                        String tableName,
                        Planner planner,
                        boolean systemPlan) {
            super(planName, planner, systemPlan);

            final TableImpl table = TablePlanGenerator.
                getAndCheckTable(namespace, tableName, getMetadata());
            this.tableOwner = table.getOwner();
            this.tableId = table.getId();
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            final ResourceOwner currentUser =
                SecurityUtils.currentUserAsOwner();
            if ((currentUser != null) && (currentUser.equals(tableOwner))) {
                return SystemPrivilege.usrviewPrivList;
            }
            return Collections.singletonList(
                new TablePrivilege.EvolveTable(tableId));
        }
    }

    /**
     * AddIndexPlan requires CREATE_INDEX(tableId) privilege.
     */
    static class AddIndexPlan extends DeployTableMetadataPlan {
        private static final long serialVersionUID = 1L;
        private final ResourceOwner tableOwner;
        private final long tableId;

        AddIndexPlan(String planName,
                     String namespace,
                     String tableName,
                     Planner planner) {
            super(planName, planner);

            final TableImpl table = TablePlanGenerator.
                getAndCheckTable(namespace, tableName, getMetadata());
            this.tableOwner = table.getOwner();
            this.tableId = table.getId();
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            final ResourceOwner currentUser =
                SecurityUtils.currentUserAsOwner();
            if ((currentUser != null) && (currentUser.equals(tableOwner))) {
                return SystemPrivilege.usrviewPrivList;
            }
            return Collections.singletonList(
                new TablePrivilege.CreateIndex(tableId));
        }
    }

    /**
     * RemoveAddIndexPlan requires DROP_INDEX(tableId) privilege.
     */
    static class RemoveIndexPlan extends DeployTableMetadataPlan {
        private static final long serialVersionUID = 1L;
        private final ResourceOwner tableOwner;
        private final long tableId;

        RemoveIndexPlan(String planName,
                        String namespace,
                        String tableName,
                        Planner planner) {
            super(planName, planner);

            final TableImpl table = TablePlanGenerator.
                getAndCheckTable(namespace, tableName, getMetadata());
            this.tableOwner = table.getOwner();
            this.tableId = table.getId();
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            final ResourceOwner currentUser =
                SecurityUtils.currentUserAsOwner();
            if ((currentUser != null) && (currentUser.equals(tableOwner))) {
                return SystemPrivilege.usrviewPrivList;
            }
            return Collections.singletonList(
                new TablePrivilege.DropIndex(tableId));
        }
    }

    static class SetTableLimitPlan extends DeployTableMetadataPlan {
        private static final long serialVersionUID = 1L;
        private final String namespace;
        private final String tableName;
        private final ResourceOwner tableOwner;
        private final long tableId;

        SetTableLimitPlan(String planName,
                          String namespace,
                          String tableName,
                          Planner planner) {
            super(planName, planner);
            this.namespace = namespace;
            this.tableName = tableName;
            final TableImpl table = TablePlanGenerator.
                    getAndCheckTable(namespace, tableName, getMetadata());
            tableOwner = table.getOwner();
            tableId = table.getId();
        }

        // BIG TODO - what are the privs for setting the limits?
        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            final ResourceOwner currentUser =
                SecurityUtils.currentUserAsOwner();
            if ((currentUser != null) && (currentUser.equals(tableOwner))) {
                return SystemPrivilege.sysoperPrivList;
            }
            return Collections.singletonList(
                new TablePrivilege.EvolveTable(tableId));
        }

        @Override
        public String getOperation() {
            return "plan set-table-limits -name " + tableName + " ...";
        }

        @Override
        public ObjectNode getPlanJson() {
            final ObjectNode jsonTop = JsonUtils.createObjectNode();
            jsonTop.put("plan_id", getId());
            jsonTop.put("namespace", namespace);
            jsonTop.put("tableName", tableName);
            return jsonTop;
        }
    }
}

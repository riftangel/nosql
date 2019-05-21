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
import java.util.Set;

import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.plan.task.RemoveTablePrivileges;
import oracle.kv.impl.admin.plan.task.RemoveTableV2;
import oracle.kv.impl.admin.plan.task.RemoveUserV2;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;

/**
 * Remove user plan supporting cascade option.
 */
public class RemoveUserPlanV2 extends MultiMetadataPlan {

    private static final long serialVersionUID = 1L;

    public RemoveUserPlanV2(String planName,
                            Planner planner,
                            String userName,
                            boolean cascade) {
        super(planName, planner);

        /* Find and drop tables this user owned */
        final Set<TableImpl> ownedTables = TablePlanGenerator.
            getOwnedTables(getTableMetadata(), getSecurityMetadata(), userName);

        if (!ownedTables.isEmpty()) {
            /* Must specify cascade option if user owns tables */
            if (!cascade) {
                RemoveUserPlan.ownsTableWarning(ownedTables);
            }

            /*
             * Check if current user has DROP_ANY_TABLE and DROP_ANY_INDEX
             * privileges
             */
            final ExecutionContext execCtx = ExecutionContext.getCurrent();
            if (!execCtx.hasPrivilege(SystemPrivilege.DROP_ANY_TABLE) ||
                !execCtx.hasPrivilege(SystemPrivilege.DROP_ANY_INDEX)) {
                throw new ClientAccessException(
                    new UnauthorizedException(
                        "DROP_ANY_TABLE and DROP_ANY_INDEX privileges are " +
                        "required in order to drop user with cascade."));
            }
        }
        for (TableImpl table : ownedTables) {
            addTask(RemoveTableV2.newInstance(
                        this, table.getNamespace(),
                        table.getName(), false /* markForDelete */));

            /*
             * Find roles having privileges on this table, and remove
             * table privileges from these roles.
             */
            final Set<String> involvedRoles = TablePlanGenerator.
                getInvolvedRoles(table.getId(), getSecurityMetadata());

            for (String role : involvedRoles) {
                addTask(new RemoveTablePrivileges(
                    this, role, TablePrivilege.getAllTablePrivileges(
                        table.getId(), table.getNamespaceName())));
            }
        }
        addTask(RemoveUserV2.newInstance(this, userName));
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        return SystemPrivilege.sysoperPrivList;
    }

    @Override
    protected Set<MetadataType> getMetadataTypes() {
        return TABLE_SECURITY_TYPES;
    }
}

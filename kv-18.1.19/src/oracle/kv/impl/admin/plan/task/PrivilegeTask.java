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

import java.util.HashSet;
import java.util.Set;

import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.SecurityMetadataPlan.PrivilegePlan;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.security.metadata.SecurityMetadata;

/**
 * The super class of privilege granting or revocation task.
 */
public class PrivilegeTask extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private static final String ALLPRIVS = "ALL";

    final String roleName;
    final String tableName;
    final String namespace;

    final Set<KVStorePrivilege> privileges = new HashSet<KVStorePrivilege>();

    public PrivilegeTask(PrivilegePlan plan,
                         String roleName,
                         String namespace,
                         String tableName,
                         Set<String> privNames) {
        super(plan);

        final SecurityMetadata secMd = plan.getMetadata();
        this.roleName = roleName;
        this.tableName = tableName;
        this.namespace = namespace;

        if ((secMd == null) || (secMd.getRole(roleName) == null)) {
            throw new IllegalCommandException(
                "Role with name " + roleName + " does not exist in store");
        }

        if (secMd.getRole(roleName).readonly()) {
            throw new IllegalCommandException(
                "Cannot grant or revoke privileges to or from a read-only " +
                "role: " + roleName);
        }

        parseToPrivileges(privNames);
    }

    /**
     * Parse and validate string of privilege name to KVStorePrivilege.
     */
    void parseToPrivileges(Set<String> privNames) {
        /* Case of operation for system privileges */
        if (tableName == null) {
            for (String privName : privNames) {
                if (ALLPRIVS.equalsIgnoreCase(privName)) {
                    privileges.addAll(SystemPrivilege.getAllSystemPrivileges());
                    return;
                }
                privileges.add(SystemPrivilege.get(
                    KVStorePrivilegeLabel.valueOf(privName.toUpperCase())));
            }
            return;
        }

        /* Case of operation for table privileges */
        final TableMetadata tableMd =
            getPlan().getAdmin().getMetadata(TableMetadata.class,
                                             MetadataType.TABLE);
        final String nsName = TableMetadata.makeNamespaceName(namespace,
                                                             tableName);
        if (tableMd == null || tableMd.getTable(namespace, tableName) == null) {
            throw new IllegalCommandException(
                "Table with name " + nsName + " does not exist");
        }

        final TableImpl table = tableMd.getTable(namespace, tableName);
        checkPermission(table);

        for (String privName : privNames) {
            if (ALLPRIVS.equalsIgnoreCase(privName)) {
                privileges.addAll(TablePrivilege.getAllTablePrivileges(
                                      table.getId(),
                                      table.getNamespaceName()));
                return;
            }
            final KVStorePrivilegeLabel privLabel =
                KVStorePrivilegeLabel.valueOf(privName.toUpperCase());

            /*
             * Only READ_TABLE privilege on system table is allowed to be
             * granted or revoked explicitly.
             */
            if (!privLabel.equals(KVStorePrivilegeLabel.READ_TABLE) &&
                table.isSystemTable()) {
                throw new ClientAccessException(
                    new UnauthorizedException(
                        "Granting privileges other than read privilege for" +
                        " system tables is not permitted"));
            }
            privileges.add(TablePrivilege.get(privLabel, table.getId(),
                                              table.getNamespaceName()));
        }
    }

    /**
     * Check if current user has enough permission to operation given table
     * privilege granting and revocation.
     */
    private void checkPermission(TableImpl table) {
        final ExecutionContext execCtx = ExecutionContext.getCurrent();
        if (execCtx == null) {
            return;
        }
        if (!AccessCheckUtils.currentUserOwnsResource(table) &&
            !execCtx.hasPrivilege(SystemPrivilege.SYSOPER)) {
               throw new ClientAccessException(
                   new UnauthorizedException(
                       "Insufficient privilege granted to grant or revoke " +
                       "privilege on non-owned tables."));
        }
    }
}

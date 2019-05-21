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

import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVBuiltInRoleResolver;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.RoleResolver;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.Cache;
import oracle.kv.impl.security.util.CacheBuilder;
import oracle.kv.impl.security.util.CacheBuilder.CacheConfig;
import oracle.kv.impl.security.util.CacheBuilder.CacheEntry;

/**
 * A role resolver resides on admin
 */
public class AdminRoleResolver implements RoleResolver {

    /* The AdminService being supported */
    private final AdminService adminService;

    /* Role cache */
    private final Cache<String, RoleEntry> roleCache;

    public AdminRoleResolver(AdminService adminService,
                             CacheConfig cacheConfig) {
        this.adminService = adminService;
        this.roleCache = CacheBuilder.build(cacheConfig);
    }

    /**
     * Update by removing the entry if role instance passed in was cached.
     *
     * @return true if role instance was in cache and removed successfully.
     */
    public boolean updateRoleCache(RoleInstance role) {
        return (roleCache.invalidate(role.name()) != null);
    }

    @Override
    public RoleInstance resolve(String roleName) {
        /* Try to resolve it as a built-in role */
        RoleInstance role = KVBuiltInRoleResolver.resolveRole(roleName);

        /* Not a built-in role, resolve it as a user-defined role */
        if (role == null) {
            final RoleEntry entry = roleCache.get(
                RoleInstance.getNormalizedName(roleName));

            if (entry != null) {
                return entry.getRole();
            }

            final SecurityMetadata secMd = adminService.getAdmin().
                getMetadata(SecurityMetadata.class, MetadataType.SECURITY);
            if ((secMd == null) || (secMd.getAllRoles().isEmpty())) {
                return null;
            }
            role = secMd.getRole(roleName);
            if (role != null) {
                roleCache.put(role.name(),
                              new RoleEntry(role));
            }
        }
        return role;
    }

    /**
     * Cache entry for RoleInstance
     */
    private final class RoleEntry extends CacheEntry {

        private final RoleInstance role;

        RoleEntry(final RoleInstance roleInstance) {
            super();
            role = roleInstance;
        }

        RoleInstance getRole() {
            return role;
        }
    }
}

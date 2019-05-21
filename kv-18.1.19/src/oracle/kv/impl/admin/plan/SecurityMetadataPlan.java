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

import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.AddExternalUser;
import oracle.kv.impl.admin.plan.task.AddRole;
import oracle.kv.impl.admin.plan.task.AddUser;
import oracle.kv.impl.admin.plan.task.ChangeUser;
import oracle.kv.impl.admin.plan.task.GrantPrivileges;
import oracle.kv.impl.admin.plan.task.GrantRoles;
import oracle.kv.impl.admin.plan.task.GrantRolesToRole;
import oracle.kv.impl.admin.plan.task.NewSecurityMDChange;
import oracle.kv.impl.admin.plan.task.RemoveRole;
import oracle.kv.impl.admin.plan.task.RemoveUser;
import oracle.kv.impl.admin.plan.task.RevokePrivileges;
import oracle.kv.impl.admin.plan.task.RevokeRoles;
import oracle.kv.impl.admin.plan.task.RevokeRolesFromRole;
import oracle.kv.impl.admin.plan.task.UpdateMetadata;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilege.PrivilegeType;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.PasswordHash;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.RoleResolver;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.PasswordHashDigest;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.AdminId;

import com.sleepycat.persist.model.Persistent;

/**
 * Plan class representing all security metadata operations
 */
@Persistent
public class SecurityMetadataPlan extends MetadataPlan<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private static final SecureRandom random = new SecureRandom();

    /** The first version that supports basic authentication. */
    private static final KVVersion BASIC_AUTHENTICATION_VERSION =
        KVVersion.R3_0; /* R3.0 Q1/2014 */

    /** The first version that supports role-based authorization. */
    public static final KVVersion BASIC_AUTHORIZATION_VERSION =
        KVVersion.R3_1; /* R3.1 Q3/2014 */

    /** The first version that supports real-time session update. */
    public static final KVVersion REALTIME_SESSION_UPDATE_VERSION =
        KVVersion.R3_2; /* R3.2 Q4/2014 */

    /** The first version that supports user-defined role. */
    public static final KVVersion USER_DEFINED_ROLE_VERSION =
        KVVersion.R3_3; /* R3.3 Q1/2015 */

    /** The first version that supports creating external user */
    public static final KVVersion CREATE_EXTERNAL_USER_VERSION =
        KVVersion.R3_5; /* R3.5 Q4/2015 */

    /** The first version support password complexity check version */
    public static final KVVersion PASSWORD_COMPLEXITY_POLICY_VERSION =
        KVVersion.R4_1; /* R4.1 Q2/2016 */

    /** The first version support cascade drop user version */
    public static final KVVersion CASCADE_DROP_USER_VERSION =
        KVVersion.R4_3; /* R4.3 Q4/2016 */

    private static final String userDefinedRoleNotSupported =
        "Could not perform operation until all nodes in the store support" +
        " user-defined role feature";

    private static final String passwordExpireNotSupported =
        "Could not perform operation until all nodes in the store support" +
        " password expiration feature";

    private static final String createExternalUserNotSupported =
        "Could not perform operation until all nodes in the store support" +
        " creation of external user";

    private static final String passwordComplexityCheckNotSupported =
        "Could not perform create user or alter user's password operation " +
        "until all nodes in the store support password complexity check";

    public SecurityMetadataPlan(String planName, Planner planner) {
        super(planName, planner, false);

        /* Ensure all nodes in the store support basic authentication support */
        checkVersion(planner.getAdmin(), BASIC_AUTHENTICATION_VERSION,
                     "Cannot perform plan " + planName + " when not all" +
                     " nodes in the store support security feature.");
    }

    /* No-arg ctor for DPL */
    private SecurityMetadataPlan() {
    }

    /*
     * Ensure operator does not drop itself
     */
    private static void ensureNotSelfDrop(final String droppedUserName) {
        final KVStoreUserPrincipal currentUserPrincipal =
                KVStoreUserPrincipal.getCurrentUser();
        if (currentUserPrincipal == null) {
            throw new IllegalCommandException(
                "Could not identify current user");
        }
        if (droppedUserName.equals(currentUserPrincipal.getName())) {
            throw new IllegalCommandException(
                "A current online user cannot drop itself");
        }
    }

    @Override
    protected MetadataType getMetadataType() {
        return MetadataType.SECURITY;
    }

    @Override
    protected Class<SecurityMetadata> getMetadataClass() {
        return SecurityMetadata.class;
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    void preExecutionSave() {
        /* Nothing to do since the security metadata has been saved */
    }

    @Override
    public String getDefaultName() {
        return "Change SecurityMetadata";
    }

    @Override
    public void getCatalogLocks() throws PlanLocksHeldException {
        /*
         * Use the elasticity lock to coordinate the concurrent execution of
         * multiple SecurityMetadataPlans since they may read/update the
         * security metadata simultaneously. Also, the update of security
         * metadata will miss for some RepNodes if happens during topology
         * elasticity operation. Synchronize on the elasticity lock can help
         * prevent this.
         *
         * TODO: need to implement a lock only for security metadata plan?
         */
        planner.lockElasticity(getId(), getName());
        getPerTaskLocks();
    }

    /**
     * Get a PasswordHashDigest instance with default hash algorithm, hash
     * bytes, and iterations
     *
     * @param plainPassword the plain password
     * @return a PasswordHashDigest containing the hashed password and hashing
     * information
     */
    public PasswordHashDigest
        makeDefaultHashDigest(final char[] plainPassword) {

        /* TODO: fetch the parameter from global store configuration */
        final byte[] saltValue =
            PasswordHash.generateSalt(random, PasswordHash.SUGG_SALT_BYTES);
        return PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                                PasswordHash.SUGG_HASH_ITERS,
                                                PasswordHash.SUGG_SALT_BYTES,
                                                saltValue, plainPassword);
    }


    /**
     * Add security metadata change notification tasks.
     */
    static void addNewMDChangeTasks(Admin admin, AbstractPlan plan) {
        final Parameters parameters = admin.getCurrentParameters();

        for (AdminId adminId : parameters.getAdminIds()) {
            plan.addTask(new NewSecurityMDChange(plan, adminId));
        }
    }

    public static SecurityMetadataPlan
        createCreateUserPlan(String planName,
                             Planner planner,
                             String userName,
                             boolean isEnabled,
                             boolean isAdmin,
                             char[] plainPassword,
                             Long pwdLifetime) {

        /*
         * If specify pwdLifetime, check if all nodes reach required version,
         * otherwise the password lifetime will be configured as default value.
         */
        if (pwdLifetime != null) {
            checkVersion(planner.getAdmin(), USER_DEFINED_ROLE_VERSION,
                         passwordExpireNotSupported);
        }

        if (plainPassword != null) {
            checkVersion(planner.getAdmin(),
                         PASSWORD_COMPLEXITY_POLICY_VERSION,
                         passwordComplexityCheckNotSupported);
        }

        final String subPlanName =
                (planName != null) ? planName : "Create User";
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan(subPlanName, planner);
        plan.addTask(new AddUser(plan, userName, isEnabled, isAdmin,
                                 plainPassword, pwdLifetime));
        return plan;
    }

    public static SecurityMetadataPlan
        createCreateExternalUserPlan(String planName,
                                     Planner planner,
                                     String userName,
                                     boolean isEnabled,
                                     boolean isAdmin) {

        checkVersion(planner.getAdmin(), CREATE_EXTERNAL_USER_VERSION,
                     createExternalUserNotSupported);
        final String subPlanName =
            (planName != null) ? planName : "Create External User";
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan(subPlanName, planner);
        plan.addTask(new AddExternalUser(plan, userName, isEnabled, isAdmin));
        return plan;
    }

    public static SecurityMetadataPlan
        createChangeUserPlan(String planName,
                             Planner planner,
                             String userName,
                             Boolean isEnabled,
                             char[] plainPassword,
                             boolean retainPassword,
                             boolean clearRetainedPassword,
                             Long pwdLifetime) {
        /*
         * If specify pwdLifetime, check if all nodes reach required version,
         * otherwise the password lifetime will be configured as default value.
         */
        if (pwdLifetime != null) {
            checkVersion(planner.getAdmin(), USER_DEFINED_ROLE_VERSION,
                         passwordExpireNotSupported);
        }

        if (plainPassword != null) {
            checkVersion(planner.getAdmin(),
                         PASSWORD_COMPLEXITY_POLICY_VERSION,
                         passwordComplexityCheckNotSupported);
        }

        final String subPlanName =
                (planName != null) ? planName : "Change User";
        final SecurityMetadataPlan plan;
        if (Utils.storeHasVersion(planner.getAdmin(),
                                  BASIC_AUTHORIZATION_VERSION)) {
            plan = new ChangeUserPlan(subPlanName, planner);
        } else {
            plan = new SecurityMetadataPlan(subPlanName, planner);
        }
        plan.addTask(new ChangeUser(plan, userName, isEnabled, plainPassword,
                                    retainPassword, clearRetainedPassword,
                                    pwdLifetime));
        return plan;
    }

    public static AbstractPlan createDropUserPlan(String planName,
                                                  Planner planner,
                                                  String userName,
                                                  boolean cascade) {
        ensureNotSelfDrop(userName);
        final String subPlanName =
            (planName != null) ? planName : "Drop User";
        final SecurityMetadataPlan plan;
        if (Utils.storeHasVersion(planner.getAdmin(),
                                  CASCADE_DROP_USER_VERSION)) {

            return createDropUserPlanV2(planName, planner, userName,
                                        cascade);
        } else if (Utils.storeHasVersion(planner.getAdmin(),
                                         USER_DEFINED_ROLE_VERSION)) {
            plan = new RemoveUserPlan(subPlanName, planner, userName,
                                      cascade);
            addNewMDChangeTasks(planner.getAdmin(), plan);
        } else {
            if (cascade) {
                throw new IllegalCommandException(
                    "The CASCADE option is not enabled until all nodes in " +
                    "the store have been upgraded to " +
                    USER_DEFINED_ROLE_VERSION + " or higher");
            }
            plan = new SecurityMetadataPlan(subPlanName, planner);
            plan.addTask(RemoveUser.newInstance(plan, userName));
        }
        return plan;
    }

    private static RemoveUserPlanV2 createDropUserPlanV2(String planName,
                                                         Planner planner,
                                                         String userName,
                                                         boolean cascade) {
        final String subPlanName = planName + " CASCADE";
        final RemoveUserPlanV2 plan = new RemoveUserPlanV2(
            subPlanName, planner, userName, cascade);
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for granting roles to a user.
     */
    public static SecurityMetadataPlan
        createGrantPlan(String planName,
                        Planner planner,
                        String grantee,
                        Set<String> roles) {
        final String subPlanName =
            (planName != null) ? planName : "Grant Roles";
        final RolePlan plan = new RolePlan(subPlanName, planner, roles);
        plan.addTask(new GrantRoles(plan, grantee, roles));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for granting roles to a role.
     */
    public static SecurityMetadataPlan
        createGrantRolesToRolePlan(String planName,
                                   Planner planner,
                                   String grantee,
                                   Set<String> roles) {
        checkVersion(planner.getAdmin(), USER_DEFINED_ROLE_VERSION,
                     userDefinedRoleNotSupported);

        final String subPlanName =
            (planName != null) ? planName : "Grant Roles (To Role)";
        final RolePlan plan = new RolePlan(subPlanName, planner, roles);
        plan.addTask(new GrantRolesToRole(plan, grantee, roles));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for granting privileges to a role.
     */
    public static SecurityMetadataPlan
        createGrantPrivsPlan(String planName,
                             Planner planner,
                             String roleName,
                             String namespace,
                             String tableName,
                             Set<String> privs) {
        final String subPlanName =
            (planName != null) ? planName : "Grant Privileges";
        final PrivilegePlan plan =
             new PrivilegePlan(subPlanName, planner, privs,
                               (tableName == null));
        plan.addTask(new GrantPrivileges(plan, roleName, namespace,
                                         tableName, privs));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for revoking privileges from a user.
     */
    public static SecurityMetadataPlan
        createRevokePlan(String planName,
                         Planner planner,
                         String revokee,
                         Set<String> roles) {
        final String subPlanName =
            (planName != null) ? planName : "Revoke Roles";
        final RolePlan plan = new RolePlan(subPlanName, planner, roles);

        plan.addTask(new RevokeRoles(plan, revokee, roles));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for revoking privileges from a role or a user.
     */
    public static SecurityMetadataPlan
        createRevokeRolesFromRolePlan(String planName,
                                      Planner planner,
                                      String revokee,
                                      Set<String> roles) {
        checkVersion(planner.getAdmin(), USER_DEFINED_ROLE_VERSION,
                     userDefinedRoleNotSupported);

        final String subPlanName =
            (planName != null) ? planName : "Revoke Roles (From Role)";
        final RolePlan plan = new RolePlan(subPlanName, planner, roles);

        plan.addTask(
            new RevokeRolesFromRole(plan, revokee, roles));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for revoking privileges from a role.
     */
    public static SecurityMetadataPlan
        createRevokePrivsPlan(String planName,
                              Planner planner,
                              String roleName,
                              String namespace,
                              String tableName,
                              Set<String> privs) {
        final String subPlanName =
            (planName != null) ? planName : "Revoke Privileges";
        final PrivilegePlan plan =
            new PrivilegePlan(subPlanName, planner, privs,
                              (tableName == null));
        plan.addTask(new RevokePrivileges(plan, roleName,
                                          namespace, tableName, privs));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    public static SecurityMetadataPlan createCreateRolePlan(String planName,
                                                            Planner planner,
                                                            String roleName) {
        checkVersion(planner.getAdmin(), USER_DEFINED_ROLE_VERSION,
                     userDefinedRoleNotSupported);
        final String subPlanName =
            (planName != null) ? planName : "Create Role";
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan(subPlanName, planner);
        plan.addTask(new AddRole(plan, roleName));
        return plan;
    }

    public static SecurityMetadataPlan createDropRolePlan(String planName,
                                                          Planner planner,
                                                          String roleName) {
        checkVersion(planner.getAdmin(), USER_DEFINED_ROLE_VERSION,
                     userDefinedRoleNotSupported);
        final String subPlanName =
            (planName != null) ? planName : "Drop Role";
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan(subPlanName, planner);
        plan.addTask(new RemoveRole(plan, roleName));
        addNewMDChangeTasks(planner.getAdmin(), plan);

        /*
         * Revoke this role from all users have been granted.
         */
        final SecurityMetadata secMd = plan.getMetadata();
        for (final KVStoreUser user : secMd.getAllUsers()) {

            if (user.getGrantedRoles().contains(roleName.toLowerCase())) {
                plan.addTask(new RevokeRoles(plan, user.getName(),
                                             Collections.singleton(roleName)));
                addNewMDChangeTasks(planner.getAdmin(), plan);
            }
        }

        for (final RoleInstance role : secMd.getAllRoles()) {
            if (role.getGrantedRoles().contains(
                    RoleInstance.getNormalizedName(roleName))) {
                plan.addTask(new RevokeRolesFromRole(
                    plan, role.name(), Collections.singleton(roleName)));
                addNewMDChangeTasks(planner.getAdmin(), plan);
            }
        }
        return plan;
    }

    public static SecurityMetadataPlan createBroadcastSecurityMDPlan
        (Planner planner) {
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan("Broadcast Security MD", planner);

        plan.addTask(new UpdateMetadata<>(plan));
        return plan;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }

    /* ChangeUserPlan needs to override the getRequiredPrivilege */
    @Persistent
    private static class ChangeUserPlan extends SecurityMetadataPlan {
        private static final long serialVersionUID = 1L;

        private ChangeUserPlan(String planName, Planner planner) {
            super(planName, planner);
        }

        /* DPL Ctor */
        private ChangeUserPlan() {}

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            /* Requires USRVIEW at a minimum */
            return SystemPrivilege.usrviewPrivList;
        }
    }

    /**
     * Represents the grant and revoke operation for roles. Needs to ensure the
     * basic authorization version is met.
     */
    @Persistent
    public static class RolePlan extends SecurityMetadataPlan {
        private static final long serialVersionUID = 1L;
        private static final String roleUnsupportedMsg =
            "Cannot grant or revoke roles when not all nodes in the store " +
            "support role management.";

        public RolePlan(String planName,
                        Planner planner,
                        Set<String> roles) {
            super(planName, planner);

            /*
             * Introduce session real-time update, RolePlan cannot be created
             * until all nodes version reach R3.2.
             */
            checkVersion(planner.getAdmin(),
                         REALTIME_SESSION_UPDATE_VERSION, roleUnsupportedMsg);
            validateRoleNames(roles);
        }

        /* DPL Ctor */
        @SuppressWarnings("unused")
        private RolePlan() {}

        /**
         * Check if given role names are valid and assignable system
         * predefined roles, or existing user-defined roles.
         */
        private void validateRoleNames(Set<String> roleNames) {
            final RoleResolver roleResolver =
                planner.getAdmin().getRoleResolver();

            /*
             * Normally, the role resolver should not be null, unless the
             * security is not enabled
             */
            if (roleResolver == null) {
                throw new IllegalCommandException(
                    "Cannot grant or revole roles. Please make sure the " +
                    "security feature is enabled");
            }

            for (String roleName : roleNames) {
                final RoleInstance role = roleResolver.resolve(roleName);
                if (role == null) {
                    throw new IllegalCommandException(
                        "Role with name : " + roleName + " does not exist");
                } else if (!role.assignable()) {
                    throw new IllegalCommandException(
                        "Role " + roleName + " cannot be granted or revoked");
                }
            }
        }
    }

    /**
     * Privilege plans have different permission requirement from generic
     * SecurityMetadataPlan.
     */
    public static class PrivilegePlan extends SecurityMetadataPlan {
        private static final long serialVersionUID = 1L;

        private static final String ALLPRIVS = "ALL";
        private static final String versionNotMetMsg =
            "Cannot grant or revoke privileges when not all nodes in the " +
            "store supports user-defined role.";

        /* If the operation is for system privileges only */
        private final boolean isSystemPrivsOp;

        private PrivilegePlan(String planName,
                              Planner planner,
                              Set<String> privs,
                              boolean isSystemPrivsOp) {
            super(planName, planner);

            checkVersion(planner.getAdmin(),
                         USER_DEFINED_ROLE_VERSION, versionNotMetMsg);

            this.isSystemPrivsOp = isSystemPrivsOp;
            validatePrivileges(privs);
        }

        /**
         * Check if given privilege names are valid.
         */
        private void validatePrivileges(Set<String> privNames) {
            for (String privName : privNames) {
                if (!ALLPRIVS.equalsIgnoreCase(privName)) {
                    try {
                        final KVStorePrivilegeLabel privLabel =
                            KVStorePrivilegeLabel.valueOf(
                                privName.toUpperCase(java.util.Locale.ENGLISH));

                        if (!checkPrivConsistency(privLabel)) {
                            throw new IllegalCommandException(
                                "Could not use " + privName + " with type of " +
                                privLabel.getType() + " in this operation " +
                                "which needs privilege type of " +
                                (isSystemPrivsOp ? "SYSTEM" : "TABLE"));
                        }
                    } catch (IllegalArgumentException iae) {
                        throw new IllegalCommandException(
                            privName + " is not valid privilege name");
                    }
                }
            }
        }

        /*
         * A convenient method to check whether a privilege matches the
         * required type of this operation.
         */
        private boolean checkPrivConsistency(KVStorePrivilegeLabel privLabel) {
            if (privLabel.getType().equals(PrivilegeType.SYSTEM)) {
                return isSystemPrivsOp;
            }
            return !isSystemPrivsOp;
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            /*
             * If it is an operation on system privileges, SYSOPER is required.
             * Otherwise only USRVIEW is checked, and nuanced check will be
             * deferred to tasks.
             */
            return isSystemPrivsOp ?
                   SystemPrivilege.sysoperPrivList :
                   SystemPrivilege.usrviewPrivList;
        }
    }
}

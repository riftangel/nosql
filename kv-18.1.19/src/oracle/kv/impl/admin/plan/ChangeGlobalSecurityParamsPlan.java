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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.BroadcastMetadata;
import oracle.kv.impl.admin.plan.task.NewAdminGlobalParameters;
import oracle.kv.impl.admin.plan.task.NewRNGlobalParameters;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.admin.plan.task.WriteNewGlobalParams;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.oauth.IDCSOAuthUtils;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.VersionUtil;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class ChangeGlobalSecurityParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    private static final KVVersion SECURITY_VERSION =
        KVVersion.R3_0; /* R3.0 Q1/2014 */

    /** The first version that supports Kerberos authentication */
    private static final KVVersion KERBEROS_AUTH_VERSION =
        KVVersion.R3_5; /* R3.5 Q4/2015 */

    /** The first version that supports IDCS OAuth authentication */
    private static final KVVersion IDCS_OAUTH_AUTH_VERSION =
        KVVersion.R4_2; /* R4.2 Q3/2016*/

    private ParameterMap newParams = null;
    private Parameters currentParams;
    private static final Set<AdminId> allAdminIds = new HashSet<>();

    public ChangeGlobalSecurityParamsPlan(String name,
                                          Planner planner,
                                          Topology topology,
                                          ParameterMap map) {
        super(name, planner);

        checkSecurityVersion();

        this.newParams = map;
        Admin admin = planner.getAdmin();
        currentParams = admin.getCurrentParameters();
        allAdminIds.addAll(currentParams.getAdminIds());

        final ParameterMap filtered = newParams.readOnlyFilter().
            filter(EnumSet.of(ParameterState.Info.GLOBAL,
                              ParameterState.Info.SECURITY));

        final GlobalParams currentGlobalParams =
            currentParams.getGlobalParams();
        final boolean needsRestart =
            filtered.hasRestartRequiredDiff(currentGlobalParams.getMap());

        /* There should be no restart required */
        if (needsRestart) {
            throw new NonfatalAssertionException(
                "Parameter change would require an admin restart, which is " +
                "not supported.");
        }

        /* Check if parameter userExternalAuth is changed */
        final String newAuthMethods =
            filtered.get(ParameterState.GP_USER_EXTERNAL_AUTH).asString();

        if (newAuthMethods != null) {
            final String[] currentAuthMethods =
                currentGlobalParams.getUserExternalAuthMethods();

            if (newAuthMethods.split(",").length > 1) {
                throw new IllegalCommandException(
                    "Cannot enable multiple external authentication mechanisms");
            }

            if (SecurityUtils.hasKerberos(newAuthMethods) &&
                !SecurityUtils.hasKerberos(currentAuthMethods)) {

                planner.getLogger().info("Enable Kerberos as one of " +
                    "user external authentication methods");
                enableKerberosAsAuthMethod();
            }

            if (SecurityUtils.hasIDCSOAuth(newAuthMethods) &&
                !SecurityUtils.hasIDCSOAuth(currentAuthMethods)) {
                planner.getLogger().info("Enable OAuth as one of " +
                    "user external authentication methods");
                enableIDCSOAuthAsAuthMethod();
            }
        }

        /* Check if parameter sessionExtendAllowed is changed */
        final String sessionExtendAllowed =
            filtered.get(ParameterState.GP_SESSION_EXTEND_ALLOW).asString();
        if (sessionExtendAllowed != null &&
            sessionExtendAllowed.equalsIgnoreCase("true")) {
            final String[] currentAuthMethods =
                currentGlobalParams.getUserExternalAuthMethods();
            final boolean addingIDSCOAuth =
                (newAuthMethods != null) &&
                SecurityUtils.hasIDCSOAuth(newAuthMethods);
            final boolean keepingIDSCOAuth =
                SecurityUtils.hasIDCSOAuth(currentAuthMethods) &&
                (newAuthMethods == null);
            if (addingIDSCOAuth || keepingIDSCOAuth) {
                throw new IllegalCommandException(
                    "Cannot enable session extension when " +
                    "IDCS OAuth is enabled");
            }
        }

        /* Check if IDCS OAuth signature verify algorithm is supported */
        final String idcsAlg = filtered.get(
            ParameterState.GP_IDCS_OAUTH_SIG_VERIFY_ALG_NAME).asString();
        if (idcsAlg != null &&
            !IDCSOAuthUtils.idcsSupportedAlgorithm(idcsAlg)) {
            throw new IllegalCommandException(idcsAlg + " is not supported, " +
                "the supported signature verification are " +
                IDCSOAuthUtils.getIdcsSupportedAlgorithm());
        }

        final List<StorageNodeId> snIds = topology.getStorageNodeIds();
        for (final StorageNodeId snId : snIds) {

            /*
             * First write the new global security parameters on all storage
             * nodes
             */
            addTask(new WriteNewGlobalParams(this, filtered, snId, false));

            addNewGlobalParametersTasks(snId, topology);
        }
    }

    /* Non-arg ctor for DPL */
    @SuppressWarnings("unused")
    private ChangeGlobalSecurityParamsPlan() {
    }

    /*
     * Add newGlobalParameter tasks for all components in the specified storage
     * node, including Admin and RepNode services
     */
    private void addNewGlobalParametersTasks(final StorageNodeId snId,
                                             final Topology topo) {

        final Set<RepNodeId> refreshRns = topo.getHostedRepNodeIds(snId);
        for (final RepNodeId rnid : refreshRns) {
            addTask(new NewRNGlobalParameters(this, rnid));
        }

        for (final AdminId aid : allAdminIds) {
            final StorageNodeId sidForAdmin =
                currentParams.get(aid).getStorageNodeId();
            if (sidForAdmin.equals(snId)) {
                final StorageNodeParams snp = currentParams.get(sidForAdmin);
                final String hostname = snp.getHostname();
                final int registryPort = snp.getRegistryPort();

                addTask(new NewAdminGlobalParameters(
                    this, hostname, registryPort, aid));
            }
        }
    }

    private void checkSecurityVersion() {

        /* Ensure all nodes in the store support security feature */
        final Admin admin = planner.getAdmin();
        final KVVersion storeVersion = admin.getStoreVersion();

        if (VersionUtil.compareMinorVersion(
                storeVersion, SECURITY_VERSION) < 0) {
            throw new IllegalCommandException(
                "Cannot perform security metadata related operations when" +
                " not all nodes in the store support security feature." +
                " The highest version supported by all nodes is " +
                storeVersion.getNumericVersionString() +
                ", but security metadata operations require version "
                + SECURITY_VERSION.getNumericVersionString() + " or later.");
        }
    }

    private void enableKerberosAsAuthMethod() {
        final Admin admin = planner.getAdmin();

        if (!Utils.storeHasVersion(admin, KERBEROS_AUTH_VERSION)){
            throw new IllegalCommandException(String.format(
                "The highest version supported by all nodes " +
                "is lower than the required version of %s or later." +
                "Could not enable Kerberos as user external authentication " +
                "method until all nodes in the store support Kerberos",
                KERBEROS_AUTH_VERSION.getNumericVersionString()));
        }

        final SecurityMetadata md = admin.getMetadata(SecurityMetadata.class,
                                                      MetadataType.SECURITY);
        final SecurityParams secParams = admin.getParams().getSecurityParams();

        if (!secParams.isSecure()) {
            return;
        }

        try {
            /*
             * If new Kerberos information is stored in metadata,
             * broadcast metadata on Admin to all RNs.
             */
            if (Utils.storeKerberosInfo(this, md)) {
                addTask(new BroadcastMetadata<>(this, md));
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                "Unexpected error occur while storing Kerberos " +
                "principal in metadata: " + e.getMessage(),
                e);
        }
    }

    private void enableIDCSOAuthAsAuthMethod() {
        final Admin admin = planner.getAdmin();

        if (!Utils.storeHasVersion(admin, IDCS_OAUTH_AUTH_VERSION)){
            throw new IllegalCommandException(String.format(
                "The highest version supported by all nodes " +
                "is lower than the required version of %s or later." +
                "Could not enable IDCS OAuth as user external authentication " +
                "method until all nodes in the store support IDCS OAuth",
                IDCS_OAUTH_AUTH_VERSION.getNumericVersionString()));
        }

        /*
         * Enable IDCS OAuth authentication must also disable session extension
         * and configure login cache timeout
         */
        final boolean currentSessExt =
            currentParams.getGlobalParams().getSessionExtendAllow();
        final String newSessionExt =
            newParams.get(ParameterState.GP_SESSION_EXTEND_ALLOW).asString();

        if (currentSessExt) {
            /*
             * If session extension is already enabled, user must disable
             * session extension at the same time if enable the IDCS OAuth.
             */
            if (newSessionExt != null &&
                newSessionExt.equalsIgnoreCase("false")) {
                return;
            }
        } else {
            /*
             * If session extension is already disable, check if new parameters
             * try to enable it.
             */
            if (newSessionExt == null ||
                newSessionExt.equalsIgnoreCase("false")) {
                return;
            }
        }
        throw new IllegalCommandException(
            "To enable IDCS OAuth, session extension must be disabled");
    }

    @Override
    public String getDefaultName() {
        return "Change Global Security Params";
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
    public void stripForDisplay() {
        newParams = null;
        currentParams = null;
    }

    @Override
    public boolean updatingMetadata(Metadata<?> metadata) {
        if (metadata.getType().equals(MetadataType.SECURITY)) {
            final SecurityMetadata currentSecMd = this.getAdmin().
                getMetadata(SecurityMetadata.class, MetadataType.SECURITY);

            if (currentSecMd == null) {
                return true;
            }
            return metadata.getSequenceNumber() >
                currentSecMd.getSequenceNumber();
        }
        return false;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}

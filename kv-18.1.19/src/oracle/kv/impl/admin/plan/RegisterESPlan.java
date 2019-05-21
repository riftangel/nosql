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

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.UpdateESConnectionInfo;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.tif.ElasticsearchHandler;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.table.Index;

/**
 * A plan for informing the store of the existence of an Elasticsearch node.
 */
public class RegisterESPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    /** The first version that supports text indexes. */
    private static final KVVersion TEXT_INDEX_VERSION = KVVersion.R4_0;
    private static final KVVersion SECURE_TEXT_INDEX_VERSION = KVVersion.R18_1;

    public RegisterESPlan(String name,
            Planner planner,
            String clusterName,
            String hostPort,
            boolean secure,
            boolean forceClear) {
        super(name, planner);

        final Admin admin = getAdmin();
        checkVersionRequirement(admin,secure);
        checkSecureStoreRequirement(admin, secure);
        final Parameters p = admin.getCurrentParameters();
        final TableMetadata tmd = admin.getMetadata(TableMetadata.class,
                                                    MetadataType.TABLE);
        final GlobalParams gp = p.getGlobalParams();
        final HostPort transportHp = HostPort.parse(hostPort);

        /*
         * Pick one SNA to verify that we are not trying to introduce
         * a different cluster from one that is already registered.
         */
        final StorageNodeParams sp = p.getStorageNodeParams().iterator().next();
        final String oldClusterName = sp.getSearchClusterName();
        if (!("".equals(oldClusterName) ||
              clusterName.equals(oldClusterName))) {
            final String eol = System.getProperty("line.separator");
            throw new IllegalCommandException
                ("Cannot register a new ES cluster with a different " + eol +
                 "cluster name from the cluster that is" +
                 " already registered." + eol +
                 "The currently registered cluster name is " + oldClusterName +
                 ".");
        }


        /*
         * Determine whether any text indexes exist in the store.  If not, then
         * we expect to connect to a store that does not yet contain an ES
         * Index for the store.  This can happen if kvstore is re-initialized
         * with the same store name after creating text indexes.
         * 
         * If we expect to have no ES Index, then the name of the store is
         * passed into getAllTransports, so that it can check whether the
         * ES Index exists while it is connected to the ES cluster.  If the
         * ES Index exists when it should not, getAllTransports will throw
         * IllegalStateException, unless forceClear is true, in which case
         * getAllTransports will remove the offending ES Index.
         */

        final String storeNameForESIndexCheck;
        if (tmd == null) {
            storeNameForESIndexCheck = null;
        } else {
            final List<Index> textIndexes = tmd.getTextIndexes();
            storeNameForESIndexCheck =
                (0 == textIndexes.size() ? gp.getKVStoreName() : null);
        }

        String allHttpHostPorts = null;

        /*
         * The below method will check connection to ES and verify clusterName.
         */
        allHttpHostPorts =
            ElasticsearchHandler.getAllTransports(clusterName, transportHp,
                                                  storeNameForESIndexCheck,
                                                  secure,
                                                  admin.getParams()
                                                       .getSecurityParams(),
                                                  forceClear,
                                                  admin.getLogger());

        /*
         *  update the ES connection info list on each SNA.
         */
        for (StorageNodeParams snaParams : p.getStorageNodeParams()) {
            addTask(new UpdateESConnectionInfo(this,
                                               snaParams.getStorageNodeId(),
                                               clusterName, allHttpHostPorts,
                                               secure));
        }
    }

    /**
     * Ensure an Elasticsearch cluster is not registered prior to the entire
     * store's being upgraded to a version that supports text indexing.
     *
     * The registration of an ES cluster is a prerequisite for creation of a
     * text index, so this check also gates that operation, which requires that
     * all nodes (even non-Admin nodes) be upgraded such that every potential
     * master RepNode can create a TextIndexFeeder.
     */
    private void checkVersionRequirement(final Admin admin, boolean secure) {
        final KVVersion minVer;
        try {
            minVer = admin.getStoreVersion();
        } catch (AdminFaultException e) {
            throw new IllegalCommandException
                ("Cannot register an Elasticsearch cluster, because some " +
                 "configured nodes' versions can't be determined. " +
                 "All nodes must be at version " +
                 TEXT_INDEX_VERSION.getNumericVersionString() + " or later.",
                 e);
        }
        if (VersionUtil.compareMinorVersion(minVer, TEXT_INDEX_VERSION) < 0) {
            throw new IllegalCommandException
                ("Cannot register an Elasticsearch cluster, because some " +
                 "configured nodes do not support the full text indexing " +
                 "feature. The highest version supported by all nodes in " +
                 "the store is " +
                 minVer.getNumericVersionString() +
                 ", but full text indexing requires version " +
                 TEXT_INDEX_VERSION.getNumericVersionString() + " or later.");
        }
        if(secure && minVer.getReleaseEdition() != null &&
                 !"Enterprise".equals(minVer.getReleaseEdition())
          ) {
            throw new IllegalCommandException
            ("At least one of the nodes in the KVStore is not" +
             " running the Enterprise Edition. " +
             " Secure Text Index is only supported in Enterprise Edition ");
        }
        if(secure && minVer.compareTo(SECURE_TEXT_INDEX_VERSION) < 0) {
            throw new IllegalCommandException
            ("Cannot register a Secure Elasticsearch cluster, because some " +
             "configured nodes do not support the secure full text indexing " +
             "feature. The highest version supported by all nodes in " +
             "the store is " +
             minVer.getNumericVersionString() +
             ", but full text indexing requires version " +
             SECURE_TEXT_INDEX_VERSION.getNumericVersionString() +
             " or later.");
        }
    }

    /**
     * Ensure an Elasticsearch cluster is not registered if the store is
     * configured as a secure store
     *
     * @param admin  admin instance of the store
     */
    private void
            checkSecureStoreRequirement(final Admin admin, boolean secureES) {
        final AdminServiceParams adminParams = admin.getParams();
        final SecurityParams securityParams = adminParams.getSecurityParams();

        if (securityParams != null) {
            if (securityParams.isSecure() ^ secureES) {
                throw new IllegalCommandException
                   ("Can not register ES Cluster. Please Register" +
                    " secure ES on secure KV and non-secure ES on non-secure KV");
            }
        }

    }

    @Override
    public void preExecutionSave() {
    }

    @Override
    public boolean isExclusive() {
        return true;
    }

    @Override
    public String getDefaultName() {
        return "Register Elasticsearch cluster";
    }

    @Override
    public void stripForDisplay() {
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}

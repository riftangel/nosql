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

package oracle.kv.util.kvlite;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Collections;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.SecurityConfigCreator;
import oracle.kv.impl.util.SecurityConfigCreator.GenericIOHelper;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig;
import oracle.kv.impl.util.ServiceUtils;

/**
 * See KVLite.
 * This class creates a simple store with an Admin and RepNode.
 */
public class KVLiteAdmin {

    private final String kvstore;
    private final BootstrapParams bp;
    private final ParameterMap policyMap;
    private final int numPartitions;
    /**
     * The first admin user created for secured KVLite.
     */
    private static final String firstUser = "admin";

    public KVLiteAdmin(String kvstore,
                       BootstrapParams bp,
                       ParameterMap policyMap,
                       int numPartitions) {
        this.kvstore = kvstore;
        this.bp = bp;
        this.policyMap = policyMap;
        this.numPartitions = numPartitions;
    }

    public void run()
        throws Exception {

        deployStore();
    }

    /**
     * Use the CommandService to configure/deploy a simple store.
     */
    private void deployStore()
        throws Exception {

        String host = bp.getHostname();
        int port = bp.getRegistryPort();
        final boolean isSecure = bp.getSecurityDir() != null;

        AdminLoginManager alm = null;
        if (isSecure) {
            alm = KVLite.waitForSecurityStartUp(host, port);
        }
        CommandServiceAPI admin = ServiceUtils.waitForAdmin
            (host, port, alm, 5, ServiceStatus.RUNNING);

        admin.configure(kvstore);
        int planId = admin.createDeployDatacenterPlan(
            "Deploy KVLite", "KVLite", 1, DatacenterType.PRIMARY, false, false);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        if (policyMap != null) {
            admin.setPolicies(policyMap);
        }

        planId = admin.createDeploySNPlan
            ("Deploy Storage Node", new DatacenterId(1), host, port, null);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        planId = admin.createDeployAdminPlan
            ("Deploy Admin Service", new StorageNodeId(1));

        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        admin.addStorageNodePool("KVLitePool");
        admin.addStorageNodeToPool("KVLitePool", new StorageNodeId(1));
        admin.createTopology("KVLite", "KVLitePool", numPartitions, false);
        planId = admin.createDeployTopologyPlan("Deploy KVStore", "KVLite",
                                                null);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        if (isSecure) {
            createSecurity(admin);
        }
    }

    /*
     * 1. Create the first admin user.
     * 2. Grant readwrite access to the created user.
     * 3. Create security login file for the first admin user.
     */
    private void createSecurity(CommandServiceAPI admin)
        throws RemoteException {
        final char[] password = SecurityUtils.generateUserPassword();
        int planId = admin.createCreateUserPlan("Create User", firstUser,
            true, true, password);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        planId = admin.createGrantPlan("Grant User",
            firstUser, Collections.singleton(RoleInstance.READWRITE_NAME));
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        final ParsedConfig config = new ParsedConfig();
        SecurityConfigCreator scCreator =
            new SecurityConfigCreator(bp.getRootdir(),
                                      config,
                                      new GenericIOHelper(System.out));
        try {
            scCreator.createUserLoginFile(firstUser, password,
                new File(bp.getRootdir(), bp.getSecurityDir()));
        } catch (Exception e) {
            throw new RuntimeException("Caught exception when creating " +
                "security configuration", e);
        }
    }
}

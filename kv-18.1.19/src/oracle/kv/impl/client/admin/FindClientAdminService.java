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

package oracle.kv.impl.client.admin;

import java.net.URI;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * FindClientAdminService locates the master Admin and obtains an
 * ClientAdminServiceAPI.
 *
 * In this implementation, a brute force approach is taken. SNs are pinged in
 * turn until an admin service is successfully contacted. Once an Admin service
 * can reached, it's used to ask for the set of all Admins in the cluster.
 *
 * Another, more efficient method, would have been to maintain knowledge of the
 * admin location in the cluster. In the future, we plan to maintain more
 * structure about admin location, possibly via the topology. In that case,
 * this functionality can be restructured take advantage of that knowledge.
 *
 * This class is not thread safe and assumes that it is called by a single
 * thread. A new instance should be create each time a connection is needed, so
 * as to ensure that the search is based on a fresh topology and current login
 * manager.
 */
class FindClientAdminService {

    /*
     * For informational/support/debug purposes, keep a list of SNs that were
     * investigated, and SNs that couldn't be reached. Used an ordered list,
     * because it's easier for the user to peruse and compare an ordered list
     * of SNs.
     */
    private final List<StorageNodeId> inspected;
    private final List<StorageNodeId> problematic;

    private final RegistryUtils regUtils;
    private final Logger logger;
    private final LoginManager loginManager;

    private ClientAdminServiceAPI foundService;

    FindClientAdminService(Topology topo, 
                           Logger logger, 
                           LoginManager loginManager) {

        inspected = new ArrayList<StorageNodeId>();
        problematic = new ArrayList<StorageNodeId>();
        this.logger = logger;
        this.loginManager = loginManager;

        regUtils = new RegistryUtils(topo, loginManager);

        for (StorageNode sn : topo.getSortedStorageNodes()) {
            StorageNodeId snId = sn.getResourceId();
            try {
                logger.fine("Checking sn " + snId);
                Registry reg = regUtils.getRegistry(snId);

                /*
                 * we were able to reach this SN, add it to the list of
                 * inspected ones for debugging support.
                 */
                inspected.add(snId);
                String[] bound = reg.list();
                for (String boundName : bound) {
                    if (boundName.equals
                        (GlobalParams.CLIENT_ADMIN_SERVICE_NAME)) {

                        /*
                         * Contact, and see if this Admin is the master. Note
                         * that even if we find a service, it may not have come
                         * from this SN; connectDDL can do redirects from one
                         * admin to another.
                         */
                        foundService = connectDDL(snId);
                        if (foundService != null) {
                            return;
                        }
                    }
                }
            } catch (RemoteException e) {
                problematic.add(snId);
            }
        }

    }

    /*
     * There is a ClientAdminService for DDL support bound on this SN. Attempt
     * to connect to it.  Handle a redirect to a master if we are connecting to
     * a replica.
     *
     * @return a handle to the RMI interface if connection succeeded, null
     * if a connection couldn't be established.
     */
    private ClientAdminServiceAPI connectDDL(StorageNodeId hostingSNId)  {

        ClientAdminServiceAPI ddlService = null;
        try {
            ddlService = regUtils.getAdminDDL(hostingSNId);
            if (ddlService.canHandleDDL()) {
                return ddlService;
            }

            URI rmiaddr = ddlService.getMasterRmiAddress();
            if (rmiaddr == null) {
                return null;
            }

            logger.fine("DDL redirecting to master at " + rmiaddr);
            String adminHostname = rmiaddr.getHost();
            int adminRegistryPort = rmiaddr.getPort();
            ddlService = RegistryUtils.getAdminDDL
                (adminHostname, adminRegistryPort, loginManager);
            if (ddlService.canHandleDDL()) {
                return ddlService;
            }
        } catch (RemoteException re) {
            logger.fine("Couldn't connect to ClientAdmin service on admin on " +
                        hostingSNId);
        } catch (NotBoundException nbe) {
            logger.fine("Couldn't connect to ClientAdmin service on admin on " +
                        hostingSNId);
        }

        /* 
         * Other types of exceptions, including AuthenticationFailedException,
         * should throw out to the application.
         */

        return null;
    }

    ClientAdminServiceAPI getDDLService() {
        return foundService;
    }

    /**
     * Return an error message that lists SNs which were visited in search of
     * an admin master, and SNs that couldn't be queried. In general, the
     * latter category would represent SNs that had communication issues.
     * For error message support.
     */
    public String getTargets() {
        StringBuilder sb = new StringBuilder();

        sb.append("Inspected nodes: ").append(inspected).append("\n");
        sb.append("experienced problems at: ").append(problematic);
        return sb.toString();
   }

    /**
     * For unit test support. This is the list of SNs which were contacted in
     * the search.
     */
    List<StorageNodeId> getInspected() {
        return inspected;
    }

    /**
     * For unit test support. This is the list of SNs which could not be
     * contacted, or had a remote exception.
     */
    List<StorageNodeId> getProblematic() {
        return problematic;
    }
}

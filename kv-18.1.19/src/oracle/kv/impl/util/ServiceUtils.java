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

package oracle.kv.impl.util;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Utilities that provide helper methods for service access.
 */
public class ServiceUtils {

    /**
     * Get and wait for a RepNodeAdmin handle to reach one of the states in
     * the ServiceStatus array parameter.
     */
    @SuppressWarnings("null")
    public static RepNodeAdminAPI waitForRepNodeAdmin
    (String storeName,
     String hostName,
     int port,
     RepNodeId rnid,
     StorageNodeId snid,
     LoginManager loginMgr,
     long timeoutSec,
     ServiceStatus[] targetStatus)
    throws Exception {

        Exception exception = null;
        RepNodeAdminAPI rnai = null;
        ServiceStatus status = null;

        long limitMs = System.currentTimeMillis() + 1000 * timeoutSec;

        while (System.currentTimeMillis() <= limitMs) {

            /**
             * The stub may be stale, get it again on exception.
             */
            if (exception != null) {
                rnai = null;
            }
            try {
                if (rnai == null) {
                    rnai = RegistryUtils.getRepNodeAdmin
                        (storeName, hostName, port, rnid, loginMgr);
                }
                status = rnai.ping().getServiceStatus();
                for (ServiceStatus tstatus : targetStatus) {
                    /**
                     * Treat UNREACHABLE as "any".
                     */
                    if (tstatus == ServiceStatus.UNREACHABLE) {
                        return rnai;
                    }
                    if (status == tstatus) {
                        return rnai;
                    }
                }
                exception = null;
            } catch (RemoteException e) {
                exception = e;
            } catch (NotBoundException e) {
                exception = e;
            }

            /*
             * Check now for any process startup problems before
             * sleeping.
             */
            if (snid != null) {
                RegistryUtils.checkForStartupProblem(storeName,
                                                     hostName,
                                                     port,
                                                     rnid,
                                                     snid,
                                                     loginMgr);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }

        if (status != null) {
            throw new IllegalStateException
                ("RN current status: " + status + " target status: " +
                 Arrays.toString(targetStatus));
        }
        throw exception;
    }

    /**
     * A version of waitForRepNodeAdmin where the Topology and RepNodeId are
     * known.  Derive the rest.
     */
    public static RepNodeAdminAPI waitForRepNodeAdmin(
        Topology topology,
        RepNodeId rnid,
        LoginManager loginMgr,
        long timeoutSec,
        ServiceStatus[] targetStatus)
        throws Exception {

        RepNode rn = topology.get(rnid);
        StorageNode sn = topology.get(rn.getStorageNodeId());
        return waitForRepNodeAdmin(topology.getKVStoreName(),
                                   sn.getHostname(),
                                   sn.getRegistryPort(),
                                   rnid,
                                   sn.getStorageNodeId(),
                                   loginMgr,
                                   timeoutSec,
                                   targetStatus);

    }

    /**
     * Get and wait for a CommandService handle to reach the requested status.
     * Treat UNREACHABLE as "any" and return once the handle is acquired.
     */
    @SuppressWarnings("null")
    public static CommandServiceAPI waitForAdmin(String hostname,
                                                 int registryPort,
                                                 LoginManager loginMgr,
                                                 long timeoutSec,
                                                 ServiceStatus targetStatus)
        throws Exception {

        Exception exception = null;
        CommandServiceAPI admin = null;
        ServiceStatus status = null;

        long limitMs = System.currentTimeMillis() + 1000 * timeoutSec;

        while (System.currentTimeMillis() <= limitMs) {

            /**
             * The stub may be stale, get it again on exception.
             */
            if (exception != null) {
                admin = null;
            }
            try {
                if (admin == null) {
                    admin = RegistryUtils.getAdmin(hostname, registryPort,
                                                   loginMgr);
                }

                status = admin.ping() ;

                /**
                 * Treat UNREACHABLE as "any".
                 */
                if (targetStatus == ServiceStatus.UNREACHABLE) {
                    return admin;
                }
                if (status == targetStatus) {
                    return admin;
                }
                exception = null;
            } catch (RemoteException e) {
                exception = e;
            } catch (NotBoundException e) {
                exception = e;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }

        if (status != null) {
            throw new IllegalStateException("Admin status: " + status +
                                            "Target status: " + targetStatus);
        }
        throw exception;
    }

    /**
     * Get and wait for a ArbNodeAdmin handle to reach one of the states in
     * the ServiceStatus array parameter.
     */
    @SuppressWarnings("null")
    public static ArbNodeAdminAPI
        waitForArbNodeAdmin(String storeName,
                            String hostName,
                            int port,
                            ArbNodeId arid,
                            StorageNodeId snid,
                            LoginManager loginMgr,
                            long timeoutSec,
                            ServiceStatus[] targetStatus)
                            throws Exception {

        Exception exception = null;
        ArbNodeAdminAPI anai = null;
        ServiceStatus status = null;

        long limitMs = System.currentTimeMillis() + 1000 * timeoutSec;

        while (System.currentTimeMillis() <= limitMs) {

            /**
             * The stub may be stale, get it again on exception.
             */
            if (exception != null) {
                anai = null;
            }
            try {
                if (anai == null) {
                    anai = RegistryUtils.getArbNodeAdmin
                        (storeName, hostName, port, arid, loginMgr);
                }
                status = anai.ping().getServiceStatus();
                for (ServiceStatus tstatus : targetStatus) {
                    /**
                     * Treat UNREACHABLE as "any".
                     */
                    if (tstatus == ServiceStatus.UNREACHABLE) {
                        return anai;
                    }
                    if (status == tstatus) {
                        return anai;
                    }
                }
                exception = null;
            } catch (RemoteException e) {
                exception = e;
            } catch (NotBoundException e) {
                exception = e;
            }

            /*
             * Check now for any process startup problems before
             * sleeping.
             */
            if (snid != null) {
                RegistryUtils.checkForStartupProblem(storeName,
                                                     hostName,
                                                     port,
                                                     arid,
                                                     snid,
                                                     loginMgr);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }

        if (status != null) {
            throw new IllegalStateException
                ("ARB current status: " + status + " target status: " +
                 Arrays.toString(targetStatus));
        }
        throw exception;
    }

    /**
     * Try to ping the service to determine status.  Returns ServiceStatus
     * and returns ServiceStatus.UNREACHABLE in all failure paths.
     * @param rid resource Id, only StorageNodeId, RepNodeId and ArbNodeId are
     * supported.
     * @throws IllegalArgumentException if specified resource id is invalid or
     * unsupported.
     */
    public static ServiceStatus ping(ResourceId rid, Topology topology) {

        final RegistryUtils regutils =
            new RegistryUtils(topology, null/* loginManager */);

        ServiceStatus serviceStatus = null;
        try {
            if (rid instanceof StorageNodeId) {
                final StorageNodeAgentAPI sna =
                    regutils.getStorageNodeAgent((StorageNodeId)rid);
                final StorageNodeStatus status = sna.ping();
                serviceStatus = status.getServiceStatus();
            } else if (rid instanceof RepNodeId) {
                final RepNodeAdminAPI rna =
                    regutils.getRepNodeAdmin((RepNodeId)rid);
                final RepNodeStatus status = rna.ping();
                serviceStatus = status.getServiceStatus();
            } else if (rid instanceof ArbNodeId) {
                final ArbNodeAdminAPI ana =
                    regutils.getArbNodeAdmin((ArbNodeId)rid);
                final ArbNodeStatus status = ana.ping();
                serviceStatus = status.getServiceStatus();
            } else {
                throw new IllegalArgumentException(
                    "Unsupported or invalid resource Id " + rid);
            }
        } catch (IllegalArgumentException iae) {
            throw iae;
        } catch (Exception e) {
            /* ignored, see below */
        }

        if (serviceStatus != null) {
            return serviceStatus;
        }

        /*
         * Exceptions or service status as null imply that the service does not
         * exist so mark it as UNREACHABLE as does the Ping command.
         */
        return ServiceStatus.UNREACHABLE;
    }
}

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
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Locates a current Topology given a list of SN registry locations
 */
public class TopologyLocator {

    public static final String HOST_PORT_SEPARATOR = ":";

    /**
     * Obtains a current topology from the SNs identified via
     * <code>registryHostPorts</code>.
     * <p>
     * The location of the Topology is done as a two step process.
     * <ol>
     * <li>It first identifies an initial Topology that is the most up to date
     * one know to the RNs hosted by the supplied SNs.</li>
     * <li>
     * If <code>maxRNs</code> > 0 it then uses the initial Topology to further
     * search some bounded number of RNs, for an even more up to date Topology
     * and returns it.</li>
     * </ol>
     *
     * @param registryHostPorts one or more strings containing the registry
     * host and port associated with the SN. Each string has the format:
     * hostname:port.
     *
     * @param maxRNs the maximum number of RNs to examine for an up to date
     * Topology. If maxRNs == 0 then the initial topology is returned.
     *
     * @param loginMgr a login manager that will be used to supply login tokens
     * to api calls.
     */
    public static Topology get(String[] registryHostPorts,
                               int maxRNs,
                               LoginManager loginMgr,
                               String expectedStoreName)
        throws KVStoreException {

        /* The highest topo seq # found */
        final AtomicInteger maxTopoSeqNum = new AtomicInteger(0);

        final Topology initialTopology = getInitialTopology(registryHostPorts,
                                                            maxTopoSeqNum,
                                                            loginMgr,
                                                            expectedStoreName);
        assert maxTopoSeqNum.get() > 0;

        if (maxRNs <= 0) {
            return initialTopology;
        }

        /* Keep the pool large enough to minimize the odds of a stall */
        final int poolSize = Math.max(maxRNs, 10);
        final ThreadPoolExecutor executor =
                 new ThreadPoolExecutor(poolSize, // corePoolSize
                                        poolSize, // maximumPoolSize - ignored
                                        0L, TimeUnit.MILLISECONDS,
                                        new  LinkedBlockingQueue<Runnable>(),
                                        new KVThreadFactory(" topology locator",
                                                            null));

        /* The number of RNs to check */
        final AtomicInteger nRNs = new AtomicInteger(maxRNs);

        /*
         * Now use the initial Topology to find an even more current version
         * if it exists.
         */
        final AtomicReference<RepNodeAdminAPI> currentAdmin =
                                new AtomicReference<RepNodeAdminAPI>();

        final RegistryUtils registryUtils =
            new RegistryUtils(initialTopology, loginMgr);

        for (RepGroup rg : initialTopology.getRepGroupMap().getAll()) {
            for (final RepNode rn : rg.getRepNodes()) {

                executor.submit(new Runnable() {

                @Override
                public void run() {

                    if (nRNs.get() <= 0) {
                        return;
                    }
                    try {
                        RepNodeAdminAPI admin =
                            registryUtils.getRepNodeAdmin(rn.getResourceId());
                        int seqNum = admin.getTopoSeqNum();
                        synchronized (maxTopoSeqNum) {
                            if (seqNum > maxTopoSeqNum.get()) {
                                maxTopoSeqNum.set(seqNum);
                                currentAdmin.set(admin);
                            }
                        }
                        nRNs.decrementAndGet();
                    } catch (SessionAccessException sae) {
                    } catch (RemoteException re) {
                    } catch (NotBoundException nbe) { }
                }});
            }
        }
        executor.shutdown();

        /*
         * Wait until the required number of RNs have been checked or all tasks
         * have run.
         */
        while (nRNs.get() > 0) {
            try {
                if (executor.awaitTermination(100L, TimeUnit.MILLISECONDS)) {
                    break;
                }
            } catch (InterruptedException ie) { }
        }
        executor.shutdownNow();

        /* If a newer topo was found attempt to get it and return */
        if (currentAdmin.get() != null) {
            try {
                return currentAdmin.get().getTopology();
            } catch (RemoteException re) { }
        }

        /*
         * If there was an issue getting the latest, or none newer were found
         * return the initial topo.
         */
        return initialTopology;
    }

    /**
     * A convenience method to locate the initial topology, specifying a single
     * host/port
     * @param hostname
     * @param port
     * @return the initial topology that is found.
     * @throws KVStoreException
     */
    public static Topology get(String hostname,
                               int port) throws KVStoreException {
        String[] hostports = new String[1];
        hostports[0] = hostname + HOST_PORT_SEPARATOR + port;
        return get(hostports, 0, null, null);
    }

    /**
     * Locates an initial Topology based upon the supplied SNs. This method
     * is intended for use in the KVS client and meets the client side
     * exception interfaces.
     */
    private static Topology getInitialTopology(String[] registryHostPorts,
                                              final AtomicInteger maxTopoSeqNum,
                                              final LoginManager loginMgr,
                                              final String expectedStoreName)
        throws KVStoreException {

        /* The exception cause collector */
        final AtomicReference<Throwable> cause =
            new AtomicReference<Throwable>();

        /* The RN admin which has the higest topo */
        final AtomicReference<RepNodeAdminAPI> currentAdmin =
            new AtomicReference<RepNodeAdminAPI>();

        applyToRNs(registryHostPorts,
                   expectedStoreName,
                   "topology locator",
                   loginMgr,
                   cause,
                   new RNAdminCallback() {

                    @Override
                    public void callback(RepNodeAdminAPI rnAdmin)
                        throws RemoteException {

                        final int seqNum = rnAdmin.getTopoSeqNum();

                        synchronized (maxTopoSeqNum) {
                            if (seqNum > maxTopoSeqNum.get()) {
                                maxTopoSeqNum.set(seqNum);
                                currentAdmin.set(rnAdmin);
                            }
                        }
                    }
                });

        /*
         * If no initial topology was found, can't continue so throw an
         * exception
         */
        if (currentAdmin.get() == null) {

            /* If there was already a KVSecurityException, throw that */
            if (cause.get() instanceof KVSecurityException) {
                throw (KVSecurityException)cause.get();
            }

            /* If there was already a FaultException, throw that */
            if (cause.get() instanceof FaultException) {
                throw (FaultException)cause.get();
            }
            throw new KVStoreException
                ("Could not contact any RepNode at: " +
                 Arrays.toString(registryHostPorts), cause.get());
        }

        try {
            return currentAdmin.get().getTopology();
        } catch (RemoteException e) {
            throw new KVStoreException
                ("Could not establish an initial Topology from: " +
                 Arrays.toString(registryHostPorts), cause.get());
        } catch (InternalFaultException e) {
            /* Clients expect FaultException */
            throw new FaultException(e, false);
        }
    }

    public static void applyToRNs(final String[] registryHostPorts,
                                  final String expectedStoreName,
                                  final String threadFactoryName,
                                  final LoginManager loginMgr,
                                  final AtomicReference<Throwable> cause,
                                  final RNAdminCallback rnAdmincallback) {

        final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(0, registryHostPorts.length,
                                   0L, TimeUnit.MILLISECONDS,
                                   new SynchronousQueue<Runnable>(),
                                   new KVThreadFactory(" " + threadFactoryName,
                                                       null));
        for (final String registryHostPort : registryHostPorts) {

            executor.submit(new Runnable() {

            @Override
            public void run() {
                final HostPort hostPort = HostPort.parse(registryHostPort);
                final String registryHostname = hostPort.hostname();
                final int registryPort = hostPort.port();

                try {
                    final Registry snRegistry =
                                   RegistryUtils.getRegistry(registryHostname,
                                                             registryPort,
                                                             expectedStoreName);

                    for (String serviceName : snRegistry.list()) {

                        try {

                            /**
                             * Skip things that don't look like RepNodes (this
                             * is for the client).
                             */
                            if (!RegistryUtils.isRepNodeAdmin(serviceName)) {
                                continue;
                            }

                            final Remote stub = snRegistry.lookup(serviceName);
                            if (!(stub instanceof RepNodeAdmin)) {
                                continue;
                            }
                            final LoginHandle loginHdl =
                                (loginMgr == null) ?
                                 null :
                                 loginMgr.getHandle(new HostPort(
                                     registryHostname, registryPort),
                                     ResourceType.REP_NODE);
                            final RepNodeAdminAPI admin = RepNodeAdminAPI.
                                wrap((RepNodeAdmin) stub, loginHdl);
                            rnAdmincallback.callback(admin);
                        } catch (SessionAccessException e) {
                            cause.set(e);
                        } catch (RemoteException re) {
                            cause.set(re);
                        } catch (NotBoundException e) {
                            /*
                             * Should not happen since we are iterating over a
                             * bound list.
                             */
                            cause.set(e);
                        } catch (InternalFaultException e) {
                            /*
                             * Be robust even in the presence of internal faults
                             * Keep trying with other functioning nodes.
                             * Preserve non-fault exception as the reason
                             * if one's already present.
                             */
                            cause.compareAndSet(null, e);
                        } catch (Throwable e) {
                            cause.compareAndSet(null, e);
                        }
                    }
                } catch (RemoteException e) {
                    cause.compareAndSet(null, e);
                }
            }});
        }

        /* Wait until all of the tasks have finished */
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.MINUTES); // Nearly forever
        } catch (InterruptedException ie) {
            /* Leave a more meaningful exception if one is present */
            cause.compareAndSet(null, ie);
        }
    }

    public interface RNAdminCallback {
        void callback(RepNodeAdminAPI rnAdmin) throws RemoteException;
    }
}

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

package oracle.kv.impl.pubsub;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeInfo;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.pubsub.PublisherFailureException;

/**
 * Object that manages all topology related information of kvstore. The
 * manager is created and initialized by NoSQLPublisher and shared by all
 * subscriptions spawned by the publisher.
 */
public class PublisherTopoManager {

    private final static int CHECK_VERSION_RETRY_SLEEP_MS = 500;
    /* private logger */
    private final Logger logger;
    /* store name */
    private final String storeName;
    /* handle to store from publisher */
    private final KVStoreImpl kvs;
    /* handle to store topology */
    private final Topology topology;

    /*
     * master RN id and its HA address for each shard, only master that
     * supports streaming will be put in the map
     */
    private final
    ConcurrentHashMap<RepGroupId, ShardMasterInfo> masterInfoMap;

    /* test only, true if in some special unit test */
    private static boolean inSpecialUnitTest = false;

    /**
     * Creates an instance of toplogy manager and initialize the master HA
     * hostport for each shard
     *
     * @param storeName   name of source kvstore
     * @param kvs         handle to source kvstore
     * @param logger      private logger
     *
     * @throws PublisherFailureException raised when fail to initialize the
     * master HA hostport for each shard
     */
    public PublisherTopoManager(String storeName, KVStore kvs, Logger logger)
        throws PublisherFailureException {

        this.storeName = storeName;
        this.kvs = (KVStoreImpl) kvs;
        this.logger = logger;
        topology = ((KVStoreImpl) kvs).getTopology();

        /* initialize master RN HA for each shard */
        masterInfoMap = new ConcurrentHashMap<>();
        for (RepGroupId rid : topology.getRepGroupIds()) {
            try {
                final ShardMasterInfo info = buildMasterInfo(rid, null);
                if (info != null) {
                    masterInfoMap.put(rid, info);
                }
            } catch (FaultException fe) {
                final String msg = "Cannot create topology manager because " +
                                   fe.getMessage();
                throw new PublisherFailureException(msg, true, fe);
            }
        }
    }

    /**
     * Returns master RN HA address for a shard. If the master RN HA has been
     * refreshed and different from current one, returns the refreshed master
     * HA hostport. Otherwise, refresh and return the updated one.
     *
     * @param gid        id of given shard
     * @param prev       previous cached master ha hostport
     *
     * @return a refreshed master HA host port for a shard, or null if cannot
     * find new master or the new master does not support streaming.
     */
    synchronized ShardMasterInfo buildMasterInfo(RepGroupId gid,
                                                 ShardMasterInfo prev)
        throws IllegalArgumentException, FaultException {

        logger.fine(lm("Try get master for " + gid + ", curr: " + prev));

        final ShardMasterInfo exist = masterInfoMap.get(gid);
        if (exist != null) {
            /* no prev master, return whatever we have */
            if (prev == null) {
                logger.info(lm("No previous master, return cached master: " +
                               exist));
                return exist;
            }
            /* already have a newer master, return it */
            if (exist.getTimestamp() > prev.getTimestamp()) {
                logger.info(lm("Already has a newer master for " + gid +
                               ": " + exist));
                return exist;
            }
        }

        final long start = System.currentTimeMillis();
        logger.info(lm("Need refresh master info for " + gid));

        /* refresh if not exists or stale */
        final ShardMasterInfo ret = refreshMasterInfo(gid);

        if (ret == null) {
            logger.info(lm("Fail to refresh master for shard " + gid));
            masterInfoMap.remove(gid);
            return null;
        }

        /* check if server supports streaming */
        final RegistryUtils regUtil = kvs.getDispatcher().getRegUtils();
        if (regUtil == null) {
            final String msg = "Cannot check, the request dispatcher has not" +
                               " initialized itself yet.";
            logger.info(lm(msg));
            masterInfoMap.remove(gid);
            return null;
        }

        final RepNodeId rnId = ret.getMasterRepNodeId();
        try {
            if (!supportsStreaming(regUtil, rnId, 3)) {
                /* RN does not support streaming */
                final String err = "Master node " + rnId +
                                   " does not support streaming";
                logger.warning(lm(err));
                masterInfoMap.remove(gid);
                /* caller should deal with it  */
                throw new FaultException(err, false);
            }
        } catch (RemoteException | NotBoundException | InterruptedException e) {
            /* fail to check */
            final String err = "Cannot verify if node " + rnId +
                               " supports streaming, reason: " + e.getMessage();
            logger.info(lm(err));
            masterInfoMap.remove(gid);
            return null;
        }

        /* everything looks good if reach here */
        logger.info(lm("Succeed refresh master for shard " + gid +
                       ": " + ret +
                       ", elapsed time in ms: " +
                       (System.currentTimeMillis() - start)));
        masterInfoMap.put(gid, ret);
        return ret;
    }

    /**
     * Returns true if the software running on RN supports streaming
     *
     * @param regUtil registry util
     * @param rid     node id to check
     * @param maxTry  max # tries
     *
     * @return true if RN supports streaming, false otherwise
     *
     * @throws RemoteException if unable to reach the RN
     * @throws NotBoundException if fail to look up in registry
     * @throws InterruptedException if interrupted
     */
    public boolean supportsStreaming(RegistryUtils regUtil,
                                     RepNodeId rid,
                                     int maxTry)
        throws RemoteException, NotBoundException, InterruptedException {

        int count = 0;
        while(true) {
            try {
                count++;
                /*
                 * for all pre-4.5, stream is not supported
                 *
                 * for 4.5, the only release is EE and stream is supported,
                 * note the edition is null for this release.
                 *
                 * since 4.6, only EE server allows streaming
                 */
                final KVVersion ver = getServerVersion(regUtil, rid);
                if (ver.compareTo(KVVersion.R4_5) < 0) {
                    /* pre-4.5 */
                    final String err = "Software on RN " + rid +
                                       " does not support streaming. " +
                                       "Streaming requires minimal version " +
                                       KVVersion.R4_5.getVersionString() +
                                       ", while the version is " +
                                       ver.getVersionString();
                    logger.info(lm(err));
                    return false;
                }

                final String edition = ver.getReleaseEdition();
                if (edition == null || "Enterprise".equals(edition)) {
                    return true;
                }

                final String err = "Software on RN " + rid +
                                   " does not support streaming.  Streaming" +
                                   " requires the Enterprise edition, but " +
                                   "server edition found: " + edition;
                logger.info(lm(err));
                return false;
            } catch (RemoteException | NotBoundException e) {
                /*
                 * cannot start stream before we are sure the server edition
                 * supports streaming. If the shard is offline for long time,
                 * we would return false after max retries and the caller
                 * should retry.
                 */
                if (count < maxTry ) {
                    logger.info(lm("Cannot ping node " + rid +
                                   ", will retry after " +
                                   CHECK_VERSION_RETRY_SLEEP_MS +
                                   " ms, reason: " + e.getMessage()));
                    try {
                        Thread.sleep(CHECK_VERSION_RETRY_SLEEP_MS);
                    } catch (InterruptedException ie) {
                        logger.fine(lm("Sleep interrupted, give up"));
                        throw ie;
                    }
                } else {
                    logger.info(lm("Cannot ping node " + rid +
                                   " after " + count + " attempts, give up."));
                    throw e;
                }
            }
        }
    }

    /**
     * Returns the server software version for given rep node id
     *
     * @param regUtil registry util
     * @param rid     rep node id
     *
     * @return version of the server running on given rep node
     *
     * @throws RemoteException  if unable to reach server
     * @throws NotBoundException if unable to look up the sna in registry
     */
    private KVVersion getServerVersion(RegistryUtils regUtil, RepNodeId rid)
        throws RemoteException, NotBoundException {

        /*
         * normally we shall ping sn for version, but for some unit test
         * using KVRepTestConfig which does not seem have a full-blown
         * store, we could not ping sna for these tests, instead, we ping RN.
         */
        if (inSpecialUnitTest) {
            final RepNodeAdminAPI api = regUtil.getRepNodeAdmin(rid);
            final RepNodeInfo info = api.getInfo();
            return info.getSoftwareVersion();
        }

        /* normal cases, ping sn */
        final StorageNodeId snId = regUtil.getTopology()
                                          .get(rid)
                                          .getStorageNodeId();
        return regUtil.getStorageNodeAgent(snId).ping().getKVVersion();
    }

    /* Refreshes shard master info */
    private ShardMasterInfo refreshMasterInfo(RepGroupId gid) {

        logger.fine(lm("Try refresh master for " + gid));

        /* invalid shard id */
        if (!topology.getRepGroupIds().contains(gid)) {
            final String err = "Shard " + gid + " does not exist in topology " +
                               "of store " + storeName + " with topology id " +
                               kvs.getTopology().getId() + ", all existing " +
                               "shards " + Arrays.toString(topology
                                                               .getRepGroupIds()
                                                               .toArray());
            throw new PublisherFailureException(err, false, null);
        }

        /* try to get it from dispatcher */
        ShardMasterInfo ret =
            ShardMasterInfo.buildFromDispatcher(kvs.getDispatcher(),
                                                gid, logger);
        if (ret != null) {
            logger.info(lm("Refreshed master from request dispatcher for " +
                           gid));
            return ret;
        }

        /* try get master by ping collector */
        ret = ShardMasterInfo.buildFromPingColl(kvs, gid);
        if (ret == null) {
            logger.info(lm("Ping collector is unable locate the master for " +
                           "shard " + gid));
        } else {
            logger.info(lm("Refreshed master from ping collector for shard " +
                           gid));
        }
        return ret;
    }
    /**
     * Test only, set if in some special unit tests
     */
    public static void setInSpecialUnitTest(boolean in) {
        inSpecialUnitTest = in;
    }

    private String lm(String msg) {
        return "[TopoMan-" + storeName + "] " + msg;
    }
}

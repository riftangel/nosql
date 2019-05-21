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

package oracle.kv.impl.api;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;

/**
 * Factory class used to produce handles to an existing KVStore.
 * This factory class in intended for internal use.
 */
public class KVStoreInternalFactory {

    /**
     * Get a handle to an existing KVStore for internal use.
     *
     * @param config the KVStore configuration parameters.
     * @param dispatcher a KVStore request dispatcher
     * @param logger a Logger instance
     * @throws IllegalArgumentException if an illegal configuration parameter
     * is specified.
     */
    public static KVStore getStore(KVStoreConfig config,
                                   RequestDispatcher dispatcher,
                                   LoginManager loginMgr,
                                   Logger logger)
        throws IllegalArgumentException {

        final long requestTimeoutMs =
                config.getRequestTimeout(TimeUnit.MILLISECONDS);
        final long readTimeoutMs =
            config.getSocketReadTimeout(TimeUnit.MILLISECONDS);
        if (requestTimeoutMs > readTimeoutMs) {
            final String format = "Invalid KVStoreConfig. " +
                "Request timeout: %,d ms exceeds " +
                "socket read timeout: %,d ms" ;
            throw new IllegalArgumentException
                (String.format(format, requestTimeoutMs, readTimeoutMs));
        }

        RegistryUtils.setRegistrySocketTimeouts
            ((int) config.getRegistryOpenTimeout(TimeUnit.MILLISECONDS),
             (int) config.getRegistryReadTimeout(TimeUnit.MILLISECONDS),
             config.getStoreName());

        final String csfName =
            ClientSocketFactory.factoryName(config.getStoreName(),
                                            RepNodeId.getPrefix(),
                                            InterfaceType.MAIN.
                                            interfaceName());
        final int openTimeoutMs =
            (int) config.getSocketOpenTimeout(TimeUnit.MILLISECONDS);

        ClientSocketFactory.configureStoreTimeout
            (csfName, openTimeoutMs, (int) readTimeoutMs);

        /*
         * construct a KVStore that is constrained in the usual way
         * w.r.t. internal namespace access.
         */
        return new KVStoreImpl(logger, dispatcher, config, loginMgr);
    }


    /**
     * Obtain a KVStore handle for internal use by an RN. The handle tries to
     * be lightweight primarily by avoiding the overhead associated with a
     * RequestDispatcher.
     *
     * It's different from a regular KVS handle in the following ways:
     *
     * 1) It reuses the RequestDispatcher that is part of the RepNodeService
     *
     * 2) The close method does not shutdown the dispatcher
     *
     * @param repNodeService the RN service associated with the KVS handle
     *
     * @param logger the logger to be associated with the handle
     *
     * @return the lightweight KVS handle
     */
    public static KVStore getStore(RepNodeService repNodeService,
                                   Logger logger) {
        final String storeName =
            repNodeService.getParams().getGlobalParams().getKVStoreName();

        final KVStoreConfig config =
            new KVStoreConfig(storeName,
                              "unknownhost:0" /* Don't need a host */ );

        return getStore(config,
                        repNodeService.getRepNode().getRequestDispatcher(),
                        repNodeService.getLoginManager(),
                        logger);
    }
}

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

package oracle.kv;

import java.lang.Thread.UncaughtExceptionHandler;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.KVStoreLogin.StoreLoginCredentialsProvider;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;

/**
 * Factory class used to produce handles to an existing KVStore.
 */
public class KVStoreFactory {

    /**
     * The name of the system property that can be set to specify the number of
     * threads to use for the async endpoint group.  The number should be an
     * integer greater than zero.  If this property is not set, the async
     * endpoint group uses twice the number of available processors as
     * determined by calling {@link Runtime#availableProcessors}.
     *
     * @hidden For internal use only - part of async API
     */
    public static final String ENDPOINT_GROUP_NUM_THREADS_PROPERTY =
        "oracle.kv.async.endpoint.group.num.threads";

    private static final SecureRandom secureRandom = new SecureRandom();

    /**
     * Get a handle to an existing KVStore. Note that the KVStore itself can
     * only be created via the KV administration console or API. The
     * application must invoke {@link KVStore#close}, when it is done accessing
     * the store, to free up resources associated with the handle.
     *
     * @param config the KVStore configuration parameters.
     *
     * @throws IllegalArgumentException if an illegal configuration parameter
     * is specified.
     *
     * @see KVStore#close
     */
    public static KVStore getStore(KVStoreConfig config)
        throws FaultException {

        return getStore(config, null, null);
    }

    /**
     * Get a handle to an existing KVStore, with optional authentication
     * arguments for accessing a secure KVStore instance. Note that the KVStore
     * itself can only be created via the KV administration console or API. The
     * application must invoke {@link KVStore#close}, when it is done accessing
     * the store, to free up resources associated with the handle.
     * <p>
     * If no {@link LoginCredentials} are provided in this call, this method
     * will attempt to locate credentials through other sources, in the
     * following search order.
     * <ol>
     * <li>KVStoreConfig.getLoginProperties()</li>
     * <li>A login file referenced by the <code>oracle.kv.security</code> Java
     * system property.</li>
     * </ol>
     *
     * @param config the KVStore configuration parameters.
     * @param creds the KVStore user login credentials. If null, the KVStore
     * client will attempt to locate credentials based on Java system
     * properties.
     * @param reauthHandler an optional re-authentication handler to be used in
     * the event a login session expires and must be renewed. If both creds and
     * reauthHandler are null, but login information is located using either of
     * the above lookup methods, an internally-supplied reauthentication
     * handler is automatically provided which will re-read login credentials
     * as needed for reauthentication. User passwords are not retained in
     * memory by the KVStore client, so if you explicitly provide
     * LoginCredentials, you are also responsible for supplying a
     * reauthentication handler, if desired.
     * @throws IllegalArgumentException if an illegal configuration parameter
     * is specified.
     *
     * @see KVStore#close
     */
    public static KVStore getStore(KVStoreConfig config,
                                   LoginCredentials creds,
                                   ReauthenticateHandler reauthHandler)
        throws FaultException {

        final Properties securityProps = config.getSecurityProperties();

        /*
         * If login credentials is not specified, try to get it from login
         * properties
         */
        final LoginCredentials loginCreds =
            creds == null ?
            KVStoreLogin.makeLoginCredentials(securityProps) :
            creds;

        /*
         * If reauthenticate handler is not specified, try to get it using the
         * loginProps
         */
        final ReauthenticateHandler loginReauthHandler =
            reauthHandler == null ?
            KVStoreLogin.makeReauthenticateHandler(
                new StoreLoginCredentialsProvider(securityProps)) :
            reauthHandler;

        return getStoreInternal(config, loginCreds, loginReauthHandler,
                                securityProps);
    }

    /**
     * Get a handle to an existing KVStore. Note that the KVStore itself can
     * only be created via the KV administration console or API.
     *
     * @param config the KVStore configuration parameters.
     * @param loginCreds the KVStore user login credentials.
     * @param reauthHandler a re-authentication handler to be use in the event
     * a login session expires and must be renewed.
     * @param securityProps a set of security properties, which should be used
     * for determining the communication transport, if non-null
     * @throws IllegalArgumentException if an illegal configuration parameter
     * is specified.
     */
    private static KVStore getStoreInternal(KVStoreConfig config,
                                            LoginCredentials loginCreds,
                                            ReauthenticateHandler reauthHandler,
                                            Properties securityProps)
        throws FaultException {

        final ClientId clientId;
        synchronized (secureRandom) {
            clientId = new ClientId(secureRandom.nextLong());
        }

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

        final RequestDispatcher handler;
        final Logger logger =
            ClientLoggerUtils.getLogger(KVStore.class, clientId.toString());

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        try {
            ClientSocketFactory.setRMIPolicy(securityProps,
                                             config.getStoreName());
        } catch (RuntimeException rte) {
            throw new FaultException(rte, false /* isRemote */);
        }

        /*
         * Initialize the async endpoint group.  All KVStore instances will
         * share a single endpoint group, much as there is a single RMI
         * runtime.
         */
        try {
            final int numThreads = Integer.getInteger(
                ENDPOINT_GROUP_NUM_THREADS_PROPERTY,
                2 * Runtime.getRuntime().availableProcessors());
            if (numThreads < 1 ) {
                throw new IllegalStateException(
                    "The " + ENDPOINT_GROUP_NUM_THREADS_PROPERTY +
                    " must be set to a value greater than 0, found: " +
                    numThreads);
            }
            AsyncRegistryUtils.initEndpointGroup(
                ClientLoggerUtils.getLogger(KVStoreFactory.class,
                                            "endpoint-group"),
                numThreads);
        } catch (RuntimeException rte) {
            throw new FaultException(rte, false /* isRemote */);
        }

        RepNodeLoginManager loginMgr = null;
        try {

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

            final KVSHandler exceptionHandler = new KVSHandler(logger);

            if (loginCreds != null) {
                loginMgr = new RepNodeLoginManager(loginCreds.getUsername(),
                                                   true);

                if (loginCreds instanceof KerberosCredentials) {

                    /* Retrieve client Kerberos credentials from KDC */
                    loginCreds = KVStoreLogin.getKrbClientCredentials(
                        (KerberosCredentials) loginCreds);
                }
                loginMgr.bootstrap(config.getHelperHosts(),
                                   loginCreds, config.getStoreName());
            }

            handler = RequestDispatcherImpl.createForClient(
                config, clientId, loginMgr, exceptionHandler, logger);

            if (loginMgr != null) {
                loginMgr.setTopology(handler.getTopologyManager());

                if (loginCreds instanceof KerberosCredentials) {
                    loginMgr.locateKrbPrincipals(config.getHelperHosts(),
                                                 config.getStoreName());
                }
            }

        } catch (KVStoreException e) {
            throw new FaultException(e, false /*isRemote*/);
        }

        return new KVStoreImpl(logger, handler, config,
                               loginMgr, reauthHandler);
    }

    /**
     * Just log the uncaught exception for now. In future we may want to
     * invalidate the handle as well and have the client re-establish it.
     */
    private static class KVSHandler implements UncaughtExceptionHandler {
        final Logger logger;

        public KVSHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.log(Level.SEVERE,
                       "Uncaught exception in thread:" + t.getName(), e);
        }
    }
}

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

package oracle.kv.impl.async.registry;

import static oracle.kv.impl.async.StandardDialogTypeFamily.SERVICE_REGISTRY_DIALOG_TYPE;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.AsyncVersionedRemoteAPI;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.registry.ServiceRegistry.RegistryMethodOp;
import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * The API for the service registry, which maps service names to service
 * endpoints.  The service registry is the asynchronous replacement for the RMI
 * registry used for synchronous operations.  This is the API that clients use
 * to register, unregister, lookup, and list the available services.  The
 * {@link ServiceRegistry} interface represents the remote interface that is
 * used to communicate requests over the network.  The {@link
 * ServiceRegistryImpl} class provides the server-side implementation.  The
 * {@link ServiceEndpoint} class is used to represent information about an
 * available remote service.
 *
 * @see ServiceRegistry
 * @see ServiceRegistryImpl
 * @see ServiceEndpoint
 */
public class ServiceRegistryAPI extends AsyncVersionedRemoteAPI {
    private final ServiceRegistry proxyRemote;

    /**
     * Creates a new instance for the specified server and serial version.
     */
    protected ServiceRegistryAPI(ServiceRegistry remote,
                                 short serialVersion) {
        super(serialVersion);
        proxyRemote = remote;
    }

    /**
     * Makes an asynchronous request to create an instance of this class,
     * returning the result to {@code handler}.
     *
     * @param endpoint the remote endpoint representing the server
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param logger for debug logging
     * @param handler the result handler
     */
    public static void wrap(CreatorEndpoint endpoint,
                            long timeoutMillis,
                            Logger logger,
                            ResultHandler<ServiceRegistryAPI> handler) {
        Factory.INSTANCE.wrap(endpoint, timeoutMillis, logger, handler);
    }

    /**
     * A factory class for obtain ServiceRegistryAPI instances.
     */
    public static class Factory {

        static final Factory INSTANCE = new Factory();

        /**
         * Create a ServiceRegistryAPI instance.
         */
        protected ServiceRegistryAPI createAPI(ServiceRegistry remote,
                                               short serialVersion) {
            return new ServiceRegistryAPI(remote, serialVersion);
        }

        /**
         * Makes an asynchronous request to create a ServiceRegistryAPI
         * instance, returning the result to {@code handler}.
         *
         * @param endpoint the remote endpoint representing the server
         * @param timeoutMillis the timeout for the operation in milliseconds
         * @param logger for debug logging
         * @param handler the result handler
         */
        public void wrap(final CreatorEndpoint endpoint,
                         long timeoutMillis,
                         final Logger logger,
                         final ResultHandler<ServiceRegistryAPI> handler) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE,
                           "ServiceRegistryAPI.Factory.wrap called" +
                           " for endpoint {0}\n{1}",
                           new Object[] {
                               endpoint,
                               CommonLoggerUtils.getStackTrace(new Throwable())
                           });
            }
            final ServiceRegistry initiator =
                new ServiceRegistryInitiator(endpoint, logger);
            class WrapHandler implements ResultHandler<Short> {
                @Override
                public void onResult(Short serialVersion, Throwable t) {
                    if (t != null) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(
                                Level.FINE,
                                "ServiceRegistryAPI.wrap fails for endpoint " +
                                endpoint,
                                t);
                        }
                        handler.onResult(null, t);
                    } else {
                        logger.log(
                            Level.FINE,
                            "ServiceRegistryAPI.wrap returns for endpoint {0}",
                            endpoint);
                        handler.onResult(createAPI(initiator, serialVersion),
                                         null);
                    }
                }
                @Override
                public String toString() {
                    return "Handler[" +
                        "dialogType=" + SERVICE_REGISTRY_DIALOG_TYPE +
                        " methodOp=" + RegistryMethodOp.GET_SERIAL_VERSION +
                        "]";
                }
            }
            computeSerialVersion(initiator, timeoutMillis, new WrapHandler());
        }
    }

    /**
     * Look up an entry in the registry.
     *
     * @param name the name of the entry
     * @param dialogTypeFamily the expected dialog type family, or null to
     * support returning any type
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param resultHandler handler to receive the service endpoint with the
     * requested dialog type family, if specified, or {@code null} if the entry
     * is not found
     */
    public void lookup(String name,
                       final DialogTypeFamily dialogTypeFamily,
                       long timeoutMillis,
                       final ResultHandler<ServiceEndpoint> resultHandler) {
        class LookupResultHandler implements ResultHandler<ServiceEndpoint> {
            @Override
            public void onResult(ServiceEndpoint result, Throwable exception) {
                if ((result != null) && (dialogTypeFamily != null)) {
                    final DialogTypeFamily serviceDialogTypeFamily =
                        result.getDialogType().getDialogTypeFamily();
                    if (serviceDialogTypeFamily != dialogTypeFamily) {
                        resultHandler.onResult(
                            null,
                            new IllegalStateException(
                                "Unexpected dialog type family for service" +
                                " endpoint.  Expected: " + dialogTypeFamily +
                                ", found: " + serviceDialogTypeFamily));
                        return;
                    }
                }
                resultHandler.onResult(result, exception);
            }
        }
        proxyRemote.lookup(getSerialVersion(), name, timeoutMillis,
                           new LookupResultHandler());
    }

    /**
     * Set an entry in the registry.
     *
     * @param name the name of the entry
     * @param endpoint the endpoint to associate with the name
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param handler handler to call when the operation is complete
     */
    public void bind(String name,
                     ServiceEndpoint endpoint,
                     long timeoutMillis,
                     ResultHandler<Void> handler) {
        proxyRemote.bind(getSerialVersion(), name, endpoint, timeoutMillis,
                         handler);
    }

    /**
     * Remove an entry from the registry.
     *
     * @param name the name of the entry
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param handler handler to call when the operation is complete
     */
    public void unbind(String name,
                       long timeoutMillis,
                       ResultHandler<Void> handler) {
        proxyRemote.unbind(getSerialVersion(), name, timeoutMillis, handler);
    }

    /**
     * List the entries in the registry.
     *
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param handler handler to call with the list of entry names
     */
    public void list(long timeoutMillis, ResultHandler<List<String>> handler) {
        proxyRemote.list(getSerialVersion(), timeoutMillis, handler);
    }
}

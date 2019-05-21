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

package oracle.kv.impl.util.registry;

import static oracle.kv.impl.async.StandardDialogTypeFamily.ASYNC_REQUEST_HANDLER;
import static oracle.kv.impl.async.StandardDialogTypeFamily.SERVICE_REGISTRY_DIALOG_TYPE;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Logger;

import javax.net.ssl.SSLHandshakeException;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVStoreConfig;
import oracle.kv.ResultHandler;
import oracle.kv.impl.api.AsyncRequestHandlerAPI;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.registry.ServiceEndpoint;
import oracle.kv.impl.async.registry.ServiceRegistry;
import oracle.kv.impl.async.registry.ServiceRegistryAPI;
import oracle.kv.impl.async.registry.ServiceRegistryImpl;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;

/**
 * Utilities for managing asynchronous services.
 */
public class AsyncRegistryUtils extends AsyncControl {

    /**
     * The endpoint group, for connecting to dialog-layer-based services, or
     * null if not initialized.  Synchronize on the class when accessing this
     * field.
     */
    private static EndpointGroup endpointGroup = null;

    /**
     * The host name to use when registering async service endpoints for
     * services created on this host, or null if not yet set.
     *
     * @see #getServerHostName
     * @see #setServerHostName
     */
    private static String serverHostName = null;

    /** A ServiceRegistryAPI factory that wraps some exceptions. */
    private static final ServiceRegistryAPI.Factory registryFactory =
        new TranslatingExceptionsRegistryFactory();

    private AsyncRegistryUtils() {
        throw new AssertionError();
    }

    /**
     * Returns the endpoint group to use for asynchronous operations.  Throws
     * an IllegalStateException if the endpoint group is not available.
     *
     * @return the endpoint group
     * @throws IllegalStateException if the endpoint group is not available
     */
    public static synchronized EndpointGroup getEndpointGroup() {
        if (!serverUseAsync) {
            throw new IllegalStateException("Async is disabled on the server");
        }
        if (endpointGroup == null) {
            throw new IllegalStateException(
                "The async EndpointGroup is not initialized");
        }
        return endpointGroup;
    }

    /**
     * Creates a new endpoint group to use for asynchronous operations, if one
     * does not already exist.  Note that maintaining a single endpoint group
     * for the life of the JVM is necessary so that server sockets shared with
     * RMI continue to be usable, since RMI expects server sockets to remain
     * open until it closes them.
     *
     *
     * @param logger the logger to be used by the endpoint group
     * @param numThreads the number of threads
     * @throws RuntimeException if a problem occurs creating the endpoint group
     */
    public static synchronized void initEndpointGroup(Logger logger,
                                                      int numThreads) {
        checkNull("logger", logger);
        if (endpointGroup != null) {
            return;
        }
        if (!serverUseAsync) {
            return;
        }
        try {
            endpointGroup = new NioEndpointGroup(logger, numThreads);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Unexpected exception creating the async endpoint group: " +
                e.getMessage(),
                e);
        }
    }

    /**
     * Returns the host name to use when registering async service endpoints
     * for services created on this host.  Returns the value specified by the
     * most recent call to setServerHostName, if any, or else returns the host
     * name of the address returned by {@link InetAddress#getLocalHost}.
     *
     * @return the host name
     * @throws UnknownHostException if there is a problem obtaining the host
     * name of the local host
     */
    public static synchronized String getServerHostName()
        throws UnknownHostException {

        if (serverHostName != null) {
            return serverHostName;
        }
        return InetAddress.getLocalHost().getHostName();
    }

    /**
     * Sets the host name for use when registering remote objects, either RMI
     * or async ones, created on this host.
     *
     * @param hostName the server host name
     */
    public static synchronized void setServerHostName(String hostName) {

        /* Set the name used for async services */
        serverHostName = checkNull("hostName", hostName);

        /* Set the hostname to be associated with RMI stubs */
        System.setProperty("java.rmi.server.hostname", hostName);
    }

    /**
     * Get the API wrapper for the async request handler for the RN identified
     * by {@code repNodeId}. Returns {@code null} if the RN is not present in
     * the topology or if the request handler cannot be obtained within the
     * requested timeout.  Callers should include the resource ID of the
     * caller, or null if none is available.
     */
    public static void getRequestHandler(
        Topology topology,
        RepNodeId repNodeId,
        final ResourceId callerId,
        final long timeoutMs,
        final Logger logger,
        final ResultHandler<AsyncRequestHandlerAPI> resultHandler) {

        final long stopTime = System.currentTimeMillis() + timeoutMs;
        final RepNode repNode = topology.get(repNodeId);
        if (repNode == null) {
            resultHandler.onResult(null, null);
            return;
        }
        final EndpointConfig registryEndpointConfig;
        try {
            registryEndpointConfig =
                getRegistryEndpointConfig(topology.getKVStoreName());
        } catch (IOException e) {
            resultHandler.onResult(null, e);
            return;
        }
        class LookupResultHandler implements ResultHandler<ServiceEndpoint> {
            @Override
            public void onResult(ServiceEndpoint serviceEndpoint,
                                 Throwable exception) {
                if (serviceEndpoint == null) {
                    resultHandler.onResult(null, exception);
                    return;
                }
                final EndpointConfig endpointConfig;
                try {
                    endpointConfig = serviceEndpoint.getClientSocketFactory()
                        .getEndpointConfigBuilder()
                        .configId(callerId)
                        .build();
                } catch (IOException e) {
                    resultHandler.onResult(null, e);
                    return;
                }
                final long timeout = stopTime - System.currentTimeMillis();
                if (timeout < 0) {
                    resultHandler.onResult(null, null);
                    return;
                }
                AsyncRequestHandlerAPI.wrap(
                    getEndpointGroup().getCreatorEndpoint(
                        serviceEndpoint.getNetworkAddress(), endpointConfig),
                    serviceEndpoint.getDialogType(), timeout, logger,
                    resultHandler);
            }
        }
        final String bindingName = RegistryUtils.bindingName(
            topology.getKVStoreName(), repNodeId.getFullName(),
            RegistryUtils.InterfaceType.MAIN);
        class LookupRegistryResultHandler
            implements ResultHandler<ServiceRegistryAPI> {
            @Override
            public void onResult(ServiceRegistryAPI registry, Throwable e) {
                if (e != null) {
                    resultHandler.onResult(null, e);
                    return;
                }
                final long timeout = stopTime - System.currentTimeMillis();
                if (timeout < 0) {
                    resultHandler.onResult(null, null);
                    return;
                }
                registry.lookup(bindingName, ASYNC_REQUEST_HANDLER, timeout,
                                new LookupResultHandler());
            }
        }
        final StorageNode sn = topology.get(repNode.getStorageNodeId());
        getRegistry(sn.getHostname(), sn.getRegistryPort(),
                    registryEndpointConfig, timeoutMs, logger,
                    new LookupRegistryResultHandler());
    }

    private static EndpointConfig getRegistryEndpointConfig(String storeName)
        throws IOException {

        final ClientSocketFactory csf =
            RegistryUtils.getRegistryCSF(storeName);
        if (csf != null) {
            return csf.getEndpointConfig();
        }
        return new EndpointConfigBuilder()
            .option(AsyncOption.DLG_CONNECT_TIMEOUT,
                    KVStoreConfig.DEFAULT_REGISTRY_OPEN_TIMEOUT)
            .option(AsyncOption.DLG_IDLE_TIMEOUT,
                    KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT)
            .build();
    }

    private static void getRegistry(String hostname,
                                    int port,
                                    EndpointConfig endpointConfig,
                                    long timeout,
                                    Logger logger,
                                    ResultHandler<ServiceRegistryAPI> hand) {
        registryFactory.wrap(
            getEndpointGroup().getCreatorEndpoint(
                new NetworkAddress(hostname, port), endpointConfig),
            timeout, logger,
            new TranslateExceptions<ServiceRegistryAPI>(hand));
    }

    /** A ServiceRegistryAPI factory that translates exceptions. */
    private static class TranslatingExceptionsRegistryFactory
            extends ServiceRegistryAPI.Factory {
        @Override
        protected ServiceRegistryAPI createAPI(ServiceRegistry remote,
                                               short serialVersion) {
            return new ExceptionWrappingServiceRegistryAPI(
                remote, serialVersion);
        }
    }

    /**
     * A result handler that translates an underlying SSLHandshakeException
     * into an AuthenticationFailureException.
     */
    private static class TranslateExceptions<T> implements ResultHandler<T> {
        private final ResultHandler<T> handler;
        TranslateExceptions(ResultHandler<T> handler) {
            this.handler = handler;
        }
        @Override
        public void onResult(T result, Throwable exception) {
            if (exception instanceof DialogException) {
                final Exception underlyingException =
                    ((DialogException) exception).getUnderlyingException();
                if (underlyingException instanceof SSLHandshakeException) {
                    exception = new AuthenticationFailureException(
                        underlyingException);
                }
            }
            handler.onResult(result, exception);
        }
    }

    /**
     * A ServiceRegistryAPI that provides better exceptions, performing the
     * same alterations as RegistryUtils.ExceptionWrappingRegistry.
     */
    private static class ExceptionWrappingServiceRegistryAPI
            extends ServiceRegistryAPI {
        ExceptionWrappingServiceRegistryAPI(ServiceRegistry remote,
                                            short serialVersion) {
            super(remote, serialVersion);
        }
        @Override
        public void lookup(String name,
                           DialogTypeFamily family,
                           long timeout,
                           ResultHandler<ServiceEndpoint> handler) {
            super.lookup(name, family, timeout, translate(handler));
        }
        @Override
        public void bind(String name,
                         ServiceEndpoint endpoint,
                         long timeout,
                         ResultHandler<Void> handler) {
            super.bind(name, endpoint, timeout, translate(handler));
        }
        @Override
        public void unbind(String name, long timeout, ResultHandler<Void> h) {
            super.unbind(name, timeout, translate(h));
        }
        @Override
        public void list(long timeout, ResultHandler<List<String>> handler) {
            super.list(timeout, translate(handler));
        }
        private <T> ResultHandler<T> translate(ResultHandler<T> handler) {
            return new TranslateExceptions<T>(handler);
        }
    }

    /** Create an async service registry. */
    public static ListenHandle createRegistry(@SuppressWarnings("unused")
                                              String hostname,
                                              int port,
                                              ServerSocketFactory ssf,
                                              Logger logger)
        throws IOException {

        checkNull("ssf", ssf);

        if (port != 0) {
            ssf = ssf.newInstance(port);
        }

        final ServiceRegistryImpl server = new ServiceRegistryImpl(logger);
        class ServiceRegistryDialogHandlerFactory
            implements DialogHandlerFactory {
            @Override
            public DialogHandler create() {
                return server.createDialogHandler();
            }
            @Override
            public void onChannelError(ListenerConfig config,
                                       Throwable e,
                                       boolean channelClosed) {
            }
        }
        return getEndpointGroup().listen(
            ssf.getListenerConfig(),
            SERVICE_REGISTRY_DIALOG_TYPE.getDialogTypeId(),
            new ServiceRegistryDialogHandlerFactory());
    }

    /**
     * Bind a service entry in the async service registry, returning the
     * timeout allowed for the operation to complete.
     */
    public static long rebind(String hostname,
                              int registryPort,
                              String storeName,
                              final String serviceName,
                              final DialogTypeFamily dialogTypeFamily,
                              final DialogHandlerFactory dialogHandlerFactory,
                              final ClientSocketFactory clientSocketFactory,
                              final ServerSocketFactory serverSocketFactory,
                              Logger logger,
                              final ResultHandler<ListenHandle>
                              resultHandler) {
        final EndpointConfig endpointConfig;
        try {
            endpointConfig = getRegistryEndpointConfig(storeName);
        } catch (IOException e) {
            resultHandler.onResult(null, e);
            return 0;
        }
        final long timeout =
            endpointConfig.getOption(AsyncOption.DLG_IDLE_TIMEOUT).longValue();
        class ListenAndRebindResultHandler
            implements ResultHandler<ServiceRegistryAPI> {
            @Override
            public void onResult(ServiceRegistryAPI registry, Throwable e) {
                if (e != null) {
                    resultHandler.onResult(null, e);
                    return;
                }
                listenAndRebind(serviceName,
                                dialogTypeFamily,
                                dialogHandlerFactory,
                                registry,
                                timeout,
                                clientSocketFactory,
                                serverSocketFactory,
                                resultHandler);
            }
        }
        getRegistry(hostname, registryPort, endpointConfig, timeout, logger,
                    new ListenAndRebindResultHandler());

        /* Two registry operations: get registry and bind */
        return 2 * timeout;
    }

    /**
     * Establish a listener for the service and bind the service endpoint in
     * the async service registry.
     */
    private static void listenAndRebind(String serviceName,
                                        DialogTypeFamily dialogTypeFamily,
                                        DialogHandlerFactory
                                        dialogHandlerFactory,
                                        ServiceRegistryAPI registry,
                                        long timeout,
                                        ClientSocketFactory csf,
                                        ServerSocketFactory ssf,
                                        final ResultHandler<ListenHandle> rh) {
        final DialogType dialogType = new DialogType(dialogTypeFamily);
        try {
            final ListenHandle listenHandle = getEndpointGroup().listen(
                ssf.getListenerConfig(), dialogType.getDialogTypeId(),
                dialogHandlerFactory);
            class BindResultHandler implements ResultHandler<Void> {
                @Override
                public void onResult(Void ignore, Throwable exception) {
                    if (exception != null) {
                        rh.onResult(null, exception);
                    } else {
                        rh.onResult(listenHandle, null);
                    }
                }
            }
            final NetworkAddress addr = new NetworkAddress(
                getServerHostName(), listenHandle.getLocalAddress().getPort());
            registry.bind(serviceName,
                          new ServiceEndpoint(addr, dialogType, csf), timeout,
                          new BindResultHandler());
        } catch (IOException e) {
            rh.onResult(null, e);
        }
    }

    /**
     * Unbind a service entry in the async service registry, returning the
     * timeout allowed for the operation to complete.
     */
    public static long unbind(String hostname,
                              int registryPort,
                              String storeName,
                              final String serviceName,
                              Logger logger,
                              final ResultHandler<Void> resultHandler) {
        final EndpointConfig endpointConfig;
        try {
            endpointConfig = getRegistryEndpointConfig(storeName);
        } catch (IOException e) {
            resultHandler.onResult(null, e);
            return 0;
        }
        final long timeout =
            endpointConfig.getOption(AsyncOption.DLG_IDLE_TIMEOUT).longValue();
        class UnbindResultHandler
            implements ResultHandler<ServiceRegistryAPI> {
            @Override
            public void onResult(ServiceRegistryAPI registry, Throwable e) {
                if (e != null) {
                    resultHandler.onResult(null, e);
                } else {
                    registry.unbind(serviceName, timeout, resultHandler);
                }
            }
        }
        getRegistry(hostname, registryPort, endpointConfig, timeout, logger,
                    new UnbindResultHandler());

        /* Two registry operations: get registry and unbind */
        return timeout * 2;
    }
}

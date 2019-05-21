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

package oracle.kv.impl.async;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Abstract class for the endpoint group.
 */
public abstract class AbstractEndpointGroup implements EndpointGroup {

    private final Logger logger;
    /*
     * TODO:
     * Currently we do not remove items from creators, responders and
     * listenerDialogHandlerFactories. And therefore, we should use some sort
     * of weak reference technique so that they are auto-removed if no one is
     * referencing them.
     */
    /*
     * Creator and responder endpoints. Use ConcurrentHashMap for thread-safety
     * and high concurrency.
     */
    private final
        ConcurrentHashMap<CreatorKey, AbstractCreatorEndpoint> creators =
        new ConcurrentHashMap<CreatorKey, AbstractCreatorEndpoint>();
    private final
        ConcurrentHashMap<ResponderKey, AbstractResponderEndpoint> responders =
        new ConcurrentHashMap<ResponderKey, AbstractResponderEndpoint>();
    /*
     * Listeners and dialog factory maps. All access should be inside a
     * synchronization block of this endpoint group. We expect listen
     * operations are operated with low frequency.
     */
    private final HashMap<ListenerConfig, Map<Integer, DialogHandlerFactory>>
        listenerDialogHandlerFactoryMap =
        new HashMap<ListenerConfig, Map<Integer, DialogHandlerFactory>>();
    private final HashMap<ListenerConfig, AbstractListener> listeners =
        new HashMap<ListenerConfig, AbstractListener>();
    /*
     * Whether the endpoint group is shut down. Once it is set to true, it will
     * never be false again. Use volatile for thread-safety.
     */
    private volatile boolean isShutdown = false;

    protected AbstractEndpointGroup(Logger logger) {
        this.logger = logger;
    }

    public Logger getLogger() {
        return logger;
    }

    /**
     * Returns a creator endpoint.
     *
     * Gets the endpoint if exists, creates and adds the endpoint otherwise.
     *
     * TODO: currently the endpoint is never removed, we could use a data
     * structure that uses soft references to allow an endpoint to be removed
     * if no one is referencing it.
     */
    @Override
    public AbstractCreatorEndpoint
        getCreatorEndpoint(NetworkAddress address,
                           EndpointConfig endpointConfig) {

        checkNull("address", address);
        checkNull("endpointConfig", endpointConfig);
        CreatorKey key = new CreatorKey(address, endpointConfig);
        AbstractCreatorEndpoint endpoint = creators.get(key);
        if (endpoint == null) {
            endpoint = newCreatorEndpoint(address, endpointConfig);
            final AbstractCreatorEndpoint existingEndpoint =
                creators.putIfAbsent(key, endpoint);
            if (existingEndpoint != null) {
                endpoint.shutdown("Concurrent creation of endpoint", true);
                endpoint = existingEndpoint;
            }
        }
        if (isShutdown) {
            endpoint.shutdown("Endpoint group is shutdown", true);
            throw new IllegalStateException(
                    "Endpoint group is already shut down");
        }
        return endpoint;
    }

    /**
     * Returns a responder endpoint.
     */
    @Override
    public AbstractResponderEndpoint
        getResponderEndpoint(NetworkAddress remoteAddress,
                             ListenerConfig listenerConfig) {

        checkNull("remoteAddress", remoteAddress);
        checkNull("listenerConfig", listenerConfig);
        ResponderKey key = new ResponderKey(remoteAddress, listenerConfig);
        AbstractResponderEndpoint endpoint = responders.get(key);
        if (endpoint != null) {
            return endpoint;
        }
        return new AbstractResponderEndpoint.NullEndpoint(
                this, remoteAddress, listenerConfig, null);
    }

    /**
     * Adds a responder endpoint.
     *
     * The method is called when a new connection is accepted.
     */
    public void addResponderEndpoint(AbstractResponderEndpoint endpoint) {

        checkNull("endpoint", endpoint);
        ResponderKey key = new ResponderKey(
                endpoint.getRemoteAddress(), endpoint.getListenerConfig());
        final AbstractResponderEndpoint existingEndpoint =
            responders.putIfAbsent(key, endpoint);
        if (existingEndpoint != null) {
            /*
             * There is a race here. One possible case is when the old
             * connection is terminated, but the old endpoint is not removed
             * yet while the new connection is being accepted. We shut down
             * both endpoints when race happens just to be safe. Such race
             * should be very rare.
             *
             * The existing endpoint will be removed from the map after its
             * shut down and the creator side will just reconnect. The caller
             * does not need to do anything for this race.
             */
            existingEndpoint.shutdown(
                    "Race condition, should have been shut down", true);
            endpoint.shutdown(
                    "Race condition, existing one not shut down", true);
        }
    }

    /**
     * Removes a responder endpoint.
     */
    public void removeResponderEndpoint(AbstractResponderEndpoint endpoint) {

        checkNull("endpoint", endpoint);
        ResponderKey key = new ResponderKey(
                endpoint.getRemoteAddress(), endpoint.getListenerConfig());
        AbstractResponderEndpoint existingEndpoint = responders.remove(key);
        if (existingEndpoint != endpoint) {
            /*
             * There is a race here. One possible case is when the old
             * connection is terminated but not removed, a new connection was
             * accepted, failed again and is being removed. Just shut down the
             * old endpoint. Another case is a race in the addResponderEndpoint
             * could cause existingEndpoint to be null.
             *
             * The purpose of this method is to clean up inactive endpoints. It
             * is sufficient to just shut down the existing endpoint. The
             * caller does not need to do anything for this race.
             */
            if (existingEndpoint != null) {
                existingEndpoint.shutdown(
                        "Race condition, not removing existing one", true);
            }
        }
    }

    /**
     * Listens for incoming async connections to respond to the specified type
     * of dialogs.
     */
    @Override
    public synchronized EndpointGroup.ListenHandle
        listen(ListenerConfig listenerConfig,
               int dialogType,
               DialogHandlerFactory handlerFactory)
        throws IOException {

        checkNull("listenerConfig", listenerConfig);
        checkNull("handlerFactory", handlerFactory);
        AbstractListener listener = getListener(listenerConfig);
        return listener.newListenHandle(dialogType, handlerFactory);
    }

    /**
     * Listens for incoming sync connections.
     *
     * The method is synchronized on this class, therefore, it should not be
     * called in a critical path that requires high concurrency.
     */
    @Override
    public synchronized EndpointGroup.ListenHandle
        listen(ListenerConfig listenerConfig,
               SocketPrepared socketPrepared)
        throws IOException {

        checkNull("listenerConfig", listenerConfig);
        checkNull("socketPrepared", socketPrepared);
        AbstractListener listener = getListener(listenerConfig);
        return listener.newListenHandle(socketPrepared);
    }

    /**
     * Shuts down all endpoints and listening channels in the group.
     */
    @Override
    public void shutdown(boolean force) {
        isShutdown = true;
        /*
         * We want to make sure all creator endpoints are shut down.
         *
         * Since isShutdown is volatile, the above set statement creates a
         * memory barrier. After each put of the endpoints, there is a read on
         * isShutdown. If isShutdown is false, the put will be visible by the
         * following enumeration and the endpoint/listener is shutdown in the
         * enumeration. Otherwise, the endpoint is shut down in the get
         * methods.
         */
        while (!creators.isEmpty()) {
            final Iterator<AbstractCreatorEndpoint> iter =
                creators.values().iterator();
            final AbstractCreatorEndpoint endpoint = iter.next();
            iter.remove();
            if (endpoint != null) {
                endpoint.shutdown("Endpoint group is shut down", force);
            }
        }
        /*
         * Synchronization block for listeners operation. We do not need to
         * shut down responder endpoints, they are shut down by the listeners.
         */
        synchronized(this) {
            for (AbstractListener listener :
                    new ArrayList<AbstractListener>(listeners.values())) {
                listener.shutdown();
            }
            listeners.clear();
        }

        shutdownInternal(force);
    }

    /**
     * Removes a listener.
     */
    void removeListener(AbstractListener listener) {
        assert Thread.holdsLock(this);
        listeners.remove(listener.getListenerConfig());
    }

    /* Abstract methods. */

    /**
     * Creates a creator endpoint.
     */
    protected abstract AbstractCreatorEndpoint newCreatorEndpoint(
            NetworkAddress address,
            EndpointConfig endpointConfig);

    /**
     * Creates a listener.
     */
    protected abstract AbstractListener newListener(
            AbstractEndpointGroup endpointGroup,
            ListenerConfig listenerConfig,
            Map<Integer, DialogHandlerFactory> listenerDialogHandlerFactories);

    /**
     * Shuts down the actual implementation and cleans up
     * implementation-dependent resources (e.g., the executor thread pool).
     */
    protected abstract void shutdownInternal(boolean force);

    /* Private implementation methods */

    /**
     * Gets a listener if exists, creates and adds otherwise.
     */
    private AbstractListener getListener(ListenerConfig listenerConfig) {
        assert Thread.holdsLock(this);
        if (isShutdown) {
            throw new IllegalStateException(
                    "Endpoint group is already shut down");
        }
        Map<Integer, DialogHandlerFactory> listenerDialogHandlerFactories =
            listenerDialogHandlerFactoryMap.get(listenerConfig);
        if (listenerDialogHandlerFactories == null) {
            listenerDialogHandlerFactories =
                new ConcurrentHashMap<Integer, DialogHandlerFactory>();
        }
        AbstractListener listener = listeners.get(listenerConfig);
        if (listener == null) {
            listener = newListener(
                    this, listenerConfig, listenerDialogHandlerFactories);
            listeners.put(listenerConfig, listener);
        }
        return listener;
    }

    /* Key classes */

    private class CreatorKey {

        private final NetworkAddress remoteAddress;
        private final EndpointConfig endpointConfig;

        public CreatorKey(NetworkAddress remoteAddress,
                          EndpointConfig endpointConfig) {
            this.remoteAddress = remoteAddress;
            this.endpointConfig = endpointConfig;
        }

        /**
         * Compares endpoints based on peer address and channel factory.
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CreatorKey)) {
                return false;
            }
            CreatorKey that = (CreatorKey) obj;
            return (this.remoteAddress.equals(that.remoteAddress) &&
                    this.endpointConfig.equals(that.endpointConfig));
        }

        /**
         * Returns hashcode for the endpoint based on peer address and channel
         * factory.
         */
        @Override
        public int hashCode() {
            return 37 * remoteAddress.hashCode() +
                endpointConfig.hashCode();
        }
    }

    private class ResponderKey {

        private final NetworkAddress remoteAddress;
        private final ListenerConfig listenerConfig;

        public ResponderKey(NetworkAddress remoteAddress,
                            ListenerConfig listenerConfig) {
            this.remoteAddress = remoteAddress;
            this.listenerConfig = listenerConfig;
        }

        /**
         * Compares endpoints based on peer address and channel factory.
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ResponderKey)) {
                return false;
            }
            ResponderKey that = (ResponderKey) obj;
            return (this.remoteAddress.equals(that.remoteAddress) &&
                    this.listenerConfig.equals(that.listenerConfig));
        }

        /**
         * Returns hashcode for the endpoint based on peer address and channel
         * factory.
         */
        @Override
        public int hashCode() {
            return 37 * remoteAddress.hashCode() +
                listenerConfig.hashCode();
        }
    }
}

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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Abstract class for an endpoint group listener.
 */
public abstract class AbstractListener {

    /* The parent endpoint group. */
    private final AbstractEndpointGroup endpointGroup;
    /* The configuration for creating the listening channel. */
    protected final ListenerConfig listenerConfig;
    /* The configuration for creating the channels of accepted connections. */
    protected final EndpointConfig endpointConfig;
    /*
     * The set of responder endpoints that are accepted by this listener. It is
     * used for shut down responder endpoints when the listener is shut down.
     * Use concurrent hash map for thread-safety and high concurrency.
     */
    protected final Set<AbstractResponderEndpoint> acceptedEndpoints =
        Collections.newSetFromMap(
                new ConcurrentHashMap<AbstractResponderEndpoint, Boolean>());
    /*
     * The dialog types enabled for responding. Passed down from EndpointGroup.
     * All listeners of the same ListenerConfig share this map. Endpoint
     * handlers of this listener (e.g., future handlers of newly accepted
     * responder endpoints) share this map as well. It should be a concurrent
     * hash map for thread-safety and high concurrency.
     */
    private final Map<Integer, DialogHandlerFactory> dialogHandlerFactories;
    /*
     * The reference to socket prepared object. Endpoint handlers of this
     * listener share this reference. Use atomic reference for thread-safety.
     */
    private final AtomicReference<SocketPrepared> socketPreparedRef =
        new AtomicReference<SocketPrepared>(null);
    /*
     * Indicates whether the listener is shut down. The listener is shut down
     * because it is not listening. Once set to true, it will never be false
     * again. Use volatile for write visibility among threads.
     */
    private volatile boolean isShutdown = false;


    protected AbstractListener(AbstractEndpointGroup endpointGroup,
                               ListenerConfig listenerConfig,
                               Map<Integer, DialogHandlerFactory>
                               dialogHandlerFactories) {
        this.endpointGroup = endpointGroup;
        this.listenerConfig = listenerConfig;
        this.dialogHandlerFactories = dialogHandlerFactories;
        this.endpointConfig = listenerConfig.getEndpointConfig();
    }

    /**
     * Returns the listener config.
     */
    ListenerConfig getListenerConfig() {
        return listenerConfig;
    }

    /**
     * Starts listening for an async connection and returns a handle.
     */
    AsyncHandle newListenHandle(int dialogType,
                                DialogHandlerFactory handlerFactory)
        throws IOException {

        assert Thread.holdsLock(endpointGroup);

        if (dialogHandlerFactories.containsKey(dialogType)) {
            throw new IllegalStateException(
                    String.format(
                        "Already start listening for dialogType=%s with %s",
                        dialogType, dialogHandlerFactories.get(dialogType)));
        }

        createChannel();
        dialogHandlerFactories.put(dialogType, handlerFactory);
        return new AsyncHandle(dialogType);
    }

    /**
     * Starts listening for a sync connection and returns a handle.
     */
    SyncHandle newListenHandle(SocketPrepared socketPrepared)
        throws IOException{

        assert Thread.holdsLock(endpointGroup);

        SocketPrepared existing = socketPreparedRef.get();
        if (existing != null) {
            throw new IllegalStateException(
                    String.format("Already start listening with %s",
                        existing));
        }

        createChannel();
        socketPreparedRef.set(socketPrepared);
        return new SyncHandle(socketPrepared);
    }

    /**
     * Shuts down this listener.
     */
    public void shutdown() {
        assert Thread.holdsLock(endpointGroup);

        endpointGroup.removeListener(this);
        closeChannel();
        isShutdown = true;
    }

    /**
     * Returns {@code true} if is shut down.
     */
    public boolean isShutdown() {
        return isShutdown;
    }

    /**
     * Returns the dialog handler factories.
     */
    public Map<Integer, DialogHandlerFactory> getDialogHandlerFactories() {
        return dialogHandlerFactories;
    }

    /**
     * Returns the socket prepared handler.
     */
    public SocketPrepared getSocketPrepared() {
        return socketPreparedRef.get();
    }

    /**
     * Called by the transport layer when error occurred to the listening
     * channel.
     */
    public void onChannelError(Throwable t, boolean channelClosed) {

        /*
         * Acquire endpoint group lock for thread-safety with adding new
         * handlers.
         */
        synchronized(endpointGroup) {
            shutdown();

            for (DialogHandlerFactory factory :
                    dialogHandlerFactories.values()) {
                factory.onChannelError(listenerConfig, t, channelClosed);
            }
            final SocketPrepared socketPrepared =
                socketPreparedRef.get();
            if (socketPrepared != null) {
                socketPrepared.onChannelError(
                        listenerConfig, t, channelClosed);
            }
        }
    }

    /**
     * Remove a responder endpoint when it is shut down.
     */
    public void removeResponderEndpoint(AbstractResponderEndpoint endpoint) {
        acceptedEndpoints.remove(endpoint);
    }

    /**
     * Abstract handle class.
     */
    abstract class Handle implements EndpointGroup.ListenHandle {

        private final NetworkAddress localAddress;

        Handle() {
            this.localAddress =
                AbstractListener.this.getLocalAddress();
        }

        @Override
        public ListenerConfig getListenerConfig() {
            return listenerConfig;
        }

        @Override
        public NetworkAddress getLocalAddress() {
            return localAddress;
        }

        /**
         * Shuts down the listener and shuts down accepted responder endpoint
         * handlers if all handles have been shut down.
         */
        protected void shutdownIfInactive(boolean force) {
            assert Thread.holdsLock(endpointGroup);

            if ((!dialogHandlerFactories.isEmpty()) ||
                (socketPreparedRef.get() != null)) {
                return;
            }

            AbstractListener.this.shutdown();
            /*
             * Since the method is called inside the synchronization block of
             * the parent endpoint group, no new listener can be created (which
             * is also inside the synchronization block). Furthermore, we have
             * closed our listening channel, therefore no new connections will
             * be accepted. That is, no new endpoint will be added to the set
             * during the following iteration. On the other hand, endpoints are
             * removed either because of iter.remove or the endpoint fails by
             * itself.
             */
            while (!acceptedEndpoints.isEmpty()) {
                Iterator<AbstractResponderEndpoint> iter =
                    acceptedEndpoints.iterator();
                while (iter.hasNext()) {
                    AbstractResponderEndpoint endpoint = iter.next();
                    /*
                     * Do iter.remove first, since endpoint.shutdown will try to
                     * remove itself.
                     */
                    iter.remove();
                    endpoint.shutdown("Stop listening", force);
                }
            }
        }
    }

    /**
     * An async handle.
     */
    class AsyncHandle extends Handle {

        private final int dialogType;

        AsyncHandle(int dialogType) {
            this.dialogType = dialogType;
        }

        @Override
        public void shutdown(boolean force) {
            synchronized(endpointGroup) {
                dialogHandlerFactories.remove(dialogType);
                shutdownIfInactive(force);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "AsyncHandle[" +
                    " listener=%s" +
                    " dialogType=%d" +
                    " dialogHandlerFactory=%s ]",
                    AbstractListener.this,
                    dialogType,
                    dialogHandlerFactories.get(dialogType));
        }
    }

    /**
     * A sync handle.
     */
    class SyncHandle extends Handle {

        private final SocketPrepared socketPrepared;

        SyncHandle(SocketPrepared socketPrepared) {
            this.socketPrepared = socketPrepared;
        }

        @Override
        public void shutdown(boolean force) {
            synchronized(endpointGroup) {
                if (socketPreparedRef.compareAndSet(socketPrepared, null)) {
                    /*
                     * This can happen when the shutdown method is called
                     * multiple times and after the first shut down a new
                     * SocketPrepared is added. There is nothing we need to do
                     * in this case.
                     */
                }
                shutdownIfInactive(force);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "SyncHandle[" +
                    " listener=%s" +
                    " socketPrepared=%s ]",
                    AbstractListener.this,
                    socketPrepared);
        }
    }

    /**
     * Creates the listening channel if not existing yet.
     */
    protected abstract void createChannel() throws IOException;

    /**
     * Close the created listening channel.
     */
    protected abstract void closeChannel();

    /**
     * Returns the local address.
     */
    protected abstract NetworkAddress getLocalAddress();

}

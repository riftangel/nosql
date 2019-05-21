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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;

import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.async.exception.InitialConnectIOException;

/**
 * Abstract class for a creator endpoint.
 */
public abstract class AbstractCreatorEndpoint
    implements CreatorEndpoint, EndpointHandlerManager {

    protected final NetworkAddress remoteAddress;
    protected final EndpointConfig endpointConfig;
    /*
     * The dialog types enabled for responding. Endpoint handlers of this
     * endpoint (e.g., future handlers) share this map. Use concurrent hash map
     * for thread-safety.
     */
    private final Map<Integer, DialogHandlerFactory> dialogHandlerFactories =
        new ConcurrentHashMap<Integer, DialogHandlerFactory>();
    /*
     * The reference to the endpoint handler. The handler may die due to errors
     * and flip the reference to null.
     *
     * Currently only support one handler for the endpoint. In the future, we
     * may create more handlers to exploit more network bandwidth.
     */
    private final AtomicReference<EndpointHandler> handlerRef =
        new AtomicReference<EndpointHandler>(null);
    /*
     * Indicates whether the endpoint is shut down. The endpoint is shut down
     * because the parent endpoint group is shut down. Once set to true, it
     * will never be false again. All access should be inside a synchronization
     * block of this object.
     */
    private boolean isShutdown = false;

    protected AbstractCreatorEndpoint(NetworkAddress remoteAddress,
                                      EndpointConfig endpointConfig) {
        checkNull("remoteAddress", remoteAddress);
        checkNull("endpointConfig", endpointConfig);
        this.remoteAddress = remoteAddress;
        this.endpointConfig = endpointConfig;
    }

    /**
     * Starts a dialog.
     */
    @Override
    public void startDialog(int dialogType,
                            DialogHandler dialogHandler,
                            long timeoutMillis) {

        checkNull("dialogHandler", dialogHandler);
        EndpointHandler handler = handlerRef.get();
        if (handler != null) {
            handler.startDialog(
                    dialogType, dialogHandler, timeoutMillis);
            return;
        }
        synchronized(this) {
            if (isShutdown) {
                NullDialogStart.fail(
                        dialogHandler,
                        (new ConnectionEndpointShutdownException(
                             false, "endpoint already shutdown")).
                        getDialogException(false));
                return;
            }
            try {
                handler = getOrConnect();
            } catch (IOException e) {
                NullDialogStart.fail(
                        dialogHandler,
                        new InitialConnectIOException(e, remoteAddress).
                        getDialogException(false));
                return;
            }
        }
        handler.startDialog(
                dialogType, dialogHandler, timeoutMillis);
    }

    /**
     * Enables responding to a dialog type.
     */
    @Override
    public void enableResponding(int dialogType,
                                 DialogHandlerFactory factory) {
        checkNull("factory", factory);
        dialogHandlerFactories.put(dialogType, factory);
    }

    /**
     * Disables responding to a dialog type.
     */
    @Override
    public void disableResponding(int dialogType) {
        dialogHandlerFactories.remove(dialogType);
    }

    /**
     * Returns the network address of the remote endpoint.
     */
    @Override
    public NetworkAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Returns the channel factory of the endpoint.
     */
    @Override
    public EndpointConfig getEndpointConfig() {
        return endpointConfig;
    }

    /**
     * Returns the limit on the number of dialogs this endpoint can
     * concurrently start.
     */
    @Override
    public int getNumDialogsLimit() {
        final EndpointHandler handler = handlerRef.get();
        if (handler == null) {
            return -1;
        }
        return handler.getNumDialogsLimit();
    }

    /**
     * Called when an endpoint handler is shutting down.
     */
    @Override
    public void onHandlerShutdown(EndpointHandler handler) {
        if (!handlerRef.compareAndSet(handler, null)) {
            throw new IllegalStateException(
                    "The endpoint handler reference should not fail " +
                    "compareAndSet upon Shutting down");
        }
    }

    /**
     * Shuts down the endpoint.
     *
     * @param detail information about the cause of the shutdown
     * @param force if true, does not wait for currently active dialogs to
     * finish
     */
    public void shutdown(String detail, boolean force) {
        synchronized (this) {
            isShutdown = true;
        }
        final EndpointHandler handler = handlerRef.get();
        if (handler != null) {
            handler.shutdown(detail, force);
        }
    }

    /**
     * Returns the dialog handler factories.
     */
    public Map<Integer, DialogHandlerFactory> getDialogHandlerFactories() {
        return dialogHandlerFactories;
    }

    /**
     * Creates the endpoint handler.
     */
    protected abstract EndpointHandler newEndpointHandler() throws IOException;

    /**
     * Gets the endpoint handler if it exists or creates a new one.
     */
    private EndpointHandler getOrConnect() throws IOException {
        EndpointHandler handler = handlerRef.get();
        if (handler != null) {
            return handler;
        }
        handler = newEndpointHandler();
        if (!handlerRef.compareAndSet(null, handler)) {
            throw new AssertionError();
        }
        return handler;
    }
}

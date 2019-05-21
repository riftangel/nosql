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

import java.util.logging.Logger;

import oracle.kv.impl.async.exception.ConnectionNotEstablishedException;

/**
 * Abstract class for a responder endpoint.
 *
 * <p>Life cycle of the responder endpoint: The endpoint is created when a
 * creator endpoint is connecting and the connection is being accepted. The
 * reference to the endpoint is put in two maps:
 * AbstractEndpointGroup#responders (so that upper layer can start
 * responder-side dialog) and AbstractListener#acceptedEndpoints (so that shut
 * down listener will shut down this endpoint). Each endpoint corresponds to
 * one connection which is managed by one endpoint handler. The endpoint is
 * shut down and removed from the two maps when (1) the connection is
 * terminating, e.g., due to fault and the endpoint handler is shutting down;
 * (2) the listener is being shut down.
 */
public abstract class AbstractResponderEndpoint
        implements ResponderEndpoint, EndpointHandlerManager {

    private final AbstractEndpointGroup endpointGroup;
    private final NetworkAddress remoteAddress;
    private final ListenerConfig listenerConfig;
    private final AbstractListener listener;

    /*
     * The endpoint handler set by subclasses. It should only be set once from
     * null to a non-null value in the constructor of the subclasses.
     */
    protected volatile EndpointHandler handler;

    public AbstractResponderEndpoint(AbstractEndpointGroup endpointGroup,
                                     NetworkAddress remoteAddress,
                                     ListenerConfig listenerConfig,
                                     AbstractListener listener) {
        checkNull("endpointGroup", remoteAddress);
        checkNull("remoteAddress", remoteAddress);
        checkNull("listenerConfig", listenerConfig);
        checkNull("listener", listener);
        this.endpointGroup = endpointGroup;
        this.remoteAddress = remoteAddress;
        this.listenerConfig = listenerConfig;
        this.listener = listener;
    }

    /**
     * Starts a dialog.
     */
    @Override
    public void startDialog(int dialogType,
                            DialogHandler dialogHandler,
                            long timeoutMillis) {

        checkNull("dialogHandler", dialogHandler);

        if (handler != null) {
            /* Once the handler is not null, it will never be null again. */
            handler.startDialog(
                    dialogType, dialogHandler, timeoutMillis);
        } else {
            NullDialogStart.fail(
                    dialogHandler,
                    (new ConnectionNotEstablishedException()).
                    getDialogException(false));
        }
    }

    /**
     * Returns the address of the remote endpoint.
     */
    @Override
    public NetworkAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Returns the listener configuration of the listener that accepted the
     * endpoint.
     */
    @Override
    public ListenerConfig getListenerConfig() {
        return listenerConfig;
    }

    /**
     * Returns the limit on the number of dialogs this endpoint can
     * concurrently start.
     */
    @Override
    public int getNumDialogsLimit() {
        if (handler == null) {
            return -1;
        }
        return handler.getNumDialogsLimit();
    }

    /**
     * Called when an endpoint handler is shutting down.
     */
    @Override
    public void onHandlerShutdown(EndpointHandler handlerToRemove) {
        if (handlerToRemove != handler) {
            if (handler != null) {
                /*
                 * This should never happen since the handler should hold the
                 * correct reference.
                 */
                throw new IllegalStateException(String.format(
                            "Wrong endpoint handler calling onHandlerShutdown " +
                            "for a responder endpoint, " +
                            "expected=%s, got=%s", handler, handlerToRemove));
            }
            /*
             * The fall-through case might happen when the handler is created
             * but before it is assigned to the endpoint's handler reference,
             * the handler is shut down.
             */
        }
        endpointGroup.removeResponderEndpoint(this);
        listener.removeResponderEndpoint(this);
    }

    /**
     * Shuts down the endpoint.
     *
     * @param detail description of why the endpoint is being shut down
     * @param force whether to shut down active dialogs
     */
    public void shutdown(String detail, boolean force) {
        /* Just shut down the handler which will call onHandlerShutdown. */
        handler.shutdown(detail, force);
    }

    /**
     * Gets the logger.
     */
    public Logger getLogger() {
        return endpointGroup.getLogger();
    }

    public static class NullEndpoint extends AbstractResponderEndpoint {

        NullEndpoint(AbstractEndpointGroup endpointGroup,
                     NetworkAddress remoteAddress,
                     ListenerConfig listenerConfig,
                     AbstractListener listener) {
            super(endpointGroup, remoteAddress, listenerConfig, listener);
            this.handler = null;
        }
    }
}

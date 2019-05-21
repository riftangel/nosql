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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.util.SerialVersion;

/**
 * Server implementation of the {@link ServiceRegistry} interface.
 *
 * @see ServiceRegistryAPI
 */
public class ServiceRegistryImpl implements ServiceRegistry {

    private final Logger logger;

    private final Map<String, ServiceEndpoint> registry =
        Collections.synchronizedMap(new HashMap<String, ServiceEndpoint>());

    /**
     * Creates an instance of this class.
     *
     * @param logger for debug logging
     */
    public ServiceRegistryImpl(Logger logger) {
        logger.log(Level.FINE, "Created {0}", this);
        this.logger = logger;
    }

    /**
     * Returns a responder (server-side) dialog handler for this instance.
     */
    public DialogHandler createDialogHandler() {
        return new ServiceRegistryResponder(this, logger);
    }

    @Override
    public void getSerialVersion(short serialVersion,
                                 long timeoutMillis,
                                 ResultHandler<Short> handler) {

        handler.onResult(SerialVersion.CURRENT, null);
    }

    @Override
    public void lookup(short serialVersion,
                       String name,
                       long timeout,
                       ResultHandler<ServiceEndpoint> handler) {
        if (name == null) {
            handler.onResult(
                null,
                new IllegalArgumentException(
                    "Name parameter must not be null"));
        } else {
            final ServiceEndpoint result = registry.get(name);
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("ServiceRegistryImpl.lookup this=" + this +
                            " name=" + name + " returns " + result);
            }
            handler.onResult(result, null);
        }
    }

    @Override
    public void bind(short serialVersion,
                     String name,
                     ServiceEndpoint endpoint,
                     long timeout,
                     ResultHandler<Void> handler) {
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ServiceRegistryImpl.bind this=" + this +
                        " name=" + name + " endpoint=" + endpoint);
        }
        if ((name == null) || (endpoint == null)) {
            handler.onResult(
                null,
                new IllegalArgumentException(
                    "Name and endpoint parameters must not be null"));
        } else {
            registry.put(name, endpoint);
            handler.onResult(null, null);
        }
    }

    @Override
    public void unbind(short serialVersion,
                       String name,
                       long timeout,
                       ResultHandler<Void> handler) {
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ServiceRegistryImpl.unbind this=" + this +
                        " name=" + name);
        }
        if (name == null) {
            handler.onResult(
                null,
                new IllegalArgumentException(
                    "Name parameter must not be null"));
        } else {
            registry.remove(name);
            handler.onResult(null, null);
        }
    }

    @Override
    public void list(short serialVersion,
                     long timeout,
                     ResultHandler<List<String>> handler) {
        final List<String> result;
        synchronized (registry) {
            result = new ArrayList<>(registry.keySet());
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ServiceRegistryImpl.list this=" + this +
                        " returns " + result);
        }
        handler.onResult(result, null);
    }

    @Override
    public String toString() {
        return "ServiceRegistryImpl@" + Integer.toHexString(hashCode());
    }
}

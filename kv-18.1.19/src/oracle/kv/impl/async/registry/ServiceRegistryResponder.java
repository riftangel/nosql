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

import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResultHandler;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.StandardDialogTypeFamily;
import oracle.kv.impl.async.registry.ServiceRegistry.RegistryMethodOp;
import oracle.kv.impl.util.SerialVersion;

/**
 * A responder (server-side) dialog handler for {@link ServiceRegistry}
 * dialogs.  As with the standard RMI registry, this implementation rejects
 * modifications to the registry unless the caller is on the same host as the
 * server.
 *
 * @see ServiceRegistryImpl
 */
class ServiceRegistryResponder
        extends AsyncVersionedRemoteDialogResponder {

    /**
     * A set of network addresses that are known to be the local host.  Used to
     * enforce local access for modify operations.
     */
    private static final Set<InetAddress> checkedLocalAddresses =
        Collections.synchronizedSet(new HashSet<>());

    private final ServiceRegistry server;

    ServiceRegistryResponder(ServiceRegistry server, Logger logger) {
        super(StandardDialogTypeFamily.SERVICE_REGISTRY, logger);
        this.server = server;
    }

    @Override
    protected MethodOp getMethodOp(int methodOpValue) {
        return RegistryMethodOp.valueOf(methodOpValue);
    }

    @Override
    protected void handleRequest(MethodOp methodOp,
                                 MessageInput request,
                                 DialogContext context) {
        final short serialVersion;
        try {
            serialVersion = request.readShort();
        } catch (IOException e) {
            sendException(
                new IllegalStateException(
                    "Problem deserializing request: " + e, e),
                SerialVersion.STD_UTF8_VERSION);
            return;
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                       "ServiceRegistryResponder.handleRequest" +
                       " methodOp={0}" +
                       " dialogContext={1}",
                       new Object[] { methodOp, getSavedDialogContext() });
        }
        switch ((RegistryMethodOp) methodOp) {
        case GET_SERIAL_VERSION:
            final long timeoutMillis;
            try {
                timeoutMillis = readPackedLong(request);
            } catch (IOException e) {
                sendException(
                    new IllegalStateException(
                        "Problem deserializing timeout for" +
                        " GET_SERIAL_VERSION: " + e,
                        e),
                    SerialVersion.STD_UTF8_VERSION);
                return;
            }
            supplySerialVersion(serialVersion, timeoutMillis, server);
            break;
        case LOOKUP:
            lookup(serialVersion, request);
            break;
        case BIND:
            bind(serialVersion, request);
            break;
        case UNBIND:
            unbind(serialVersion, request);
            break;
        case LIST:
            list(serialVersion, request);
            break;
        default:
            throw new AssertionError();
        }
    }

    private void lookup(short serialVersion, MessageInput request) {
        final String name;
        final long timeout;
        try {
            name = readString(request, serialVersion);
            timeout = readPackedLong(request);
        } catch (IOException e) {
            throw new IllegalStateException(
                "Problem deserializing request: " + e, e);
        }
        final ResultHandler<ServiceEndpoint> handler =
            new AsyncVersionedRemoteDialogResultHandler<ServiceEndpoint>(
                serialVersion, this) {
                @Override
                protected void writeResult(ServiceEndpoint result,
                                           MessageOutput response)
                    throws IOException {

                    if (result != null) {
                        response.writeBoolean(true);
                        result.writeFastExternal(response, serialVersion);
                    } else {
                        response.writeBoolean(false);
                    }
                }
            };
        server.lookup(serialVersion, name, timeout, handler);
    }

    private void bind(short serialVersion, MessageInput request) {
        if (!checkAccess(serialVersion, "bind")) {
            return;
        }
        final String name;
        final ServiceEndpoint endpoint;
        final long timeout;
        try {
            name = readString(request, serialVersion);
            endpoint = new ServiceEndpoint(request, serialVersion);
            timeout = readPackedLong(request);
        } catch (IOException e) {
            throw new IllegalStateException(
                "Problem deserializing request: " + e, e);
        }
        final ResultHandler<Void> handler =
            new AsyncVersionedRemoteDialogResultHandler<Void>(
                serialVersion, this) {
                @Override
                protected void writeResult(Void result,
                                           MessageOutput response) {
                }
            };
        server.bind(serialVersion, name, endpoint, timeout, handler);
    }

    /**
     * Check that the specified modify operation is permitted.  This
     * implementation sends an UnauthorizedException if the remote connection
     * is not being made by the local host.  This behavior is inspired by the
     * implementation of the RMI registry.
     *
     * @param serialVersion the serial version to use for communications
     * @param method the method being called
     * @return whether the check passed
     */
    protected boolean checkAccess(short serialVersion, String method) {
        final DialogContext context = getSavedDialogContext();
        final NetworkAddress remoteAddress = context.getRemoteAddress();
        final InetAddress inetAddress;
        try {
            inetAddress = remoteAddress.getInetAddress();
        } catch (UnknownHostException e) {
            sendException(new UnauthorizedException(
                              "Call to ServiceRegistry." + method +
                              " is unauthorized: caller host is unknown",
                              e),
                          serialVersion);
            return false;
        }
        if (checkedLocalAddresses.contains(inetAddress)) {
            return true;
        }
        if (inetAddress.isAnyLocalAddress()) {
            sendException(new UnauthorizedException(
                              "Call to ServiceRegistry." + method +
                              " is unauthorized: caller address is unknown"),
                          serialVersion);
            return false;
        }
        try {
            new ServerSocket(0, 10, inetAddress).close();
            /* If we can bind to this address, then it is local */
            checkedLocalAddresses.add(inetAddress);
            return true;
        } catch (IOException e) {
            sendException(new UnauthorizedException(
                              "Call to ServiceRegistry." + method +
                              " is unauthorized: caller has non-local host",
                              e),
                          serialVersion);
            return false;
        }
    }

    private void unbind(short serialVersion, MessageInput request) {
        if (!checkAccess(serialVersion, "bind")) {
            return;
        }
        final String name;
        final long timeout;
        try {
            name = readString(request, serialVersion);
            timeout = readPackedLong(request);
        } catch (IOException e) {
            throw new IllegalStateException(
                "Problem deserializing request: " + e, e);
        }
        final ResultHandler<Void> handler =
            new AsyncVersionedRemoteDialogResultHandler<Void>(
                serialVersion, this) {
                @Override
                protected void writeResult(Void result,
                                           MessageOutput response) {
                }
            };
        server.unbind(serialVersion, name, timeout, handler);
    }

    private void list(short serialVersion, MessageInput request) {
        final long timeout;
        try {
            timeout = readPackedLong(request);
        } catch (IOException e) {
            throw new IllegalStateException(
                "Problem deserializing request: " + e, e);
        }
        final ResultHandler<List<String>> handler =
            new AsyncVersionedRemoteDialogResultHandler<List<String>>(
                serialVersion, this) {
                @Override
                protected void writeResult(List<String> result,
                                           MessageOutput response)
                    throws IOException {

                    writeNonNullSequenceLength(response, result.size());
                    for (final String name : result) {
                        writeString(response, serialVersion, name);
                    }
                }
            };
        server.list(serialVersion, timeout, handler);
    }
}

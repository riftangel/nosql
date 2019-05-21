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
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogInitiator;
import oracle.kv.impl.async.AsyncVersionedRemoteInitiator;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;

/**
 * An initiator (client-side) implementation of {@link ServiceRegistry}.
 *
 * @see ServiceRegistryAPI
 */
class ServiceRegistryInitiator extends AsyncVersionedRemoteInitiator
    implements ServiceRegistry {

    ServiceRegistryInitiator(CreatorEndpoint endpoint,
                             Logger logger) {
        super(endpoint, SERVICE_REGISTRY_DIALOG_TYPE, logger);
    }

    @Override
    public void getSerialVersion(short serialVersion,
                                 long timeoutMillis,
                                 ResultHandler<Short> handler) {
        getSerialVersion(serialVersion,
                         RegistryMethodOp.GET_SERIAL_VERSION,
                         timeoutMillis,
                         handler);
    }

    @Override
    public void lookup(short serialVersion,
                       final String name,
                       final long timeout,
                       final ResultHandler<ServiceEndpoint> handler) {
        final MessageOutput request =
            startRequest(RegistryMethodOp.LOOKUP, serialVersion);
        try {
            writeString(request, serialVersion, name);
            writePackedLong(request, timeout);
        } catch (IOException e) {
            handler.onResult(
                null,
                new RuntimeException(
                    "Unexpected problem writing request: " + e, e));
            return;
        }
        startDialog(
            new Handler<ServiceEndpoint>(request, handler,
                                         RegistryMethodOp.LOOKUP) {
                @Override
                String getMethodArgs() {
                    return " name=" + name;
                }
                @Override
                public ServiceEndpoint readResult(MessageInput in,
                                                  short serialVersionRead)
                    throws IOException {

                    final ServiceEndpoint result =
                        in.readBoolean() ?
                        new ServiceEndpoint(in, serialVersionRead) :
                        null;
                    return result;
                }
            },
            timeout);
    }

    /**
     * Use a named class and provide a toString method, both to improve
     * debugging.
     */
    private abstract class Handler<V>
            extends AsyncVersionedRemoteDialogInitiator<V> {
        private final MethodOp methodOp;
        Handler(MessageOutput request,
                ResultHandler<V> handler,
                MethodOp methodOp) {
            super(request, logger, handler);
            this.methodOp = methodOp;
        }
        String getMethodArgs() {
            return "";
        }
        @Override
        public String toString() {
            return "Handler[dialogType=" + dialogType +
                " methodOp=" + methodOp + getMethodArgs() + "]";
        }
    }

    @Override
    public void bind(short serialVersion,
                     final String name,
                     @SuppressWarnings("hiding") final ServiceEndpoint endpoint,
                     final long timeout,
                     final ResultHandler<Void> handler) {
        final MessageOutput request =
            startRequest(RegistryMethodOp.BIND, serialVersion);
        try {
            writeString(request, serialVersion, name);
            endpoint.writeFastExternal(request, serialVersion);
            writePackedLong(request, timeout);
        } catch (IOException e) {
            handler.onResult(
                null,
                new RuntimeException(
                    "Unexpected problem writing request: " + e, e));
            return;
        }
        startDialog(
            new Handler<Void>(request, handler, RegistryMethodOp.BIND) {
                @Override
                String getMethodArgs() {
                    return " name=" + name + " endpoint=" + endpoint;
                }
                @Override
                public Void readResult(MessageInput in,
                                       short serialVersionRead) {
                    return null;
                }
            },
            timeout);
    }

    @Override
    public void unbind(short serialVersion,
                       final String name,
                       long timeout,
                       ResultHandler<Void> handler) {
        final MessageOutput request =
            startRequest(RegistryMethodOp.UNBIND, serialVersion);
        try {
            writeString(request, serialVersion, name);
            writePackedLong(request, timeout);
        } catch (IOException e) {
            handler.onResult(
                null,
                new RuntimeException(
                    "Unexpected problem writing request: " + e, e));
            return;
        }
        startDialog(
            new Handler<Void>(request, handler, RegistryMethodOp.UNBIND) {
                @Override
                String getMethodArgs() {
                    return " name=" + name;
                }
                @Override
                public Void readResult(MessageInput in,
                                       short serialVersionRead) {
                    return null;
                }
            },
            timeout);
    }

    @Override
    public void list(short serialVersion,
                     long timeout,
                     ResultHandler<List<String>> handler) {
        final MessageOutput request =
            startRequest(RegistryMethodOp.LIST, serialVersion);
        try {
            writePackedLong(request, timeout);
        } catch (IOException e) {
            handler.onResult(
                null,
                new RuntimeException(
                    "Unexpected problem writing request: " + e, e));
            return;
        }
        startDialog(
            new Handler<List<String>>(request, handler,
                                      RegistryMethodOp.LIST) {
                @Override
                public List<String> readResult(MessageInput in,
                                               short serialVersionRead)
                    throws IOException {

                    final int count = readNonNullSequenceLength(in);
                    final List<String> result = new ArrayList<String>(count);
                    for (int i = 0; i < count; i++) {
                        result.add(readString(in, serialVersionRead));
                    }
                    return result;
                }
            },
            timeout);
    }
}

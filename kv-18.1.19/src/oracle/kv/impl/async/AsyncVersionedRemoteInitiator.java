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
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;

import java.io.IOException;
import java.util.logging.Logger;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;

/**
 * Base class used to implement remote calls by initiators (clients) of
 * asynchronous service interfaces.  Subclasses should provide implementations
 * of service methods by calling {@link #startDialog startDialog} after
 * marshaling requests, and should subclass {@link
 * AsyncVersionedRemoteDialogInitiator} to create dialog handlers used to read
 * responses through the dialog layer.
 *
 * @see AsyncVersionedRemote
 * @see AsyncVersionedRemoteDialogInitiator
 */
public abstract class AsyncVersionedRemoteInitiator
        implements AsyncVersionedRemote {

    protected final CreatorEndpoint endpoint;
    protected final DialogType dialogType;
    protected final Logger logger;

    /**
     * Creates an instance of this class.
     *
     * @param endpoint the associated endpoint
     * @param dialogType the dialog type
     * @param logger the logger
     */
    protected AsyncVersionedRemoteInitiator(CreatorEndpoint endpoint,
                                            DialogType dialogType,
                                            Logger logger) {
        this.endpoint = checkNull("endpoint", endpoint);
        this.dialogType = checkNull("dialogType", dialogType);
        this.logger = checkNull("logger", logger);
    }

    /**
     * Starts a request by getting an output stream for writing a request, and
     * writing the method op and serial version.
     *
     * @param methodOp the message op
     * @param serialVersion the serial version
     * @return the output stream
     */
    protected MessageOutput startRequest(MethodOp methodOp,
                                         short serialVersion) {
        final MessageOutput request = new MessageOutput();
        try {
            writePackedInt(request, methodOp.getValue());
            request.writeShort(serialVersion);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return request;
    }

    /**
     * Requests the responder's serial version.
     *
     * @param serialVersion the initiator's serial version
     * @param methodOp the method op representing this request
     * @param timeoutMillis the timeout for the remote request
     * @param handler the result handler
     */
    protected void getSerialVersion(short serialVersion,
                                    MethodOp methodOp,
                                    long timeoutMillis,
                                    final ResultHandler<Short> handler) {

        final MessageOutput request = startRequest(methodOp, serialVersion);
        try {
            writePackedLong(request, timeoutMillis);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        startDialog(
            new AsyncVersionedRemoteDialogInitiator<Short>(
                request, logger, handler) {
                @Override
                public Short readResult(
                    MessageInput in, short responderSerialVersion)
                    throws IOException {

                    return in.readShort();
                }
                @Override
                public String toString() {
                    return handler.toString();
                }
            },
            timeoutMillis);
    }

    /**
     * Starts a dialog.
     *
     * @param handler the dialog handler
     * @param timeoutMillis the dialog timeout
     */
    protected void startDialog(AsyncVersionedRemoteDialogInitiator<?> handler,
                               long timeoutMillis) {
        endpoint.startDialog(
            dialogType.getDialogTypeId(), handler, timeoutMillis);
    }
}

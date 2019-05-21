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
import java.util.logging.Level;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.AsyncVersionedRemote.ResponseType;
import oracle.kv.impl.async.MessageOutput;

/**
 * Base class for result handlers used by responders (servers) to write
 * responses to the output stream.  Subclasses should implement {@link
 * #writeResult writeResult} to write results to the output stream.
 *
 * @param <R> the type of the response
 * @see AsyncVersionedRemote
 * @see AsyncVersionedRemoteDialogResponder
 */
public abstract class AsyncVersionedRemoteDialogResultHandler<R>
        implements ResultHandler<R> {

    protected final short serialVersion;
    private final AsyncVersionedRemoteDialogResponder handler;

    /**
     * Creates an instance of this class.
     *
     * @param serialVersion the serial version to use for communication
     * @param handler the dialog handler
     */
    protected AsyncVersionedRemoteDialogResultHandler(
        short serialVersion, AsyncVersionedRemoteDialogResponder handler) {

        checkNull("handler", handler);
        this.serialVersion = serialVersion;
        this.handler = handler;
    }

    /**
     * Writes the specified result to the output stream.
     *
     * @param result the result to write
     * @param out the output stream
     * @throws IOException if there is a problem serializing the output for the
     * result
     */
    protected abstract void writeResult(R result, MessageOutput out)
        throws IOException;

    /* -- From ResultHandler -- */

    @Override
    public void onResult(R result, Throwable exception) {
        if (exception != null) {
            handler.sendException(exception, serialVersion);
            return;
        }
        final MessageOutput response = new MessageOutput();
        try {
            ResponseType.SUCCESS.writeFastExternal(response, serialVersion);
            response.writeShort(serialVersion);
            writeResult(result, response);
            if (handler.logger.isLoggable(Level.FINE)) {
                final DialogContext dialogContext =
                    handler.getSavedDialogContext();
                handler.logger.fine(
                    String.format(
                        "Call responder got result" +
                        " dialogId=%x:%x" +
                        " peer=%s" +
                        " result=%s",
                        ((dialogContext != null) ?
                         dialogContext.getDialogId() :
                         -1),
                        ((dialogContext != null) ?
                         dialogContext.getConnectionId() :
                         -1),
                        ((dialogContext != null) ?
                         dialogContext.getRemoteAddress() :
                         ""),
                        result));
            }
        } catch (Throwable e) {
            handler.sendException(
                new RuntimeException(
                    "Unexpected problem writing result: " + e, e),
                serialVersion);
            return;
        }
        handler.write(response);
    }
}

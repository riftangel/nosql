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

import java.util.concurrent.ScheduledExecutorService;

import oracle.kv.impl.async.exception.ContextWriteException;
import oracle.kv.impl.async.exception.DialogException;

/**
 * A context that enables a {@link DialogHandler} to interact with the
 * underlying dialog layer.
 */
public interface DialogContext {

    /**
     * Writes a new message output for the dialog.
     *
     * <p>If the write is successful, the method returns {@code true}, the
     * entire message is buffered inside the context and will be written to the
     * network buffer eventually.
     *
     * <p>The write may fail if the previous buffered message has not been
     * totally written to the network buffer. In that case, the message is not
     * buffered and the caller can try again to write the same message. The
     * write may also fail when there is an IO exception or the dialog is
     * already aborted. In these cases, the method will return {@code false}
     * and {@link DialogHandler#onAbort} will be called with a {@link
     * DialogException} as its cause.
     *
     * <p>For a buffered message <i>M</i> of a successful write <i>W</i>, if at
     * least one write after <i>W</i> failed due to <i>M</i>, the method {@link
     * DialogHandler#onCanWrite} associated with this context will be called
     * exactly once when <i>M</i> has been written to the network buffer.
     *
     * <p>Although it is allowed to call this method concurrently or
     * repeatedly, the typical use of the method is to write one message at a
     * time and wait for the {@code onCanWrite} method before try again if a
     * write fails.
     *
     * <p>The parameter {@code finished} should be set to {@code true} when
     * writing the last message.
     *
     * <p>The method may throw subclasses of {@link ContextWriteException} when
     * the message size exceeds the limit or the last message is already
     * written.
     *
     * <p>The method is thread-safe.
     *
     * @param mesg message to write
     * @param finished true if the message is the last to write
     * @return {@code true} if the message is succesfully written to the
     * context
     */
    boolean write(MessageOutput mesg, boolean finished);

    /**
     * Reads a new message input for the dialog.
     *
     * <p>The method retrieves and removes the first (in the order of arrival)
     * input message from the context.
     *
     * <p>The method returns {@code null} if there is no message arrived or all
     * arrived messages have been retrieved or the dialog is aborted. Note that
     * a {@code null} return does not give any indication of whether there will
     * be more messages in the future. The indication is given by the {@code
     * finished} argument in {@link DialogHandler#onCanRead}.
     *
     * <p>When new messages arrived, the method {@link DialogHandler#onCanRead}
     * associated with this context will be called. A typical use of the method
     * is to read a message after each {@code onCanRead} is called. Note that
     * new arrived messages may be visible through this method before the
     * corresponding {@code onCanRead} is called. Therefore, when one {@code
     * onCanRead} is called, mutiple calls to this method may have non-{@code
     * null} returns and following calls to this method may return {@code null}
     * after {@code onCanRead} calls.
     *
     * <p>The method is thread-safe.
     *
     * @return the message, {@code null} if no arrived message or all arrived
     * messages has been retrieved or the context is aborted.
     */
    MessageInput read();

    /**
     * Returns the dialog ID.
     *
     * The dialog ID might not be assigned when the method is called. The ID
     * will only be assigned when the dialog starts reading/writing messages.
     *
     * @return the dialog ID, zero if not assigned yet
     */
    long getDialogId();

    /**
     * Returns the statistically universal unique connection ID.
     *
     * The ID might not be assigned yet for responder endpoints before the
     * connection handshake is done.
     *
     * @return the connection ID, zero if not assigned yet
     */
    long getConnectionId();

    /**
     * Returns the remote network address.
     *
     * The returned address might be null if there is no established underlying
     * connection. The dialog should be in the process of being aborted in this
     * case and {@link DialogHandler#onAbort} is expected to be called soon if
     * not yet.
     *
     * @return the network address, null if no connection is present
     */
    NetworkAddress getRemoteAddress();

    /**
     * Returns the executor service associated with this context.
     *
     * @return the executor service
     */
    ScheduledExecutorService getSchedExecService();

}

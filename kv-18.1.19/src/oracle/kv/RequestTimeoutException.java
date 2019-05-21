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
package oracle.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Thrown when a request cannot be processed because the configured timeout
 * interval is exceeded.
 *
 * <p>The default timeout interval (specified by {@link
 * KVStoreConfig#getRequestTimeout}) is five seconds, and this exception should
 * rarely be thrown.</p>
 *
 * <p>Note that the durability of an update operation is uncertain if it
 * results in a {@link RequestTimeoutException} being thrown. The changes
 * requested by the update may or may not have been committed to the master or
 * propagated to one or more replicas. Applications may want to retry the
 * update operation if it is idempotent, or perform read operations to
 * determine the outcome of the previous update.</p>
 *
 * <p>Note also that if the consistency specified for a read operation
 * is {@link Consistency#NONE_REQUIRED_NO_MASTER}, then this exception
 * will be thrown if the operation is attempted when the only node
 * available is the Master.</p>
 *
 * <p>Depending on the nature of the application, when this exception is thrown
 * the client may wish to</p>
 * <ul>
 * <li>retry the operation,</li>
 * <li>fall back to using a larger timeout interval, and resume using the
 * original timeout interval at a later time, or</li>
 * <li>give up and report an error at a higher level.</li>
 * </ul>
 *
 * @hiddensee {@link #writeFastExternal FastExternalizable format}
 */

/* Ignore warning about reference to deprecated NONE_REQUIRED_NO_MASTER */
@SuppressWarnings("javadoc")
public class RequestTimeoutException extends FaultException {

    private static final long serialVersionUID = 1L;

    private volatile int timeoutMs;

    /**
     * For internal use only.
     * @hidden
     */
    public RequestTimeoutException(int timeoutMs,
                                   String msg,
                                   Exception cause,
                                   boolean isRemote) {
        super(msg, cause, isRemote);
        this.timeoutMs = timeoutMs;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public RequestTimeoutException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        timeoutMs = in.readInt();
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FaultException}) {@code super}
     * <li> ({@link DataOutput#writeInt int}) {@link #getTimeoutMs timeoutMs}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        /* Use super's message so that the timeout isn't included twice */
        writeFastExternal(out, serialVersion, super.getMessage());
        out.writeInt(timeoutMs);
    }

    @Override
    public String getMessage() {
        if (timeoutMs == 0) {
            return super.getMessage();
        }
        return super.getMessage() + " Timeout: " + timeoutMs + "ms";
    }

    /**
     * Returns the timeout that was in effect for the operation.
     */
    public int getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Sets the timeout that was in effect for the operation.
     */
    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
}

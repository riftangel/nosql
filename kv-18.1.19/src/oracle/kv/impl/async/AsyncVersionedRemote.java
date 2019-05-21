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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.registry.ServiceRegistry;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.SerializeExceptionUtil;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * Base interface for all asynchronous service interfaces.
 *
 * <p>This class is analogous to the {@link VersionedRemote} interface used for
 * synchronous service interfaces.  Like that class, this one includes
 * facilities for negotiating the serial version used for communication, in
 * addition to basic facilities for performing remote calls.  For synchronous
 * services, remote call facilities are provided by RMI.  For asynchronous
 * services, we provide our own implementation based on the dialog layer.  As
 * with RMI, service interface implementations only provide the server logic,
 * and other parts of the infrastructure are responsible for communication,
 * including unmarshaling requests and marshaling responses, and for service
 * lookup.
 *
 * <p>APIs should be implemented as subclasses of {@link
 * AsyncVersionedRemoteAPI}, which provides support for negotiating the serial
 * version.  The API implementation should make use of a subclass of {@link
 * AsyncVersionedRemoteInitiator} to implement the initiator side of the remote
 * call infrastructure, including request marshaling and response unmarshaling.
 * RMI implements these facilities automatically, but this scheme requires that
 * they be implemented by hand.
 *
 * <p>The service interface should extend {@link AsyncVersionedRemote}, and
 * should define an enum that implements {@link MethodOp} to provide unique IDs
 * for each remote method.  The first parameter of each method in the service
 * interface should be a {@code short} that represents the serial version.
 *
 * <p>In addition to implementing the server-side behavior of the service
 * interface, the service implementation should also provide a subclass of
 * {@link AsyncVersionedRemoteDialogResponder} to implement the responder side
 * of the remote call infrastructure, including marshaling and unmarshaling,
 * which are also implemented manually here, unlike RMI.  The responder should
 * be registered with the dialog layer to provide support for incoming calls.
 * Applications can also use the {@link ServiceRegistry}, analogous to the RMI
 * registry, to provide a way for clients to look up remote services.
 *
 * <p>Remote calls are implemented using the dialog layer, with a single frame
 * to represent the requested call and a single frame representing the
 * response.
 *
 * <p>The request frame has the format:
 * <ul>
 * <li> ({@link SerializationUtil#writePackedInt packed int}) <i>method op
 *      value</i>
 * <li> ({@code short}) <i>serial version</i>
 * <li> Method arguments are written in {@link FastExternalizable} format
 * <li> ({@link SerializationUtil#writePackedLong packed long}) <i>timeout</i>
 * </ul>
 *
 * <p>The response frame has the format:
 * <ul>
 * <li> ({@link ResponseType ResponseType}) <i>response type</i>
 * <li> If the response type is {@link ResponseType#SUCCESS SUCCESS}:
 *   <ul>
 *   <li> ({@code short}) <i>serial version</i>
 *   <li> The result object is written in {@code FastExternalizable} format
 *   </ul>
 * <li> If the response type is {@link ResponseType#FAILURE FAILURE}, the
 *      exception is written by {@link SerializeExceptionUtil#writeException
 *      SerializeExceptionUtil.writeException}
 * </ul>
 *
 * @see AsyncVersionedRemoteAPI
 * @see AsyncVersionedRemoteInitiator
 * @see AsyncVersionedRemoteDialogResponder
 * @see ServiceRegistry
 */
public interface AsyncVersionedRemote {

    /**
     * The types of responses to asynchronous method calls.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    enum ResponseType implements FastExternalizable {

        /** A successful result. */
        SUCCESS(0),

        /** An exception from a failure. */
        FAILURE(1);

        private static final ResponseType[] VALUES = values();

        private ResponseType(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        /**
         * Reads an instance from the input stream.
         *
         * @param in the input stream
         * @param serialVersion the version of the serialized form
         * @throws IOException if an I/O error occurs or if the format of the
         * input data is invalid
         */
        public static ResponseType readFastExternal(DataInput in,
                                                    short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (IllegalArgumentException e) {
                throw new IOException(
                    "Wrong ordinal for ResponseType: " + ordinal, e);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>value</i> // {@link #SUCCESS}=0, {@link
         *      #FAILURE}=1
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    /**
     * An identifier for an asynchronous method.  Subclasses of {@link
     * AsyncVersionedRemote} typically define a enumeration that implements
     * this interface and whose values represent the methods supported by the
     * remote interface.
     */
    interface MethodOp {

        /** The integer value associated with the method. */
        int getValue();
    }

    /**
     * Gets the serial version of the call responder.  Queries the server for
     * its serial version and calls the handler with the result. <p>
     *
     * If the server does not support the client version, then it will supply
     * an {@link UnsupportedOperationException} to the handler.
     *
     * @param serialVersion the serial version of the call initiator
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param handler the result handler
     */
    void getSerialVersion(short serialVersion,
                          long timeoutMillis,
                          ResultHandler<Short> handler);
}

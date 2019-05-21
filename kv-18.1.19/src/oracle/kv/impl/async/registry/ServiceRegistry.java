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

import java.util.List;

import oracle.kv.ResultHandler;
import oracle.kv.impl.async.AsyncVersionedRemote;

/**
 * The remote interface for the service registry, which maps service names to
 * service endpoints.
 *
 * @see ServiceRegistryAPI
 */
public interface ServiceRegistry extends AsyncVersionedRemote {

    /** The IDs for methods in this interface. */
    enum RegistryMethodOp implements MethodOp {

        /**
         * The ID for the {@link AsyncVersionedRemote#getSerialVersion}
         * method, with ordinal 0.
         */
        GET_SERIAL_VERSION(0),

        /**
         * The ID for the {@link ServiceRegistry#lookup} method, with ordinal
         * 1.
         */
        LOOKUP(1),

        /**
         * The ID for the {@link ServiceRegistry#bind} method, with ordinal 2.
         */
        BIND(2),

        /**
         * The ID for the {@link ServiceRegistry#unbind} method, with ordinal
         * 3.
         */
        UNBIND(3),

        /**
         * The ID for the {@link ServiceRegistry#list} method, with ordinal 4.
         */
        LIST(4);

        private static final RegistryMethodOp[] VALUES = values();

        private RegistryMethodOp(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        /**
         * Returns the {@code RegistryMethodOp} with the specified ordinal.
         *
         * @param ordinal the ordinal
         * @return the {@code RegistryMethodOp}
         * @throws IllegalArgumentException if the value is not found
         */
        public static RegistryMethodOp valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Wrong ordinal for RegistryMethodOp: " + ordinal, e);
            }
        }

        @Override
        public int getValue() {
            return ordinal();
        }

        @Override
        public String toString() {
            return name() + '(' + ordinal() + ')';
        }
    }

    /**
     * Look up an entry in the registry.
     *
     * @param serialVersion the serial version to use for communication
     * @param name the name of the entry
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param handler handler to receive the service endpoint or {@code null}
     * if the entry is not found
     */
    void lookup(short serialVersion,
                String name,
                long timeoutMillis,
                ResultHandler<ServiceEndpoint> handler);

    /**
     * Set an entry in the registry.
     *
     * @param serialVersion the serial version to use for communication
     * @param name the name of the entry
     * @param endpoint the endpoint to associate with the name
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param handler handler to call when the operation is complete
     */
    void bind(short serialVersion,
              String name,
              ServiceEndpoint endpoint,
              long timeoutMillis,
              ResultHandler<Void> handler);

    /**
     * Remove an entry from the registry.
     *
     * @param serialVersion the serial version to use for communication
     * @param name the name of the entry
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param handler handler to call when the operation is complete
     */
    void unbind(short serialVersion,
                String name,
                long timeoutMillis,
                ResultHandler<Void> handler);

    /**
     * List the entries in the registry.
     *
     * @param serialVersion the serial version to use for communication
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @param handler handler to call with a list entry names
     */
    void list(short serialVersion,
              long timeoutMillis,
              ResultHandler<List<String>> handler);
}

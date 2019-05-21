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

package oracle.kv.impl.util.registry;

import oracle.kv.impl.mgmt.jmx.JmxAgent;
import oracle.kv.impl.util.PortRange;

/**
 * Provides an implementation of RMISocketPolicy that transmits information
 * "in the clear", with no encryption.
 */
public class ClearSocketPolicy implements RMISocketPolicy {

    public ClearSocketPolicy() {
    }

    /**
     * Prepare for use as standard client policy.
     */
    @Override
    public void prepareClient(String storeContext) {
        /* No action needed */
    }

    /**
     * Registry creation sockets factories.
     */
    @Override
    public SocketFactoryPair getRegistryPair(SocketFactoryArgs args) {
        final ServerSocketFactory ssf = (args.getSsfName() == null) ?
            /* Provide a default server socket factory if none is requested */
            ClearServerSocketFactory.create(0, PortRange.UNCONSTRAINED) :
            ClearServerSocketFactory.create(args.getSsfBacklog(),
                                            args.getSsfPortRange());

        /* Any CSF args specified are ignored */
        final ClientSocketFactory csf = null;

        return new SocketFactoryPair(ssf, csf);
    }

    /*
     * Return a Client socket factory appropriate for registry access by the
     * client.
     */
    @Override
    public ClientSocketFactory getRegistryCSF(SocketFactoryArgs args) {
        /*
         * Until we get to the next upgrade release boundary beyond 3.0,
         * return ClientSocketFactory.
         */
        return new ClearClientSocketFactory(args.getCsfName(),
                                            args.getCsfConnectTimeout(),
                                            args.getCsfReadTimeout());
    }

    /**
     * Standard RMI export socket factories.
     */
    @Override
    public SocketFactoryPair getBindPair(SocketFactoryArgs args) {
        if (args.getSsfPortRange() == null ||

            /*
             * When exporting for JMX access, don't supply a CSF.  JMX clients
             * probably won't have our client library available, so they'll
             * need to use the Java-provided CSF class.
             */
            JmxAgent.JMX_SSF_NAME.equals(args.getSsfName())) {
            return new SocketFactoryPair(null, null);
        }

        final ServerSocketFactory ssf =
            ClearServerSocketFactory.create(args.getSsfBacklog(),
                                            args.getSsfPortRange());

        ClientSocketFactory csf = null;

        if ((args.getCsfName() != null) && args.getUseCsf()) {
            /*
             * Although it would be good to use ClearClientSocketFactory here,
             * the R2 -> R3 upgrade clients might not have ClientSocketFactory
             * available, so we continue to use ClientSocketFactory for
             * purposes of compatibility.
             *
             * TODO: convert to ClearClientSocketFactory at the next upgrade
             * version boundary.
             */
            csf = new ClientSocketFactory(args.getCsfName(),
                                          args.getCsfConnectTimeout(),
                                          args.getCsfReadTimeout());
        }
        return new SocketFactoryPair(ssf, csf);
    }

    /**
     * Reports whether the policy allows a server to be able to "trust" an
     * incoming client connection.
     */
    @Override
    public boolean isTrustCapable() {
        return false;
    }
}

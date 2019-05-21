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

package oracle.kv.impl.util.registry.ssl;

import static oracle.kv.impl.util.registry.ssl.SSLClientSocketFactory.Use;

import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;

/**
 * An RMISocketPolicy implementation that is responsible for producing socket
 * factories for RMI.  This class supports both client and server side
 * operations.
 */
public class SSLServerSocketPolicy extends SSLSocketPolicy {

    /**
     * Creates an instance of this class.
     */
    public SSLServerSocketPolicy(SSLControl serverSSLControl,
                                 SSLControl clientSSLControl) {
        super(serverSSLControl, clientSSLControl);
    }

    /**
     * Create a SocketFactoryPair appropriate for creation of an RMI registry.
     */
    @Override
    public SocketFactoryPair getRegistryPair(SocketFactoryArgs args) {
        final ServerSocketFactory ssf =
            SSLServerSocketFactory.create(serverSSLControl,
                                          args.getSsfBacklog(),
                                          args.getSsfPortRange());
        final ClientSocketFactory csf = null;

        return new SocketFactoryPair(ssf, csf);
    }

    /**
     * Create a SocketFactoryPair appropriate for exporting an object over RMI.
     */
    @Override
    public SocketFactoryPair getBindPair(SocketFactoryArgs args) {
        final ServerSocketFactory ssf =
            SSLServerSocketFactory.create(serverSSLControl,
                                          args.getSsfBacklog(),
                                          args.getSsfPortRange());

        ClientSocketFactory csf = null;
        if (args.getCsfName() != null) {
            if (isTrusted()) {
                csf = new SSLClientSocketFactory(args.getCsfName(),
                                                 args.getCsfConnectTimeout(),
                                                 args.getCsfReadTimeout(),
                                                 Use.TRUSTED);
            } else {
                csf = new SSLClientSocketFactory(args.getCsfName(),
                                                 args.getCsfConnectTimeout(),
                                                 args.getCsfReadTimeout(),
                                                 args.getKvStoreName());
            }
        }

        return new SocketFactoryPair(ssf, csf);
    }
}


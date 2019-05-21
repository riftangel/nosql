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

package oracle.kv.impl.monitor;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils.ReplaceableRMIClientSocketFactory;

public abstract class TrackerListenerImpl
    extends UnicastRemoteObject implements TrackerListener {

    private static final long serialVersionUID = 1L;

    /**
     * Items with timestamps earlier than this are of no
     * interest to this subscriber.
     */
    long interestingTime;

    public TrackerListenerImpl(ServerSocketFactory ssf,
                               long interestingTime)
        throws RemoteException {

        super(0, ReplaceableRMIClientSocketFactory.INSTANCE, ssf);

        this.interestingTime = interestingTime;
    }

    /**
     * TODO: Check whether users of this constructor are legitimate test
     * users, or client side users where the server factory does not matter
     */
    public TrackerListenerImpl(long interestingTime)
        throws RemoteException {

        super(0);

        this.interestingTime = interestingTime;
    }

    public void setInterestingTime(long t) {
        interestingTime = t;
    }

    @Override
    public long getInterestingTime() {
        return interestingTime;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " intTime=" + interestingTime;
    }
}

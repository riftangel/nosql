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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * A range of ports that the listener can choose from.
 */
public class ListenerPortRange {

    private static final InetAddress ANY_LOCAL =
        new InetSocketAddress(0).getAddress();

    private final InetAddress address;
    private final int portStart;
    private final int portEnd;

    /**
     * Constructor identifying the interface and the port range within which to
     * choose to listen on.
     *
     * @param hostname the host name of the interface, any local address if null
     * @param portStart the starting port (inclusive)
     * @param portEnd the ending port (inclusive)
     */
    public ListenerPortRange(String hostname, int portStart, int portEnd)
        throws UnknownHostException {

        this((hostname == null) ? ANY_LOCAL : InetAddress.getByName(hostname),
                portStart, portEnd);
    }

    /**
     * Constructor identifying the port range on any interface within which to
     * choose to listen on.
     *
     * @param portStart the starting port (inclusive)
     * @param portEnd the ending port (inclusive)
     */
    public ListenerPortRange(int portStart, int portEnd) {
        this(ANY_LOCAL, portStart, portEnd);
    }

    /**
     * Constructor identifying the interface and any port to listen on.
     *
     * @param hostname the host name of the interface, null if loopback interface
     */
    public ListenerPortRange(String hostname) throws UnknownHostException {
        this(InetAddress.getByName(hostname));
    }

    /**
     * Constructor for any interface and any port to listen on.
     */
    public ListenerPortRange() {
        this(ANY_LOCAL);
    }

    /**
     * Returns the interface address.
     *
     * @return the address
     */
    public InetAddress getAddress() {
        return address;
    }

    /**
     * Returns the starting port (inclusive).
     *
     * @return the starting port
     */
    public int getPortStart() {
        return portStart;
    }

    /**
     * Returns the ending port (inclusive).
     *
     * @return the ending port
     */
    public int getPortEnd() {
        return portEnd;
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj == null) || !(obj instanceof ListenerPortRange)) {
            return false;
        }
        ListenerPortRange that = ((ListenerPortRange) obj);
        return ((this.address.equals(that.address)) &&
                (this.portStart == that.portStart) &&
                (this.portEnd == that.portEnd));
    }

    @Override
    public int hashCode() {
        int hash = address.hashCode() *31 + portStart;
        hash = hash * 31 + portEnd;
        return hash;
    }

    @Override
    public String toString() {
         return String.format("%s:%s-%s", address, portStart, portEnd);
    }

    /**
     * Constructs a port range with specified interface, starting port and
     * ending port.
     *
     * @param address interface address
     * @param portStart starting port, must be positive
     * @param portEnd ending port, must be larger than portStart, but less than
     * 65536.
     */
    private ListenerPortRange(InetAddress address,
                              int portStart,
                              int portEnd) {
        if (portStart <= 0) {
            throw new IllegalArgumentException(
                "Starting port should be greater than 0");
        }
        if (portEnd < portStart) {
            throw new IllegalArgumentException(
                    "Starting port is larger than ending port");
        }
        if (portEnd >= 1 << 16) {
            throw new IllegalArgumentException(
                    "Port cannot be larger than 16 bits");
        }
        this.address = address;
        this.portStart = portStart;
        this.portEnd = portEnd;
    }

    /**
     * Constructs a port range with specified interface.
     *
     * @param address interface address
     */
    private ListenerPortRange(InetAddress address) {
        this.address = address;
        this.portStart = 0;
        this.portEnd = 0;
    }
}

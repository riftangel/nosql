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
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * A wrapper class to replace the InetSocketAddress for dealing with the
 * IP-address-change problem in the cloud.
 *
 * The problem is that InetAddress resolves a hostname to IP address upon
 * creation. After that, when we use the InetAddress to connect, it will use
 * the resolved IP address. When in the cloud, the IP address of a hostname is
 * susceptible to change and thus the already-resolved
 * InetAddress/InetSocketAddress will be invalid after the change.
 *
 * The class simply constructs a new InetAddress/InetSocketAddress when one is
 * needed and this will do a new address resolution at the point it is
 * required. The resolution part of the InetAddress uses a cache, so this
 * should not have too much performance impact.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class NetworkAddress extends SocketAddress
        implements FastExternalizable {

    private static final long serialVersionUID = 1;
    private static final int MAX_PORT = 65535;

    private static final InetAddress ANY_LOCAL =
        new InetSocketAddress(0).getAddress();

    private final String hostname;
    private final int port;

    /**
     * Creates a network address from a host name and a port.
     *
     * @param hostname the host name
     * @param port the port
     */
    public NetworkAddress(String hostname, int port) {
        checkNull("hostname", hostname);
        this.hostname = hostname;
        this.port = port;
        checkPort(port);
    }

    private static void checkPort(int port) {
        if ((port < 0) || (port > MAX_PORT)) {
            throw new IllegalArgumentException("Illegal port: " + port);
        }
    }

    /**
     * Creates a network address from an IP address and a port.
     *
     * @param address the IP address
     * @param port the port
     */
    public NetworkAddress(InetAddress address, int port) {
        checkNull("address", address);
        this.hostname = address.getHostName();
        this.port = port;
        checkPort(port);
    }

    /**
     * Creates a network address where the IP address is the wildcard address
     * and the port number a specified value.
     *
     * @param port the port
     */
    public NetworkAddress(int port) {
        this(ANY_LOCAL, port);
        checkPort(port);
    }

    /**
     * Initializes an instance from an input stream.
     *
     * @param in the input stream
     * @param serialVersion the version of the serialized form
     */
    public NetworkAddress(DataInput in, short serialVersion)
        throws IOException {

        hostname = readNonNullString(in, serialVersion);
        port = in.readUnsignedShort();
        try {
            checkPort(port);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid field: " + e.getMessage(), e);
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString String}) {@link
     *      #getHostName hostname}
     * <li> ({@link DataOutput#writeShort short}) {@link #getPort port}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullString(out, serialVersion, hostname);
        out.writeShort(port);
    }

    /**
     * Gets the IP address.
     *
     * The method will do a hostname-IP resolution every time the method is
     * called.
     *
     * @return the IP address
     */
    public InetAddress getInetAddress() throws UnknownHostException {
        return InetAddress.getByName(hostname);
    }

    /**
     * Gets the socket address.
     *
     * The method will do a hostname-IP resolution every time the method is
     * called.
     *
     * @return the socket address
     */
    public InetSocketAddress getInetSocketAddress() {
         return new InetSocketAddress(hostname, port);
    }

    /**
     * Gets the host name.
     *
     * @return the host name
     */
    public String getHostName() {
         return hostname;
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj == null) || !(obj instanceof NetworkAddress)) {
            return false;
        }
        NetworkAddress addr = ((NetworkAddress) obj);
        if (!addr.hostname.equals(hostname)) {
            return false;
        }
        return addr.port == port;
    }

    @Override
    public int hashCode() {
        return hostname.hashCode() *31 + port;
    }

    @Override
    public String toString() {
         return hostname + ":" + port;
    }
}

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

package oracle.kv.impl.async.dialog.nio;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLEngine;

import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.ListenerPortRange;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.security.ssl.SSLControl;

import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.utilint.net.SSLDataChannel;
import com.sleepycat.je.rep.utilint.net.SimpleDataChannel;


/**
 * Util class for nio channel operations.
 */
public class NioUtil {

    /**
     * A mapping from supported socket options to async options.
     *
     * The mapping includes all supported socket options that has a async
     * correspondence. When configuring a socket channel, all proper options
     * are looked up for their async value and set accordingly.
     *
     * The setters are for 1.6 compatibility. If we do not need to support java
     * 1.6 for clients, we should simply use StandardSocketOptions.
     */
    public static final Map<SocketSetter<?>, AsyncOption<?>> socketSetterMap;
    static {
        Map<SocketSetter<?>, AsyncOption<?>> mapping =
            new HashMap<SocketSetter<?>, AsyncOption<?>>();
        mapping.put(new SoKeepAlive(), AsyncOption.SO_KEEPALIVE);
        mapping.put(new SoLinger(), AsyncOption.SO_LINGER);
        mapping.put(new SoRcvBuf(), AsyncOption.SO_RCVBUF);
        mapping.put(new SoReuseAddr(), AsyncOption.SO_REUSEADDR);
        mapping.put(new SoSndBuf(), AsyncOption.SO_SNDBUF);
        mapping.put(new TcpNoDelay(), AsyncOption.TCP_NODELAY);
        socketSetterMap = Collections.unmodifiableMap(mapping);
    }

    /**
     * A mapping from supported server socket options to async options.
     *
     * The mapping includes all supported server socket options that has a
     * async correspondence. When configuring a server socket channel, all
     * proper options are looked up for their async value and set accordingly.
     *
     * The setters are for 1.6 compatibility. If we do not need to support java
     * 1.6 for clients, we should simply use StandardSocketOptions.
     */
    public static final Map<ServerSocketSetter<?>, AsyncOption<?>>
        serverSocketSetterMap;
    static {
        Map<ServerSocketSetter<?>, AsyncOption<?>> mapping =
            new HashMap<ServerSocketSetter<?>, AsyncOption<?>>();
        mapping.put(new SsoRcvBuf(), AsyncOption.SO_RCVBUF);
        mapping.put(new SsoReuseAddr(), AsyncOption.SO_REUSEADDR);
        serverSocketSetterMap = Collections.unmodifiableMap(mapping);
    }

    /**
     * Creates a data channel for a client.  Note that the underlying NIO
     * channel may not be connected, so the caller should check for that and
     * register for connect events if it isn't connected yet.
     */
    public static DataChannel getDataChannel(NetworkAddress address,
                                             EndpointConfig endpointConfig,
                                             Logger logger)
        throws IOException {

        SocketChannel socketChannel =
            SocketChannel.open();
        configureSocketChannel(socketChannel, endpointConfig);
        socketChannel.connect(address.getInetSocketAddress());
        return getDataChannel(
                socketChannel, endpointConfig.getSSLControl(),
                true, address.getHostName(), logger);
    }

    /**
     * Creates a data channel for a server when accepting the channel.
     */
    public static DataChannel getDataChannel(SocketChannel socketChannel,
                                             EndpointConfig endpointConfig,
                                             Logger logger)
        throws IOException {

        configureSocketChannel(socketChannel, endpointConfig);
        return getDataChannel(
                socketChannel, endpointConfig.getSSLControl(),
                false, null, logger);
    }

    /**
     * Listens with listener channel factory.
     */
    public static ServerSocketChannel listen(
            final ListenerConfig listenerConfig) throws IOException {

        final ServerSocketChannel serverSocketChannel =
            ServerSocketChannel.open();
        /* Set options */
        for (final ServerSocketSetter<?> s : serverSocketSetterMap.keySet()) {
            new ServerSocketSetHelper() {
                @Override
                public <T> void run() throws IOException {
                    @SuppressWarnings("unchecked")
                    final AsyncOption<T> ao =
                        (AsyncOption<T>) serverSocketSetterMap.get(s);
                    @SuppressWarnings("unchecked")
                    final ServerSocketSetter<T> setter =
                        (ServerSocketSetter<T>) s;
                    if (ao != null) {
                        final T val = listenerConfig.getOption(ao);
                        if (val != null) {
                            setter.set(serverSocketChannel.socket(), val);
                        }
                    }
                }
            }.run();
        }

        int backlog = listenerConfig.getOption(AsyncOption.SSO_BACKLOG);
        ListenerPortRange portRange = listenerConfig.getPortRange();
        final InetAddress addr = portRange.getAddress();
        final int portStart = portRange.getPortStart();
        final int portEnd = portRange.getPortEnd();
        for (int port = portStart; port <= portEnd; ++port) {
            try {
                serverSocketChannel.socket().bind(
                        new InetSocketAddress(addr, port), backlog);
                return serverSocketChannel;
            } catch (IOException e) {
                /* Address in use or unusable, continue the scan */
            }
        }

        /*
         * For compatibility with
         * ServerSocketFactory.createStandardServerSocket, throw a
         * BindException in the single port case, since I believe that is what
         * the standard server socket constructor does.
         */
        final String msg = String.format(
            "No free local address to bind for range %s", portRange);
        if (portStart == portEnd) {
            throw new BindException(msg);
        }
        throw new IOException(msg);
    }

    /**
     * Returns the remote network address of a socket channel.
     */
    public static NetworkAddress
        getRemoteAddress(SocketChannel socketChannel) {

        Socket socket = socketChannel.socket();
        return new NetworkAddress(socket.getInetAddress(), socket.getPort());
    }


    /**
     * Returns the local network address of a server socket channel.
     */
    public static NetworkAddress
        getLocalAddress(ServerSocketChannel serverSocketChannel) {

        ServerSocket serverSocket = serverSocketChannel.socket();
        return new NetworkAddress(
                serverSocket.getInetAddress(),
                serverSocket.getLocalPort());
    }

    /**
     * Configures the socket channel with options.
     */
    private static void configureSocketChannel(
        final SocketChannel socketChannel,
        final EndpointConfig endpointConfig)
        throws IOException {

        socketChannel.configureBlocking(false);

        for (final SocketSetter<?> s : socketSetterMap.keySet()) {
            new SocketSetHelper() {
                @Override
                public <T> void run() throws IOException {
                    @SuppressWarnings("unchecked")
                    final AsyncOption<T> ao =
                        (AsyncOption<T>) socketSetterMap.get(s);
                    @SuppressWarnings("unchecked")
                    final SocketSetter<T> setter = (SocketSetter<T>) s;
                    if (ao != null) {
                        final T val = endpointConfig.getOption(ao);
                        if (val != null) {
                            setter.set(socketChannel.socket(), val);
                        }
                    }
                }
            }.run();
        }
    }

    /**
     * Creates the data channel.
     */
    private static DataChannel getDataChannel(SocketChannel socketChannel,
                                              SSLControl sslControl,
                                              boolean isClient,
                                              String hostname,
                                              Logger logger) {

        if (sslControl == null) {
            return new SimpleDataChannel(socketChannel);
        }

        final SSLEngine engine = sslControl.sslContext().createSSLEngine(
                hostname, socketChannel.socket().getPort());
        engine.setSSLParameters(sslControl.sslParameters());
        engine.setUseClientMode(isClient);
        if (!isClient) {
            if (sslControl.peerAuthenticator() != null) {
                engine.setWantClientAuth(true);
            }
        }
        return new SSLDataChannel(
                socketChannel, engine, hostname,
                sslControl.hostVerifier(), sslControl.peerAuthenticator(),
                new WrappedLogger(logger));
    }

    /**
     * A Wrapped class for je instance logger.
     */
    private static class WrappedLogger implements InstanceLogger {

        private final Logger logger;

        WrappedLogger(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void log(Level logLevel, String msg) {
            logger.log(logLevel, msg);
        }
    }

    /*
     * The following socket option setting classes is for the purpose of java
     * 1.6 compatibility. If we do not need to support 1.6 for clients, we
     * should simply use SocketOption instead.
     */

    /**
     * Sets an option for a socket.
     */
    private interface SocketSetter<T> {
        void set(Socket s, T v) throws IOException;
    }

    /**
     * Sets an option for a server socket.
     */
    private interface ServerSocketSetter<T> {
        void set(ServerSocket ss, T v) throws IOException;
    }

    private static class SoKeepAlive implements SocketSetter<Boolean> {
        @Override
        public void set(Socket s, Boolean v) throws IOException {
            s.setKeepAlive(v);
        }
    }

    private static class SoLinger implements SocketSetter<Integer> {
        @Override
        public void set(Socket s, Integer v) throws IOException {
            s.setSoLinger(true, v);
        }
    }

    private static class SoRcvBuf implements SocketSetter<Integer> {
        @Override
        public void set(Socket s, Integer v) throws IOException {
            s.setReceiveBufferSize(v);
        }
    }

    private static class SoReuseAddr implements SocketSetter<Boolean> {
        @Override
        public void set(Socket s, Boolean v) throws IOException {
            s.setReuseAddress(v);
        }
    }

    private static class SoSndBuf implements SocketSetter<Integer> {
        @Override
        public void set(Socket s, Integer v) throws IOException {
            s.setSendBufferSize(v);
        }
    }

    private static class TcpNoDelay implements SocketSetter<Boolean> {
        @Override
        public void set(Socket s, Boolean v) throws IOException {
            s.setTcpNoDelay(v);
        }
    }

    private static class SsoRcvBuf implements ServerSocketSetter<Integer> {
        @Override
        public void set(ServerSocket ss, Integer v) throws IOException {
            ss.setReceiveBufferSize(v);
        }
    }

    private static class SsoReuseAddr implements ServerSocketSetter<Boolean> {
        @Override
        public void set(ServerSocket ss, Boolean v) throws IOException {
            ss.setReuseAddress(v);
        }
    }

    /**
     * Helper for wild card capture.
     */
    private interface SocketSetHelper {
        <T> void run() throws IOException;
    }

    /**
     * Helper for wild card capture.
     */
    private interface ServerSocketSetHelper {
        <T> void run() throws IOException;
    }

}

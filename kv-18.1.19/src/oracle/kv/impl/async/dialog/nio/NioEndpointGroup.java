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
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.AbstractEndpointGroup;
import oracle.kv.impl.async.AbstractListener;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.util.CommonLoggerUtils;


public class NioEndpointGroup extends AbstractEndpointGroup {

    private final NioChannelThreadPool channelThreadPool;

    public NioEndpointGroup(Logger logger, int nthreads) throws Exception {
        super(logger);
        channelThreadPool = new NioChannelThreadPool(logger, nthreads);
    }

    @Override
    public ScheduledExecutorService getSchedExecService() {
        return channelThreadPool.next();
    }

    @Override
    protected NioCreatorEndpoint
        newCreatorEndpoint(NetworkAddress address,
                           EndpointConfig endpointConfig) {

        return new NioCreatorEndpoint(
                this, channelThreadPool, address, endpointConfig);
    }

    @Override
    protected NioListener newListener(AbstractEndpointGroup endpointGroup,
                                      ListenerConfig listenerConfig,
                                      Map<Integer, DialogHandlerFactory>
                                      dialogHandlerFactories) {

        return new NioListener(
                endpointGroup, listenerConfig, dialogHandlerFactories);
    }

    @Override
    protected void shutdownInternal(boolean force) {
        channelThreadPool.shutdown(force);
    }

    class NioListener extends AbstractListener {

        private volatile ServerSocketChannel listeningChannel = null;
        private final NioChannelAccepter channelAccepter =
            new NioChannelAccepter();

        NioListener(AbstractEndpointGroup endpointGroup,
                    ListenerConfig listenerConfig,
                    Map<Integer, DialogHandlerFactory>
                    dialogHandlerFactories) {
            super(endpointGroup, listenerConfig, dialogHandlerFactories);
        }

        /**
         * Creates the listening channel if not existing yet.
         *
         * The method is called inside a synchronization block of the parent
         * endpoint group.
         */
        @Override
        protected void createChannel() throws IOException {
            if (listeningChannel == null) {
                listeningChannel = NioUtil.listen(listenerConfig);
                try {
                    listeningChannel.configureBlocking(false);
                    channelThreadPool.next().registerAccept(
                            listeningChannel, channelAccepter);
                } catch (IOException e) {
                    listeningChannel.close();
                    listeningChannel = null;
                    throw e;
                }
            }
        }

        /**
         * Close the created listening channel.
         *
         * The method is called inside a synchronization block of the parent
         * endpoint group.
         */
        @Override
        protected void closeChannel() {
            if (listeningChannel == null) {
                return;
            }
            try {
                listeningChannel.close();
            } catch (IOException e) {
                getLogger().log(Level.INFO,
                        "Error closing server channel: {0}", e);
            }
            listeningChannel = null;
        }

        @Override
        protected NetworkAddress getLocalAddress() {
            if (listeningChannel == null) {
                return null;
            }
            return NioUtil.getLocalAddress(listeningChannel);
        }

        @Override
        public String toString() {
            return String.format(
                    "NioListener[listeningChannel=%s]", listeningChannel);
        }

        /**
         * Accepter for the listener.
         */
        class NioChannelAccepter implements ChannelAccepter {

            /**
             * Called when the connection is accepted.
             */
            @Override
            public void onAccept(final SocketChannel socketChannel)
                throws IOException {

                final Logger logger = getLogger();
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "{0} accepting a connection: {1}",
                            new Object[] {
                                getClass().getSimpleName(),
                                socketChannel.socket() });
                }

                NetworkAddress remoteAddress =
                    NioUtil.getRemoteAddress(socketChannel);
                NioChannelExecutor executor = channelThreadPool.next();
                final NioResponderEndpoint endpoint =
                    new NioResponderEndpoint(
                            NioEndpointGroup.this,
                            remoteAddress,
                            listenerConfig,
                            NioListener.this,
                            endpointConfig,
                            executor,
                            NioUtil.getDataChannel(
                                socketChannel, endpointConfig, getLogger()));
                acceptedEndpoints.add(endpoint);
                addResponderEndpoint(endpoint);
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        endpoint.getHandler().onConnected();
                    }
                });
            }

            /**
             * Called when the handler executor is closing this handler.
             */
            @Override
            public void onClosing() {
                getLogger().log(Level.FINE, "Accept handler closed: {0}",
                                NioListener.this);
            }

            /**
             * Called when there is an error uncaught from the above methods or occured
             * to the executor.
             */
            @Override
            public void onError(Throwable t, SelectableChannel channel) {
                final Logger logger = getLogger();
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO, "Got error when accept, {0}: {1}",
                            new Object[] {
                                NioListener.this,
                                CommonLoggerUtils.getStackTrace(t)});
                }

                try {
                    if (!(t instanceof SecurityException)) {
                        try {
                            /*
                             * TODO: if the error is caused by running out of
                             * file descriptors, we should probably do an
                             * optimization to close some idle connections
                             * accepted by this listener.
                             */
                        } catch (Throwable tt) {
                        }
                    }

                    /*
                     * A NoClassDefFoundError can occur if no file
                     * descriptors are available.
                     */
                    if (t instanceof Exception ||
                        t instanceof OutOfMemoryError ||
                        t instanceof NoClassDefFoundError) {
                        /*
                         * TODO: do some throttling for large burst of
                         * exceptions.
                         */
                    } else {
                        shutdown();
                    }
                } finally {
                    ServerSocketChannel serverSocketChannel =
                        (ServerSocketChannel) channel;
                    onChannelError(t, !serverSocketChannel.isOpen());
                }
            }
        }

    }
}

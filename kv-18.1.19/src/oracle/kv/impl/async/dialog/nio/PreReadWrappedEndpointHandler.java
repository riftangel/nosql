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
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.AbstractResponderEndpoint;
import oracle.kv.impl.async.SocketPrepared;
import oracle.kv.impl.async.dialog.ProtocolMesg;
import oracle.kv.impl.util.CommonLoggerUtils;

import com.sleepycat.je.rep.net.DataChannel;

class PreReadWrappedEndpointHandler
    implements EndpointHandler, EndpointHandlerManager, ChannelHandler {

    /** The magic number at the start of an async connection. */
    private static final int ASYNC_MAGIC_NUMBER =
        BytesUtil.bytesToInt(ProtocolMesg.MAGIC_NUMBER, 0);

    /** The magic number at the start of an RMI connection. */
    private static final int RMI_MAGIC_NUMBER = 0x4a524d49;

    /**
     * The magic number at the start of most TLS v1.2 connections. I think the
     * 0 in the 4th byte could be different for longer messages, but I don't
     * think we've seen that in practice.
     */
    private static final int TLS12_MAGIC_NUMBER = 0x16030300;

    private final AbstractResponderEndpoint responderEndpoint;
    private final NetworkAddress remoteAddress;
    private final NioChannelExecutor channelExecutor;
    private final SocketChannel socketChannel;
    private final NioEndpointGroup.NioListener listener;
    private final byte[] magicNumber =
        new byte[ProtocolMesg.MAGIC_NUMBER.length];
    private final ByteBuffer magicNumberBuf = ByteBuffer.wrap(magicNumber);
    private final NioEndpointHandler endpointHandler;
    private volatile boolean isShutdown = false;

    PreReadWrappedEndpointHandler(AbstractResponderEndpoint responderEndpoint,
                                  EndpointConfig endpointConfig,
                                  NetworkAddress remoteAddress,
                                  NioChannelExecutor channelExecutor,
                                  NioEndpointGroup.NioListener listener,
                                  DataChannel dataChannel) {
        this.responderEndpoint = responderEndpoint;
        this.remoteAddress = remoteAddress;
        this.channelExecutor = channelExecutor;
        this.socketChannel = dataChannel.getSocketChannel();
        this.listener = listener;
        this.endpointHandler = new NioEndpointHandler(
                responderEndpoint.getLogger(),
                this, endpointConfig, false, remoteAddress, channelExecutor,
                listener.getDialogHandlerFactories(), dataChannel);
    }

    /**
     * Returns the network address of the remote endpoint.
     */
    @Override
    public NetworkAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Returns the executor service associated with this context.
     */
    @Override
    public ScheduledExecutorService getSchedExecService() {
        return channelExecutor;
    }

    /**
     * Returns the statistically universal unique ID for the handler.
     */
    @Override
    public long getUUID() {
        return endpointHandler.getUUID();
    }

    /**
     * Returns the statistically universal unique ID for the connection.
     */
    @Override
    public long getConnID() {
        return endpointHandler.getConnID();
    }

    /**
     * Returns a string format of the handler ID.
     */
    @Override
    public String getStringID() {
        return endpointHandler.getStringID();
    }

    /**
     * Starts a dialog.
     */
    @Override
    public void startDialog(int dialogType,
                            DialogHandler dialogHandler,
                            long timeoutMillis) {
        endpointHandler.startDialog(
                dialogType, dialogHandler, timeoutMillis);
    }

    /**
     * Returns the limit on the number of dialogs this endpoint can
     * concurrently start.
     */
    @Override
    public int getNumDialogsLimit() {
        return endpointHandler.getNumDialogsLimit();
    }

    /**
     * Shuts down the handler.
     */
    @Override
    public void shutdown(String detail, boolean force) {
        if (isShutdown) {
            return;
        }
        isShutdown = true;
        responderEndpoint.onHandlerShutdown(this);
        endpointHandler.shutdown(detail, force);
    }

    /**
     * Called when an endpoint handler is shutting down.
     */
    @Override
    public void onHandlerShutdown(EndpointHandler handler) {
        if (isShutdown) {
            return;
        }
        isShutdown = true;
        responderEndpoint.onHandlerShutdown(this);
    }

    /**
     * Called when the connection is established.
     */
    @Override
    public void onConnected() {
        try {
            if (!onChannelPreRead()) {
                channelExecutor.registerRead(socketChannel, this);
            }
        } catch (IOException e) {
            if (getLogger().isLoggable(Level.FINE)) {
                getLogger().log(Level.FINE,
                        "Error registering for read after pre-read: " +
                        "endpoint={0}, error={1}",
                        new Object[] {
                            endpointHandler,
                            CommonLoggerUtils.getStackTrace(e)});
            }
            shutdown(e.getMessage(), true);
        }
    }

    /**
     * Called when the channel is ready for read.
     */
    @Override
    public void onRead() {
        onChannelPreRead();
    }

    /**
     * Called when the channel is ready for write
     */
    @Override
    public void onWrite() {
        /*
         * We should not be writing before the pre-read is done. After it is
         * done, this handler should not be notified.
         */
        throw new IllegalStateException();
    }

    /**
     * Called when the handler executor is closing this handler.
     */
    @Override
    public void onClosing() {
        shutdown("Executor closing", true);
    }

    /**
     * Called when there is an error uncaught from the above methods or occured
     * to the executor.
     */
    @Override
    public void onError(Throwable t, SelectableChannel channel) {
        endpointHandler.onError(t, channel);
    }

    private boolean onChannelPreRead() {

        final Logger logger = getLogger();
        try {
            socketChannel.read(magicNumberBuf);
            if (magicNumberBuf.remaining() != 0) {
                return false;
            }

            if (logger.isLoggable(Level.FINE)) {
                final int magicInt = BytesUtil.bytesToInt(magicNumber, 0);
                logger.log(Level.FINE,
                           "Done pre-reading: magic number={0}{1}, " +
                           "remoteAddress={2}, localAddress={3}",
                           new Object[] {
                               BytesUtil.toString(
                                   magicNumber, 0, magicNumber.length),
                               ((magicInt == ASYNC_MAGIC_NUMBER) ?
                                " (Async)" :
                               (magicInt == RMI_MAGIC_NUMBER) ?
                                " (RMI)" :
                                (magicInt == TLS12_MAGIC_NUMBER) ?
                                " (TLSv1.2)" :
                                ""),
                               remoteAddress, listener.getLocalAddress() });
            }

            if (!Arrays.equals(magicNumber, ProtocolMesg.MAGIC_NUMBER)) {
                /* Not something for our nio async */
                try {
                    final SocketPrepared socketPrepared =
                        listener.getSocketPrepared();
                    if (socketPrepared == null) {
                        logger.log(Level.INFO,
                                "Got non-async connection, " +
                                "but no sync handler is present: " +
                                "remoteAddress={0}, localAddress={1}",
                                new Object[] {
                                    remoteAddress, listener.getLocalAddress()
                                });
                    } else {
                        endpointHandler.handedOffToSync();
                        channelExecutor.deregister(socketChannel);
                        socketChannel.configureBlocking(true);
                        /*
                         * Clear buffer so that SocketPrepared#onPrepared will get
                         * a cleared buffer.
                         */
                        magicNumberBuf.clear();
                        logger.log(Level.FINE,
                                "Handing off the connection " +
                                "to the non-async handler: " +
                                "handler={0}, buf={1}, socket={2}",
                                new Object[] {
                                    socketPrepared,
                                    BytesUtil.toString(
                                            magicNumberBuf,
                                            magicNumberBuf.limit()),
                                    socketChannel.socket() });
                        socketPrepared.onPrepared(
                                magicNumberBuf, socketChannel.socket());
                    }
                } finally {
                    shutdown("Got non-async connection", true);
                }
                return true;
            }

            if (listener.getDialogHandlerFactories().isEmpty()) {
                logger.log(Level.INFO,
                        "Got async connection, " +
                        "but no available dialog factory for responding");
                shutdown("No factory for async connection", true);
                return true;
            }

            logger.log(
                Level.FINE,
                "Async endpoint handler enabled for a new connection: {0}",
                endpointHandler);

            channelExecutor.registerRead(socketChannel, endpointHandler);
            endpointHandler.onChannelReady();
            endpointHandler.onRead();
            return true;
        } catch (Throwable t) {
            Level level = Level.INFO;
            if (t instanceof IOException) {
                /* IOException is expected, log with fine */
                level = Level.FINE;
            }
            if (logger.isLoggable(level)) {
                logger.log(level,
                        "Error while doing pre-read, " +
                        "remoteAddress={0}, localAddress={1}: {2}",
                        new Object[] {
                            remoteAddress, listener.getLocalAddress(),
                            CommonLoggerUtils.getStackTrace(t) });
            }
            shutdown(t.getMessage(), true);
            return true;
        }
    }

    private Logger getLogger() {
        return responderEndpoint.getLogger();
    }
}

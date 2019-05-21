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

package oracle.kv.impl.async.dialog.netty;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.CommonLoggerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.logging.LoggingHandler;

/**
 * Pre-reads some bytes from the channel and see if the channel is of async.
 */
class PreReadWrappedEndpointHandler
    extends SimpleChannelInboundHandler<ByteBuf>
    implements EndpointHandler, EndpointHandlerManager {

    private final AbstractResponderEndpoint responderEndpoint;
    private final EndpointConfig endpointConfig;
    private final NetworkAddress remoteAddress;
    private final NettyEndpointGroup.NettyListener listener;
    private final NettyEndpointHandler endpointHandler;
    private volatile boolean preReadDone = false;
    private volatile boolean isShutdown = false;

    private final byte[] magicNumber =
        new byte[ProtocolMesg.MAGIC_NUMBER.length];
    private final ByteBuffer magicNumberBuf = ByteBuffer.wrap(magicNumber);

    PreReadWrappedEndpointHandler(AbstractResponderEndpoint responderEndpoint,
                                  EndpointConfig endpointConfig,
                                  NetworkAddress remoteAddress,
                                  NettyEndpointGroup.NettyListener listener) {
        this.responderEndpoint = responderEndpoint;
        this.endpointConfig = endpointConfig;
        this.remoteAddress = remoteAddress;
        this.listener = listener;
        this.endpointHandler = new NettyEndpointHandler(
                responderEndpoint.getLogger(),
                this, endpointConfig, false, remoteAddress,
                listener.getDialogHandlerFactories());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext context) {
        ChannelPipeline pipeline = context.pipeline();
        if (NettyEndpointGroup.logHandlerEnabled()) {
            pipeline.addFirst(new LoggingHandler());
        }
        SSLControl sslControl = endpointConfig.getSSLControl();
        if (sslControl != null) {
            VerifyingSSLHandler handler = NettyUtil.newSSLHandler(
                    endpointHandler.getStringID(),
                    sslControl, remoteAddress, false,
                    responderEndpoint.getLogger());
            pipeline.addLast(handler.sslHandler());
            pipeline.addLast(handler);
        }
        pipeline.addLast(endpointHandler.decoder());
    }

    @Override
    public void channelRead0(ChannelHandlerContext context, ByteBuf msg)
        throws Exception {

        final Logger logger = responderEndpoint.getLogger();

        if (!preReadDone) {
            msg.readBytes(magicNumberBuf);
            if (magicNumberBuf.remaining() != 0) {
                return;
            }

            preReadDone = true;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE,
                        "Got connection with magic number: {0}",
                        BytesUtil.toString(magicNumber, 0, magicNumber.length));
            }

            if (!Arrays.equals(magicNumber, ProtocolMesg.MAGIC_NUMBER)) {
                try {
                    /* Not something for our nio async */
                    final ByteBuffer preReadBytes = ByteBuffer.allocate(
                            magicNumber.length + msg.readableBytes());
                    magicNumberBuf.clear();
                    preReadBytes.put(magicNumberBuf);
                    msg.getBytes(msg.readerIndex(), preReadBytes);
                    preReadBytes.clear();

                    endpointHandler.handedOffToSync();
                    context.deregister().addListener(
                            new SocketInitializer(preReadBytes, context));
                } finally {
                    shutdown("Got non-async connection", true);
                }
                return;
            }

            if (listener.getDialogHandlerFactories().isEmpty()) {
                logger.log(Level.INFO,
                        "Got async connection, " +
                        "but no available dialog factory for responding");
                shutdown("No factory for async connection", true);
                return;
            }

            endpointHandler.onChannelReady();
        }

        /*
         * Retain the message once since it will be released in the
         * decoder.
         */
        msg.retain();
        context.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {

        final Logger logger = responderEndpoint.getLogger();

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO,
                    "{0} got exception, preReadDone={1}, " +
                    "endpointHandler={2}, cause={3}",
                    new Object[] {
                        getClass().getSimpleName(),
                        preReadDone,
                        endpointHandler,
                        CommonLoggerUtils.getStackTrace(cause) });
        }
        context.fireExceptionCaught(cause);
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
        return endpointHandler.getSchedExecService();
    }

    /**
     * Returns the statistically universal unique ID.
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

    private class SocketInitializer implements ChannelFutureListener {

        private final ByteBuffer preReadBytes;
        private final ChannelHandlerContext context;

        SocketInitializer(ByteBuffer preReadBytes,
                          ChannelHandlerContext context) {
            this.preReadBytes = preReadBytes;
            this.context = context;
        }

        @Override
        public void operationComplete(ChannelFuture future) {

            final Logger logger = responderEndpoint.getLogger();

            if (!future.isSuccess()) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO,
                            "Error deregistering socket: {0}",
                            CommonLoggerUtils.getStackTrace(future.cause()));
                }
                future.channel().close();
                return;
            }

            final SocketPrepared socketPrepared = listener.getSocketPrepared();
            if (socketPrepared == null) {
                logger.log(Level.INFO,
                        "Got non-async connection, " +
                        "but no sync handler is present");
                return;
            }
            SocketChannel socketChannel =
                NettyUtil.getSocketChannel(context.channel());
            try {
                socketChannel.configureBlocking(true);
                logger.log(Level.FINE,
                        "Handing off the connection " +
                        "to the non-async handler: " +
                        "handler={0}, buf={1}, socket={2}",
                        new Object[] {
                            socketPrepared,
                            BytesUtil.toString(
                                    preReadBytes,
                                    preReadBytes.limit()),
                            socketChannel.socket() });
                socketPrepared.onPrepared(preReadBytes, socketChannel.socket());
            } catch (IOException e) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO,
                            "Error handing off the channel " +
                            "to non-async handler: {0}",
                            CommonLoggerUtils.getStackTrace(e));
                }
            }
        }
    }
}

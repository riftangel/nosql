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

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.ProtocolMesg;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.CommonLoggerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.logging.LoggingHandler;

/**
 * Pre-writes the async magic number and add the real handlers afterward.
 */
class PreWriteWrappedEndpointHandler
    extends SimpleChannelInboundHandler<ByteBuf>
    implements EndpointHandler, EndpointHandlerManager {

    private final NettyCreatorEndpoint creatorEndpoint;
    private final EndpointConfig endpointConfig;
    private final NetworkAddress remoteAddress;
    private final NettyEndpointHandler endpointHandler;
    private volatile boolean preWriteDone = false;
    private volatile boolean isShutdown = false;

    PreWriteWrappedEndpointHandler(NettyCreatorEndpoint creatorEndpoint,
                                   EndpointConfig endpointConfig,
                                   NetworkAddress remoteAddress) {
        this.creatorEndpoint = creatorEndpoint;
        this.endpointConfig = endpointConfig;
        this.remoteAddress = remoteAddress;
        this.endpointHandler = new NettyEndpointHandler(
                creatorEndpoint.getLogger(),
                this, endpointConfig, true, remoteAddress,
                creatorEndpoint.getDialogHandlerFactories());
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
                    sslControl, remoteAddress, true,
                    creatorEndpoint.getLogger());
            pipeline.addLast(handler.sslHandler());
            pipeline.addLast(handler);
        }
        pipeline.addLast(endpointHandler.decoder());
    }

    @Override
    public void channelActive(ChannelHandlerContext context) {
        final ByteBuf preWriteBytes =
            Unpooled.wrappedBuffer(ProtocolMesg.MAGIC_NUMBER);
        context.writeAndFlush(preWriteBytes).addListener(
                new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        preWriteDone = true;
                    }
                });
        endpointHandler.onChannelReady();
    }

    @Override
    public void channelRead0(ChannelHandlerContext context, ByteBuf msg)
        throws Exception {
        if (!preWriteDone) {
            throw new IllegalStateException(String.format(
                        "Creator endpoint received bytes " +
                        "before pre-write is done, bytes=%s",
                        ByteBufUtil.hexDump(
                            msg, 0, Math.min(16, msg.capacity()))));
        }
        /*
         * Retain the message once since it will be released in the
         * decoder.
         */
        msg.retain();
        context.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context,
                                Throwable cause) {

        final Logger logger = creatorEndpoint.getLogger();

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO,
                    "{0} got exception, preWriteDone={1}, " +
                    "endpointHandler={2}, cause={3}",
                    new Object[] {
                        getClass().getSimpleName(),
                        preWriteDone,
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
        creatorEndpoint.onHandlerShutdown(this);
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
        creatorEndpoint.onHandlerShutdown(this);
    }
}

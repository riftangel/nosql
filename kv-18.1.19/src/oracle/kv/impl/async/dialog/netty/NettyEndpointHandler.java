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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;
import java.util.logging.Level;

import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.dialog.ChannelOutput;
import oracle.kv.impl.async.dialog.ProtocolReader;
import oracle.kv.impl.async.dialog.ProtocolWriter;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.codec.ByteToMessageDecoder;

public class NettyEndpointHandler extends AbstractDialogEndpointHandler {

    private final NettyChannelInput channelInput;
    private final ChannelOutput channelOutput;
    private final ProtocolReader protocolReader;
    private final ProtocolWriter protocolWriter;
    private final Decoder decoder;

    private volatile boolean handedOffToSync = false;

    private volatile ChannelHandlerContext context = null;
    private volatile boolean lastChunkWrittenToContext = false;
    private final Queue<ChannelFuture> pendingWriteFutures =
        new LinkedList<ChannelFuture>();
    private final Runnable invokeWriteTask = new Runnable() {
        @Override
        public void run() {
            invokeWrite();
        }
    };

    public NettyEndpointHandler(
            Logger logger,
            EndpointHandlerManager parent,
            EndpointConfig endpointConfig,
            boolean isCreator,
            NetworkAddress remoteAddress,
            Map<Integer, DialogHandlerFactory> dialogHandlerFactories) {
        super(logger, parent, endpointConfig, isCreator, remoteAddress,
                dialogHandlerFactories);
        this.channelInput = new NettyChannelInput();
        this.channelOutput = new ChannelOutput();
        this.protocolReader =
            new ProtocolReader(channelInput, getMaxInputProtocolMesgLen());
        this.protocolWriter =
            new ProtocolWriter(channelOutput, getMaxOutputProtocolMesgLen());
        this.decoder = new Decoder();
    }

    /**
     * Returns the executor service associated with this context.
     */
    @Override
    public ScheduledExecutorService getSchedExecService() {
        if (context == null) {
            return null;
        }
        return context.executor();
    }

    /**
     * Returns the {@link ProtocolReader}.
     */
    @Override
    public ProtocolReader getProtocolReader() {
        return protocolReader;
    }

    /**
     * Returns the {@link ProtocolWriter}.
     */
    @Override
    public ProtocolWriter getProtocolWriter() {
        return protocolWriter;
    }

    /**
     * Asserts that the method is called inside the executor thread.
     */
    @Override
    public void assertInExecutorThread() {
        if ((context == null) || (!context.executor().inEventLoop())) {
            throw new IllegalStateException(
                    "The method is not executed in the thread of executor");
        }
    }

    @Override
    protected boolean flushInternal(boolean writeHasRemaining) {
        if (handedOffToSync) {
            return true;
        }

        /*
         * If the write happens in the executor's thread, netty writes
         * directly, else a task is scheduled to write in the executor's
         * thread. Therefore, to maintain our message order, we need to always
         * do our actual writes in the executor's thread.
         */
        if (context.executor().inEventLoop()) {
            invokeWrite();
            return pendingWriteFutures.isEmpty();
        }
        context.executor().execute(invokeWriteTask);
        return false;
    }

    @Override
    protected void cleanup() {
        channelInput.close();
        channelOutput.close();
        if (!handedOffToSync) {
            context.close();
        }
    }

    private void invokeWrite() {
        while (!lastChunkWrittenToContext) {
            ChannelOutput.Chunk chunk = channelOutput.getChunkQueue().poll();
            if (chunk == null) {
                break;
            }
            ByteBuf buf = Unpooled.wrappedBuffer(chunk.chunkArray());
            pendingWriteFutures.add(context.write(buf));
            if (chunk.last()) {
                lastChunkWrittenToContext = true;
                break;
            }
        }
        context.flush();
        Iterator<ChannelFuture> iter = pendingWriteFutures.iterator();
        while (iter.hasNext()) {
            ChannelFuture future = iter.next();
            if (future.isDone()) {
                iter.remove();
            }
        }
    }

    void handedOffToSync() {
        handedOffToSync = true;
    }

    Decoder decoder() {
        return decoder;
    }

    /**
     * Used mainly for the accumulating behavior.
     *
     * This is actually not a decoder and it is the last one in the pipeline.
     * When there is not enough data for a protocol message, we want the data
     * saved for the next read, which is implemented by the
     * ByteToMessageDecoder.
     */
    class Decoder extends ByteToMessageDecoder {
        @Override
        public void handlerAdded(ChannelHandlerContext ctx)
            throws Exception {
            context = ctx;
            setCumulator(COMPOSITE_CUMULATOR);
            onExecutorReady();
        }

        @Override
        protected void decode(ChannelHandlerContext ctx,
                              ByteBuf in,
                              List<Object> out) throws Exception {
            if (ctx != context) {
                getLogger().log(Level.FINE,
                        "Endpoint handler context switched, handler={0}",
                        this);
                context = ctx;
            }
            channelInput.feed(in);
            onChannelInputRead();
            flush();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx,
                                       Object evt) throws Exception {
            if (evt instanceof ChannelInputShutdownEvent) {
                markTerminating(
                        new ConnectionEndpointShutdownException(
                            true, "Got eof when reading"));
                terminate();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
            markTerminating(cause);
            terminate();
        }
    }
}

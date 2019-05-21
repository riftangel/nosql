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
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;
import java.util.logging.Level;

import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.ProtocolMesg;
import oracle.kv.impl.util.CommonLoggerUtils;

import com.sleepycat.je.rep.net.DataChannel;

class PreWriteWrappedEndpointHandler
    implements EndpointHandler, EndpointHandlerManager, ChannelHandler {

    private final NioCreatorEndpoint creatorEndpoint;
    private final NetworkAddress remoteAddress;
    private final NioChannelExecutor channelExecutor;
    private final SocketChannel socketChannel;
    private final ByteBuffer preWriteBytes =
        ByteBuffer.wrap(ProtocolMesg.MAGIC_NUMBER);
    private final NioEndpointHandler endpointHandler;
    private volatile boolean isShutdown = false;

    PreWriteWrappedEndpointHandler(NioCreatorEndpoint creatorEndpoint,
                                   EndpointConfig endpointConfig,
                                   NetworkAddress remoteAddress,
                                   NioChannelExecutor channelExecutor,
                                   Map<Integer, DialogHandlerFactory>
                                   dialogHandlerFactories,
                                   DataChannel dataChannel) {
        this.creatorEndpoint = creatorEndpoint;
        this.remoteAddress = remoteAddress;
        this.channelExecutor = channelExecutor;
        this.socketChannel = dataChannel.getSocketChannel();
        this.endpointHandler = new NioEndpointHandler(
                creatorEndpoint.getLogger(),
                this, endpointConfig, true, remoteAddress, channelExecutor,
                dialogHandlerFactories, dataChannel);
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

    /**
     * Called when the connection is established.
     */
    @Override
    public void onConnected() {
        getLogger().log(Level.FINE,
                "Connected to {0} and pre-writing",
                remoteAddress);
        try {
            if (!onChannelPreWrite()) {
                channelExecutor.registerReadWrite(socketChannel, this);
            }
        } catch (IOException e) {
            if (getLogger().isLoggable(Level.FINE)) {
                getLogger().log(Level.FINE,
                                // CRC: Update wording?
                        "Error registering for read/write after pre-write: " +
                        "remoteAddress={0}, error={1}",
                        new Object[] {
                            remoteAddress,
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
        /*
         * It should be an error that the remote send something  before we
         * pre-write.
         */
        throw new IllegalStateException();
    }

    /**
     * Called when the channel is ready for write
     */
    @Override
    public void onWrite() {
        onChannelPreWrite();
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
     *
     * @param t the throwable for the error
     */
    @Override
    public void onError(Throwable t, SelectableChannel channel) {
        endpointHandler.onError(t, channel);
    }

    public Logger getLogger() {
        return creatorEndpoint.getLogger();
    }

    private boolean onChannelPreWrite() {
        try {
            socketChannel.write(preWriteBytes);
            if (preWriteBytes.remaining() != 0) {
                return false;
            }
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
            if (getLogger().isLoggable(level)) {
                getLogger().log(level,
                        "Error while doing pre-write, " +
                        "remoteAddress={0}: {1}",
                        new Object[] {
                            remoteAddress,
                            CommonLoggerUtils.getStackTrace(t) });
            }
            shutdown(t.getMessage(), true);
            return true;
        }
    }

}

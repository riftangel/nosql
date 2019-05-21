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

import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.dialog.ProtocolReader;
import oracle.kv.impl.async.dialog.ProtocolWriter;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.util.CommonLoggerUtils;

import com.sleepycat.je.rep.net.DataChannel;

public class NioEndpointHandler
    extends AbstractDialogEndpointHandler implements ChannelHandler {

    private final ChannelExecutor channelExecutor;
    private final DataChannel dataChannel;
    private final NioChannelInput channelInput;
    private final NioChannelOutput channelOutput;
    private final ProtocolReader protocolReader;
    private final ProtocolWriter protocolWriter;

    private volatile boolean handedOffToSync = false;

    public NioEndpointHandler(
            Logger logger,
            EndpointHandlerManager parent,
            EndpointConfig endpointConfig,
            boolean isCreator,
            NetworkAddress remoteAddress,
            ChannelExecutor channelExecutor,
            Map<Integer, DialogHandlerFactory> dialogHandlerFactories,
            DataChannel dataChannel) {
        super(logger, parent, endpointConfig, isCreator, remoteAddress,
                dialogHandlerFactories);
        this.channelExecutor = channelExecutor;
        this.dataChannel = dataChannel;
        this.channelInput = new NioChannelInput();
        this.channelOutput = new NioChannelOutput();
        this.protocolReader =
            new ProtocolReader(channelInput, getMaxInputProtocolMesgLen());
        this.protocolWriter =
            new ProtocolWriter(channelOutput, getMaxOutputProtocolMesgLen());

        onExecutorReady();
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                    "Created endpoint handler: handler={0}, executor={1}",
                    new Object[] { this, channelExecutor });
        }
    }

    /**
     * Returns the executor service associated with this context.
     */
    @Override
    public ScheduledExecutorService getSchedExecService() {
        return channelExecutor;
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
        if (!channelExecutor.inExecutorThread()) {
            throw new IllegalStateException(
                    "The method is not executed in the thread of executor");
        }
    }

    /**
     * Called when the connection is established.
     */
    @Override
    public void onConnected() {
        /* This method should not be called due to the pre-write handler. */
        throw new IllegalStateException();
    }

    /**
     * Called when the channel is ready for read.
     */
    @Override
    public void onRead() {
        try {
            boolean eos = false;
            ByteBuffer[] buffers = channelInput.flipToChannelRead();
            while (true) {
                long n = dataChannel.read(buffers);
                if (n < 0) {
                    eos = true;
                }
                if (n <= 0) {
                    break;
                }

            }
            channelInput.flipToProtocolRead();
            onChannelInputRead();

            if (eos) {
                markTerminating(
                        new ConnectionEndpointShutdownException(
                            true, "Got eof when reading"));
                terminate();
            }

            /* We might have written something in response to the read, flush */
            flush();
        } catch (IOException e) {
            markTerminating(e);
            terminate();
        }
    }

    /**
     * Called when the channel is ready for write
     */
    @Override
    public void onWrite() {
        try {
            flush();
        } catch (IOException e) {
            markTerminating(e);
            terminate();
        }
    }

    /**
     * Called when there is an error uncaught from the above methods or occured
     * to the executor.
     */
    @Override
    public void onError(Throwable t, SelectableChannel channel) {
        final Logger logger = getLogger();
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO,
                    "Error on read/write, terminating handler: {0}",
                    CommonLoggerUtils.getStackTrace(t));
        }
        markTerminating(t);
        terminate();
    }

    /**
     * Called when the executor is closing.
     */
    @Override
    public void onClosing() {
        markTerminating(
                new ConnectionEndpointShutdownException(
                    false, "Executor is closing"));
        terminate();
    }

    void handedOffToSync() {
        handedOffToSync = true;
    }

    /**
     * Flush the channel output.
     *
     * The caller should already have acquired a flush lock such that buffer
     * data is not flushed to the data channel in a interleaved manner.
     */
    @Override
    protected boolean flushInternal(boolean writeHasRemaining)
        throws IOException {

        if (handedOffToSync) {
            return true;
        }

        boolean flushHasRemaining = true;
        while (true) {
            NioChannelOutput.Bufs bufs = channelOutput.getBufs();
            long flushed = dataChannel.write(
                    bufs.array(), bufs.offset(), bufs.length());
            flushHasRemaining = channelOutput.hasRemaining();
            /* Flush the channel */
            DataChannel.FlushStatus flushStatus = dataChannel.flush();
            /*
             * Check if we should break the loop. The check will also
             * register/unregister write as needed.
             */
            if (!continueFlushLoop(
                        writeHasRemaining, flushHasRemaining,
                        flushed, flushStatus)) {
                break;
            }
        }
        return !flushHasRemaining;
    }

    @Override
    protected void cleanup() throws IOException {
        channelExecutor.execute(new Runnable() {
            @Override
            public void run() {
                channelInput.close();
                channelOutput.close();
            }
        });
        if (!handedOffToSync) {
            (new CloseHandler()).closeAsync();
        }
    }

    /**
     * Returns {@code true} if we should continue to flush, given the writing
     * and flushing status.
     *
     * The method also register or deregister write interest if necessary.
     *
     * @param writeHasRemaining {@code true} if the endpoint handler has more
     * data (e.g. dialog frames) to write to the channel output
     * @param flushHasRemaining {@code true} if the channel output has more
     * data to flush
     * @param flushed number of bytes flushed
     * @param flushStatus the flush status of last socket write
     *
     * @return {@code true} if the flush loop should continue
     */
    private boolean continueFlushLoop(boolean writeHasRemaining,
                                      boolean flushHasRemaining,
                                      long flushed,
                                      DataChannel.FlushStatus flushStatus)
        throws IOException {

        /* Continue the loop? */
        boolean loopContinue;
        /* Register for write interest? */
        boolean writeInterested;

        if (flushed != 0) {
            /*
             * We wrote something and thus we probably can write something
             * again.
             */
            return true;
        }

        /* flushed == 0 */
        switch(flushStatus) {

        case DISABLED:
            loopContinue = false;
            writeInterested = (writeHasRemaining || flushHasRemaining);
            break;
        case DONE:
            loopContinue = flushHasRemaining;
            writeInterested = writeHasRemaining;
            break;
        case AGAIN:
            loopContinue = true;
            writeInterested = writeHasRemaining;
            break;
        case SO_WAIT_WRITE:
            loopContinue = false;
            writeInterested = (writeHasRemaining || flushHasRemaining);
            break;
        case CONTENTION:
            loopContinue = false;
            writeInterested = false;
            break;
        default:
            throw new IllegalStateException(
                    String.format("Unknown state: %s", flushStatus));
        }

        /* If loop continues, then don't register our interest too early. */
        if (!loopContinue) {
            final SocketChannel socketChannel = dataChannel.getSocketChannel();
            final boolean alreadyWriteInterested =
                channelExecutor.writeInterested(socketChannel);
            if (writeInterested && !alreadyWriteInterested) {
                channelExecutor.registerReadWrite(socketChannel, this);
            } else if (!writeInterested && alreadyWriteInterested){
                channelExecutor.registerRead(socketChannel, this);
            }
        }

        return loopContinue;
    }

    /**
     * A handler that asynchronously close the channel.
     */
    private class CloseHandler implements ChannelHandler {
        @Override
        public void onConnected() {
            /* not needed */
        }

        @Override
        public void onRead() {
            closeAsync();
        }

        @Override
        public void onWrite() {
            closeAsync();
        }

        @Override
        public void onError(Throwable throwable, SelectableChannel channel) {
            try {
                dataChannel.closeForcefully();
            } catch (Throwable t) {
                /* do nothing */
            }
        }

        @Override
        public void onClosing() {
            try {
                dataChannel.closeForcefully();
            } catch (Throwable t) {
                /* do nothing */
            }
        }

        private void closeAsync() {
            final SocketChannel socketChannel = dataChannel.getSocketChannel();
            try {
                DataChannel.CloseAsyncStatus status = dataChannel.closeAsync();
                switch(status) {
                    case SO_WAIT_READ:
                        channelExecutor.registerRead(socketChannel, this);
                        break;
                    case SO_WAIT_WRITE:
                        channelExecutor.registerReadWrite(socketChannel, this);
                        break;
                    case DONE:
                        return;
                    default:
                        throw new IllegalStateException(
                                "Unknown close async status: " + status);
                }
            } catch (Throwable t) {
                if (getLogger().isLoggable(Level.FINE)) {
                    getLogger().log(Level.FINE,
                            "Error close channel asynchronously, " +
                            "handler={0}: {1}",
                            new Object[] {
                                NioEndpointHandler.this,
                                CommonLoggerUtils.getStackTrace(t) });
                }
            }
        }
    }
}

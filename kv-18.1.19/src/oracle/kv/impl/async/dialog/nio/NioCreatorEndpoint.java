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
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import oracle.kv.impl.async.AbstractCreatorEndpoint;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.NetworkAddress;

import com.sleepycat.je.rep.net.DataChannel;

/**
 * Nio creator endpoint.
 */
class NioCreatorEndpoint extends AbstractCreatorEndpoint {

    private final NioEndpointGroup endpointGroup;
    private final NioChannelThreadPool channelThreadPool;

    NioCreatorEndpoint(NioEndpointGroup endpointGroup,
                       NioChannelThreadPool channelThreadPool,
                       NetworkAddress remoteAddress,
                       EndpointConfig endpointConfig) {
        super(remoteAddress, endpointConfig);
        this.endpointGroup = endpointGroup;
        this.channelThreadPool = channelThreadPool;
    }

    @Override
    protected EndpointHandler newEndpointHandler() throws IOException {
        EndpointConfig config = getEndpointConfig();
        NioChannelExecutor executor = channelThreadPool.next();
        DataChannel dataChannel =
            NioUtil.getDataChannel(remoteAddress, config, getLogger());
        PreWriteWrappedEndpointHandler handler =
            new PreWriteWrappedEndpointHandler(
                this,
                endpointConfig,
                remoteAddress,
                executor,
                getDialogHandlerFactories(),
                dataChannel);
        SocketChannel socketChannel = dataChannel.getSocketChannel();
        if (socketChannel.isConnected()) {
            /* The socket channel may be established immediately. */
            handler.onConnected();
        } else {
            executor.registerConnect(socketChannel, handler);
        }
        return handler;
    }

    Logger getLogger() {
        return endpointGroup.getLogger();
    }

    @Override
    public String toString() {
        return String.format("NioCreatorEndpoint[%s]", remoteAddress);
    }
}

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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ScheduledExecutorService;

public interface ChannelExecutor extends ScheduledExecutorService {

    /**
     * Returns {@code true} when executed in the thread of the executor.
     *
     * @return {@code true} if in executor thread
     */
    boolean inExecutorThread();

    /**
     * Registers for interest only in accepting a connection.
     *
     * Server socket channels can register for accept interest.
     *
     * @param channel the server socket channel to register
     * @param handler the handler for preparing the socket channel after
     * accepting a connection
     * @throws IOException if there is an I/O error
     */
    void registerAccept(ServerSocketChannel channel,
                        ChannelAccepter handler) throws IOException;

    /**
     * Registers for interest only in finishing a connection.
     *
     * <p>The method will remove any other interest (e.g., read and write) of
     * the channel.
     *
     * @param channel the socket channel to register
     * @param handler the handler to call when connection is established
     * @throws IOException if there is an I/O error
     */
    void registerConnect(SocketChannel channel, ChannelHandler handler)
        throws IOException;

    /**
     * Registers for interest in reading data from a channel.
     *
     * <p>The method will remove any other interest (e.g., write and connect)
     * of the channel.
     *
     * @param channel the socket channel to register
     * @param handler the handler to call when some data is ready for read on
     * the channel
     */
    void registerRead(SocketChannel channel, ChannelHandler handler)
        throws IOException;

    /**
     * Registers for interest only in reading and writing on a channel.
     *
     * <p>The method will remove any other interest (e.g., connect) of the
     * channel.
     *
     * @param channel the socket channel to register
     * @param handler the handler to call when either some data is ready for
     * read on the channel or the channel is ready for write
     */
    void registerReadWrite(SocketChannel channel, ChannelHandler handler)
        throws IOException;

    /**
     * Removes any registered interest of the channel.
     *
     * @param channel the socket channel to register
     */
    void deregister(SelectableChannel channel) throws IOException;

    /**
     * Checks if the channel is interested in write.
     *
     * @return {@code true} if interested
     */
    boolean writeInterested(SelectableChannel channel);

    /**
     * Checks if the channel is deregistered.
     *
     * @param channel the socket channel to check
     * @return {@code true} if deregistered
     */
    boolean deregistered(SelectableChannel channel);

}

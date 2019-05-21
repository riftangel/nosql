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

/**
 * Handling a channel of the establishing/established connection.
 */
interface ChannelHandler extends NioHandler {

    /**
     * Called when the connection is established.
     */
    void onConnected();

    /**
     * Called when the channel is ready for read.
     */
    void onRead();

    /**
     * Called when the channel is ready for write
     */
    void onWrite();

}

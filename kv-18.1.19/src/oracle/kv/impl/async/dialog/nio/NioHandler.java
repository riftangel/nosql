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

import java.nio.channels.SelectableChannel;

/**
 * Base class for Nio events handling .
 */
interface NioHandler {

    /**
     * Called when the handler executor is closing this handler.
     */
    void onClosing();

    /**
     * Called when there is an error uncaught from the above methods or occured
     * to the executor.
     *
     * @param t the throwable for the error
     * @param ch the channel associated with the handler
     */
    void onError(Throwable t, SelectableChannel ch);

}

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

/**
 * INTERNAL: Implements the dialog and transport layer for async project.
 *
 * <p>
 * The dialog layer provides an async API and implementation for the upper
 * layer to exchange requests/responses in the form of dialogs. It also
 * implements the <a
 * href="https://sleepycat-tools.us.oracle.com/trac/wiki/JEKV/AsyncDialogProtocol">Dialog
 * Protocol</a>
 *
 * <p>
 * The transport layer implements the actual data read/write for the dialog
 * layer. The code currently provides two versions of the transport layer
 * implementation: one with native java nio library; the other with netty.
 */
package oracle.kv.impl.async;

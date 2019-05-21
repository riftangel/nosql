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
 * INTERNAL: Implementations of client side functionality, and network
 * protocols from the client -> kvstore. Care should be taken to segregate
 * client side vs server side classes. The classes within this package and
 * its subpackages will be included in the kvclient.jar; they should not
 * have any dependencies on kvstore.jar.
 */
package oracle.kv.impl.client;

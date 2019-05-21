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
 * The code and scripts in this package demonstrate some of the basic functions
 * of the Oracle NoSQL Database table API, including creation and use of
 * secondary indexes.
 * The examples are independent of one another and have no dependencies.
 *
 * <h3>Before Running the Table Examples</h3>
 * <h4>Start a Store</h4>
 * The examples require a running KVStore instance.  The examples themselves
 * create required tables and indexes.  The examples do not clean up the store,
 * but are designed so that if they are run more than once on the same store,
 * they still work.  A KVLite instance is suitable as a running
 * store.  These instructions assume that a store instance has been started
 * and uses <em>hostName</em> to refer to the host name used, <em>port</em> to
 * refer to the port, and <em>storename</em> to refer to the name of the store.
 * <b>install_dir</b> is used to refer to the location of the Oracle NoSQL
 * Database installation.  Here is an example of how to start KVLite.  The
 * root directory needs to exist before creating the store.  The first time
 * KVLite is started it creates the store.  Subsequent restarts will use an
 * existing store if pointed to a root directory that has a store installed.
 * <pre>
 *  java -jar <b>install_dir</b>/lib/kvstore.jar kvlite -host <em>hostName</em> -port <em>port</em>\
         -store <em>storeName</em> -root <em>rootDirectoryOfStore</em>
 * </pre>
 *
 * <h3>Building and Running Examples</h3>
 * <h4>Build the example code</h4>
 * In the directory <b>install_dir</b>/examples/table:
 * <pre>
 *  javac -d . -cp <b>install_dir</b>/lib/kvclient.jar *.java
 * </pre>
 * <h4>Run the example code</h4>
 * There are a number of examples that can be run individually or as a
 * group. To run as a group:
 * <pre>
 *  java -cp .:<b>install_dir</b>/lib/kvclient.jar table.TableAPIExample \
 *    [-host <em>hostName</em>] [-port <em>port</em>] \
 *    [-store <em>storeName</em>]
 * </pre>
 * The default host is <em>localhost</em>, the default port is <em>5000</em>,
 * and the default store name is <em>kvstore</em>.
 * <p>
 * The individual examples can be added as a list of examples at the end of the
 * command line, e.g.
 * <pre>
 *  java -cp .:<b>install_dir</b>/lib/kvclient.jar table.TableAPIExample \
 *    [-host <em>host name</em>] [-port <em>port</em>] \
 *    [-store <em>store name</em>] [example name]*
 * </pre>
 * Get a usage message by using the -help flag:
 * <pre>
 *  java -cp .:<b>install_dir</b>/lib/kvclient.jar table.TableAPIExample -help
 * </pre>
 * The available example names, which correspond to the documented example
 * classes include:
 * <ul>
 * <li> SimpleReadWrite - Demonstrates basic read and write operations</li>
 * <li> IndexReadWrite - Demonstrates use of a secondary index for read </li>
 * <li> ShardKeys - Demonstrates use of a shard key to retrieve related rows</li>
 * <li> ChildTable - Demonstrates use of a child table</li>
 * <li> ComplexField - Demonstrates use of non-atomic field types</li>
 * <li> ArrayIndex - Demonstrates a secondary index on an array</li>
 * <li> MapIndex - Demonstrates use of a secondary index on a map</li>
 * <li> TimeToLive - Demonstrates use of a TTL value for rows</li>
 * </ul>
 *
 *<p>
 */

package table;

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
package oracle.kv.impl.admin;

public class IndexNotFoundException extends IllegalCommandException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs an instance of <code>IndexNotFoundException</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public IndexNotFoundException(String msg) {
        super(msg);
    }
}

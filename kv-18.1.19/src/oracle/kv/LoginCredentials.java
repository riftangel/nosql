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

package oracle.kv;

/**
 * The common interface of KVStore credential class implementations.
 * Applications can supply objects that implement this interface in order to
 * authenticate a user when accessing a KVStore instance as an alternative to
 * specifying authentication information in login properties.
 *
 * @since 3.0
 */
public interface LoginCredentials {
    /**
     * Identifies the user owning the credentials.
     *
     * @return the name of the user for which the credentials belong.
     */
    public String getUsername();
}

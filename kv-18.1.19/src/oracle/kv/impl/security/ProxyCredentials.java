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

package oracle.kv.impl.security;

import java.io.Serializable;

import oracle.kv.LoginCredentials;

/**
 * A set of login credentials that allow a KVStore component to log in to a
 * KVStore on behalf of a user. These are processed locally on the client
 * side.
 */
public class ProxyCredentials implements LoginCredentials, Serializable {

    private static final long serialVersionUID = 1L;

    private final KVStoreUserPrincipal user;

    public ProxyCredentials(KVStoreUserPrincipal user) {
        this.user = user;
    }

    /**
     * Identify the user owning the credentials.
     *
     * @return the name of the user for which the credentials belong.
     */
    @Override
    public String getUsername() {
        return user.getName();
    }

    /**
     * Returns the user to be logged in.
     */
    public KVStoreUserPrincipal getUser() {
        return user;
    }
}

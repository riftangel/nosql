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

/**
 * KVStore role defined in R3. Reserved to maintain compatibility during
 * online upgrade.
 */
public enum KVStoreRole {
    /*
     * Any authenticated user, or internal. Only reserved for upgrade,
     * unused since R3.Q3.
     */
    AUTHENTICATED,

    /* KVStore infrastructure. */
    INTERNAL,

    /* KVStore admin user. Only reserved for upgrade, unused since R3.Q3. */
    ADMIN
}

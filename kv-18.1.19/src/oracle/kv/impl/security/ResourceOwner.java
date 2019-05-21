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

import oracle.kv.impl.util.ObjectUtil;

/**
 * A simple structure recording the owner of an resource in KVStore security
 * systems, including plan, table, and keyspace in future. General, an owner of
 * a resource is a KVStoreUser. Here only the id and the user name are recorded
 * for simplicity.
 */
public class ResourceOwner implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;
    private final String name;

    public ResourceOwner(String id, String name) {
        ObjectUtil.checkNull("id", id);
        ObjectUtil.checkNull("name", name);
        this.id = id;
        this.name = name;
    }

    /* Copy ctor */
    public ResourceOwner(ResourceOwner other) {
        ObjectUtil.checkNull("Other owner", other);
        this.id = other.id;
        this.name = other.name;
    }

    /**
     * constructs ResourceOwner from a string created by toString().
     */
    public static ResourceOwner fromString(String resourceString) {
        int lp = resourceString.indexOf('(');
        int rp = resourceString.indexOf(')');
        String nameString = resourceString.substring(0, lp);
        String idString = resourceString.substring((lp+4), rp);
        return new ResourceOwner(idString, nameString);
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return String.format("%s(id:%s)", name, id);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ResourceOwner)) {
            return false;
        }
        final ResourceOwner otherOwner = (ResourceOwner) other;
        return id.equals(otherOwner.id) && name.equals(otherOwner.name);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 17 * prime + id.hashCode();
        result = result * prime + name.hashCode();
        return result;
    }
}

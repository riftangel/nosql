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

package oracle.kv.impl.metadata;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Wrapper class for persisting metadata in an entity store. By wrapping the
 * metadata object it can be proxied using the PersistentProxy<T> mechanism.
 */
@Entity
public class MetadataHolder {

    @PrimaryKey
    private /*final*/ String key;

    private /*final*/ Metadata<?> md;

    @SuppressWarnings("unused")
    private MetadataHolder() {
    }

    /**
     * Holder constructor.
     *
     * @param md a metadata object
     */
    public MetadataHolder(Metadata<?> md) {
        this.md = md;
        this.key = md.getType().getKey();
    }

    public Metadata<?> getMetadata() {
        return md;
    }
}

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

import oracle.kv.impl.metadata.Metadata.MetadataType;

/**
 * Interface implemented by all metadata information objects. Objects
 * implementing this interface are used to transmit metadata information in
 * response to a metadata request. The information may be in the form of changes
 * in metadata or a subset of metadata and is implementation dependent. The
 * responder may not have the requested information in which case the object
 * will be empty. Objects implementing MetadataInfo should also implement
 * <code>Serializable</code>.
 */
public interface MetadataInfo {

    /**
     * Gets the type of metadata this information object contains.
     *
     * @return the type of metadata
     */
    public MetadataType getType();

    /**
     * Returns the highest sequence number associated with the metadata that
     * is known to the source of this object.
     *
     * @return the source's metadata sequence number
     */
    public int getSourceSeqNum();

    /**
     * Returns true if this object does not include any metadata information
     * beyond the type and sequence number.
     *
     * @return true if this object does not include any metadata information
     */
    public boolean isEmpty();
}

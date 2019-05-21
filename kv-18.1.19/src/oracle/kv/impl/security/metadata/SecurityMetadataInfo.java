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

package oracle.kv.impl.security.metadata;

import java.io.Serializable;
import java.util.List;

import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;

public class SecurityMetadataInfo implements MetadataInfo, Serializable {

    private static final long serialVersionUID = 1L;

    private final List<SecurityMDChange> changeList;
    private final String securityMetadataId;
    private final int sequenceNum;

    public static final SecurityMetadataInfo EMPTY_SECURITYMD_INFO =
        new SecurityMetadataInfo(null, -1, null);

    public SecurityMetadataInfo(final SecurityMetadata secMD,
                                final List<SecurityMDChange> changes) {
        this(secMD.getId(), secMD.getSequenceNumber(), changes);
    }

    public SecurityMetadataInfo(final String secMDId,
                                final int latestSeqNum,
                                final List<SecurityMDChange> changes) {
        this.securityMetadataId = secMDId;
        this.sequenceNum = latestSeqNum;
        this.changeList = changes;
    }

    @Override
    public MetadataType getType() {
        return MetadataType.SECURITY;
    }

    @Override
    public int getSourceSeqNum() {
        return sequenceNum;
    }

    @Override
    public boolean isEmpty() {
        return (changeList == null) || (changeList.isEmpty());
    }

    public String getSecurityMetadataId() {
        return securityMetadataId;
    }

    public List<SecurityMDChange> getChanges() {
        return changeList;
    }
}

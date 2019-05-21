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

package oracle.kv.impl.api.avro;

import java.util.EnumSet;

/**
 * Schema status values.  Stored as part of the schema key.
 */
public enum AvroSchemaStatus {

    /**
     * Schema is in use and accessible from the client.  This is intentionally
     * the first declared value (lowest ordinal) so that when enumerating an
     * EnumSet we will process it first; we rely on this in
     * SchemaAccessor.readSchema.
     */
    ACTIVE("A"),

    /**
     * Schema is disabled and not accessible to clients.  We disable rather
     * than delete schemas, so they can be reinstated if necessary.
     */
    DISABLED("D");

    private final String code;

    private AvroSchemaStatus(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    static final EnumSet<AvroSchemaStatus> ALL =
        EnumSet.allOf(AvroSchemaStatus.class);

    static AvroSchemaStatus fromCode(String code) {
        for (AvroSchemaStatus status : ALL) {
            if (code.equals(status.getCode())) {
                return status;
            }
        }
        return null;
    }
}

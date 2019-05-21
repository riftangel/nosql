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
package oracle.kv.avro;

/**
 * Thrown when the application attempts to use a schema that has not been
 * defined using the NoSQL Database administration interface.
 * <p>
 * As described in detail under Avro Schemas in the {@link AvroCatalog} class
 * documentation, all schemas must be defined using the NoSQL Database
 * administration interface before they can be used to store values.
 * <p>
 * Depending on the nature of the application, when this exception is thrown
 * the client may wish to
 * <ul>
 * <li>retry the operation at a later time, if the schema is expected to be
 * available,</li>
 * <li>give up and report an error at a higher level so that a human being can
 * be made aware of the need to define the schema.</li>
 * </ul>
 * <p>
 * WARNING: Blocking and internal schema queries may occur frequently if
 * multiple threads repeatedly try to use a schema that is undefined in the
 * store.  To avoid this, it is important to delay before retrying an operation
 * using the undefined schema.
 *
 * @since 2.0
 *
 * @deprecated as of 4.0, use the table API instead.
 */
@Deprecated
public class UndefinedSchemaException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    final private String schemaName;

    /**
     * For internal use only.
     * @hidden
     */
    public UndefinedSchemaException(String msg, String schemaName) {
        super(msg, null /*cause*/);
        this.schemaName = schemaName;
    }

    /**
     * Returns the full name of the undefined schema.
     */
    public String getSchemaName() {
        return schemaName;
    }
}

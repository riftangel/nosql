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
 * Thrown when a schema is passed to a binding method that is not allowed for
 * the binding.
 * <p>
 * The schemas allowed for a particular binding are those specified when the
 * binding is created using one of the {@link AvroCatalog} getXxxBinding
 * methods.
 * <p>
 * This exception may indicate a programming error if the application uses a
 * value with the wrong binding.  In that case, when this exception is thrown
 * the client should treat it as if it were an {@link IllegalArgumentException}
 * and report an error at a higher level.
 * <p>
 * However, an application may also use this exception to determine whether a
 * binding supports the value's schema or not.  In that case, depending on the
 * nature of the application, when this exception is thrown the client may wish
 * to
 * <ul>
 * <li>use a different binding that supports the schema,or </li>
 * <li>ignore the value having the unknown schema.</li>
 * </ul>
 * <p>
 * See {@link GenericAvroBinding} and {@link JsonAvroBinding} for an example of
 * handling {@code SchemaNotAllowedException}.
 *
 * @since 2.0
 *
 * @deprecated as of 4.0, use the table API instead.
 */
@Deprecated
public class SchemaNotAllowedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    final private String schemaName;

    /**
     * For internal use only.
     * @hidden
     */
    public SchemaNotAllowedException(String msg, String schemaName) {
        super(msg, null /*cause*/);
        this.schemaName = schemaName;
    }

    /**
     * Returns the full name of the schema that is not allowed.
     */
    public String getSchemaName() {
        return schemaName;
    }
}

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
package oracle.kv.impl.security.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies that the dynamic proxy should find another method (R3) that
 * performs the same function, and redirect the method invocation to that
 * R3 method.
 *<p>
 * An R3 method is a method of the same name as this method, but which takes
 * an additional AuthContext parameter as the next-to-last argument.
 * The proxy constructs a new argument list by adding a null AuthContext
 * argument to the provided argument list and calls invokes the R3 method
 * such that authentication checks are performed as if the R3 method had been
 * invoked directly. The actual body of this method should never get called, so
 * it is probably appropriate to throw a runtime exception as the method body,
 * as it exists solely to satify the RMI interface defintiion.
 * <p>
 * Note that it is an error if the R3 method for an R2 compatibility method is
 * not implemented within the same class.  If either class is re-implemented
 * on a derived class then both must be re-implemented.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SecureR2Method {
}

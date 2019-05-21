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

import java.lang.reflect.Method;
import java.rmi.Remote;
import java.util.HashSet;
import java.util.Set;

/**
 * A collection of common code used by proxy-building code in this package.
 */
final class ProxyUtils {

    /* Not instantiable */
    private ProxyUtils() {
    }

    static Set<Class<?>> findRemoteInterfaces(Class<?> implClass) {

        /*
         * Build a set of the most-derived interfaces that extend the
         * Remote interface.
         */
        final Set<Class<?>> remoteInterfaces = new HashSet<Class<?>>();
        for (Class<?> iface : implClass.getInterfaces()) {
            if (Remote.class.isAssignableFrom(iface)) {

                /* This interface either is, or extends Remote */
                for (Class<?> knownIface : remoteInterfaces) {
                    if (knownIface.isAssignableFrom(iface)) {
                        /* iface is a more derived class - replace knowIface */
                        remoteInterfaces.remove(knownIface);
                    }
                }
                remoteInterfaces.add(iface);
            }
        }

        return remoteInterfaces;
    }

    static Set<Method> findRemoteInterfaceMethods(Class<?> implClass) {

        /*
         * First, build a set of the most-derived interfaces that extend
         * the Remote interface.
         */
        final Set<Class<?>> remoteInterfaces = findRemoteInterfaces(implClass);

        /* Then collect the methods that are included in those interfaces */
        final Set<Method> remoteInterfaceMethods = new HashSet<Method>();
        for (Class<?> remoteInterface : remoteInterfaces) {
            for (Method method : remoteInterface.getMethods()) {
                remoteInterfaceMethods.add(method);
            }
        }

        return remoteInterfaceMethods;
    }

}

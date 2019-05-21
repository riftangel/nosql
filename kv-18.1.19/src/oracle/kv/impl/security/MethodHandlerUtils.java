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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;

/**
 * Common code for MethodHandler interaction
 */
final class MethodHandlerUtils {

    /* Not instantiable */
    private MethodHandlerUtils() {
    }

    /**
     * Call the method with the provided arguments.
     *
     * @param target the target object of an invocation
     * @param method a Method that should be called
     * @param args an argument list that should be passed to the method
     * @return an unspecified return type
     * @throws Exception anything that the underlying method could produce,
     * except that anything that is not Error or Exception is wrapped in an
     * UndeclaredThrowableException.
     */
    static Object invokeMethod(Object target, Method method, Object[] args)
        throws Exception {
        try {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException ite) {
                throw ite.getCause();
            }
        } catch (Exception e) {
            throw e;
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    /**
     * An implementation of MethodHandler that provides basic method
     * invocation support.
     */
    static class DirectHandler implements MethodHandler {
        private final Object target;

        DirectHandler(Object target) {
            this.target = target;
        }

        @Override
        public Object invoke(Method method, Object[] args)
            throws Exception {

            return invokeMethod(target, method, args);
        }
    }

    /**
     * An implementation of MethodHandler that provides basic method
     * invocation support by stripping the next-to-last argument from the
     * argument list.  This is used when calling an R2 implementation.
     */
    static class StripAuthCtxHandler implements MethodHandler {
        private final Object target;
        private final Method useMethod;

        /*
         * Constructor.
         *
         * @throws UnsupportedOperationException if the peer is running pre-R3
         * and a pre-R3 variant of the method being called cannot be found.
         */
        StripAuthCtxHandler(Object target, Method method) {
            this.target = target;

            final Class<?>[] newTypes =
                MethodHandlerUtils.stripAuthCtxArg(method.getParameterTypes());
            try {
                final Method newMethod =
                    target.getClass().getMethod(method.getName(), newTypes);

                this.useMethod = newMethod;
            } catch (NoSuchMethodException nsme) {
                throw new UnsupportedOperationException(
                    "Unable to call method " + method.getName() +
                    " on a pre-R3 implementation");
            }
        }

        @Override
        public Object invoke(Method method, Object[] args)
            throws Exception {

            return invokeMethod(target, useMethod, stripAuthCtxArg(args));
        }
    }

    /**
     * Create an array that contains all of the content of the input array
     * but with the next-to-last entry (expected to be AuthContext actual or
     * formal) removed.
     * The input array must contain at least 2 elements.
     */
    static <T> T[] stripAuthCtxArg(T[] args) {
        final T[] newArgs = Arrays.copyOfRange(args, 0, args.length - 1);
        newArgs[newArgs.length - 1] = args[args.length - 1];
        return newArgs;
    }
}

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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.util.SerialVersion;

/**
 * Provide a proxy for an object that implements one or more Remote interfaces.
 * The proxy uses a supplied LoginHandle to create AuthContext objects
 * for methods that require them, as well as providing automated retry
 * capability in the event that a login token needs to be refreshed.
 */
public final class ContextProxy<T> implements InvocationHandler {

    /* The security-enabled object that be will be proxying for */
    private final Object proxyTo;

    /* Map of Method to the MethodHandlers that will be used */
    private final Map<Method, MethodHandler> methodMap;

    /* The LoginHandle to use for acquiring LoginTokens */
    private final LoginHandle loginHdl;

    private final int serialVersion;

    /**
     * InvocationHandler required method.
     * @throws UnsupportedOperationException if the peer is running pre-R3
     * and a pre-R3 variant of the method being called cannot be found.
     * @throws IllegalStateException if a suitable method handler cannot be
     * found
     */
    @Override
    public Object invoke(Object unusedProxy, Method method, Object[] args)
        throws Exception {

        final MethodHandler handler = getHandler(method);
        if (handler == null) {
            throw new IllegalStateException(
                "MethodHandler for method " + method + " was not found");
        }
        return handler.invoke(method, args);
    }

    /**
     * Create a proxy object for the input object.
     *
     * @param proxyTo the object for which a context proxy is to be created.
     *        The object must implement one or more Remote interfaces.
     * @param loginHdl an object that manages login token information for
     *        the caller.
     * @param serialVersion the SerialVersion level at which the method
     *        invocations should take place.
     *
     * @return a proxy instance
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(T proxyTo,
                               LoginHandle loginHdl,
                               int serialVersion) {

        final ContextProxy<T> proxyHandler =
            new ContextProxy<T>(proxyTo, loginHdl, serialVersion);

        /*
         * Create a dynamic proxy instance that implements all of the
         * supplied interfaces by calling invoke on a ContextProxy instance.
         */
        final Class<?>[] remoteInterfaces =
            ProxyUtils.findRemoteInterfaces(
                proxyTo.getClass()).toArray(new Class<?>[0]);

        return (T) Proxy.newProxyInstance(proxyTo.getClass().getClassLoader(),
                                          remoteInterfaces, proxyHandler);
    }

    /**
     * @throws UnsupportedOperationException if the peer is running pre-R3
     * and a pre-R3 variant of the method being called cannot be found.
     */
    private MethodHandler getHandler(Method method) {
        MethodHandler handler = methodMap.get(method);
        if (handler == null) {
            handler = makeHandler(method);
            methodMap.put(method, handler);
        }
        return handler;
    }

    /**
     * Make a MethodHandler for the specified method
     *
     * @throws UnsupportedOperationException if the peer is running pre-R3
     * and a pre-R3 variant of the method being called cannot be found.
     */
    private MethodHandler makeHandler(Method method) {
        final Class<?>[] args = method.getParameterTypes();
        if (args.length < 2 ||
            AuthContext.class != args[args.length - 2]) {

            /*
             * This doesn't have the right signature, so don't
             * do any automatic handling
             */
            return new MethodHandlerUtils.DirectHandler(proxyTo);
        }

        if (serialVersion < SerialVersion.V4) {
            /*
             * The method takes a next-to-last argument of type AuthContext
             * but we are calling an R2 implementation, so we'll arrange to
             * throw that argument away.
             */
            return new MethodHandlerUtils.StripAuthCtxHandler(proxyTo, method);
        }

        return new ContextMethodHandler();
    }

    /**
     * Only for use by create()
     */
    private ContextProxy(Object proxyTo,
                         LoginHandle loginHdl,
                         int serialVersion) {
        this.proxyTo = proxyTo;
        this.loginHdl = loginHdl;
        this.methodMap = new ConcurrentHashMap<Method, MethodHandler>();
        this.serialVersion = serialVersion;
    }

    /**
     * An implementation of MethodHandler that automatically supplies
     * a AuthContext argument as the next-to-last value in the argument
     * list. This class should only be applied to methods that have a minimum
     * of two arguments where the next-to-last argument is of type
     * AuthContext.
     */
    class ContextMethodHandler
        implements MethodHandler {

        /**
         * The number of handle attempts to allow.  Must be > 0 in order to
         * enable handle renewal.
         */
        private static final int MAX_RENEW_ATTEMPTS = 1;

        /**
         * The number of retries due to SessionAccessException to allow.  Must
         * be > 0 in order to enable retries.
         */
        private static final int MAX_SAE_RETRIES = 5;

        @Override
        public Object invoke(Method method, Object[] args)
            throws Exception {

            final AuthContext initialAuthContext =
                (AuthContext) args[args.length - 2];
            final int maxRenewAttempts =
                (loginHdl == null || initialAuthContext != null) ?
                0 :
                MAX_RENEW_ATTEMPTS;

            int renews = 0;
            int saeRetries = 0;

            while (true) {
                LoginToken token = null;
                if (initialAuthContext == null && loginHdl != null) {
                    token = loginHdl.getLoginToken();
                    if (token != null) {
                        args[args.length - 2] = new AuthContext(token);
                    }
                }

                try {
                    return MethodHandlerUtils.invokeMethod(proxyTo, method,
                                                           args);
                } catch (SessionAccessException sae) {
                    if (sae.getIsReturnSignal() ||
                        saeRetries++ >= MAX_SAE_RETRIES) {
                        throw sae;
                    }
                    saeRetries++;
                } catch (AuthenticationRequiredException are) {
                    if (are.getIsReturnSignal() ||
                        renews++ >= maxRenewAttempts) {
                        throw are;
                    }
                    final LoginToken newToken = loginHdl.renewToken(token);
                    if (newToken == null || newToken == token) {
                        throw are;
                    }
                }
            }
        }
    }
}

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

package oracle.kv.impl.pubsub.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.security.login.LoginToken;

import com.sleepycat.je.rep.subscription.StreamAuthenticator;

/**
 * Object represents an authenticator that is running at the server side and
 * used by by feeder to authenticate the subscriber.
 */
public class StreamServerAuthHandler implements StreamAuthenticator {

    /* access checker from host rn */
    private final AccessChecker accessChecker;

    /* auth context built from token */
    private volatile AuthContext authCtx;

    /* subscribed table id strings, null if all tables subscribed */
    private volatile String[] tableIdStr;

    /* last time of security check */
    private volatile long lastCheckTime;

    /* logger */
    private final Logger logger;

    private StreamServerAuthHandler(AccessChecker accessChecker,
                                    Logger logger) {

        if (accessChecker == null) {
            throw new IllegalArgumentException("Null access checker");
        }

        this.accessChecker = accessChecker;
        this.logger = logger;
        authCtx = null;
        tableIdStr = null;
        lastCheckTime = 0;
    }

    /**
     * Gets an instance of server side authentication handler
     *
     * @param ac      access checker
     * @param logger  logger
     *
     * @return an instance of server side authentication handler
     *
     * @throws IllegalArgumentException if access checker is null
     */
    public static StreamServerAuthHandler getAuthHandler(AccessChecker ac,
                                                         Logger logger)
        throws IllegalArgumentException {

        final StreamServerAuthHandler ret =
            new StreamServerAuthHandler(ac, logger);

        logger.fine(lm("Server authenticator created."));
        return ret;
    }

    /**
     * Creates auth context from login token
     *
     * @param token login token in byte array
     */
    @Override
    public void setToken(byte[] token) {
        authCtx = new AuthContext(LoginToken.fromByteArray(token));
        logger.fine(lm("Token set and auth context created"));
    }

    /**
     * Sets subscribed table id strings
     *
     * @param ids subscribed table id strings, null if all tables subscribed.
     */
    @Override
    public void setTableIds(String[] ids) {
        tableIdStr = ids;
        /* make it INFO level for diagnosis purpose */
        logger.info(lm("Set subscribed table (id): " +
                       ((ids == null || ids.length == 0) ?
                           " all tables." : Arrays.toString(ids))));
    }

    /**
     * Authenticate login token. If no token has been set in the
     * authenticator, returns false.
     *
     * @return true if token is authenticated, false otherwise.
     */
    @Override
    public boolean authenticate() {

        lastCheckTime = System.currentTimeMillis();

        logger.fine(lm("Authentication starts"));

        if (authCtx == null) {
            /*
             * A protection in the case caller incorrectly call authenticate()
             * without an auth ctx. For example, in a secure store, feeder
             * which servers a replica does not have auth ctx initialized
             * in handshake since it relies on channel authentication. Also,
             * authenticate() should never be called for a non-secure store.
             */
            logger.warning(lm("Fail to authenticate because auth context is " +
                              "unavailable"));
            return false;
        }

        //TODO: do we really need check expiration, any api to do that?
        if (isTokenExpired(authCtx)) {
            logger.warning(lm("Fail to authenticate because token has " +
                              "expired"));
            return false;
        }

        try {
             if (accessChecker.identifyRequestor(authCtx) != null) {
                 logger.info(lm("Authentication succeeded"));
                 return true;
             }

            logger.warning(lm("Authentication failed because requestor " +
                              "cannot be identified"));
            return false;

        } catch (SessionAccessException sae) {
            logger.warning(lm("Authentication failed because of session " +
                              "access exception: " + sae.getMessage()));
            return false;
        }
    }

    /**
     * Checks if token is valid and the owner has enough privilege to stream
     * updates from subscribed tables
     *
     * @return true if token owner has enough privilege, false otherwise.
     */
    @Override
    public boolean checkAccess() {

        /*
         * If no auth context has been created, check fails
         *
         * For a replica in secure store, which does not create any auth ctx
         * in service handshake, we shall never reach here because the data
         * channel is trusted. Check feeder and feeder replica sync-up in JE
         * for details.
         *
         * For all other cases, if we reach here without a valid auth ctx,
         * something wrong already happened and we shall fail access check.
         */
        if (authCtx == null) {
            logger.warning(lm("Check access failed because auth context is " +
                              "not initialized"));
            return false;
        }

        lastCheckTime = System.currentTimeMillis();

        logger.fine(lm("Security check starts"));

        /* authenticate token */
        if (!authenticate()) {
            logger.warning(lm("Security check failed in authentication."));
            return false;
        }

        final SubscriptionOpsCtx opCtx = new SubscriptionOpsCtx(tableIdStr);
        try {
            final ExecutionContext ec =
                ExecutionContext.create(accessChecker, authCtx, opCtx);

            /*
             * Calling ExecutionContext.create has checked if execution ctx
             * has all privileges, and raise exception if it fails.
             */
            logger.fine(lm("Privilege check passed for " +
                           opCtx.describe() +
                           ", requestor client host: " +
                           ec.requestorContext().getClientHost()));
            return true;
        } catch (AuthenticationRequiredException | UnauthorizedException |
            SessionAccessException exp) {
            logger.warning(lm("Check access failed, reason: " +
                              exp.getMessage()));
            return false;
        }
    }

    /**
     * Gets the time stamp of last security check in milliseconds
     *
     * @return the time stamp of last security check, or 0 if no check has
     * been made.
     */
    @Override
    public long getLastCheckTimeMs() {
        return lastCheckTime;
    }

    private static boolean isTokenExpired(AuthContext ac) {
        return (ac.getLoginToken().getExpireTime() <=
                System.currentTimeMillis());
    }

    private static String lm(String msg) {
        return "[StreamServerAuth] " + msg;
    }

    /**
     * Subscription operation context
     */
    static class SubscriptionOpsCtx implements OperationContext {

        private final String[] ids;
        SubscriptionOpsCtx(String[] tableIds) {
            ids = tableIds;
        }

        @Override
        public String describe() {
            return "Table subscription request of " +
                ((ids == null || ids.length == 0) ?
                 " all tables" :
                 " tables IDs: " + Arrays.toString(ids));
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            /* required read privileges for all subscribed tables */
            final List<KVStorePrivilege> privileges = new ArrayList<>();
            if (ids == null || ids.length == 0) {
                /* read all tables */
                privileges.add(SystemPrivilege.READ_ANY_TABLE);
            } else {
                /* privilege to read each table */
                for (String idStr : ids) {
                    final long tid = TableImpl.createIdFromIdStr(idStr);
                    privileges.add(new TablePrivilege.ReadTable(tid));
                }
            }
            return privileges;
        }
    }
}

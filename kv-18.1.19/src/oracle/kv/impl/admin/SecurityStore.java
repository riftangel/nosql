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

package oracle.kv.impl.admin;

import java.util.logging.Logger;

import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;
import oracle.kv.impl.admin.AdminDatabase.LongKeyDatabase;
import oracle.kv.impl.admin.AdminStores.AdminStore;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataHolder;
import oracle.kv.impl.security.metadata.SecurityMetadata;

public abstract class SecurityStore extends AdminStore {

    public static SecurityStore getReadOnlyInstance(Logger logger,
                                                    Environment env) {
        return new SecurityDatabaseStore(logger, env, true);
    }

    static SecurityStore getStoreByVersion(int schemaVersion,
                                           Admin admin, EntityStore eStore) {
        if (schemaVersion < AdminSchemaVersion.SCHEMA_VERSION_5) {
            assert eStore != null;
            return new SecurityDPLStore(admin.getLogger(), eStore);
        }
        return new SecurityDatabaseStore(admin.getLogger(), admin.getEnv(),
                                         false /* read only */);
    }

    /* For unit test only */
    public static SecurityStore getTestStore(Environment env) {
        return new SecurityDatabaseStore(
                                Logger.getLogger(SecurityStore.class.getName()),
                                env, false);
    }

    /**
     * Gets the SecurityMetadata object using the specified transaction.
     */
    public abstract SecurityMetadata getSecurityMetadata(Transaction txn);

    /**
     * Persists the specified metadata object with the specified transaction.
     */
    public abstract boolean putSecurityMetadata(Transaction txn,
                                                SecurityMetadata md,
                                                boolean noOverwrite);

    protected SecurityStore(Logger logger) {
        super(logger);
    }

    private static class SecurityDatabaseStore extends SecurityStore {
        private final LongKeyDatabase<SecurityMetadata> metadataDb;

         SecurityDatabaseStore(Logger logger, Environment env,
                               boolean readOnly) {
            super(logger);
            metadataDb = new LongKeyDatabase<>(DB_TYPE.SECURITY, logger,
                                               env, readOnly);
        }

        @Override
        public SecurityMetadata getSecurityMetadata(Transaction txn) {
            return metadataDb.get(txn, LongKeyDatabase.ZERO_KEY,
                                  LockMode.READ_COMMITTED,
                                  SecurityMetadata.class);
        }

        @Override
        public boolean putSecurityMetadata(Transaction txn,
                                           SecurityMetadata md,
                                           boolean noOverwrite) {
            return metadataDb.put(txn, LongKeyDatabase.ZERO_KEY,
                                  md, noOverwrite);
        }

        @Override
        public void close() {
            metadataDb.close();
        }
    }

    private static class SecurityDPLStore extends SecurityStore {
         private final EntityStore eStore;

         SecurityDPLStore(Logger logger, EntityStore eStore) {
            super(logger);
            this.eStore = eStore;
        }

        @Override
        public SecurityMetadata getSecurityMetadata(Transaction txn) {
            final PrimaryIndex<String, MetadataHolder> pi =
                    eStore.getPrimaryIndex(String.class, MetadataHolder.class);
            final MetadataHolder holder = pi.get(txn,
                                             MetadataType.SECURITY.getKey(),
                                             LockMode.READ_UNCOMMITTED);

            return SecurityMetadata.class.cast((holder == null) ? null :
                                                          holder.getMetadata());
        }

        @Override
        public boolean putSecurityMetadata(Transaction txn,
                                           SecurityMetadata md,
                                           boolean noOverwrite) {
            readOnly();
            return false;
        }

        @Override
        protected void convertTo(int existingVersion, AdminStore newStore,
                                 Transaction txn) {
            final SecurityStore newSecurityStore = (SecurityStore)newStore;
            newSecurityStore.putSecurityMetadata(txn,
                                                 getSecurityMetadata(txn),
                                                 false);
        }
    }
}

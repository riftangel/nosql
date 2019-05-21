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
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataHolder;

public abstract class TableStore extends AdminStore {

    public static TableStore getReadOnlyInstance(Logger logger,
                                                 Environment repEnv) {
        return new TableDatabaseStore(logger, repEnv, true);
    }

    static TableStore getStoreByVersion(int schemaVersion,
                                        Admin admin, EntityStore eStore) {
        if (schemaVersion < AdminSchemaVersion.SCHEMA_VERSION_5) {
            assert eStore != null;
            return new TableDPLStore(admin.getLogger(), eStore);
        }
        return new TableDatabaseStore(admin.getLogger(), admin.getEnv(),
                                      false /* read only */);
    }

    /**
     * Gets the TableMetadata object using the specified transaction.
     */
    public abstract TableMetadata getTableMetadata(Transaction txn);

    /**
     * Persists the specified metadata object with the specified transaction.
     */
    public abstract boolean putTableMetadata(Transaction txn,
                                             TableMetadata md,
                                             boolean noOverwrite);

    private TableStore(Logger logger) {
        super(logger);
    }

    private static class TableDatabaseStore extends TableStore {
        private final LongKeyDatabase<TableMetadata> metadataDb;


        TableDatabaseStore(Logger logger, Environment env, boolean readOnly) {
            super(logger);
            metadataDb = new LongKeyDatabase<>(DB_TYPE.TABLE, logger,
                                               env, readOnly);
        }

        @Override
        public TableMetadata getTableMetadata(Transaction txn) {
            return metadataDb.get(txn, LongKeyDatabase.ZERO_KEY,
                                  (txn != null ? LockMode.RMW :
                                                 LockMode.READ_COMMITTED),
                                  TableMetadata.class);
        }

        @Override
        public boolean putTableMetadata(Transaction txn,
                                        TableMetadata md,
                                        boolean noOverwrite) {
            return metadataDb.put(txn, LongKeyDatabase.ZERO_KEY,
                                  md, noOverwrite);
        }

        @Override
        public void close() {
            metadataDb.close();
        }
    }

    private static class TableDPLStore extends TableStore {
        private final EntityStore eStore;

        TableDPLStore(Logger logger, EntityStore eStore) {
            super(logger);
            this.eStore = eStore;
        }

        @Override
        public TableMetadata getTableMetadata(Transaction txn) {
            final PrimaryIndex<String, MetadataHolder> pi =
                    eStore.getPrimaryIndex(String.class, MetadataHolder.class);
            final MetadataHolder holder = pi.get(txn,
                                                 MetadataType.TABLE.getKey(),
                                                 LockMode.READ_UNCOMMITTED);

            return TableMetadata.class.cast((holder == null) ? null :
                                                          holder.getMetadata());
        }

        @Override
        public boolean putTableMetadata(Transaction txn,
                                        TableMetadata md,
                                        boolean noOverwrite) {
            readOnly();
            return false;
        }

        @Override
        protected void convertTo(int existingVersion, AdminStore newStore,
                                 Transaction txn) {

            final TableStore newTableStore = (TableStore)newStore;

            final TableMetadata tableMD = getTableMetadata(txn);

            if (tableMD != null) {

                /*
                 * Any conversion is done implicitly in deserialization of the
                 * TableMetadata when it is read. TableMetadata and it's
                 * related objects such as FieldMap were never stored using DPL
                 * so the conversion implicit in TableMetadata.readObject() is
                 * sufficient. FieldMap supported DPL for use in admin plans,
                 * which were persisted. That conversion is not relevant here.
                 */
                newTableStore.putTableMetadata(txn, tableMD, false);
            }
        }
    }
}

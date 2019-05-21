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

import java.io.Closeable;
import java.util.EnumMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.evolve.Deleter;
import com.sleepycat.persist.evolve.Mutations;
import com.sleepycat.persist.model.AnnotationModel;
import com.sleepycat.persist.model.EntityModel;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin.Memo;
import static oracle.kv.impl.admin.AdminSchemaVersion.CURRENT_SCHEMA;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.topo.RealizedTopology;
import oracle.kv.impl.api.table.TableLimitsProxy;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadataProxy;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.metadata.SecurityMetadataProxy;
import oracle.kv.impl.topo.Topology;

/**
 * Container for the various persistent stores in the Admin. Some stores are
 * hidden in that this class exports get and put methods for several of the
 * persistent data types. Some stores can be accessed directly if they provide
 * more complex operations.
 */
class AdminStores {
    static final KVVersion DPL_CONVERSION = KVVersion.R4_0;
    {
        assert KVVersion.PREREQUISITE_VERSION.compareTo(DPL_CONVERSION) < 0 :
                "Code associated with the DPL store can be removed";
    }

    private static final String DPL_STORE_NAME = "AdminEntityStore";

    /* Store types */
    static enum STORE_TYPE {
        EVENT,      /* Critical events */
        GENERAL,    /* Parameters and Memos */
        PLAN,       /* Plans */
        SECURITY,   /* Security metadata */
        TABLE,      /* Table metadata */
        TOPOLOGY;   /* Topology history and canidates */
    }

    protected final Admin admin;

    /* Map of stores. Empty if the stores are not initialized */
    private final Map<STORE_TYPE, AdminStore> storeMap;

    /*
     * If true the Admin store is in read only mode until all Admins are up to
     * DPL_CONVERSION. When that occurs, upgrade the DPL stores (if they exist)
     * and set RO to false;
     */
    private boolean readOnly = true;

    /*
     * Pre-conversion EntityStore. Non-null during upgrade. If non-null then
     * the store is in a read-only mode and will remain in this mode until all
     * Admins are upgraded.
     */
    private EntityStore eStore = null;

    AdminStores(Admin admin) {
        this.admin = admin;
        storeMap = new EnumMap<>(STORE_TYPE.class);
    }

    /**
     * Returns true if the store is in read-only mode.
     * @return true if the store is in read-only mode
     */
    boolean isReadOnly() {
        return readOnly;
    }

    /**
     * (Re-)initializes the Admin stores using the specified schema version.
     * All Admin stores are opened upon return. If force is true and stores
     * are currently open, they are closed and re-opened.
     *
     * @param schemaVersion the schema version to use when opening the stores
     * @param force re-open all stores if open
     */
    synchronized void init(int schemaVersion, boolean force) {
        final boolean oldRO = readOnly;
        /*
         * If schema version < V_5 then we open the stores in read-only mode.
         * In this case we open the DPL EntityStore.
         */
        readOnly = schemaVersion < AdminSchemaVersion.SCHEMA_VERSION_5;

        /* Can't go from read-write to read-only */
        assert (oldRO == readOnly) | oldRO;

        /*
         * Init if force is true, the store isn't open (map is empty), or
         * the read-only status has changed.
         */
        if (!force && !storeMap.isEmpty() && (oldRO == readOnly)) {
            return;
        }
        close();

        admin.getLogger().log(Level.INFO,
                              "Initialize stores at schema version {0}, " +
                              "readOnly={1}",
                              new Object[]{schemaVersion, readOnly});
        if (readOnly) {
            initEstore();
        }
        storeMap.put(STORE_TYPE.EVENT,
                    EventStore.getStoreByVersion(schemaVersion, admin, eStore));
        storeMap.put(STORE_TYPE.GENERAL,
                  GeneralStore.getStoreByVersion(schemaVersion, admin, eStore));
        storeMap.put(STORE_TYPE.PLAN,
                     PlanStore.getStoreByVersion(schemaVersion, admin, eStore));
        storeMap.put(STORE_TYPE.SECURITY,
                 SecurityStore.getStoreByVersion(schemaVersion, admin, eStore));
        storeMap.put(STORE_TYPE.TABLE,
                    TableStore.getStoreByVersion(schemaVersion, admin, eStore));
        storeMap.put(STORE_TYPE.TOPOLOGY,
                 TopologyStore.getStoreByVersion(schemaVersion, admin, eStore));
    }

    /* Store accessors */

    EventStore getEventStore() {
        return (EventStore)getStore(STORE_TYPE.EVENT);
    }

    private GeneralStore getGeneralStore() {
        return (GeneralStore)getStore(STORE_TYPE.GENERAL);
    }

    PlanStore getPlanStore() {
        return (PlanStore)getStore(STORE_TYPE.PLAN);
    }

    private SecurityStore getSecurityMDStore() {
        return (SecurityStore)getStore(STORE_TYPE.SECURITY);
    }

    private TableStore getTableMDStore() {
        return (TableStore)getStore(STORE_TYPE.TABLE);
    }

    TopologyStore getTopologyStore() {
        return (TopologyStore)getStore(STORE_TYPE.TOPOLOGY);

    }

    private synchronized AdminStore getStore(STORE_TYPE type) {
        final AdminStore store = storeMap.get(type);
        if (store == null) {
            throw new AdminNotReadyException("Admin stores have not been " +
                                             "initialized.");
        }
        return store;
    }

    /* Convenience store methods */

    /**
     * Gets the Admin parameters from the store using the specified
     * transaction.
     *
     * @param txn a transaction
     * @return the Admin parameters
     */
    Parameters getParameters(Transaction txn) {
        return getGeneralStore().getParameters(txn);
    }

    /**
     * Puts the specified parameters into the store using the specified
     * transaction.
     *
     * @param txn a transaction
     * @param p parameters to store
     */
    void putParameters(Transaction txn, Parameters p) {
        getGeneralStore().putParameters(txn, p);
    }

    /**
     * Gets the memo from the store using the specified transaction.
     *
     * @param txn a transaction
     * @return the memo
     */
    Memo getMemo(Transaction txn) {
        return getGeneralStore().getMemo(txn);
    }

    /**
     * Puts the specified memo into the store using the specified transaction.
     *
     * @param txn a transaction
     * @param memo the memo to store
     */
    void putMemo(Transaction txn, Memo memo) {
        getGeneralStore().putMemo(txn, memo);
    }

    /**
     * Gets the current realized topology from the historical store using the
     * specified transaction and return a copy as a new topology instance.
     *
     * @param txn a transaction
     * @return the current realized topology
     */
    Topology getTopology(Transaction txn) {
        return getTopologyStore().getTopology(txn);
    }

    /**
     * Puts the specified realized topology into the store using the specified
     * transaction.
     *
     * @param txn a transaction
     * @param rt the realized topology to store
     */
    void putTopology(Transaction txn, RealizedTopology rt) {
        getTopologyStore().putTopology(txn, rt);
    }

    /**
     * Puts the specified plan into the store using the specified transaction.
     *
     * @param txn a transaction
     * @param plan the plan to store
     */
    void putPlan(Transaction txn, Plan plan) {
        getPlanStore().put(txn, plan);
    }

    /**
     * Gets the critical event for the specified ID from the store using the
     * specified transaction.
     *
     * @param txn a transaction
     * @param eventId an event ID
     * @return the critical event or null
     */
    CriticalEvent getEvent(Transaction txn, String eventId) {
        return getEventStore().getEvent(txn, eventId);
    }

    /**
     * Puts the specified critical event into the store using the specified
     * transaction.
     *
     * @param txn a transaction
     * @param event the critical event to store
     */
    void putEvent(Transaction txn, CriticalEvent event) {
        getEventStore().putEvent(txn, event);
    }

    /**
     * Gets the metadata of the specified type using the specified transaction.
     * The method does not support getting the topology. If the type is topology
     * a IllegalStateException is thrown.
     *
     * @param <T> the type of return class
     * @param returnType class of the return metadata type
     * @param type the metadata type
     * @param txn a transaction
     * @return the metadata object
     */
    <T extends Metadata<? extends MetadataInfo>> T
                                            getMetadata(Class<T> returnType,
                                                        MetadataType type,
                                                        Transaction txn) {
        switch (type) {
        case TABLE:
            return returnType.cast(getTableMDStore().getTableMetadata(txn));
        case SECURITY:
            return returnType.cast(getSecurityMDStore().
                                                    getSecurityMetadata(txn));
        case TOPOLOGY:
            break;
        }
        throw new IllegalStateException("Invalid metadata type: " + type);
    }

    /**
     * Puts the specified metadata into the store using the specified
     * transaction. The method does not support getting the topology. If the
     * type is topology a IllegalStateException is thrown.
     *
     * @param md the metadata to store
     * @param txn a transaction
     */
    boolean putMetadata(Metadata<? extends MetadataInfo> md,
                        Transaction txn,
                        boolean noOverwrite) {
        switch (md.getType()) {
        case TABLE:
            return getTableMDStore().putTableMetadata(txn,
                                                      (TableMetadata)md,
                                                      noOverwrite);
        case SECURITY:
            return getSecurityMDStore().putSecurityMetadata(txn,
                                                           (SecurityMetadata)md,
                                                            noOverwrite);
        case TOPOLOGY:
            break;
        }
        throw new IllegalStateException("Invalid metadata type: " +
                                        md.getType());
    }

    /**
     * Converts the contents of the stores into new Admin non-DPL stores. Upon
     * return the stores will be closed and must be re-initialized.
     *
     * @param schemaVersion the schema version of this store
     * @param txn the transaction to use for reads and writes
     */
    void convertTo(int schemaVersion, Transaction txn) {
        assert readOnly;
        assert schemaVersion != CURRENT_SCHEMA;

        /* Create a store at the latest schema version */
        final AdminStores newStores = new AdminStores(admin);
        newStores.init(CURRENT_SCHEMA, true);

        /* For each store convert its contents to the new store */
        for (STORE_TYPE type : STORE_TYPE.values()) {
            final AdminStore dplStore = storeMap.get(type);
            assert dplStore != null;
            final AdminStore newStore = newStores.storeMap.get(type);
            assert newStore != null;
            dplStore.convertTo(schemaVersion, newStore, txn);
        }
        newStores.close();
        close();
        assert eStore == null;

        /* If everything works, remove the DPL store */
        admin.getLogger().log(Level.INFO, "Converted DPL store, removing {0}",
                              DPL_STORE_NAME);

        final Environment env = admin.getEnv();

        /*
         * Code for removing DPL store cribbed from class Javadoc in
         * com.sleepycat.persist.EntityStore
         */
        final String prefix = "persist#" + DPL_STORE_NAME + "#";
        for (String dbName : env.getDatabaseNames()) {
           if (dbName.startsWith(prefix)) {
               env.removeDatabase(txn, dbName);
           }
       }
    }

    /**
     * Closes the stores.
     */
    synchronized void close() {
        for (AdminStore store : storeMap.values()) {
            if (store != null) {
                store.close();
            }
        }
        storeMap.clear();

        if (eStore != null) {
            eStore.close();
            eStore = null;
        }
    }

    /*
     * Initializes the EntityStore in read-only mode.
     */
    private void initEstore() {
        if (eStore != null) {
            return;
        }
        final EntityModel model = new AnnotationModel();
        model.registerClass(TableMetadataProxy.class);
        model.registerClass(SecurityMetadataProxy.class);
        model.registerClass(TableLimitsProxy.class);
        final StoreConfig stConfig = new StoreConfig();
        stConfig.setAllowCreate(false);
        stConfig.setTransactional(true);
        stConfig.setModel(model);
        stConfig.setReadOnly(true);
        setMutations(stConfig);
        eStore = new EntityStore(admin.getEnv(), DPL_STORE_NAME, stConfig);
    }

    /*
     * Mutations for schema evolution of the admin database:
     * 1.  Remove RegistryUtils as a class.
     * 2.  Remove RegistryUtils from DeployStorePlan version 0.
     */
    private void setMutations(StoreConfig stConfig) {
         /* Note that the mutations are needed even for read-only mode. */
        final Mutations mutations = new Mutations();
        mutations.addDeleter
            (new Deleter("oracle.kv.impl.util.registry.RegistryUtils", 0));
        mutations.addDeleter
            (new Deleter("oracle.kv.impl.admin.plan.DeployStorePlan", 0,
                         "registryUtils"));
        stConfig.setMutations(mutations);
    }

    /**
     * Base class for a store.
     */
    abstract static class AdminStore implements Closeable {
        protected final Logger logger;

        protected AdminStore(Logger logger) {
            this.logger = logger;
        }

        /* -- From Closeable -- */

        /**
         * Closes the store. The default implementation is a no-op since DPL
         * sub-classes do not close the EntityStore.
         */
        @Override
        public void close() {}

        /**
         * Reads the contents of this store and writes them to the newStore.
         * If necessary the contents are converted from schemaVersion to the
         * current schema version. DPL stores should implement this method
         * to convert to non-DPL stores. The default implementation throws
         * IllegalStateException.
         *
         * @param schemaVersion the schema version of this store
         * @param newStore the new store to copy the converted data
         * @param txn the transaction to use for reads and writes
         */
        protected void convertTo(int schemaVersion, AdminStore newStore,
                                 Transaction txn) {
            throw new IllegalStateException("Conversion call on non-DPL store");
        }

        /**
         * Throws AdminNotReadyException with a message indicating that the
         * Admin is in read-only mode.
         */
        protected void readOnly() {
            throw new AdminNotReadyException(
                    "Admin is in read-only mode during upgrade. Please " +
                    "upgrade remaining Admin nodes to software version " +
                    DPL_CONVERSION.getNumericVersionString() + " or greater.");
        }
    }

    /**
     * Base class for creating a store cursor.
     *
     * @param <K> key type
     * @param <T> object type
     */
    abstract static class AdminStoreCursor<K, T> implements Closeable {
        private final Cursor cursor;
        private final K startKey;
        private final DatabaseEntry keyEntry = new DatabaseEntry();
        private final DatabaseEntry data = new DatabaseEntry();

        protected AdminStoreCursor(Cursor cursor) {
            this(cursor, null);
        }

        protected AdminStoreCursor(Cursor cursor, K startKey) {
            this.cursor = cursor;
            this.startKey = startKey;
        }

        /**
         * Moves the cursor to the first key/value pair of the database, and
         * returns that value. Null will be returned if no data exists.
         * <p>
         * If the {@code startKey} is specified in constructor, this method
         * will return the object with the {@code startKey} if found, otherwise
         * the object with smallest key greater than or equal to {@code
         * startKey} will be returned. If such key is not found, null will be
         * returned.
         */
        public T first() {
            /*
             * Try to move cursor to the start key if the specified.
             */
            final OperationStatus status;
            if (startKey != null) {
                keyToEntry(startKey, keyEntry);
                status =
                    cursor.getSearchKeyRange(keyEntry, data, LockMode.DEFAULT);
            } else {
                status = cursor.getFirst(keyEntry, data, LockMode.DEFAULT);
            }
            if (status == OperationStatus.SUCCESS) {
                return entryToObject(keyEntry, data);
            }
            return null;
        }

        /**
         * Moves the cursor to the previous key/value and returns that value.
         * Null will be return if the first key/value pair is reached.
         */
        T prev() {
            final OperationStatus status =
                cursor.getPrev(keyEntry, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                return entryToObject(keyEntry, data);
            }
            return null;
        }

        /**
         * Moves the cursor to the next key/value pair and returns that value.
         * Null will be return if the last key/value pair is reached.
         */
        public T next() {
            final OperationStatus status =
                cursor.getNext(keyEntry, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                return entryToObject(keyEntry, data);
            }
            return null;
        }

        /**
         * Moves the cursor to the last key/value pair of the database, and
         * returns that value. Null will be returned is no data exists.
         */
        T last() {
            if (cursor.getLast(keyEntry, data, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS) {
                return entryToObject(keyEntry, data);
            }
            return null;
        }

        /**
         * Deletes the record at the cursor. Returns true if the operation
         * was successful.
         *
         * @return true if the delete was successful
         */
        boolean delete() {
            return cursor.delete() == OperationStatus.SUCCESS;
        }

        /**
         * Converts the key/value to an object.
         */
        protected abstract T entryToObject(DatabaseEntry key,
                                           DatabaseEntry value);

        /**
         * Converts a key to an entry.
         */
        @SuppressWarnings("unused")
        protected void keyToEntry(K key, DatabaseEntry entry) {
            throw new AssertionError("keyToEntry not defined");
        }

        @Override
        public void close() {
            cursor.close();
        }
    }
}

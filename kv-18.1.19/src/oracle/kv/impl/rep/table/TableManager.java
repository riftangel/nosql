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

package oracle.kv.impl.rep.table;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.impl.api.table.DroppedTableException;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.DatabaseNotReadyException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.measurement.TableInfo;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.rep.MetadataManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.admin.ResourceInfo;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.rep.admin.ResourceInfo.RateRecord;
import oracle.kv.impl.rep.table.SecondaryInfoMap.DeletedTableInfo;
import oracle.kv.impl.rep.table.SecondaryInfoMap.SecondaryInfo;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.tif.TextIndexFeederManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.StateTracker;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;

/**
 * Manages the secondary database handles for the rep node. The TableManager,
 * SecondaryInfoMap, and MaintenanceThread classes are tightly coupled and
 * there are things that are safe because of the way we know they are called
 * from one another.
 */
public class TableManager extends MetadataManager<TableMetadata>
                          implements SecondaryAssociation {

    /*
     * Create the name of the secondary DB based on the specified index
     * and table names.  Use index name first. Format is:
     *   indexName.tableName[:namespace]
     * The index name is first to more easily handle parent/child names in
     * tableName (parent.child).
     *
     * The ":" is used to separate the namespace for the same reason.
     */
    public static String createDbName(String namespace,
                                      String indexName,
                                      String tableName) {
        final StringBuilder sb = new StringBuilder();
        sb.append(indexName).append(".").append(tableName);
        if (namespace != null) {
            sb.append(":").append(namespace);
        }
        return sb.toString();
    }

    /*
     * Gets the table name from a secondary database name.
     */
    static String getTableName(String dbName) {
        if (dbName.contains(":")) {
            return dbName.split(":")[0].split("\\.", 2)[1];
        }
        return dbName.split("\\.", 2)[1];
    }

    static String getNamespace(String dbName) {
        if (dbName.contains(":")) {
            return dbName.split(":", 2)[1];
        }
        return null;
    }

    private final Params params;

    private final StateTracker stateTracker;

    /*
     * The table metadata. TableMetada is not thread safe. The instance
     * referenced by tableMetadata should be treated as immutable. Updates
     * to the table metadata must be made to a copy first and then replace the
     * reference with the copy.
     */
    private volatile TableMetadata tableMetadata = null;

    private final int maxChanges;

    /*
     * Map of secondary database handles. Modification or iteration can only
     * be made within the maintenance thread, or with the threadLock held and
     * the maintenance thread stopped.
     */
    private final Map<String, SecondaryDatabase> secondaryDbMap =
                                                            new HashMap<>();

    /*
     * Thread used to asynchronously open secondary database handles and
     * preform other maintenance operations such as populating secondary DBs.
     */
    private MaintenanceThread maintenanceThread = null;

    /*
     * Lock controlling the maintenanceThread and modification to
     * secondaryDbMap. If object synchronization and the threadLock is needed
     * the synchronization should be first.
     */
    private final ReentrantLock threadLock = new ReentrantLock();
    
    /**
     * Set to true if there is ongoing maintenance operations. The
     * isBusyMaintenance field and associated accessor method are used to
     * interlock table maintenance with partition migration. The interlock
     * scheme is:
     * 1) a maintenance thread sets isBusyMaintenance
     * 2) the maintenance thread waits for migration to complete (by calling
     *    either awaitIdle or awaitTargetIdle on the migration manager)
     * 4) when maintenance is completed (or is interrupted)
     *    isBusyMaintenance is cleared
     * 
     * Note: This assumes only one maintenance thread is running at a time. If
     * this changes, the busy indicator will need to be reworked.
     */
    private volatile boolean isBusyMaintenance;

    /*
     * The TableMetadata sequence number, it is updated in updateDbHandles()
     */
    public int metadataSeqNum = TableMetadata.EMPTY_SEQUENCE_NUMBER;

    /*
     * The map is an optimization for secondary DB lookup. This lookup happens
     * on every operation and so should be as efficient as possible. This map
     * parallels the Table map in TableMetadata except that it only contains
     * entries for tables which contain indexes (and therefore reference
     * secondary DBs). The map is reconstructed whenever the table MD is
     * updated.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * Note that secondaryLookupMap may be out-of-date relitive to the
     * non-synchronized tableMetadata.
     */
    private volatile Map<String, TableEntry> secondaryLookupMap = null;

    /*
     * Map to lookup a table via its ID.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * Note that idLookupMap may be out-of-date relative to the non-synchronized
     * tableMetadata.
     */
    private volatile Map<Long, TableImpl> idLookupMap = null;

    /*
     * Maximum of all table IDs seen so far, including child tables.
     */
    private volatile long maxTableId = 0;
    
    /**
     * Map to lookup a top-level table via its Key as a byte array.
     *
     * The map uses {@link IDBytesComparator} to allow lookup of a full record
     * key (as a byte array). Since the map only contains top-level tables,
     * only the first component of the key (the table ID) is compared.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     * 
     * A TreeMap is used, rather than a HashMap, because we cannot override
     * the equals/hashcode methods for the byte array.
     */
    private volatile Map<byte[], TableImpl> idBytesLookupMap = null;

    /**
     * Map to lookup a r2compat table id via its name.  It is used to
     * accelerate the r2compat table match test in permisson checking.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * Note that r2NameLookupMap may be out-of-date relative to the
     * non-synchronized tableMetadata.
     */
    private volatile Map<String, Long> r2NameIdLookupMap = null;

    /*
     * Mapps a table ID to a throughput collector object. A collector is
     * present only if throughput limits are present on that table. There is
     * only one collector per table hierarchy. Child table IDs are included
     * in the map and will reference the collector instance for the top
     * level table.
     */
    private volatile Map<Long, ThroughputCollector> collectorLookupMap = null;

    /* The hook affects before remove database */
    public static TestHook<Integer> BEFORE_REMOVE_HOOK;
    /* The hook affects after remove database */
    public static TestHook<Integer> AFTER_REMOVE_HOOK;

    public TableManager(RepNode repNode, Params params) {
        super(repNode, params);
        this.params = params;
        maxChanges = params.getRepNodeParams().getMaxTopoChanges();
        logger.log(Level.INFO, "Table manager created (max change history={0})",
                   maxChanges);
        stateTracker = new TableManagerStateTracker(logger);
    }

    @Override
    public void shutdown() {
        stateTracker.shutdown();
        shutdownMaintenance();
        super.shutdown();
    }

    /**
     * Returns the table metadata object. Returns null if there was an error.
     *
     * @return the table metadata object or null
     */
    public TableMetadata getTableMetadata() {

        final TableMetadata currentTableMetadata = tableMetadata;
        if (currentTableMetadata != null) {
            return currentTableMetadata;
        }

        synchronized (this) {
            if (tableMetadata == null) {
                try {
                    tableMetadata = fetchMetadata();
                } catch (DatabaseNotReadyException dnre) {
                    /* DB not ready, ignore */
                    return  null;
                }
                /*
                 * If the DB is empty, we create a new instance so
                 * that we don't keep re-reading.
                 */
                if (tableMetadata == null) {
                    /*
                     * Keep change history because it's used to push to
                     * other RNs (in other shards) that need metadata
                     * updates. See MetadataUpdateThread.
                     */
                    tableMetadata = new TableMetadata(true);
                }
            }
        }
        return tableMetadata;
    }

    /**
     * Returns the table metadata sequence number.
     *
     * @return the table metadata sequence number
     */
    public int getTableMetadataSeqNum() {
        return metadataSeqNum;
    }

    /* -- public index APIs -- */

    /**
     * Returns true if the specified index has been successfully added.
     *
     * @param indexName the index ID
     * @param tableName the fully qualified table name
     * @return true if the specified index has been successfully added
     */
    public boolean addIndexComplete(String namespace,
                                    String indexName,
                                    String tableName) {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return false;
        }

        final SecondaryInfoMap secondaryInfoMap = getSecondaryInfoMap(repEnv);

        /* If there is an issue reading the info object, punt */
        if (secondaryInfoMap == null) {
            return false;
        }

        final String dbName = createDbName(namespace, indexName, tableName);

        final SecondaryInfo info = secondaryInfoMap.getSecondaryInfo(dbName);

        if (info == null) {
            logger.log(Level.FINE, "addIndexComplete({0}), info is null, " +
                       "returning false", new Object[]{dbName});
            return false;
        }

        final String msg = info.getErrorString();
        if (msg != null) {
            logger.log(Level.INFO,
                       "addIndexComplete({0}) throwing exception {1}",
                       new Object[]{dbName, msg});
            throw new WrappedClientException(
                new IllegalArgumentException(msg));
        }

        logger.log(Level.FINE, "addIndexComplete({0}) info is {1}",
                   new Object[]{dbName, info});
        return !info.needsPopulating();
    }

    /**
     * Returns true if the data associated with the specified table has been
     * removed from the store.
     *
     * @param tableName the fully qualified table name
     * @return true if the table data has been removed
     */
    public boolean removeTableDataComplete(String namespace, String tableName) {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return false;
        }

        final SecondaryInfoMap secondaryInfoMap = getSecondaryInfoMap(repEnv);

        /* If there is an issue reading the info object, punt */
        if (secondaryInfoMap == null) {
            return false;
        }

        final DeletedTableInfo info =
            secondaryInfoMap.getDeletedTableInfo(
                TableMetadata.makeNamespaceName(namespace, tableName));
        if (info != null) {
            return info.isDone();
        }
        final TableMetadata md = getTableMetadata();

        return (md == null) ? false :
            (md.getTable(namespace, tableName) == null);
    }

   /**
    * Gets the table instance for the specified ID. If no table is defined,
    * or the table is being deleted null is returned.
    *
    * @param tableId a table ID
    * @return the table instance or null
    * @throws RNUnavailableException is the table metadata is not yet
    * initialized
    */
    public TableImpl getTable(long tableId) {
        final TableImpl table = getTableInternal(tableId);
        return table == null ? null : table.isDeleting() ? null : table;
    }
    
    /**
     * Gets the table instance for the specified ID. If no table is defined
     * null is returned. Note that the table returned may be in a deleting
     * state.
     * 
     * @param tableId a table ID
     * @return the table instance or null
     * @throws RNUnavailableException is the table metadata is not yet
     * initialized
     */
    TableImpl getTableInternal(long tableId) {
        final Map<Long, TableImpl> map = idLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                "Table metadata is not yet initialized");
        }
        return map.get(tableId);
    }

    /**
     * Gets the table instance for a given record key as a byte array.
     *
     * @param key the record key as a byte array.
     * @return the table instance or null if the key is not a table key.
     * @throws RNUnavailableException is the table metadata is not yet
     * initialized
     * @throws DroppedTableException if the key is not for an existing table,
     * and the key does not appear to be a non-table (KV API) key.
     * 
     * See IDBytesComparator.
     */
    public TableImpl getTable(byte[] key) {
        final Map<byte[], TableImpl> map = idBytesLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                "Table metadata is not yet initialized");
        }
        TableImpl table = map.get(key);
        final int nextOff = Key.findNextComponent(key, 0);
        if (table == null) {
            /* Check for a dropped top-level table. */
            TableImpl.checkForDroppedTable(key, 0, nextOff, maxTableId);
            return null;
        }
        table = table.findTargetTable(key, nextOff + 1, maxTableId);
        if (table == null) {
            return null;
        }
 
        /* A "deleting" table be considered as dropped */
        if (table.isDeleting()) {
            throw new DroppedTableException();
        }
        return table;
    }
    
    /**
     * Gets the table instance for the specified table name. Returns null if no
     * table with that name is in the metadata.
     *
     * @param tableName a table name
     * @return the table instance or null
     */
    TableImpl getTable(String namespace,
                       String tableName) {
        final TableMetadata md = getTableMetadata();

        return (md == null) ? null : md.getTable(namespace, tableName);
    }

    /**
     * Gets a r2-compat table instance for the specified table name. Returns
     * null if no  r2-compat table with that name is defined.
     *
     * @param tableName a table name
     * @return the table instance or null
     */
    public TableImpl getR2CompatTable(String tableName) {
        final Map<String, Long> map = r2NameIdLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                "Table metadata is not yet initialized");
        }
        final Long tableId = map.get(tableName);
        return tableId == null ? null : getTable(tableId);
    }

    /**
     * Gets the params used to construct this instance.
     */
    Params getParams() {
        return params;
    }

    /**
     * Gets the secondaryInfoMap for read-only. Returns null if there was an
     * error.
     */
    private SecondaryInfoMap getSecondaryInfoMap(ReplicatedEnvironment repEnv) {
        try {
            return SecondaryInfoMap.fetch(repEnv);
        } catch (RuntimeException re) {
            DatabaseUtils.handleException(re, logger, "index populate info");
        }
        return null;
    }

    /* -- Secondary DB methods -- */

    /**
     * Gets the secondary database of the specified name. Returns null if the
     * secondary database does not exist.
     *
     * @param dbName the name of a secondary database
     * @return a secondary database or null
     */
    SecondaryDatabase getSecondaryDb(String dbName) {
        return secondaryDbMap.get(dbName);
    }

    /**
     * Gets the secondary database for the specified index. Returns null if the
     * secondary database does not exist.
     *
     * @param indexName the index name
     * @param tableName the table name
     * @return a secondary database or null
     */
    public SecondaryDatabase getIndexDB(String namespace,
                                        String indexName,
                                        String tableName) {
        return getSecondaryDb(createDbName(namespace, indexName, tableName));
    }

    /**
     * Closes all secondary DB handles.
     */
    @Override
    public synchronized void closeDbHandles() {
        logger.log(Level.INFO, "Closing secondary database handles");

        threadLock.lock();
        try {
            shutdownMaintenance();

            final Iterator<SecondaryDatabase> itr =
                                        secondaryDbMap.values().iterator();
            while (itr.hasNext()) {
                closeSecondaryDb(itr.next());
                itr.remove();
            }
        } finally {
            threadLock.unlock();
            super.closeDbHandles();
        }
    }

    /**
     * Closes the specified secondary database. Returns true if the close
     * succeeded, or the database was not open.
     *
     * @param dbName the name of the secondary DB to close
     * @return true on success
     */
    boolean closeSecondary(String dbName) {
        return closeSecondaryDb(secondaryDbMap.get(dbName));
    }

    /**
     * Closes the specified secondary DB. Returns true if the close was
     * successful or db is null.
     *
     * @param db secondary database to close
     * @return true if successful or db is null
     */
    boolean closeSecondaryDb(SecondaryDatabase db) {
        if (db != null) {
            try {
                db.close();
            } catch (RuntimeException re) {
                logger.log(Level.INFO, "close of secondary DB failed: {0}",
                           re.getMessage());
                return false;
            }
        }
        return true;
    }

    /* -- From SecondaryAssociation -- */

    @Override
    public boolean isEmpty() {

        /*
         * This method is called on every operation. It must be as fast
         * as possible.
         */
        final Map<String, TableEntry> map = secondaryLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                    "Table metadata is not yet initialized");
        }
        return map.isEmpty();
    }

    @Override
    public Database getPrimary(DatabaseEntry primaryKey) {
        return repNode.getPartitionDB(primaryKey.getData());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<SecondaryDatabase>
        getSecondaries(DatabaseEntry primaryKey) {

        /*
         * This is not synchronized so the map may have nulled since isEmpty()
         * was called.
         */
        final Map<String, TableEntry> map = secondaryLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                "Table metadata is not yet initialized");
        }

        /* Start by looking up the top level table for this key */
        final BinaryKeyIterator keyIter =
                                    new BinaryKeyIterator(primaryKey.getData());

        /* The first element of the key will be the top level table */
        final String rootId = keyIter.next();
        final TableEntry entry = map.get(rootId);

        /* The entry could be null if the table doesn't have indexes */
        if (entry == null) {
            return Collections.EMPTY_SET;
        }

        /* We have a table with indexes, match the rest of the key. */
        final Collection<String> matchedIndexes = entry.matchIndexes(keyIter);

        /* This could be null if the key did not match any table with indexes */
        if (matchedIndexes == null) {
            return Collections.EMPTY_SET;
        }
        final List<SecondaryDatabase> secondaries =
                        new ArrayList<>(matchedIndexes.size());

        /*
         * Get the DB for each of the matched DB. Opening of the DBs is async
         * so we may not be able to complete the list. In this case thow an
         * exception and hopefully the operation will be retried.
         */
        for (String dbName : matchedIndexes) {
            final SecondaryDatabase db = secondaryDbMap.get(dbName);
            if (db == null) {
                /* Throwing RNUnavailableException should cause a retry */
                throw new RNUnavailableException(
                                       "Secondary db not yet opened " + dbName);
            }
            secondaries.add(db);
        }
        return secondaries;
    }

    /* -- Metadata update methods -- */

    /**
     * Updates the table metadata with the specified metadata object. Returns
     * true if the update is successful.
     *
     * @param newMetadata a new metadata
     * @return true if the update is successful
     */
    public synchronized boolean updateMetadata(Metadata<?> newMetadata) {
        if (!(newMetadata instanceof TableMetadata)) {
            throw new IllegalStateException("Bad metadata?" + newMetadata);
        }

        /* If no env, or not a master then we can't do an update */
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);
        if ((repEnv == null) || !repEnv.getState().isMaster()) {
            return false;
        }

        final TableMetadata md = getTableMetadata();

        /* Can't update if we can't read it */
        if (md == null) {
            return false;
        }
        /* If the current md is up-to-date or newer, exit */
        if (md.getSequenceNumber() >= newMetadata.getSequenceNumber()) {
            return true;
        }
        logger.log(Level.INFO, "Updating table metadata with {0}", newMetadata);
        return update((TableMetadata)newMetadata, repEnv);
    }

    /**
     * Updates the table metadata with the specified metadata info object.
     * Returns the sequence number of the table metadata at the end of the
     * update.
     *
     * @param metadataInfo a table metadata info object
     * @return the post update sequence number of the table metadata
     */
    public synchronized int updateMetadata(MetadataInfo metadataInfo) {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        /* If no env or error then report back that we don't have MD */
        if (repEnv == null) {
            return Metadata.EMPTY_SEQUENCE_NUMBER;
        }
        final TableMetadata md = getTableMetadata();
        if (md == null) {
            return Metadata.EMPTY_SEQUENCE_NUMBER;
        }

        /* Only a master can actually update the metadata */
        if (!repEnv.getState().isMaster()) {
            logger.log(Level.FINE, "Metadata update attempted at replica");
            return md.getSequenceNumber();
        }

        /*
         * If the current seq num is >= that of the highest number in the
         * update, skip it; we have a more current version.
         */
        if (md.getSequenceNumber() >= metadataInfo.getSourceSeqNum()) {
            logger.log(Level.FINE, "Metadata not updated, current seq " +
                       "number {0} is >= the update {1}",
                       new Object[]{md.getSequenceNumber(),
                                    metadataInfo.getSourceSeqNum()});
            return md.getSequenceNumber();
        }
        final TableMetadata newMetadata = md.getCopy();
        try {
            logger.log(Level.FINE,
                           "Attempting to update table metadata, seq num {0}," +
                       " with {1}", new Object[] {md.getSequenceNumber(),
                                                  metadataInfo});
            if (newMetadata.update(metadataInfo)) {
                logger.log(Level.INFO,
                           "Successful update of table metadata with {0}",
                           metadataInfo);
                if (update(newMetadata, repEnv)) {
                    return newMetadata.getSequenceNumber();
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Error updating table metadata with " +
                       metadataInfo, e);
        }

        logger.log(Level.INFO, "Metadata update failed, current seq number {0}",
                   md.getSequenceNumber());
        /* Update failed, return the current seq # */
        return md.getSequenceNumber();
    }

    /**
     * Persists the specified table metadata and updates the secondary DB
     * handles. The metadata is persisted only if this node is the master.
     *
     * @return true if the update was successful, or false if in shutdown or
     * unable to restart text index feeder.
     */
    private boolean update(final TableMetadata newMetadata,
                           ReplicatedEnvironment repEnv) {
        assert Thread.holdsLock(this);
        assert repEnv != null;

        /* Only the master can persistMetadata the metadata */
        if (repEnv.getState().isMaster()) {
            if (!persistMetadata(newMetadata, maxChanges)) {
                return true;    /* In shutdown */
            }
        }

        /*
         * The TIF needs to see the new metadata in the RepNode when it
         * restarts, so we set it now; however we must save the old metadata in
         * case the TIF metadata update fails in some way, so that it can be
         * restored.  The success or failure of a metadata update is ultimately
         * determined by whether the new metadata was installed, so if there
         * was a failure we want to make sure we don't return from here with
         * the new metadata installed.
         */
        final TableMetadata oldMetadata = tableMetadata;

        /* Tentatively update the cached metadata. */
        tableMetadata = newMetadata;

        /* Notify the text index feeder, if there is one. */
        final TextIndexFeederManager tifm = repNode.getTextIndexFeederManager();
        if (tifm != null) {
            try {
                tifm.newTableMetadata(oldMetadata, newMetadata);
            } catch (Exception e) {
                tableMetadata = oldMetadata; /* update failed, undo */
                throw e;
            }
        }

        /* Finally, update the DB handles */
        updateDbHandles(repEnv);

        return true;
    }

    /**
     * Updates the secondary database handles based on the current table
     * metadata. Update of the handles is done asynchronously. If an update
     * thread is already running it is stopped and a new thread is started.
     *
     * This is called by the RN when 1) the handles have to be renewed due to
     * an environment change 2) when the topology changes, and 3) when the
     * table metadata is updated.
     *
     * The table maps are also set.
     *
     * @param repEnv the replicated environment handle
     *
     * @return true if table metadata handle needed to be updated.
     */
    @Override
    public synchronized boolean updateDbHandles(ReplicatedEnvironment repEnv) {
        final boolean refresh = super.updateDbHandles(repEnv);

        if (updateTableMaps(repEnv)) {
            requestMaintenanceUpdate();
        }
        return refresh;
    }

    /**
     * Rebuilds the secondaryLookupMap and idLookupMap maps. Returns true if
     * the update was successful. If false is returned the table maps have been
     * set such that operations will be disabled.
     *
     * @return true if the update was successful
     */
    private synchronized boolean updateTableMaps(ReplicatedEnvironment repEnv) {
        assert repEnv != null;

        final TableMetadata tableMd = getTableMetadata();

        /*
         * If env is invalid, or tableMD null, disable ops
         */
        if (!repEnv.isValid() || (tableMd == null)) {
            secondaryLookupMap = null;
            idLookupMap = null;
            idBytesLookupMap = null;
            r2NameIdLookupMap = null;
            maxTableId = 0;
            collectorLookupMap = null;
            return false;
        }
        metadataSeqNum = tableMd.getSequenceNumber();

        /*
         * If empty, then a quick return. Note that we return true so that
         * the update thread runs because there may have been tables/indexes
         * that need cleaning.
         */
        if (tableMd.isEmpty()) {
            secondaryLookupMap = Collections.emptyMap();
            idLookupMap = Collections.emptyMap();
            idBytesLookupMap = Collections.emptyMap();
            r2NameIdLookupMap = Collections.emptyMap();
            maxTableId = 0;
            collectorLookupMap = Collections.emptyMap();
            return true;
        }

        // TODO - optimize if the MD has not changed?

        final Map<String, TableEntry> slm = new HashMap<>();
        final Map<Long, TableImpl> ilm = new HashMap<>();
        final Map<String, Long> r2nilm = new HashMap<>();
        long maxTid = 0;
        final Map<Long, ThroughputCollector> clm = new HashMap<>();
        final Map<byte[], TableImpl> iblm =
            new TreeMap<>(new IDBytesComparator());

        /* Loop through the top level tables */
        for (Table table : tableMd.getTables().values()) {
            final TableImpl tableImpl = (TableImpl)table;

            /*
             * Add an entry for each table that has indexes somewhere in its
             * hierarchy.
             */
            final TableEntry entry = new TableEntry(tableImpl);

            if (entry.hasSecondaries()) {
                slm.put(tableImpl.getIdString(), entry);
            }

            /*
             * tc is non-null if the table hierarchy has limits. Limits are
             * set on the top level table only.
             */
            ThroughputCollector tc;
            if (tableImpl.hasThroughputLimits() ||
                tableImpl.hasSizeLimit()) {
                tc = collectorLookupMap == null ? null :
                                      collectorLookupMap.get(tableImpl.getId());
                /*
                 * If no collector exists, create a new one. Otherwise
                 * reuse the previous one, updating the table instance.
                 */
                if (tc == null) {
                    tc = new ThroughputCollector(tableImpl,
                                        repNode.getAggrateThroughputTracker());
                } else {
                    tc.updateTable(tableImpl);
                }
            } else {
                tc = null;
            }

            /*
             * The id map has an entry for each table, so descend into its
             * hierarchy.
             */
            maxTid = addToMap(tableImpl, ilm, r2nilm, clm, tc, maxTid);

            /* The ID bytes map has entries only for top-level tables. */
            iblm.put(tableImpl.getIDBytes(), tableImpl);
        }
        secondaryLookupMap = slm;
        idLookupMap = ilm;
        idBytesLookupMap = iblm;
        r2NameIdLookupMap = r2nilm;
        maxTableId = maxTid;
        collectorLookupMap = clm;
        return true;
    }

    private long addToMap(TableImpl tableImpl,
                          Map<Long, TableImpl> map,
                          Map<String, Long> r2Map,
                          Map<Long, ThroughputCollector> clMap,
                          ThroughputCollector tc,
                          long maxTid) {
        map.put(tableImpl.getId(), tableImpl);
        maxTid = Math.max(maxTid, tableImpl.getId());
        if (tc != null) {
            clMap.put(tableImpl.getId(), tc);
        }
        if (tableImpl.isR2compatible()) {
            r2Map.put(tableImpl.getFullName(), tableImpl.getId());
        }
        for (Table child : tableImpl.getChildTables().values()) {
            maxTid = addToMap((TableImpl)child, map, r2Map, clMap, tc, maxTid);
        }
        return maxTid;
    }

    /**
     * Starts the state tracker
     * TODO - Perhaps start the tracker on-demand in noteStateChange()?
     */
    public void startTracker() {
        stateTracker.start();
    }

    /**
     * Notes a state change in the replicated environment. The actual
     * work to change state is made asynchronously to allow a quick return.
     */
    public void noteStateChange(StateChangeEvent stateChangeEvent) {
        stateTracker.noteStateChange(stateChangeEvent);
    }

    @Override
    protected MetadataType getType() {
        return MetadataType.TABLE;
    }

    /**
     * Refreshes the table metadata due to it being updated by the master.
     * Called from the database trigger.
     */
    @Override
    protected synchronized void update(ReplicatedEnvironment repEnv) {
        /*
         * This will force the tableMetadata to be re-read from the db (in
         * updateDBHandles)
         */
        tableMetadata = null;
        updateDbHandles(repEnv);
    }

    /**
     * Note that this method should only be called from the maintenance thread.
     */
    Map<String, SecondaryDatabase> getSecondaryDbMap() {
        return secondaryDbMap;
    }

    /**
     * Gets the throughput collector for the specified table, if the table
     * has throughput limits enforced. If the TableManager has not been
     * initialized or there is no collector for the table, null is returned.
     *
     * @param tableId a table ID
     * @return the throughput collector for the specified table or null
     */
    public ThroughputCollector getThroughputCollector(long tableId) {
        final Map<Long, ThroughputCollector> clm = collectorLookupMap;
        return (clm == null) ? null : clm.get(tableId);
    }

    public ResourceInfo getResourceInfo(long sinceMillis,
                                        Collection<UsageRecord> usageRecords,
                                        RepNodeId repNodeId, int topoSeqNum) {
        final Map<Long, ThroughputCollector> clm = collectorLookupMap;
        if (clm == null) {
            return null;
        }

        /* Reset the previous read/write rates. */
        for (ThroughputCollector tc : clm.values()) {
            tc.resetReadWriteRates();
        }

        /* If limit records were sent, update the TCs */
        if (usageRecords != null) {
            for (UsageRecord ur : usageRecords) {
                final ThroughputCollector tc = clm.get(ur.getTableId());
                if (tc != null) {
                    tc.report(ur.getSize(),
                              ur.getReadRate(),
                              ur.getWriteRate());
                }
            }
        }

        /*
         * If sinceMills is <= 0 return an empty ResourceInfo. We send this
         * instead of null so that the topo sequence number is sent.
         */
        if (sinceMillis <= 0) {
            return new ResourceInfo(repNodeId, topoSeqNum, null);
        }

        /* Get rate information for current tables */

        final Map<Long, TableImpl> map = idLookupMap;
        if (map == null) {
            return null;
        }
        final Set<RateRecord> records = new HashSet<>();

        /* Convert now and since times to seconds */
        final long nowSec = MILLISECONDS.toSeconds(System.currentTimeMillis());
        final long sinceSec = MILLISECONDS.toSeconds(sinceMillis);
        for (TableImpl table : map.values()) {
            /* Only need to get records for the top level tables */
            if (!table.isTop()) {
                continue;
            }
            final ThroughputCollector tc = clm.get(table.getId());
            if (tc != null) {
                tc.collectRateRecords(records, sinceSec, nowSec);
            }
        }
        return new ResourceInfo(repNodeId, topoSeqNum, records);
    }

    /**
     * Returns a set of table info objects. An info object will be included
     * in the set if there has been activity on that table since the last call
     * to getTableInfo. That is, if the table total read or write KBytes is
     * non-zero. If the TableManager has not been initialized, or there
     * are no tables with activity null is returned.
     *
     * Info object are only generated for top level tables.
     *
     * @return a set of table info objects
     */
    public Set<TableInfo> getTableInfo() {
        final Map<Long, TableImpl> ilm = idLookupMap;
        final Map<Long, ThroughputCollector> clm = collectorLookupMap;

        if ((ilm == null) || (clm == null)) {
            return null;
        }
        final long currentTimeMillis = System.currentTimeMillis();
        final Set<TableInfo> infoSet = new HashSet<>();
        for (TableImpl table : ilm.values()) {
            /* Only need to get records for the top level tables */
            if (!table.isTop()) {
                continue;
            }
            final ThroughputCollector tc = clm.get(table.getId());
            if (tc != null) {
                final TableInfo info = tc.getTableInfo(currentTimeMillis);
                if (info != null) {
                    infoSet.add(info);
                }
            }
        }
        return infoSet.isEmpty() ? null : infoSet;
    }
    
    /**
     * Checks if the specified table has any indexes (if indexes != null) or if
     * the table is deleted and needs it data removed. If it has indexes, add
     * them to indexes map. If the table is marked for deletion add that to the
     * deletedTables map.
     *
     * If the table has children, recursively check those.
     */
    static void scanTable(TableImpl table,
                          Map<String, IndexImpl> indexes,
                          Set<TableImpl> deletedTables) {

        if (table.getStatus().isDeleting()) {

            // TODO - should we check for consistency? If so, exception?
            if (!table.getChildTables().isEmpty()) {
                throw new IllegalStateException("Table " + table +
                                                " is deleted but has children");
            }
            if (!table.getIndexes(Index.IndexType.SECONDARY).isEmpty()) {
                throw new IllegalStateException("Table " + table +
                                                " is deleted but has indexes");
            }
            deletedTables.add(table);
            return;
        }
        for (Table child : table.getChildTables().values()) {
            scanTable((TableImpl)child, indexes, deletedTables);
        }

        if (indexes == null) {
            return;
        }
        for (Index index :
            table.getIndexes(Index.IndexType.SECONDARY).values()) {

            indexes.put(createDbName(table.getNamespace(),
                                     index.getName(),
                                     table.getFullName()),
                        (IndexImpl)index);
        }
    }
    
    /**
     * Thread to manage replicated environment state changes.
     */
    private class TableManagerStateTracker extends StateTracker {
        TableManagerStateTracker(Logger logger) {
            super(TableManagerStateTracker.class.getSimpleName(),
                  repNode.getRepNodeId(), logger,
                  repNode.getExceptionHandler());
        }

        @Override
        protected void doNotify(StateChangeEvent sce) {
            logger.log(Level.INFO, "Table manager change state to {0}",
                       sce.getState());

            /**
             * Run the maintenance thread if needed. If the master, then start
             * the maintenance thread in case maintenance operations need to
             * resume. Otherwise, if the thread is already running make a
             * request for update so that the thread notices the state change.
             */
            if (sce.getState().isMaster() || sce.getState().isReplica()) {
                requestMaintenanceUpdate();
            } else {
                shutdownMaintenance();
            }
        }
    }

    /**
     * A container class for quick lookup of secondary DBs.
     */
    private static class TableEntry {
        private final int keySize;
        private final Set<String> secondaries = new HashSet<>();
        private final Map<String, TableEntry> children = new HashMap<>();

        TableEntry(TableImpl table) {
            /* For child tables subtract the key count from parent */
            keySize = (table.getParent() == null ?
                       table.getPrimaryKeySize() :
                       table.getPrimaryKeySize() -
                       ((TableImpl)table.getParent()).getPrimaryKeySize());

            /* For each index, save the secondary DB name */
            for (Index index :
                table.getIndexes(Index.IndexType.SECONDARY).values()) {

                secondaries.add(createDbName(index.getTable().getNamespace(),
                                             index.getName(),
                                             index.getTable().getFullName()));
            }

            /* Add only children which have indexes */
            for (Table child : table.getChildTables().values()) {
                final TableEntry entry = new TableEntry((TableImpl)child);

                if (entry.hasSecondaries()) {
                    children.put(((TableImpl)child).getIdString(), entry);
                }
            }
        }

        private boolean hasSecondaries() {
            return !secondaries.isEmpty() || !children.isEmpty();
        }

        private Collection<String> matchIndexes(BinaryKeyIterator keyIter) {
            /* Match up the primary keys with the input keys, in number only */
            for (int i = 0; i < keySize; i++) {
                /* If the key is short, then punt */
                if (keyIter.atEndOfKey()) {
                    return null;
                }
                keyIter.skip();
            }

            /* If both are done we have a match */
            if (keyIter.atEndOfKey()) {
                return secondaries;
            }

            /* There is another component, check for a child table */
            final String childId = keyIter.next();
            final TableEntry entry = children.get(childId);
            return (entry == null) ? null : entry.matchIndexes(keyIter);
        }
    }

    /**
     * Requests a maintenance update. If a maintenance thread is present
     * MaintenanceThread.requestUpdate() is called. If unsuccessful (the
     * thread is already in shutdown) or there is no maintenance thread,
     * a new maintenance thread is started.
     */
    private void requestMaintenanceUpdate() {
        threadLock.lock();
        try {
            if ((maintenanceThread != null) &&
                maintenanceThread.requestUpdate()) {
                return;
            }
            maintenanceThread = new MaintenanceThread(maintenanceThread,
                                                      this, repNode,
                                                      logger);
            maintenanceThread.start();
        } finally {
            threadLock.unlock();
        }
    }

    /**
     * Shuts down the maintenance thread.
     */
    private void shutdownMaintenance() {
        threadLock.lock();
        try {
            if (maintenanceThread != null) {
                maintenanceThread.shutdownThread(logger);
                maintenanceThread = null;
            }
        } finally {
            threadLock.unlock();
        }
    }

    /**
     * Returns true if there is active table maintenance operations that
     * require migration to be idle.
     * 
     * @return true if there is active table maintenance operations
     */
    public boolean isBusyMaintenance() {
        return isBusyMaintenance;
    }
    
    /**
     * Returns true if there is active table maintenance operations that
     * require migration to be idle. Also returns true if there is pending
     * secondary DB cleaning. 
     *
     * @return true if there is active maintenance or pending secondary
     * DB cleaning
     */
    public boolean busySecondaryCleaning() {
        /* If active maintennce exit early */
        if (isBusyMaintenance) {
            return true;
        }
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        /* If not the right env. we don't know so report busy to be safe */
        if ((repEnv == null) || !repEnv.getState().isMaster()) {
            return true;
        }

        Database infoDb = null;
        try {
            infoDb = SecondaryInfoMap.openDb(repEnv);
            final SecondaryInfoMap infoMap = SecondaryInfoMap.fetch(infoDb);
    
            /* infoMap == null means nothing is going on */
            return (infoMap == null) ? false : infoMap.secondaryNeedsCleaning();
        } catch (RuntimeException re) {
            logger.log(Level.WARNING, "Unexpected exception", re);
            return true;    /* report busy on error */
        } finally {
            if (infoDb != null) {
                TxnUtil.close(logger, infoDb, "secondary info db");
            }
        }
    }
    
    /**
     * Sets is busy maintenance flag to true. This method should be called at
     * the start of the maintenance operation that must be guarded. After
     * returning, the thread should wait until migration operations have
     * ceased. After completing the maintenance operation the thread must
     * call clearBusyMaintenance().
     */
    void setBusyMaintenance() {
        isBusyMaintenance = true;
    }
    
    /**
     * Clears the busy maintenance flag.
     */
    void clearBusyMaintenance() {
        isBusyMaintenance = false;
    }

    public void notifyRemoval(PartitionId partitionId) {
        if (secondaryDbMap.isEmpty()) {
            return;
        }
        logger.log(Level.INFO, "{0} has been removed, removing obsolete " +
                   "records from secondaries", partitionId);

        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return; // TODO - humm - lost info?
        }
        SecondaryInfoMap.markForSecondaryCleaning(repEnv, logger);

        requestMaintenanceUpdate();
    }

    @Override
    public String toString() {
        return "TableManager[" +
               ((tableMetadata == null) ? "-" :
                                          tableMetadata.getSequenceNumber()) +
               ", " + secondaryDbMap.size() + "]";
    }

    /**
     * Comparator for keys that identify a top level table.
     *
     * <p>Assumes that either param may be the input key param, passed to
     * Map.get. This param will be a byte[] for the entire key, and we
     * should only compare the first component, which is the top-level table
     * ID. Therefore we must check the bytes of each param for delimiters,
     * even though keys in the map will not contain delimiters.
     *
     * <p>The ordering is simply a byte-by-byte comparison of the bytes in the
     * table ID. This is not a meaningful ordering, but we only intend the
     * map to be used for key lookups.
     *
     * <p>For use by the TableManager, an alternate implementation would be to
     * use a ThreadLocal to hold the length of the input param, since this is
     * already calculated by {@link #getTable(byte[])}. This would avoid
     * checking for the delimiter in the comparator, but would require
     * accessing a ThreadLocal every time the comparator is called. I suspect
     * the ThreadLocal access is more expensive, since we check for delimiters
     * only once per comparison.
     */
    public static class IDBytesComparator implements Comparator<byte[]> {

        @Override
        public int compare(byte[] o1, byte[] o2) {
            int o1Len = len(o1);
            int o2Len = len(o2);
            int minLen = Math.min(o1Len, o2Len);

            /* Compare bytes prior to the first delimiter. */
            for (int i = 0; i < minLen; i += 1) {
                if (o1[i] != o2[i]) {
                    return o1[i] - o2[i];
                }
            }

            /* The longer key is considered greater. */
            return (o1Len - o2Len);
        }

        private int len(byte[] o) {
            for (int i = 0; i < o.length; ++i) {
                if (Key.isDelimiter(o[i])) {
                    return i;
                }
            }
            return o.length;
        }
    }
}

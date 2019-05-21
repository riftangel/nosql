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

import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;
import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_READ_COMMITTED;
import static oracle.kv.impl.rep.PartitionManager.DB_OPEN_RETRY_MS;
import static oracle.kv.impl.rep.RNTaskCoordinator.KV_INDEX_CREATION_TASK;
import static oracle.kv.impl.rep.table.SecondaryInfoMap.CLEANER_CONFIG;
import static oracle.kv.impl.rep.table.SecondaryInfoMap.SECONDARY_INFO_CONFIG;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.Key;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.ops.MultiDeleteTable;
import oracle.kv.impl.api.ops.MultiDeleteTableHandler;
import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.Scanner;
import oracle.kv.impl.api.ops.TableIterate;
import oracle.kv.impl.api.ops.TableIterateHandler;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.rep.table.SecondaryInfoMap.DeletedTableInfo;
import oracle.kv.impl.rep.table.SecondaryInfoMap.SecondaryInfo;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Table;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.TaskCoordinator;
import com.sleepycat.je.utilint.TaskCoordinator.Permit;

/**
 * Thread to manage table maintenance. When run, the table
 * metadata is scanned looking for indexes. Each index will have a
 * corresponding secondary DB. If the index is new, (and this is the master)
 * a new secondary DB is created.
 *
 * The table MD is also checked for changes, such as indexes dropped and
 * tables that need their data deleted.
 *
 * Once the scan is complete, any new or pending maintenance is done.
 * Maintenance only runs on the master.
 *
 * Note that secondary cleaning activity and partition migration cannot occur
 * concurrently, see [#24245].
 */
public class MaintenanceThread extends StoppableThread {

    private static final int THREAD_SOFT_SHUTDOWN_MS = 10000;

    private static final int NUM_DB_OP_RETRIES = 100;

    /* DB operation delays, in ms */
    private static final long SHORT_RETRY_WAIT_MS = 500L;
    private static final long LONG_RETRY_WAIT_MS = 1000L;
    private static final long VERY_LONG_RETRY_WAIT_MS = 30000L;

    /* Number of records read from the primary during each populate call. */
    public static final int POPULATE_BATCH_SIZE = 100;

    /*
     * Number of records read from a secondary DB partition in a transaction
     * when cleaning after a partition migration.
     */
    private static final int CLEAN_BATCH_SIZE = 100;

    /*
     * Number of records deleted from the partition DB partition in a
     *  transaction during cleaning due to a removed table.
     */
    private static final int DELETE_BATCH_SIZE = 100;

    final TableManager tableManager;
    private final RepNode repNode;
    /* Used to coordinate table maint tasks with other housekeeping tasks */
    private final TaskCoordinator taskCoordinator;
    private final Logger logger;

    private ReplicatedEnvironment repEnv = null;

    private int lastSeqNum = Metadata.UNKNOWN_SEQ_NUM;

    /**
     * True if the thread has been shutdown.  Synchronize when setting this
     * field.
     */
    private volatile boolean isShutdown = false;

    /**
     * True if there has been a request to abandon the current maintenance
     * operation, and perform a new update and associated maintenance.
     * Synchronize when setting this field.
     */
    private volatile boolean updateRequested = false;

    /**
     * The previous maintenance thread, if there was one. This is so that the
     * new thread can wait for the old thread to exit. This avoids requiring
     * callers who start the thread to wait.
     */
    private MaintenanceThread oldThread;
    
    MaintenanceThread(MaintenanceThread oldThread,
                      TableManager tableManager,
                      RepNode repNode,
                      Logger logger) {
        super(null, repNode.getExceptionHandler(), "KV table maintenance");
        this.oldThread = oldThread;
        this.tableManager = tableManager;
        this.repNode = repNode;
        this.taskCoordinator = repNode.getTaskCoordinator();
        this.logger = logger;
    }

    @Override
    public void run() {
        if (isStopped()) {
            return;
        }

        /* Make sure the old thread is stopped */
        if (oldThread != null) {
            assert oldThread.isStopped();
            try {
                /*
                 * Call initiateSoftShutdown() to make sure the thread is
                 * stopped and any waiters are released.
                 */
                oldThread.join(oldThread.initiateSoftShutdown());
            } catch (InterruptedException ex) {
                /* ignore */
            }
            oldThread = null;
        }
        logger.log(Level.INFO, "Starting {0}", this);

        int retryCount = NUM_DB_OP_RETRIES;

        while (!isStopped()) {
            try {
                repEnv = repNode.getEnv(0);
                if (repEnv == null) {
                    retryWait(LONG_RETRY_WAIT_MS);
                    continue;
                }

                final TableMetadata tableMd = tableManager.getTableMetadata();
                if (tableMd == null) {
                    retryWait(LONG_RETRY_WAIT_MS);
                    continue;
                }

                if (update(tableMd) && checkForDone()) {
                    return;
                }
            } catch (DatabaseNotFoundException |
                     ReplicaWriteException |
                     InsufficientAcksException |
                     InsufficientReplicasException |
                     DiskLimitException de) {
                /*
                 * DatabaseNotFoundException and ReplicaWriteException
                 * are likely due to the replica trying to update before
                 * the master has done an initial update.
                 */
                if (!repNode.hasAvailableLogSize()) {
                    /*
                     * Use a longer period when we reaches disk limit exception
                     * (e.g., when InsufficientReplicasException or
                     * DiskLimitException is thrown).
                     */
                    retryWait(retryCount--, VERY_LONG_RETRY_WAIT_MS, de);
                } else {
                    retryWait(retryCount--, LONG_RETRY_WAIT_MS, de);
                }
            } catch (LockConflictException lce) {
                retryWait(retryCount--, SHORT_RETRY_WAIT_MS, lce);
            } catch (InterruptedException ie) {
                /* IE can happen during shutdown */
                if (isStopped()) {
                    logger.log(Level.INFO, "{0} exiting after, {1}",
                               new Object[]{this, ie});
                    return;
                }
                throw new IllegalStateException(ie);
            } catch (RuntimeException re) {
                /*
                 * If shutdown or env is bad just exit.
                 */
                if (isStopped()) {
                    logger.log(Level.INFO, "{0} exiting after, {1}",
                               new Object[]{this, re});
                    return;
                }
                throw re;
            }
        }
    }

    /**
     * Waits for a period of time. Returns if isStopped() is true or the
     * thread is woken up.
     */
    public void retryWait(long waitMS) {
        retryWait(1, waitMS, null);
    }

    /**
     * Waits for a period of time after a DatabaseException. If the count
     * is <= 0, the exception is thrown. Returns if isStopped() is true
     * or the thread is woken up.
     */
    private void retryWait(int count, long waitMS, DatabaseException de) {
        if (isStopped()) {
           return;
        }

        /* If the retry count has expired, re-throw the last exception */
        if (count <= 0) {
            throw de;
        }

        if (de != null) {
            logger.log(Level.INFO,
                       "DB op caused {0}, will retry in {1}ms," +
                       " attempts left: {2}",
                       new Object[]{de, waitMS, count});
        }
        try {
            synchronized (this) {
                wait(waitMS);
            }
        } catch (InterruptedException ie) {
            /* IE may happen during shutdown */
            if (!isStopped()) {
                getLogger().log(Level.SEVERE,
                                "Unexpected exception in {0}: {1}",
                                new Object[]{this, ie});
                throw new IllegalStateException(ie);
            }
        }
    }

    /**
     * If updateRequested is true, there is more work to be done, so clears the
     * flag and returns false unless isShutdown has already been set to true.
     * Otherwise, sets isShutdown to true and returns true so the thread will
     * exit.
     */
    private synchronized boolean checkForDone() {
        if (updateRequested) {
            updateRequested = false;
        } else {
            isShutdown = true;
        }
        return isShutdown;
    }

    /**
     * Returns false if the thread is shutdown.  Otherwise, sets
     * updateRequested to true, which will cause the current maintenance
     * operation to be abandoned and a new update requested, and returns true.
     */
    synchronized boolean requestUpdate() {
        if (isShutdown) {
            return false;
        }
        updateRequested = true;
        notifyAll();
        return true;
    }

    /* -- From StoppableThread -- */

    @Override
    protected synchronized int initiateSoftShutdown() {
        isShutdown = true;
        notifyAll();
        return THREAD_SOFT_SHUTDOWN_MS;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Updates the SecondaryInfoMap with dropped tables and added indexes
     * that require action via maintenance threads to populate indexes or
     * clean out deleted tables.
     *
     * @return true if the update was successful and maintenance complete
     */
    private boolean update(TableMetadata tableMd)
            throws InterruptedException {
        if (isStopped()) {
            return false;
        }
        logger.log(Level.INFO,
                   "Establishing secondary database handles, " +
                   "table seq#: {0}",
                   tableMd.getSequenceNumber());

        Database infoDb = null;
        try {
            infoDb = SecondaryInfoMap.openDb(repEnv);

            /* Update maps if this is new table metadata. */
            if (tableMd.getSequenceNumber() > lastSeqNum) {
                /* Return false if update failed */
                if (!updateInternal(tableMd, infoDb)) {
                    return false;
                }
                lastSeqNum = tableMd.getSequenceNumber();
            }
            doMaintenance(infoDb);
            return true;
        } finally {
            assert !tableManager.isBusyMaintenance();
            if (infoDb != null) {
                TxnUtil.close(logger, infoDb, "secondary info db");
            }
        }
    }

    private boolean updateInternal(TableMetadata tableMd, Database infoDb) {
        /*
         * Map of secondary DB name -> indexes that are defined in the table
         * metadata. This is used to determine what databases to open and if
         * an index has been dropped.
         */
        final Map<String, IndexImpl> indexes = new HashMap<>();

        /*
         * Set of tables which are being removed but first need their data
         * deleted.
         */
        final Set<TableImpl> deletedTables = new HashSet<>();

        for (Table table : tableMd.getTables().values()) {
            TableManager.scanTable((TableImpl)table, indexes, deletedTables);
        }

        if (logger.isLoggable(Level.FINE)) {
            /* Enumerate the indexes and tables for logging purposes */
            final StringBuilder indexesString = new StringBuilder();
            final StringBuilder tablesString = new StringBuilder();
            for (String indexName : indexes.keySet()) {
                if (indexesString.length() > 0) {
                    indexesString.append(" ");
                }
                indexesString.append(indexName);
            }
            for (TableImpl table : deletedTables) {
                if (tablesString.length() > 0) {
                    tablesString.append(" ");
                }
                tablesString.append(table.getNamespaceName());
            }
            logger.log(Level.FINE,
                       "Found {0} indexes({1}) and {2} tables" +
                       " marked for deletion({3}) in {4})",
                       new Object[]{indexes.size(),
                                    indexesString.toString(),
                                    deletedTables.size(),
                                    tablesString.toString(),
                                    tableMd});
        } else if (!deletedTables.isEmpty()) {
            logger.log(Level.INFO, "Found {0} tables" +
                       " marked for deletion in {1})",
                       new Object[]{deletedTables.size(), tableMd});
        }

        /*
         * Update the secondary map with any indexes that have been dropped
         * and removed tables. Changes to the secondary map can only be
         * made by the master. This call will also remove the database.
         * This call will not affect secondaryDbMap which is dealt with
         * below.
         */
        if (repEnv.getState().isMaster()) {
            SecondaryInfoMap.update(tableMd, indexes, deletedTables,
                                    this,  infoDb, repEnv, logger);
        }

        final Map<String, SecondaryDatabase> secondaryDbMap =
                tableManager.getSecondaryDbMap();

        /*
         * Remove entries from secondaryDbMap for dropped indexes.
         * The call to SecondaryInfoMap.check above will close the DB
         * when the master, so this will close the DB on the replica.
         */
        final Iterator<Entry<String, SecondaryDatabase>> itr =
                                       secondaryDbMap.entrySet().iterator();

        while (itr.hasNext()) {
            if (isStopped()) {
                return false;
            }
            final Entry<String, SecondaryDatabase> entry = itr.next();
            final String dbName = entry.getKey();

            if (!indexes.containsKey(dbName)) {
                if (tableManager.closeSecondaryDb(entry.getValue())) {
                    itr.remove();
                }
            }
        }

        int errors = 0;

        /*
         * For each index open its secondary DB
         */
        for (Entry<String, IndexImpl> entry : indexes.entrySet()) {
            if (isStopped()) {
                logger.log(Level.INFO,
                           "Update terminated, established {0} " +
                           "secondary database handles",
                           secondaryDbMap.size());
                return false;
            }

            final String dbName = entry.getKey();
            SecondaryDatabase db = secondaryDbMap.get(dbName);
            try {
                if (DatabaseUtils.needsRefresh(db, repEnv)) {
                    final IndexImpl index = entry.getValue();
                    db = openSecondaryDb(dbName, index, infoDb);
                    assert db != null;
                    setIncrementalPopulation(dbName, db, infoDb);
                    secondaryDbMap.put(dbName, db);
                } else {
                    setIncrementalPopulation(dbName, db, infoDb);
                    updateIndexKeyCreator(db, entry.getValue());
                }
            } catch (RuntimeException re) {

                /*
                 * If a DB was opened, and there was some other error (like
                 * not getting the info record), close and remove the DB
                 * from the map.
                 */
                if (db != null) {
                    secondaryDbMap.remove(dbName);
                    tableManager.closeSecondaryDb(db);
                }

                if (re instanceof IllegalCommandException) {
                    logger.log(Level.WARNING, "Failed to open database " +
                               "for {0}. {1}",
                               new Object[] {dbName, re.getMessage()});
                    continue;
                }
                if (DatabaseUtils.handleException(re, logger, dbName)) {
                    errors++;
                }
            }
        }

        /*
         * If there have been errors return false which will cause the
         * update to be retried after a delay.
         */
        if (errors > 0) {
            logger.log(Level.INFO,
                       "Established {0} secondary database handles, " +
                       "will retry in {1}ms",
                       new Object[] {secondaryDbMap.size(),
                                     DB_OPEN_RETRY_MS});
            retryWait(DB_OPEN_RETRY_MS);
            return false;
        }
        logger.log(Level.INFO, "Established {0} secondary database handles",
                   secondaryDbMap.size());

        return true;   // Success
    }

    /*
     * Update the index in the secondary's key creator.
     */
    private void updateIndexKeyCreator(SecondaryDatabase db,
                                       IndexImpl index) {
        logger.log(Level.FINE,
                   "Updating index metadata for index {0} in table {1}",
                   new Object[]{index.getName(),
                                index.getTable().getFullName()});
        final SecondaryConfig config = db.getConfig();
        final IndexKeyCreator keyCreator = (IndexKeyCreator)
                    (index.isMultiKey() ? config.getMultiKeyCreator() :
                                          config.getKeyCreator());
        assert keyCreator != null;
        keyCreator.setIndex(index);
    }

    /*
     * Set the incremental populate mode on the secondary DB.
     */
    private void setIncrementalPopulation(String dbName,
                                          SecondaryDatabase db,
                                          Database infoDb) {
        final SecondaryInfo info =
                    SecondaryInfoMap.fetch(infoDb).getSecondaryInfo(dbName);

        if (info == null) {
            throw new IllegalStateException("Secondary info record for " +
                                            dbName + " is missing");
        }

        if (info.needsPopulating()) {
            db.startIncrementalPopulation();
        } else {
            db.endIncrementalPopulation();
        }
    }

    /**
     * Opens the specified secondary database.
     */
    private SecondaryDatabase openSecondaryDb(String dbName,
                                              IndexImpl index,
                                              Database infoDb) {
        logger.log(Level.FINE, "Open secondary DB {0}", dbName);

        final IndexKeyCreator keyCreator = new IndexKeyCreator(index);

        /*
         * Use NO_CONSISTENCY so that the handle establishment is not
         * blocked trying to reach consistency particularly when the env is
         * in the unknown state and we want to permit read access.
         */
        final TransactionConfig txnConfig = new TransactionConfig().
           setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        final SecondaryConfig dbConfig = new SecondaryConfig();
        dbConfig.setExtractFromPrimaryKeyOnly(keyCreator.primaryKeyOnly()).
                 setSecondaryAssociation(tableManager).
                 setTransactional(true).
                 setAllowCreate(repEnv.getState().isMaster()).
                 setDuplicateComparator(Key.BytesComparator.class).
                 setSortedDuplicates(true);

        if (keyCreator.isMultiKey()) {
            dbConfig.setMultiKeyCreator(keyCreator);
        } else {
            dbConfig.setKeyCreator(keyCreator);
        }

        Transaction txn = null;
        try {
            txn = repEnv.beginTransaction(null, txnConfig);
            final SecondaryDatabase db =
                  repEnv.openSecondaryDatabase(txn, dbName, null, dbConfig);

            /*
             * If we are the master, add the info record for this secondary.
             */
            if (repEnv.getState().isMaster()) {
                SecondaryInfoMap.add(dbName, infoDb, txn, logger);
            }
            txn.commit();
            txn = null;
            return db;
        } catch (IllegalStateException e) {

            /*
             * The exception was most likely thrown because the environment
             * was closed.  If it was thrown for another reason, though,
             * then invalidate the environment so that the caller will
             * attempt to recover by reopening it.
             */
            if (repEnv.isValid()) {
                EnvironmentFailureException.unexpectedException(
                    DbInternal.getEnvironmentImpl(repEnv), e);
            }
            throw e;

        } finally {
           TxnUtil.abort(txn);
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
        return tableManager.closeSecondary(dbName);
    }

    /**
     * Performs maintenance operations as needed. Does not return until all
     * maintenance is complete, the thread is shutdown, or there has been
     * a request to update.
     */
    private void doMaintenance(Database infoDb) throws InterruptedException {
        if (!repEnv.getState().isMaster()) {
            return;
        }

        while (!exitMaintenance()) {
            final SecondaryInfoMap secondaryInfoMap =
                                        SecondaryInfoMap.fetch(infoDb);
            if (secondaryInfoMap == null) {
                /* No map, no work */
                return;
            }

            if (secondaryInfoMap.secondaryNeedsPopulate()) {
                populateSecondary(infoDb);
            } else if (secondaryInfoMap.tableNeedCleaning()) {
                cleanPrimary(infoDb);
            } else if (secondaryInfoMap.secondaryNeedsCleaning()) {
                cleanSecondary(infoDb);
            } else {
                /* Nothing to do */
                return;
            }
        }
        /*
         * Exiting because exitMaintenance return true. This means the
         * thread is exiting, or we need to return to update.
         */
    }

    /**
     * Returns true if maintenance operations should stop, or the thread
     * is shutdown.
     */
    public boolean exitMaintenance() {
        return updateRequested || isStopped();
    }

    /**
     * Returns true if the thread is shutdown.
     */
    boolean isStopped() {
        return isShutdown || ((repEnv != null) && !repEnv.isValid());
    }

    /**
     * Populates the secondary databases. Secondary DBs (indexes) are populated
     * by scanning through each partition DB. During the scan the secondary
     * DB has Incremental Population set to true to prevent
     * SequenceIntegrityExceptions from being thrown.
     *
     * @throws InterruptedException if the wait for a permit is interrupted
     */
    private void populateSecondary(Database infoDb)
            throws InterruptedException {
        logger.info("Running secondary population");

        final OperationHandler oh =
            new OperationHandler(repNode, tableManager.getParams());
        final TableIterateHandlerInternal opHandler =
            new TableIterateHandlerInternal(oh);
        final RepNodeParams repNodeParams = repNode.getRepNodeParams();
        final long permitTimeoutMs =
            repNodeParams.getPermitTimeoutMs(KV_INDEX_CREATION_TASK);
        final long permitLeaseMs =
            repNodeParams.getPermitLeaseMs(KV_INDEX_CREATION_TASK);

        /*
         * Since we are not using the timeout mechanism in the task coordinator
         * we keep track of the timeout for getting a permit externally.
         */
        final long startMs = System.currentTimeMillis();

        /*
         * We populate secondaries by scanning through one partition at a time.
         * This can be done without interlocking with partition migration
         * until it is time to determine if scanning is done. To know whether
         * we are done, migration target processing needs to be idle so that the
         * list of partitions is stable. The waitForTargetIdle flag indicates
         * if migration needs to be idle and is set when it appears that we
         * are done.
         */
        boolean waitForTargetIdle = false;

        while (!exitMaintenance()) {
            Transaction txn = null;
            /*
             * Attempt to get a work permit without a timeout. If it fails
             * (isDeficit == true) we wait. By waiting we will exit if there
             * is a update.
             */
            try (final Permit permit = taskCoordinator.
                acquirePermit(KV_INDEX_CREATION_TASK,
                              0 /* timeout */,
                              permitLeaseMs,
                              TimeUnit.MILLISECONDS)) {

                /*
                 * If the acquire failed wait a short bit to retry unless we
                 * are past the permit timeout, in which case proceed with the
                 * population.
                 */
                if (permit.isDeficit() &
                    (System.currentTimeMillis() - startMs) < permitTimeoutMs) {
                    permit.releasePermit();
                    retryWait(SHORT_RETRY_WAIT_MS);
                    continue; /* loop back to while (!exitMaintenance()) */
                }

                /*
                 * For all but the last pass through the loop waitForTargetIdle
                 * will be false, letting population proceed without waiting
                 * for migration to idle.
                 */
                if (waitForTargetIdle) {
                    tableManager.setBusyMaintenance();
                    repNode.getMigrationManager().awaitTargetIdle(this);
                    if (exitMaintenance()) {
                        return;
                    }
                    waitForTargetIdle = false;
                }
                txn = repEnv.beginTransaction(null, SECONDARY_INFO_CONFIG);

                final SecondaryInfoMap infoMap =
                        SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);
                final Entry<String, SecondaryInfo> entry =
                        infoMap.getNextSecondaryToPopulate();
                /* If no more, we are finally done */
                if (entry == null) {
                    logger.info("Completed populating secondary " +
                                "database(s)");
                    return;
                }
                final SecondaryInfo info = entry.getValue();
                final String dbName = entry.getKey();


                assert info.needsPopulating();
                logger.log(Level.FINE, "Started populating {0}", dbName);

                /*
                 * If there is no current partition to process, set a new
                 * one. setCurrentPartition() will return false if we should
                 * retry with migration idle. In that case set waitForTargetIdle
                 * and make another pass.
                 */
                if ((info.getCurrentPartition() == null) &&
                    !setCurrentPartition(info)) {
                        waitForTargetIdle = true;
                        continue;
                }

                final SecondaryDatabase db =
                    tableManager.getSecondaryDb(dbName);

                if (info.getCurrentPartition() == null) {
                    logger.log(Level.FINE, "Finished populating {0} {1}",
                               new Object[]{dbName, info});

                    assert db != null;
                    db.endIncrementalPopulation();

                    info.donePopulation();
                    infoMap.persist(infoDb, txn);
                    txn.commit();
                    txn = null;
                    continue;
                }

                /*
                 * db can be null if the update thread has not
                 * yet opened all of the secondary databases.
                 */
                if (db == null) {
                    /*
                     * By updating the last pass time this table will be
                     * pushed to the back of the list (if there are more
                     * than one entry).
                     */
                    info.completePass();
                    infoMap.persist(infoDb, txn);
                    txn.commit();
                    txn = null;
                    continue;
                }
                logger.log(Level.FINE, "Populating {0} {1}",
                           new Object[]{dbName, info});

                final String tableName =
                    TableManager.getTableName(db.getDatabaseName());
                final String namespace =
                    TableManager.getNamespace(db.getDatabaseName());
                final TableImpl table = tableManager.getTable(namespace,
                                                              tableName);

                if (table == null) {
                    final String nsName =
                        TableMetadata.makeNamespaceName(namespace,
                                                        tableName);
                    logger.log(Level.WARNING,
                               "Failed to populate {0}, missing table {1}",
                               new Object[]{info, nsName});
                    return;
                }
                try {
                    final boolean more =
                        populate(table,
                                 opHandler,
                                 txn,
                                 info.getCurrentPartition(),
                                 info.getLastKey());
                    info.completePass();
                    if (!more) {
                            logger.log(Level.FINE, "Finished partition for {0}",
                                   info);
                        info.completeCurrentPartition();
                    }
                    infoMap.persist(infoDb, txn);
                } catch (RNUnavailableException rue) {
                    /*
                     * This can happen if the metadata is updated during
                     * a population. In that case a new DB name can be added
                     * to secondaryLookupMap before the DB is opened and
                     * inserted into secondaryDbMap. Even though this is
                     * populating secondary A, the populate() call will
                     * result in a callback to getSecondaries().
                     * getSecondaries() requires all DBs named in
                     * secondaryLookupMap be open and present in
                     * secondaryDbMap.
                     */
                    logger.log(Level.INFO, "Index population failed " +
                               "on {0}: {1} populate will be retried",
                               new Object[]{dbName, rue.getMessage()});
                    return;
                } catch (IncorrectRoutingException ire) {
                    /*
                     * An IncorrectRoutingException is thrown if the
                     * partition has moved and its DB removed. In this
                     * case we can mark it complete and move on.
                     */
                    handleIncorrectRoutingException(infoMap, info, infoDb, txn);
                } catch (RuntimeException re) {
                    /*
                     * In addition to IncorrectRoutingException, an
                     * IllegalStateException can be thrown if the partition is
                     * moved during population (and its DB closed). The call to
                     * getPartitionDB() will throw IncorrectRoutingException
                     * if that is the case.
                     */
                    boolean exit = true;
                    if (re instanceof IllegalStateException) {
                        try {
                            repNode.getPartitionDB(info.getCurrentPartition());
                        } catch (IncorrectRoutingException ire) {
                            /*
                             * Mark this partition as done, commit that fact
                             * and continue (don't exit).
                            */
                            handleIncorrectRoutingException(infoMap, info,
                                                            infoDb, txn);
                            exit = false;
                        }
                    }

                    if (exit) {
                        info.setErrorString(re.getClass().getSimpleName() +
                                            " " + re.getMessage());
                        try {
                            infoMap.persist(infoDb, txn);
                            txn.commit();
                        } catch (Exception e) {
                            logger.log(Level.WARNING,
                                       "Index population failed, " +
                                       "unable to persist error string: {0}",
                                       e.getMessage());
                        }
                        txn = null;
                        logger.log(Level.WARNING, "Index population failed " +
                                   "on {0}: {1}", new Object[]{dbName, info});
                        /* return normally to move on */
                        return;
                    }
                }
                txn.commit();
                txn = null;
            } finally {
                /* Busy may have been set */
                tableManager.clearBusyMaintenance();
                TxnUtil.abort(txn);
            }
        }
    }

    /**
     * Sets the current partition field in the specified info object. The
     * current partition is the partition that requires maintenance. If
     * no partition is found, the field is not modified. Returns true on
     * success. If false is returned, the call should be retried with
     * migration idle.
     *
     * @return true if the current partition was set
     */
    private boolean setCurrentPartition(SecondaryInfo info) {
        assert info.needsPopulating();
        assert info.getCurrentPartition() == null;

        /* Loop to find a partition that needs processing. */
        for (PartitionId partition : repNode.getPartitions()) {
            if (!info.isCompleted(partition)) {
                info.setCurrentPartition(partition);
                return true;
            }
        }

        /*
         * No partition was found, so it appears that we are done. However,
         * if isBusyMaintenance() == false the list of partitions on this RN
         * may have changed due to migration. In that case we return false to
         * indicate we should check again while migration target processing
         * is idle.
         */
        return tableManager.isBusyMaintenance();
    }

    /**
     * Handle an IncorrectRoutingException caught during secondary population.
     * An IncorrectRoutingException is thrown if the partition has moved and
     * its DB removed. In this case we can mark it complete.
     */
    private void handleIncorrectRoutingException(SecondaryInfoMap infoMap,
                                                 SecondaryInfo info,
                                                 Database infoDb,
                                                 Transaction txn) {
        logger.log(Level.FINE, "Finished partition for {0}, " +
                   "partition no longer in this shard", info);
        info.completeCurrentPartition();
        infoMap.persist(infoDb, txn);
    }

    /**
     * Processes records for the specified table from the specified partition.
     * If the bytes in lastKey is not null then use that to start the
     * iteration. Returns true if there are more records to read from the
     * partition. If true is returned the bytes in lastKey are set to the
     * key of the last record read.
     *
     * @param table the source table
     * @param opHandler the internal operation handler
     * @param txn current transaction
     * @param partitionId the source partition
     * @param lastKey a key to start the iteration on input, set to the
     *                last key read on output
     * @param db the secondary DB to populate
     * @return true if there are more records to process
     * @throws InterruptedException if the wait for a Permit is interrupted
     */
    private boolean populate(TableImpl table,
                             TableIterateHandlerInternal opHandler,
                             Transaction txn,
                             PartitionId partitionId,
                             DatabaseEntry lastKey) {

        final byte[] resumeKey = lastKey.getData();

        final PrimaryKey pkey = table.createPrimaryKey();
        final TableKey tkey = TableKey.createKey(table, pkey, true);

        /* A single target table */
        final TargetTables targetTables = new TargetTables(table, null, null);

        /* Create and execute the iteration */
        final TableIterate op =
            new TableIterate(tkey.getKeyBytes(),
                             targetTables,
                             tkey.getMajorKeyComplete(),
                             POPULATE_BATCH_SIZE,
                             resumeKey);

        /*
         * Set the throughput collector if there is one for this table.
         */
        final ThroughputCollector tc =
                tableManager.getThroughputCollector(table.getId());
        if (tc != null) {
            /* First check to make sure we can write */
            tc.checkAccess(true, true);
            op.setThroughputTracker(tc, Consistency.NONE_REQUIRED);
        }
        return opHandler.populate(op, txn, partitionId, lastKey);
    }

    /*
     * Special internal handler which bypasses security checks.
     */
    private static class TableIterateHandlerInternal
            extends TableIterateHandler {
        TableIterateHandlerInternal(OperationHandler handler) {
            super(handler);
        }

        /*
         * Use CURSOR_DEFAULT so that primary records accessed by the iteration
         * remain locked during the transaction.
         */
        @Override
        protected CursorConfig getCursorConfig() {
            return CURSOR_DEFAULT;
        }

        /*
         * Overriding verifyTableAccess() will bypass keyspace and
         * security checks.
         */
        @Override
        public void verifyTableAccess(TableIterate op) { }

        private boolean populate(TableIterate op,
                                 Transaction txn,
                                 PartitionId partitionId,
                                 DatabaseEntry lastKey) {

            /*
             * Use READ_UNCOMMITTED_ALL and keyOnly to make it inexpensive to
             * skip records that don't match the table. Once they match, lock
             * them, and use them.
             *
             * TODO: can this loop be combined with the loop in TableIterate
             * to share code?
             */
            final OperationTableInfo tableInfo = new OperationTableInfo();
            final Scanner scanner = getScanner(op,
                                               tableInfo,
                                               txn,
                                               partitionId,
                                               CURSOR_READ_COMMITTED,
                                               LockMode.READ_UNCOMMITTED_ALL,
                                               true); // key-only
            boolean moreElements = false;

            try {
                final DatabaseEntry keyEntry = scanner.getKey();
                final DatabaseEntry dataEntry = scanner.getData();

                /* this is used to do a full fetch of data when needed */
                final DatabaseEntry dentry = new DatabaseEntry();
                final Cursor cursor = scanner.getCursor();
                int numElements = 0;
                while ((moreElements = scanner.next()) == true) {

                    final int match = keyInTargetTable(op,
                                                       tableInfo,
                                                       keyEntry,
                                                       dataEntry,
                                                       cursor,
                                                       true /* chargeReadCost */);
                    if (match > 0) {

                        /*
                         * The iteration was done using READ_UNCOMMITTED_ALL
                         * and with the cursor set to getPartial().  It is
                         * necessary to call getLockedData() here to both lock
                         * the record and fetch the data. populateSecondaries()
                         * must be called with a locked record.
                         */
                        if (scanner.getLockedData(dentry)) {

                            if (!isTableData(dentry.getData(), null)) {
                                continue;
                            }

                            final byte[] keyBytes = keyEntry.getData();
                            /* Sets the lastKey return */
                            lastKey.setData(keyBytes);
                            scanner.getDatabase().
                                populateSecondaries(txn, lastKey, dentry,
                                                    scanner.getExpirationTime(),
                                                    null);
                            ++numElements;
                        }
                    } else if (match < 0) {
                        moreElements = false;
                        break;
                    }
                    if (numElements >= POPULATE_BATCH_SIZE) {
                        break;
                    }
                }
            } finally {
                scanner.close();
            }
            return moreElements;
        }
    }

    /**
     * Cleans secondary databases. A secondary needs to be "cleaned"
     * when a partition has moved from this node. In this case secondary
     * records that are from primary records in the moved partition need to be
     * removed the secondary. Cleaning is done by reading each record in a
     * secondary DB and checking whether the primary key is from a missing
     * partition. If so, remove the secondary record. Cleaning needs to happen
     * on every secondary whenever a partition has migrated.
     *
     * The cleaner is also run if a partition migration has failed. This is to
     * remove any secondary records that were generated on the target during
     * the failed migration. Secondary cleaning activity resulting from a
     * failed migration must be completed before the failed migration target
     * can re-start, otherwise the migration may encounter
     * SecondaryIntegrityExceptions when writing a record which already has
     * a (leftover) secondary record for it.  Once a SecondaryInfo cleaning
     * record is added to the SecondaryInfoMap no new migration target will
     * start (see busySecondaryCleaning()).
     * 
     * We are only worried about a migration target restart, if the failed
     * target failed gracefully (vs. the RN crashing) it has written a
     * SecondaryInfo record before it attempts to restart. So no further
     * locking is needed to keep the restart from progressing before the
     * cleaning is done.
     * 
     * Note that the check in busySecondaryCleaning() does not distinguish
     * between secondary cleaning on the target due to a failure or cleaning
     * on the source due to a success. Since it is unlikely for a migration
     * source to also be a target it's not a worth being able to tell the
     * difference. Also, busySecondaryCleaning() only needs to check if the
     * migration target is a restart (i.e. the partition is the same as a
     * failed one) but we do not keep this information.
     */
    private void cleanSecondary(Database infoDb) {
        logger.info("Running secondary cleaning");

        /*
         * The working secondary. Working on one secondary at a time is
         * an optimization in space and time. It reduces the calls to
         * getNextSecondaryToClean() which iterates over the indexes, and
         * makes it so that only one SecondaryInfo is keeping track of
         * the cleaned partitions.
         */
        String currentSecondary = null;

        while (!exitMaintenance()) {
            Transaction txn = null;
            try {
                /*
                 * Note that we do not call setBusyMaintenance() because because
                 * we are only concerned about migration target restart and
                 * that is handled via SecondaryInfo date (see comment above).
                 */
                txn = repEnv.beginTransaction(null, CLEANER_CONFIG);

                final SecondaryInfoMap infoMap =
                    SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                if (currentSecondary == null) {
                    currentSecondary = infoMap.getNextSecondaryToClean();

                    if (currentSecondary != null) {
                        logger.log(Level.INFO, "Cleaning {0}",
                                   currentSecondary);
                    }
                }

                /* If no more, we are finally done */
                if (currentSecondary == null) {
                    logger.info("Completed cleaning secondary database(s)");
                    return;
                }

                final SecondaryInfo info =
                    infoMap.getSecondaryInfo(currentSecondary);
                assert info != null;
                assert info.needsCleaning();

                final SecondaryDatabase db =
                    tableManager.getSecondaryDb(currentSecondary);
                /*
                 * We have seen missing DB in a stress test. By throwing
                 * DNFE the thread will retry after a delay. If the DB
                 * was not yet initialized the delay may allow the DB to
                 * be opened/created. If that is not the problem, the retry
                 * loop will eventually throw the DNFE out causing the RN to
                 * restart. That should fix things since the problem here
                 * is between the secondary info and the table metadata.
                 * On restart the secondary info will be updated from the
                 * metadata before the maintenance thread is started.
                 */
                if (db == null) {
                    throw new DatabaseNotFoundException(
                                    "Failed to clean " + currentSecondary +
                                    ", the secondary database was not" +
                                    " found");
                }

                if (!db.deleteObsoletePrimaryKeys(info.getLastKey(),
                                                  info.getLastData(),
                                                  CLEAN_BATCH_SIZE)) {
                    logger.log(Level.INFO, "Completed cleaning {0}",
                               currentSecondary);
                    info.doneCleaning();
                    currentSecondary = null;
                }
                infoMap.persist(infoDb, txn);
                txn.commit();
                txn = null;
            } finally {
                TxnUtil.abort(txn);
            }
        }
    }

    /**
     * Cleans primary records. This will remove primary records associated
     * with a table which has been marked for deletion.
     */
    private void cleanPrimary(Database infoDb) throws InterruptedException {
        
        /*
         * There is a race condition which can lead to records from a dropped
         * table to get left in the store. What can happen is:
         *  1) a write operation checks whether the table exist by calling
         *     getTable() which returns true
         *  2) a metadata update comes in indicating that the table is dropped
         *  3) cleaning starts
         *  4) the operation completes
         * 
         * If the record written by the operation is behind the cleaning
         * scan (lower key), it will not be cleaned and remain in the store
         * forever. The fix is to keep track of the number of active
         * operations per table metadata sequence #, and then wait for any
         * operation using an earlier metadata to complete.
         */
        final int opsWaitedOn = repNode.awaitTableOps(lastSeqNum); 
        logger.log(Level.INFO,
                   "Running primary cleaning, waited on {0} ops to complete",
                   opsWaitedOn);

        /*
         * We can only use record extinction if all nodes in the group have
         * been upgraded.
         *
         * If primary cleaning is interrupted before finishing and then resumes
         * after a shard is upgraded, we will switch approaches from deleting
         * records to using record extinction. That is OK.
         */
        if (repEnv.isRecordExtinctionAvailable()) {
            cleanPrimaryRecordExtinction(infoDb);
        } else {
            cleanPrimaryDeleteRecords(infoDb);
        }
    }

    /**
     * Cleans primary records by using record extinction. Record deletions
     * are not logged, so this operation can be performed in a short
     * transaction. JE asynchronously discards the extinct records.
     *
     * Note that this operation does not require interlocking with partition
     * migration.
     */
    private void cleanPrimaryRecordExtinction(Database infoDb) {
        int retryCount = 10;
        while (!exitMaintenance()) {
            Transaction txn = null;
            try {
                if (exitMaintenance()) {
                    return;
                }

                txn = repEnv.beginTransaction(null, CLEANER_CONFIG);

                final SecondaryInfoMap infoMap =
                    SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                final DeletedTableInfo info = infoMap.getNextTableToClean();
                if (info == null) {
                    logger.info("Completed cleaning all tables");
                    return;
                }
                assert !info.isDone();

                final Set<String> dbNames = getPartitionDbNames(info);
                if (!dbNames.isEmpty()) {
                    final TableImpl table =
                        tableManager.getTableInternal(info.getTargetTableId());
                    assert table.isDeleting();

                    logger.log(Level.INFO,
                               "Discarding table record(s) for {0} {1}",
                               new Object[] {info.getTargetTableId(),
                                             table.getFullName()});

                    Scanner.discardTableRecords(repEnv, txn, dbNames, table);
                }
                info.setDone();
                infoMap.persist(infoDb, txn);
                txn.commit();
                txn = null;
                retryCount = 10;
            } catch (DatabaseNotFoundException dnfe) {
                /*
                 * This can happen due to async partition migration creating
                 * and removing target partition DBs. In this case we simply
                 * retry.
                 */
                logger.log(Level.INFO,
                           "Partition database not found during " +
                           "cleaning, will retry in {0} ms: {0}",
                           new Object[] {LONG_RETRY_WAIT_MS,
                                         dnfe.getMessage()});
                retryWait(retryCount--, LONG_RETRY_WAIT_MS, dnfe);
            } finally {
                TxnUtil.abort(txn);
            }
        }
    }

    /**
     * Returns the set of partition DB names that need to be cleaned.
     */
    private Set<String> getPartitionDbNames(DeletedTableInfo info) {
        final Set<String> names = new HashSet<>();
        /*
         * Start by getting the names of any partitions being migrated to this
         * node. We then get the partitions known to the RN. This order is
         * important to make sure we get all of the DBs. If the order was
         * reversed, a target migration could end after getting the RN's
         * names and before we query the migration targets and that DB would
         * be missed.
         * It is OK for a migration target to start after this point because
         * the records from any dropped table are being filtered out (see
         * MigrationTarget).
         */
        repNode.getMigrationManager().getTargetPartitionDbNames(names);

        for (PartitionId partition : repNode.getPartitions()) {
            if (info.isCompleted(partition)) {
                /* Should only happen if we mix approaches. */
                continue;
            }
            names.add(partition.getPartitionName());
        }
        return names;
    }

    /**
     * Cleans primary records by deleting individual records from the partition
     * databases.
     *
     * Primary cleaning must be interlocked with partition migration on
     * both the source and target.  On the target, we need to make sure
     * we clean the incoming partition since it is not possible to prevent
     * "dirty" records from being migrated. If cleaning is in progress
     * and a migration occurs the moved partition needs to be cleaned once
     * movement is complete. In order to do this, the table being deleted
     * needs to remain in the metadata.  To prevent this we lock on the
     * source as well. As long as cleaning is blocked on the source the
     * metadata will not be updated (cleaning will not be complete
     * on the source).
     */
    private void cleanPrimaryDeleteRecords(Database infoDb)
            throws InterruptedException {
        final OperationHandler oh =
            new OperationHandler(repNode, tableManager.getParams());
        final MultiDeleteTableHandlerInternal opHandler =
            new MultiDeleteTableHandlerInternal(oh);

        MigrationStreamHandle streamHandle = null;

        while (!exitMaintenance()) {
            Transaction txn = null;
            try {
                /*
                 * We must make sure that there are no migrations in-progress
                 * to this node.
                 */
                tableManager.setBusyMaintenance();
                repNode.getMigrationManager().awaitIdle(this);
                if (exitMaintenance()) {
                    return;
                }

                txn = repEnv.beginTransaction(null, CLEANER_CONFIG);

                final SecondaryInfoMap infoMap =
                    SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                final DeletedTableInfo info = infoMap.getNextTableToClean();

                /* If no more, we are finally done */
                if (info == null) {
                    logger.info("Completed cleaning partition database(s)" +
                                " for all tables");
                    return;
                }

                assert !info.isDone();
                if (info.getCurrentPartition() == null) {
                    for (PartitionId partition : repNode.getPartitions()) {
                        if (!info.isCompleted(partition)) {
                            info.setCurrentPartition(partition);
                            break;
                        }
                    }
                }

                final PartitionId currentPartition =
                                                info.getCurrentPartition();
                if (currentPartition == null) {
                        logger.log(Level.FINE,
                               "Completed cleaning partition database(s) " +
                               "for {0}", info.getTargetTableId());
                    info.setDone();
                } else {
                    streamHandle =
                          MigrationStreamHandle.initialize(repNode,
                                                           currentPartition,
                                                           txn);
                    // delete some...
                    if (deleteABlock(info, opHandler, txn)) {
                            logger.log(Level.FINE,
                                   "Completed cleaning {0} for {1}",
                                   new Object[] {currentPartition,
                                                 info.getTargetTableId()});
                        // Done with this partition
                        info.completeCurrentPartition();
                    }
                    info.completePass();
                }

                infoMap.persist(infoDb, txn);
                if (streamHandle != null) {
                    streamHandle.prepare();
                }
                txn.commit();
                txn = null;
            } finally {
                tableManager.clearBusyMaintenance();
                TxnUtil.abort(txn);
                if (streamHandle != null) {
                    streamHandle.done();
                }
            }
        }
    }

    /*
     * Deletes up to DELETE_BATCH_SIZE number of records from some
     * primary (partition) database.
     */
    private boolean deleteABlock(DeletedTableInfo info,
                                 MultiDeleteTableHandlerInternal opHandler,
                                 Transaction txn) {
        final MultiDeleteTable op =
            new MultiDeleteTable(info.getParentKeyBytes(),
                                 info.getTargetTableId(),
                                 info.getMajorPathComplete(),
                                 DELETE_BATCH_SIZE,
                                 info.getCurrentKeyBytes());

        final PartitionId currentPartition = info.getCurrentPartition();
        final Result result;
        try {
            result = opHandler.execute(op, txn, currentPartition);
        } catch (IllegalStateException ise) {
            /*
             * This can occur if a partition has moved while being cleaned.
             * We can tell by checking whether the partition is in the
             * local topology. We need to check the local topology because
             * a migration may be in the transfer of ownership protocol
             * (see MigrationSource) and the partition is thought to still
             * be here by the "official" topo.
             */
            final Topology topo = repNode.getLocalTopology();
            if ((topo != null) &&
                (topo.getPartitionMap().get(currentPartition) == null)) {
                /* partition moved, so done */
                logger.log(Level.INFO,
                           "Cleaning of partition {0} ended, partition " +
                           "has moved", currentPartition);
                info.setCurrentKeyBytes(null);
                return true;
            }
            throw ise;
        }
        final int num = result.getNDeletions();
        logger.log(Level.FINE, "Deleted {0} records in partition {1}{2}",
                   new Object[]{num, currentPartition,
                                (num < DELETE_BATCH_SIZE ?
                                 ", partition is complete" : "")});
        if (num < DELETE_BATCH_SIZE) {
            /* done with this partition */
            info.setCurrentKeyBytes(null);
            return true;
        }
        info.setCurrentKeyBytes(op.getLastDeleted());
        return false;
    }

    /*
     * Special internal OP which bypasses security checks and deleting
     * table check.
     */
    private class MultiDeleteTableHandlerInternal
            extends MultiDeleteTableHandler {
        MultiDeleteTableHandlerInternal(OperationHandler handler) {
            super(handler);
        }

        /*
         * Overriding verifyTableAccess() will bypass keyspace and
         * security checks.
         */
        @Override
        public void verifyTableAccess(MultiDeleteTable op) { }
        
        /*
         * Override getAndCheckTable() to allow return of deleting tables.
         */
        @Override
        protected TableImpl getAndCheckTable(final long tableId) {
            final TableImpl table = tableManager.getTableInternal(tableId);
            /* TODO - if table is null there is some internal error ??? */
            assert table != null;
            return table;
        }
    }
}

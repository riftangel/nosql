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

package oracle.kv.impl.rep.stats;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.RNTaskCoordinator;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.stats.IndexLeaseManager.IndexLeaseInfo;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.systables.TableStatsIndexDesc;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.utilint.TaskCoordinator.Permit;

/**
 * The class is to scan secondary database to get index key statistics and
 * store the scanning results into statistics tables
 *
 */
class TableIndexScan extends StatsScan<IndexLeaseInfo> {

    private final String namespace;
    private final String tableName;
    private final String indexName;
    private final int groupId;    
    private TableImpl indexStatsTable;
    private long count = 0;
    private long keyTotalSize = 0;
    private long indexSize = 0L;
    private TableImpl target = null;

    private static final long GET_ENV_TIMEOUT = 5000;
    private Database indexDB = null;

    /* The keys to record the last read one. They are used as a resume key */
    private byte[] resumeSecondaryKey;
    private byte[] resumePrimaryKey;

    /* This hook affects before re-open database */
    static TestHook<Integer> BEFORE_OPEN_HOOK;

    /* This hook affects after re-open database */
    static TestHook<Integer> AFTER_OPEN_HOOK;

    TableIndexScan(TableAPI tableAPI,
                   Table table,
                   String indexName,
                   RepNode repNode,
                   StatsLeaseManager<IndexLeaseInfo> leaseManager,
                   IndexLeaseInfo leaseInfo,
                   long scanInterval,
                   TimeToLive ttl,
                   Logger logger) {
        super(repNode, tableAPI, leaseManager, leaseInfo,
              scanInterval, ttl, logger);
        namespace = table.getNamespace();
        tableName = table.getFullName();
        this.indexName = indexName;
        this.groupId = repNode.getRepNodeId().getGroupId();
    }

    @Override
    boolean checkStatsTable(TableMetadata md) {
        if (indexStatsTable != null) {
            return true;
        }

        indexStatsTable = md.getTable(null, TableStatsIndexDesc.TABLE_NAME);
        if (indexStatsTable == null) {
            /* Table does not exist, stop to gather statistics info */
            return false;
        }

        return true;
    }

    @Override
    void accumulateResult(byte[] indexKey, Cursor cursor) {
        /* Aggregate the scanned results */
        keyTotalSize += indexKey.length;
        count++;

        // TODO - should we just always collect the size??
        if (target.hasSizeLimit()) {
            /*
             * getStorageSize returns the estimated disk storage size for the
             * record at the current position.
             */
            indexSize += DbInternal.getCursorImpl(cursor).getStorageSize();
        }
    }

    @Override
    void wrapResult() {
        /* Add scanning results into a cache list */
        /* Wrap Table statistics as the rows of table TableStatsIndex */
        final Row row = indexStatsTable.createRow();
        row.setTTL(ttl);

        row.put(TableStatsIndexDesc.COL_NAME_TABLE_NAME,
                TableMetadata.makeNamespaceName(namespace, tableName));
        row.put(TableStatsIndexDesc.COL_NAME_INDEX_NAME, indexName);
        row.put(TableStatsIndexDesc.COL_NAME_SHARD_ID, groupId);
        row.put(TableStatsIndexDesc.COL_NAME_COUNT, count);
        row.put(TableStatsIndexDesc.COL_NAME_AVG_KEY_SIZE, count != 0?
                (int)(keyTotalSize/count) : 0);
        row.put(TableStatsIndexDesc.COL_NAME_INDEX_SIZE, indexSize);
        addRow(row);
    }

    /**
     * Get secondary database. To avoid Incremental population is currently
     * enabled exception, it does not get secondary database from table manager.
     * Instead, it opens secondary database as database and open it via calling
     * Environment.openDatabase.
     */
    @Override
    Database getDatabase() {

        final ReplicatedEnvironment repEnv = repNode.getEnv(GET_ENV_TIMEOUT);
        if (repEnv == null) {
            throw new IllegalStateException("Cannot open index DB for index " +
                    indexName + ": ReplicatedEnvironment is null");
        }

        /* Create index DB name */
        final String databaseName = TableManager.createDbName(namespace,
                                                              indexName,
                                                              tableName);

        final TransactionConfig dbTxnConfig = new TransactionConfig().
            setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true).
                    setAllowCreate(false).
                    setSortedDuplicates(true).
                    setReadOnly(true);

        /* Open secondary database as normal database */
        Transaction txn = null;

        TestHookExecute.doHookIfSet(BEFORE_OPEN_HOOK, 1);
        try {
             txn = repEnv.beginTransaction(null, dbTxnConfig);
             indexDB = repEnv.openDatabase(txn, databaseName, dbConfig);
             txn.commit();
             txn = null;
        } catch (IllegalStateException e) {
             throw e;
        } finally {
            TxnUtil.abort(txn);
        }

        TestHookExecute.doHookIfSet(AFTER_OPEN_HOOK, 1);

        if (indexDB == null) {
            throw new IllegalStateException("Missing index DB for index " +
                                            indexName);
        }

        return indexDB;
    }

    @Override
    boolean preScan() {
        count = 0;
        keyTotalSize = 0;
        resumeSecondaryKey = null;
        resumePrimaryKey = null;

        final TableMetadata metadata =
                (TableMetadata)repNode.getMetadata(MetadataType.TABLE);
        if (metadata == null) {
            return false;
        }

        target = metadata.getTable(namespace, tableName);

        if (target == null) {
            return false;
        }

        return true;
    }

    @Override
    void postScan(boolean scanCompleted) {
        /* close the open database */
        if (indexDB != null) {

            final Environment env = indexDB.getEnvironment();

            if ((env == null) || !env.isValid()) {
                return;
            }
            TxnUtil.close(logger, indexDB, "secondary");
        }
    }

    @Override
    boolean scanDatabase(Environment env, Database db)
        throws InterruptedException {
        Cursor cursor = null;
        Transaction txn = null;
        final RepNodeParams repNodeParams = repNode.getRepNodeParams();
        final long permitTimeoutMs =
            repNodeParams.getPermitTimeoutMs(RNTaskCoordinator.KV_STORAGE_STATS_TASK);
        final long permitLeaseMs =
            repNodeParams.getPermitLeaseMs(RNTaskCoordinator.KV_STORAGE_STATS_TASK);
        /*
         * Acquire a permit before scanning each batch. If permits are in short
         * supply the permit may be a deficit permit, but we choose not to act
         * on it for now to keep things simple.
         */
        try (final Permit permit = repNode.getTaskCoordinator().
             acquirePermit(RNTaskCoordinator.KV_STORAGE_STATS_TASK,
                          permitTimeoutMs, permitLeaseMs, TimeUnit.MILLISECONDS)) {
            txn = env.beginTransaction(null, txnConfig);
            txn.setTxnTimeout(TXN_TIME_OUT, TimeUnit.MILLISECONDS);

            int nRecords = 0;
            cursor = db.openCursor(txn, cursorConfig);
            cursor.setCacheMode(CacheMode.UNCHANGED);

            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry dataEntry = new DatabaseEntry();
            OperationStatus status;

            if (resumeSecondaryKey == null) {
                status = cursor.getNext(keyEntry, dataEntry,
                                        LockMode.READ_UNCOMMITTED);
            } else {
                keyEntry.setData(resumeSecondaryKey);
                dataEntry.setData(resumePrimaryKey);
                status = cursor.getSearchBothRange(keyEntry, dataEntry,
                                                   LockMode.READ_UNCOMMITTED);
                if (status == OperationStatus.SUCCESS &&
                        Arrays.equals(resumeSecondaryKey, keyEntry.getData()) &&
                        Arrays.equals(resumePrimaryKey, dataEntry.getData())) {
                    status = cursor.getNext(keyEntry, dataEntry,
                                            LockMode.READ_UNCOMMITTED);
                }
            }

            if (status != OperationStatus.SUCCESS) {
                return false;
            }

            boolean hasMoreElement = false;
            while (status == OperationStatus.SUCCESS && !stop) {
                /* Record the latest keys as a resume keys */
                resumeSecondaryKey = keyEntry.getData();
                resumePrimaryKey = dataEntry.getData();

                /* Accumulate the key into results */
                accumulateResult(resumeSecondaryKey, cursor);
                nRecords++;

                if (nRecords >= BATCH_SIZE) {
                    hasMoreElement = true;
                    break;
                }
                status = cursor.getNext(keyEntry, dataEntry,
                                        LockMode.READ_UNCOMMITTED);
            }
            totalRecords += nRecords;
            return hasMoreElement;
        } catch (DatabaseException | IllegalArgumentException e) {
            logger.log(Level.FINE, "Scanning encounters exception: {0}, " +
                       "iteration scanning exits", e);
        } finally {
            if (cursor != null) {
                TxnUtil.close(cursor);
            }

            /* We are just reading. Abort every transaction */
            TxnUtil.abort(txn);
        }

        return false;
    }
}

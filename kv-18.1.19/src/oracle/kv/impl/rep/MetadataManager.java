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

package oracle.kv.impl.rep;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.fault.DatabaseNotReadyException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.trigger.ReplicatedDatabaseTrigger;
import com.sleepycat.je.trigger.TransactionTrigger;
import com.sleepycat.je.trigger.Trigger;

/**
 * Base class for objects which manage metadata. The class provides basic
 * database operations for the metadata as well as a facility for replicas to
 * maintain up-to-date metadata as it is updated on the master.
 */
public abstract class MetadataManager
                                <T extends Metadata<? extends MetadataInfo>>
                    implements TransactionTrigger, ReplicatedDatabaseTrigger {

    /*
     * Transaction config for metadata reads.
     * Note: the transaction will not wait for commit acks from the replicas.
     */
    private static final TransactionConfig NO_WAIT_CONFIG =
        new TransactionConfig().
            setDurability(new Durability(Durability.SyncPolicy.NO_SYNC,
                                         Durability.SyncPolicy.NO_SYNC,
                                         Durability.ReplicaAckPolicy.NONE)).
            setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

    /*
     * Transaction config for metadata writes.
     */
    private static final TransactionConfig SYNC_SYNC_CONFIG =
        new TransactionConfig().setDurability(
                  new Durability(Durability.SyncPolicy.SYNC,
                                 Durability.SyncPolicy.SYNC,
                                 Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

    /* Delay between DB open attempts */
    private static final int DB_OPEN_RETRY_MS = 1000;

    /* Number of times a DB operation is retried before giving up */
    private static final int NUM_DB_OP_RETRIES = 100;

    /* DB operation delays */
    private static final long SHORT_RETRY_TIME = 500;

    private static final long LONG_RETRY_TIME = 1000;

    private final DatabaseEntry metadataKey = new DatabaseEntry();

    protected final RepNode repNode;

    protected final Logger logger;

    protected volatile boolean shutdown = false;

    /* Handle to the metadata DB */
    private volatile Database metadataDatabase;

    protected MetadataManager(RepNode repNode, Params params) {
        if (repNode == null) {
            throw new NullPointerException("repNode cannot be null");
        }
        this.repNode = repNode;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        StringBinding.stringToEntry(getType().getKey(), metadataKey);
    }

    protected MetadataManager(RepNode repNode, Logger logger) {
        if (repNode == null) {
            throw new NullPointerException("repNode cannot be null");
        }
        this.repNode = repNode;
        this.logger = logger;
        StringBinding.stringToEntry(getType().getKey(), metadataKey);
    }

    /**
     * Gets the metadata type managed by this manager.
     *
     * @return the metadata type
     */
    abstract protected MetadataType getType();

    /**
     * Called when the metadata has been modified and committed by a master.
     * The implementation should update any in-memory structures.
     *
     * @param repEnv the replicated environment handle
     */
    protected abstract void update(ReplicatedEnvironment repEnv);

    /**
     * Updates the metadata database handle. Returns true if the handle
     * needed to be updated.
     *
     * @param repEnv the replicated environment handle
     * @return true if the handle needed to be updated
     */
    protected synchronized boolean updateDbHandles(
                                            ReplicatedEnvironment repEnv) {
        final boolean refresh = DatabaseUtils.needsRefresh(metadataDatabase,
                                                           repEnv);
        if (refresh) {
            closeMetadataDb();
        }
        openMetadataDb(repEnv);
        return refresh;
    }

    /**
     * Gets the metadata database handle. If the database is not open, it
     * will attempt to open the database. If the open fails, null is returned.
     *
     * @return the metadata database handle or null
     */
    protected synchronized Database getMetadataDatabase() {
        if (metadataDatabase == null) {
            openMetadataDb(repNode.getEnv(1));
        }
        return metadataDatabase;
    }

    /**
     * Closes the metadata database handle.
     */
    protected synchronized void closeDbHandles() {
        closeMetadataDb();
    }

    /**
     * Shuts down this manager.
     */
    protected void shutdown() {
        shutdown = true;
    }

    /**
     * Fetches the metadata object from the db for read-only use. If none is in
     * the db, or the db is not yet opened, an empty object is returned.
     *
     * @return the metadata object
     * @throws DatabaseNotReadyException if the metadata db is not opened yet.
     */
    @SuppressWarnings("unchecked")
    protected T fetchMetadata() {
        if (metadataDatabase == null) {
            throw new DatabaseNotReadyException(
                getType() + " database is not ready.");
        }
        final Transaction txn = metadataDatabase.getEnvironment().
                                  beginTransaction(null, NO_WAIT_CONFIG);
        try {
            final DatabaseEntry value = new DatabaseEntry();

            metadataDatabase.get(txn, metadataKey,
                                 value, LockMode.READ_COMMITTED);
            return (T)SerializationUtil.getObject(value.getData(),
                                                  Metadata.class);
        } finally {

            /* We are just reading. */
            if (txn.isValid()) {
                txn.commit();
            } else {
                TxnUtil.abort(txn);
            }
        }
    }

    /**
     * Persists the specified metadata into the metadata DB of this RepNode.
     * The operation is retried if possible. The metadata object is pruned
     * before it is persisted.
     *
     * @param metadata a metadata instance
     * @param maxChanges the maximum trailing changes to be retained
     *
     * @return true if the operation was successful, or false if in shutdown
     */
    protected boolean persistMetadata(final T metadata, int maxChanges) {
        metadata.pruneChanges(Integer.MAX_VALUE, maxChanges);
        final Boolean success  =
            tryDBOperation(new DBOperation<Boolean>() {

            final DatabaseEntry data = new DatabaseEntry(
                                        SerializationUtil.getBytes(metadata));
            @Override
            public Boolean call(Database db) {

                Transaction txn = null;
                try {
                    txn = db.getEnvironment().
                                  beginTransaction(null, SYNC_SYNC_CONFIG);

                    /*
                     * Don't store an older seq num. This means reading the
                     * existing metadata first
                     */
                    final DatabaseEntry value = new DatabaseEntry();

                    metadataDatabase.get(txn, metadataKey,
                                         value, LockMode.RMW);
                    @SuppressWarnings("unchecked")
                    T md = (T)SerializationUtil.getObject(value.getData(),
                                                          Metadata.class);
                    if (md != null &&
                        (md.getSequenceNumber() >
                         metadata.getSequenceNumber())) {
                        logger.log(Level.INFO, "Metadata not stored. Current " +
                                   "sequence number {0} is > new sequence " +
                                   "number {1} type: {2}",
                                   new Object[]{md.getSequenceNumber(),
                                                metadata.getSequenceNumber(),
                                                metadata.getType()});
                        return true;
                    }

                    db.put(txn, metadataKey, data);
                    txn.commit();
                    txn = null;
                    return true;
                } finally {
                    TxnUtil.abort(txn);
                }
            }
        });

        if ((success == null) || !success) {
            /* Shutdown */
            return false;
        }

        logger.log(Level.INFO, "Metadata stored type: {0}, seq#: {1}",
                   new Object[]{metadata.getType(),
                                metadata.getSequenceNumber()});
        return true;
    }

    private void openMetadataDb(ReplicatedEnvironment repEnv) {
        assert Thread.holdsLock(this);

        if (repEnv == null) {
            return;
        }
        while ((metadataDatabase == null) && !shutdown && repEnv.isValid()) {

            final DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true).
                     setTransactional(true).
                     getTriggers().add(this);

            try {
                metadataDatabase = openDb(repEnv, dbConfig);
                assert metadataDatabase != null;
                logger.log(Level.INFO, "Open {0} DB", getDBName());
                return;
            } catch (RuntimeException re) {
                if (!DatabaseUtils.handleException(re, logger, getDBName())){
                    return;
                }
            }

            /* Wait to retry */
            try {
                Thread.sleep(DB_OPEN_RETRY_MS);
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }

    private String getDBName() {
        return getType().getKey() + "Metadata";
    }

    private Database openDb(Environment env, DatabaseConfig dbConfig) {

        final TransactionConfig txnConfig = new TransactionConfig().
              setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        Transaction txn = null;
        Database db = null;
        try {
            txn = env.beginTransaction(null, txnConfig);
            db = env.openDatabase(txn, getDBName(), dbConfig);
            txn.commit();
            txn = null;
            final Database ret = db;
            db = null;
            return ret;
        } finally {
            TxnUtil.abort(txn);

            if (db != null) {
                try {
                    db.close();
                } catch (DatabaseException de) {
                    /* Ignore */
                }
            }
        }
    }

    private synchronized void closeMetadataDb() {
        if (metadataDatabase == null) {
            return;
        }
        logger.log(Level.INFO, "Closing {0} db", getDBName());

        TxnUtil.close(logger, metadataDatabase, getDBName());

        metadataDatabase = null;
    }

    /**
     * Executes the operation, retrying if necessary based on the type of
     * exception. The operation will be retried until 1) success, 2) shutdown,
     * or 3) the maximum number of retries has been reached.
     *
     * The return value is the value returned by op.call() or null if shutdown
     * occurs during retry or retry has been exhausted.
     *
     * If retryIAE is true and the operation throws an
     * InsufficientAcksException, the operation will be retried, otherwise the
     * exception is re-thrown.
     *
     * @param <R> type of the return value
     * @param op the operation
     * @return the value returned by op.call() or null
     */
    protected <R> R tryDBOperation(DBOperation<R> op) {
        int retryCount = NUM_DB_OP_RETRIES;

        while (!shutdown) {
            try {
                final Database db = getMetadataDatabase();

                if (db != null) {
                    return op.call(db);
                }
                if (retryCount <= 0) {
                    return null;
                }
                retrySleep(retryCount, LONG_RETRY_TIME, null);
            } catch (InsufficientAcksException |
                    InsufficientReplicasException e) {
                retrySleep(retryCount, LONG_RETRY_TIME, e);
            } catch (DiskLimitException dle) {
                /*
                 * Not retry, simply exit since the problem will not be
                 * resolved very quickly. Update operations will happen
                 * periodically.
                 */
                break;
            } catch (LockConflictException lce) {
                retrySleep(retryCount, SHORT_RETRY_TIME, lce);
            }
            retryCount--;
        }
        return null;
    }

    private void retrySleep(int count, long sleepTime, DatabaseException de) {
        logger.log(Level.FINE, "DB op caused {0} attempts left {1}",
                   new Object[]{de, count});

        /* If the cound has expired, re-throw the last exception */
        if (count <= 0) {
            throw de;
        }

        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            /* Should not happen. */
            throw new IllegalStateException(ie);
        }
    }

    /**
     * A database operation that returns a result and may throw an exception.
     *
     * @param <V>
     */
    public static interface DBOperation<V> {

        /**
         * Invokes the operation. This method may be called multiple times
         * in the course of retrying in the face of failures.
         *
         * @param db the migration db
         * @return the result
         */
        V call(Database db);
    }

    /* -- From CompletionTrigger -- */

    private String dbName;

    @Override
    public void repeatTransaction(Transaction t) {}

    @Override
    public void repeatAddTrigger(Transaction t) {}

    @Override
    public void repeatRemoveTrigger(Transaction t) {}

    @Override
    public void repeatCreate(Transaction t) {}

    @Override
    public void repeatRemove(Transaction t) {}

    @Override
    public void repeatTruncate(Transaction t) {}

    @Override
    public void repeatRename(Transaction t, String string) {}

    @Override
    public void repeatPut(Transaction t, DatabaseEntry key,
                          DatabaseEntry newData) {}

    @Override
    public void repeatDelete(Transaction t, DatabaseEntry key) {}

    @Override
    public String getName() {
        return getType().getKey() + "CompletionTrigger";
    }

    @Override
    public Trigger setDatabaseName(String string) {
        dbName = string;
        return this;
    }

    @Override
    public String getDatabaseName() {
        return dbName;
    }

    @Override
    public void addTrigger(Transaction t) {}

    @Override
    public void removeTrigger(Transaction t) {}

    @Override
    public void put(Transaction t, DatabaseEntry key, DatabaseEntry oldData,
                    DatabaseEntry newData) {}

    @Override
    public void delete(Transaction t, DatabaseEntry key,
                       DatabaseEntry oldData) {}

    @Override
    public void commit(Transaction t) {
        if (shutdown) {
            return;
        }
        logger.log(Level.FINE, "Received trigger for {0}", getDBName());

        /* Don't wait, we just care about the state: replica or not */
        final ReplicatedEnvironment repEnv = repNode.getEnv(0);
        try {
            if ((repEnv == null) || !repEnv.getState().isReplica()) {
                logger.log(Level.INFO,
                           "Environment changed, ignoring trigger for {0}",
                           getDBName());
                return;
            }
        } catch (EnvironmentFailureException efe) {
            /* It's in the process of being re-established. */
            logger.log(Level.INFO,
                       "Environment being re-established, ignoring trigger "+
                       "for {0}", getDBName());
            return;
        } catch (IllegalStateException ise) {
            /* A closed environment. */
            logger.log(Level.INFO,
                       "Environment closed, ignoring trigger for {0}",
                       getDBName());
            return;
        }
        update(repEnv);
    }

    @Override
    public void abort(Transaction t) {}
}

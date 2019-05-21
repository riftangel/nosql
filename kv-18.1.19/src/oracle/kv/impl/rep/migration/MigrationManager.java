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

package oracle.kv.impl.rep.migration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.TopologyManager.Localizer;
import oracle.kv.impl.api.table.DroppedTableException;
import oracle.kv.impl.rep.PartitionManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.admin.RepNodeAdmin.MigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.PartitionMigrations.MigrationRecord;
import oracle.kv.impl.rep.migration.PartitionMigrations.SourceRecord;
import oracle.kv.impl.rep.migration.PartitionMigrations.TargetRecord;
import oracle.kv.impl.rep.table.MaintenanceThread;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.StateTracker;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import com.sleepycat.je.trigger.ReplicatedDatabaseTrigger;
import com.sleepycat.je.trigger.TransactionTrigger;
import com.sleepycat.je.trigger.Trigger;

/**
 * Partition migration manager.
 *
 * Partition migration is initiated by the MigratePartition task invoking
 * RepNode.getPartition() which invokes getPartition() here. After some
 * checks, a MigrationTarget thread is created and started. This thread will
 * establish a channel with the source node using the JE service framework,
 * starting the transfer of partition data.
 *
 * This class also manages the migration service which handles requests for
 * partition data from migration targets.
 *
 * Lastly, this class maintains the persistent records for completed
 * transfers. These are used by both the source and target nodes during the
 * Transfer of Ownership protocol (see the class doc in MigrationSource).
 */
public class MigrationManager implements Localizer {

    private final Logger logger;

    private static final int NUM_DB_OP_RETRIES = 100;

    /* DB operation delays */
    private static final long SHORT_RETRY_TIME = 500;

    private static final long LONG_RETRY_TIME = 1000;

    /* Minimum delay for migration target retry */
    private static final long MINIMUM_DELAY = 2 * 1000;

    private final RepNode repNode;

    private final Params params;

    /* The maximum number of target streams which can run concurrently. */
    private final int concurrentTargetLimit;

    private final Map<PartitionId, MigrationTarget> targets = new HashMap<>();

    private volatile MigrationService migrationService = null;

    private TargetExecutor targetExecutor = null;

    private volatile Database migrationDb = null;

    private volatile boolean isMaster = false;

    private final MigrationStateTracker stateTracker;

    private volatile boolean shutdown = false;

    private volatile TargetMonitorExecutor targetMonitorExecutor = null;

    private long completedSequenceNum = 0;

    private volatile long lastMigrationDuration = Long.MAX_VALUE;

    public MigrationManager(RepNode repNode, Params params) {
        this.repNode = repNode;
        this.params = params;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        concurrentTargetLimit =
                        params.getRepNodeParams().getConcurrentTargetLimit();
        stateTracker = new MigrationStateTracker(logger);
    }

    /**
     * Starts the state tracker
     * TODO - Perhaps start the tracker on-demand in noteStateChange()?
     */
    public void startTracker() {
        stateTracker.start();
    }

    /**
     * Returns true if this node is the master and not shutdown.
     *
     * @return true if this node is the master and not shutdown
     */
    boolean isMaster() {
        return isMaster && !shutdown;
    }

    /**
     * Gets the status of partition migrations on this node.
     *
     * @return the partition migration status
     */
    public synchronized PartitionMigrationStatus[] getStatus() {

        if (!isMaster()) {
            return new PartitionMigrationStatus[0];
        }

        final HashSet<PartitionMigrationStatus> status =
            new HashSet<>(targets.size());

        /* Get the targets */
        for (MigrationTarget target : targets.values()) {
            status.add(target.getStatus());
        }

        /* Get the sources */
        if (migrationService != null) {
            migrationService.getStatus(status);
        }

        /*
         * If the db is not initialized, we are likely in startup, in which
         * case do not wait for it.
         */
        if (migrationDb != null) {

            /* Get the completed records */
            final PartitionMigrations migrations = getMigrations();
            if (migrations != null) {

                for (MigrationRecord record : migrations) {

                    /*
                     * If a record is found add its status only if it was set
                     * and if it is for a migration not already found in the
                     * active lists. (The status is only persisted when the
                     * migration is completed)
                     */
                    if (record.getStatus() != null) {
                        status.add(record.getStatus());
                    }
                }
            }
        }

        return status.toArray(new PartitionMigrationStatus[status.size()]);
    }

    /**
     * Gets the migration status for the specified partition if one is
     * available.
     *
     * @param partitionId
     * @return the migration status or null
     */
    public synchronized PartitionMigrationStatus
                                getStatus(PartitionId partitionId) {

        if (!isMaster()) {
            return null;
        }

        PartitionMigrationStatus status = null;

        /* Check the sources */
        if (migrationService != null) {
            status = migrationService.getStatus(partitionId);
            if (status != null) {
                return status;
            }
        }

        /* Targets */
        final MigrationTarget target = targets.get(partitionId);
        if (target != null) {
            return target.getStatus();
        }

        /* Completed migrations */
        final PartitionMigrations migrations = getMigrations();
        if (migrations != null) {
            final MigrationRecord record = migrations.get(partitionId);
            if (record != null) {
                status = record.getStatus();

                /*
                 * If this is for a target missing from the targets list,
                 * then it was likely cleared due to a failure. Attempt to
                 * restart it. Note that submitTarget() checks if the record
                 * is pending, so no need to check here.
                 */
                if (record instanceof TargetRecord) {
                    final TargetRecord targetRecord = (TargetRecord)record;
                    submitTarget(targetRecord);
                }
            }
        }
        return status;
    }

    /**
     * Notes a state change in the replicated environment. The actual
     * work to change state is made asynchronously to allow a quick return.
     */
    public void noteStateChange(StateChangeEvent stateChangeEvent) {
        stateTracker.noteStateChange(stateChangeEvent);
    }

    /**
     * Updates the handle to the partition migration db. Any in-progress
     * migrations are stopped, as their handles must also be updated.
     *
     * @param repEnv the replicated environment handle
     */
    public synchronized void updateDbHandles(ReplicatedEnvironment repEnv) {
        if (DatabaseUtils.needsRefresh(migrationDb, repEnv)) {
            logger.fine("Updating migration manager DB handles.");
            closeDbHandles(false);
            openMigrationDb(repEnv);
        }
    }

    /**
     * Closes the handle to the partition migration db. Any in-progress
     * migrations are stopped, as their handles must also be updated.
     *
     * @param force force the stop
     */
    public synchronized void closeDbHandles(boolean force) {
        stopServices(force);
        closeMigrationDb();
    }

    /**
     * Shuts down the manager and stops all in-progress migrations. If force is
     * false this call will wait for all threads to stop, otherwise shutdown
     * will return immediately.
     *
     * @param force force the shutdown
     */
    public synchronized void shutdown(boolean force) {
        logger.info("Shutting down migration manager.");
        shutdown = true;
        closeDbHandles(force);
        if (targetMonitorExecutor != null) {
            targetMonitorExecutor.shutdown();

            if (!force) {
                try {
                    targetMonitorExecutor.awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException ignore) { }
            }
            targetMonitorExecutor = null;
        }
        stateTracker.shutdown();
    }

    /**
     * Starts the migration service and restarts any pending migrations. If the
     * start fails and should be retried, a string indicating the reason for
     * the failure is returned, otherwise null is returned.
     */
    private synchronized String startServices() {
        /*
         * We cannot use the value of isMaster to detect master changes,
         * but instead must check the environment directly. (see comment
         * for MigrationStateTracker.doNotify)
         */
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return "cannot get environment";
        }

        if (!repEnv.isValid() && !repEnv.getState().isMaster()) {
            /* Env or mastership changed, no need to retry */
            return null;
        }
        openMigrationDb(repEnv);

        /* If the db could not be opened, abort the start */
        if (migrationDb == null) {
            return "cannot open migration DB";
        }

        /*
         * This will remove any migration records that are stale. A record
         * may become stale if the topology is updated, and would have resulted
         * in the record being removed, just as the node loses mastership. The
         * new master may have the new topology but got it when it was a
         * replica and could not remove the record.
         */
        localizeTopology(repNode.getTopology());

        assert migrationService == null;
        migrationService = new MigrationService(repNode, this, params);
        migrationService.start(repEnv);
        monitorTarget();
        restartTargets();
        return null;
    }

    /**
     * Stops all in-progress migrations (source or target).
     *
     * @param force force the stop
     */
    private void stopServices(boolean force) {
        assert Thread.holdsLock(this);

        if (migrationDb == null) {
            assert targets.isEmpty();
            assert targetExecutor == null;
            assert migrationService == null;
            return;
        }

        /* Cancel first, then go back and wait for the threads to stop */
        for (MigrationTarget target : targets.values()) {

            /*
             * The return value from cancel() can be ignored. If the
             * target can not be canceled, it means that the completed
             * state has been persisted.
             *
             * If this is a forced stop, don't wait for the cancel.
             */
            target.cancel(!force);
        }
        targets.clear();

        /* The threads are already stopped at this point */
        shutdownTargetExecutor();
        targetExecutor = null;

        if (migrationService != null) {
            migrationService.stop(shutdown, !force,
                                  (ReplicatedEnvironment) migrationDb.
                                  getEnvironment());
            migrationService = null;
        }
    }

    /**
     * Returns true if there are no partition migration operations (source or
     * target) on this node. Returns false if migration sources are in progress
     * or there are completed sources waiting for ToO. Returns false if the
     * migration service has not started.
     *
     * This method should only be invoked from a table maintenance thread when
     * TableManager.isBusyMaintenance() would return true. (This may not be
     * the case when called directly from unit tests)
     * 
     * @return true if idle
     */
    public boolean isIdle() {        
        /*
         * Return false if the service hasn't started, or there are pending
         * sources.
         */
        final MigrationService ms = migrationService;
        if ((ms == null) || ms.pendingSources()) {
            return false;
        }
        synchronized (this) {
            return (targetExecutor == null) || targetExecutor.isTerminated();
        }
    }
    
    /**
     * Waits until there are no partition migration operations on this node.
     * As long as waiter.exitMaintenance() is false, this method waits if
     * migration sources are in progress or there are completed sources waiting
     * for ToO. If, while waiting, waiter == null or waiter.exitMaintenance()
     * is true, false is returned.
     *
     * If migration targets are in progress the target executor is shutdown and
     * then waits for the targets to finish. Any pending target requests are
     * abandoned.
     *
     * This method should only be invoked from a table maintenance thread when
     * TableManager.isBusyMaintenance() would return true. (This may not be
     * the case when called directly from unit tests)
     *
     * @param waiter the maintenance thread which is waiting (or null for
     * unit tests)
     * @return true if idle, or false if waiter is null, or
     * waiter.exitMaintenance() is true
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitIdle(MaintenanceThread waiter)
            throws InterruptedException {
        /*
         * Wait if the service hasn't started, or there are pending sources.
         */
        while ((migrationService == null) ||
               migrationService.pendingSources()) {
            /* waiter can be null for unit tests */
            if ((waiter == null) || waiter.exitMaintenance()) {
                return false;
            }
            /* retryWait() will exit early if the thread is shutdown */
            waiter.retryWait(10 * 1000);
        }
        return awaitTargetIdle(waiter);
    }
    
    /**
     * Waits until there are no partition migration target operations on this
     * node. If migration targets are in progress the target executor is
     * shutdown and then waits for the targets to finish. Any pending target
     * requests are abandoned. If, while waiting, waiter == null or
     * waiter.exitMaintenance() is true, false is returned.
     *
     * This method should only be invoked from a table maintenance thread when
     * TableManager.isBusyMaintenance() would return true. (This may not be
     * the case when called directly from unit tests)
     *
     * @param waiter the maintenance thread which is waiting (or null for
     * unit tests)
     * @return true if idle, or false if waiter is null, or
     * waiter.exitMaintenance() is true
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitTargetIdle(MaintenanceThread waiter)
            throws InterruptedException {
        
        /* shutdownTargetExecutor() is synchronized */
        final TargetExecutor executor = shutdownTargetExecutor();
        if (executor == null) {
            return true;
        }

        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            /* waiter can be null for unit tests */
            if ((waiter == null) || waiter.exitMaintenance()) {
                return false;
            }
        }

        /*
         * Everyone is done, clear out the un-started targets. This will cause
         * the targets to be restarted when getStatus() is invoked by the
         * admin.
         */
        synchronized (this) {
            targets.clear();
        }
        return true;
    }
    
    /**
     * Initiates shutdown on the target executor if one is present.
     * @return the target executor or null
     */
    private synchronized TargetExecutor shutdownTargetExecutor() {
        if (targetExecutor != null) {
            targetExecutor.shutdown();
        }
        return targetExecutor;
    }

    /* -- Migration source related method -- */

    /**
     * Returns the migration service.
     */
    MigrationService getMigrationService() {
        return migrationService;
    }

    /* -- Migration target related methods -- */

    /**
     * Starts a migration thread to get the specified partition.
     *
     * TODO - need to check if a migration has been completed
     *
     * @param partitionId the ID of the partition to migrate
     * @param sourceRGId the ID of the partitions current location
     * @return the migration state
     */
    public synchronized MigrationState
        migratePartition(final PartitionId partitionId,
                         final RepGroupId sourceRGId) {
        if (!isMaster()) {
            final String message = "Request to migrate " + partitionId +
                " but node shutdown or not master";
            logger.fine(message);
            return new MigrationState(PartitionMigrationState.UNKNOWN,
                                      new Exception(message));
        }

        if (isStreaming()) {
            final String message = "Request to migrate " + partitionId +
                    " was rejected because the node is part of a streaming" +
                    " subscription";
            logger.warning(message);
            return new MigrationState(PartitionMigrationState.ERROR,
                                      new Exception(message));
        }

        final MigrationTarget target = targets.get(partitionId);

        if (target != null) {
            switch (target.getState().getPartitionMigrationState()) {
                case ERROR:
                    targets.remove(partitionId);
                    break;

                case SUCCEEDED:
                    targets.remove(partitionId);

                    /*
                     * If the target is for the requested source, just return
                     * success. Otherwise it is from a previous migration.
                     */
                    if (target.getSource().equals(sourceRGId)) {
                        return new MigrationState(
                            PartitionMigrationState.SUCCEEDED);
                    }
                    break;

                case PENDING:
                case RUNNING:

                    /*
                     * If the target is for the requested source, just return
                     * the state. Otherwise it is from an ongoing migration
                     * and this request can not be met.
                     */
                    if (target.getSource().equals(sourceRGId)) {
                        return new MigrationState(
                            target.getState().getPartitionMigrationState());
                    }
                    final String message = "Migration in progress from " +
                        target.getSource();
                    logger.warning(message);
                    return new MigrationState(PartitionMigrationState.ERROR,
                                              new IllegalStateException(message));
                case UNKNOWN:
                    throw new IllegalStateException("Invalid " + target);
            }
        }

        final TransactionConfig txnConfig = new TransactionConfig().
            setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        try {
            final MigrationState state =
                tryDBOperation(new DBOperation<MigrationState>() {

                @Override
                public MigrationState call(Database db) {
                    Transaction txn = null;

                    try {
                        txn = db.getEnvironment().
                            beginTransaction(null, txnConfig);

                        final PartitionMigrations migrations =
                            PartitionMigrations.fetch(db, txn);

                        final MigrationRecord record =
                            migrations.get(partitionId);

                        /*
                         * If a migration is already in progress, and is not in
                         * an error state, then just report its state (after
                         * further checks).
                         *
                         * If the existing migration is in an error state, just
                         * start a new one. The migrations.add() will replace
                         * the old with the new.
                         */
                        if (record != null) {
                            logger.log(Level.INFO,
                                       "Received request to migrate {0} from " +
                                       "{1}, migration already in progress : " +
                                       "{2}",
                                       new Object[] {partitionId, sourceRGId,
                                                     record});

                            /*
                             * If this is a completed source record reject the
                             * request since the partition is in transit.
                             */
                            if (record instanceof SourceRecord) {
                                final String message =
                                    "Received request to migrate " +
                                    partitionId + " but partition is " +
                                    " already in transit to " +
                                    record.getTargetRGId();
                                logger.warning(message);
                                return new MigrationState(
                                    PartitionMigrationState.ERROR,
                                    new IllegalStateException(message));
                            }

                            /*
                             * If here, we have a target record for the
                             * requested partition.
                             *
                             * If the source rep group is different from the
                             * running migration then something rather
                             * strange is going on so report an error.
                             */
                            if (!record.getSourceRGId().equals(sourceRGId)) {
                                final String message =
                                    "Source group " + sourceRGId +
                                    " does not match " + record;
                                logger.warning(message);
                                return new MigrationState(
                                    PartitionMigrationState.ERROR,
                                    new IllegalStateException(message));
                            }

                            /* All good, record matches the request */
                            final PartitionMigrationState state1 =
                                ((TargetRecord)record).getState();

                            /* If not an error, just return the state */
                            if (!state1.equals(PartitionMigrationState.ERROR)) {
                                return new MigrationState(state1);
                            }

                            /*
                             * Dropping out will (re)start a new migration and
                             * will replace the existing record.
                             */
                        }

                        final TargetRecord newRecord =
                            migrations.newTarget(partitionId,
                                                 sourceRGId,
                                                 repNode.getRepNodeId());
                        migrations.add(newRecord);
                        migrations.persist(db, txn, false);

                        txn.commit();
                        txn = null;

                        return submitTarget(newRecord);

                    } finally {
                        TxnUtil.abort(txn);
                    }
                }
            }, false);

            /* If status is null then we are in shutdown or the op timed out. */
            return (state == null) ?
                new MigrationState(PartitionMigrationState.UNKNOWN) :
                    state;
        } catch (InsufficientAcksException iae) {

            /*
             * If InsufficientAcksException the record was made durable
             * locally. We can report back success (PENDING) even though
             * in the long run, it may not be. In that case, the admin will
             * eventually see an error an retry. In the case that it is
             * durable but not started, a call to getMigrationState() will
             * start the migration.
             */
            return new MigrationState(PartitionMigrationState.PENDING);
        } catch (DatabaseException de) {
            final String message = "Exception starting migration for " +
                                   partitionId;
            logger.log(Level.WARNING, message, de);
            return new MigrationState(PartitionMigrationState.ERROR,
                                      new Exception(message, de));
        }
    }

    /**
     * Submits a migration target with parameters from the specified migration
     * record. The migration target is only submitted if the record is in
     * the PENDING state.
     *
     * @param record migration record
     * @return the state of the migration record
     */
    private MigrationState submitTarget(TargetRecord record) {
        assert Thread.holdsLock(this);
        assert migrationDb != null;

        /*
         * Start only if PENDING and there is no secondary cleaning activity or
         * streaming.
         */
        if (!record.isPending() ||
            repNode.getTableManager().busySecondaryCleaning() ||
            isStreaming()) {
            return new MigrationState(record.getState());
        }
        final MigrationTarget target =
            new MigrationTarget(record, repNode, this,
                                (ReplicatedEnvironment)
                                migrationDb.getEnvironment(),
                                params);
        targets.put(record.getPartitionId(), target);

        /*
         * At this point, if the executor is shutdown, shutdown must be
         * completed (isTerminated() == true). If the executor was still in
         * shutdown, isBusyMaintenance() should have returned false.
         */
        assert (targetExecutor == null) ||
               !targetExecutor.isShutdown() ||
               targetExecutor.isTerminated();

        /*
         * If targetExecutor is non-null, and not terminated then there may
         * be migrations underway but, since isBusyMaintenance() returned false,
         * its OK to start additional ones.
         *
         * If the executor is terminated, then it can be discarded and a
         * new one created.
         *
         * If targetExecutor is null, then isBusyMaintenance() returning false
         * means that cleaning has been performed and is completed.  The
         * cleaning only starts when any migrations have completed, so that's
         * how we know they are done now.
         */
        if ((targetExecutor == null) || targetExecutor.isTerminated()) {
            targetExecutor = new TargetExecutor();
        }

        /*
         * submitNew() will remove the record from targets if there is
         * a failure to submit the task.
         */
        targetExecutor.submitNew(target);

        return new MigrationState(record.getState(), null);
    }

    /**
     * Restarts partition migration targets. If this is a startup or a change
     * in mastership we check to see if there are records that represent
     * migrations that can be restarted.
     */
    private void restartTargets() {
        assert Thread.holdsLock(this);

        if (migrationDb == null) {
            return;
        }

        final PartitionMigrations migrations = getMigrations();

        /*
         * Failing to read the migration DB is not fatal here. If there are
         * pending targets in the DB then they will eventually be started when
         * a getMigrationState() call comes in for that target.
         */
        if (migrations == null) {
            return;
        }

        assert targets.isEmpty();

        for (MigrationRecord record : migrations) {

            assert targets.get(record.getPartitionId()) == null;

            if (record instanceof TargetRecord) {
                submitTarget((TargetRecord)record);
            }
        }
    }

    /**
     * Gets the state of a migration.
     *
     * If the return value is PartitionMigrationState.ERROR,
     * canceled(PartitionId, RepGroupId) must be invoked on the migration
     * source repNode.
     *
     * A check to see if the partition is actually being serviced by
     * this RN should be made if the return state is ERROR.
     *
     * @param partitionId a partition ID
     * @return the migration state
     */
    public synchronized MigrationState
                              getMigrationState(final PartitionId partitionId) {

        if (!isMaster()) {
            final String message =
                "Request migration state for " + partitionId +
                " but node shutdown or not master";
            logger.fine(message);
            return new MigrationState(PartitionMigrationState.UNKNOWN,
                                      new IllegalStateException(message));
        }

        logger.log(Level.FINE, "Migration state request for {0}", partitionId);

        /* Check for a current migration target */
        final MigrationTarget target = targets.get(partitionId);

        if (target != null) {

            /*
             * If success, we can remove the target. The partition map will
             * soon be updated and RepNode.getMigrationState will exit early.
             */
            final MigrationState state = target.getState();
            if (state.getPartitionMigrationState().
                    equals(PartitionMigrationState.SUCCEEDED)) {
                removeTarget(partitionId);
            }
            return state;
        }

        /* No target, check the db for the record of a past request */

        final PartitionMigrations migrations = getMigrations();

        if (migrations == null) {
            return new MigrationState(PartitionMigrationState.UNKNOWN,
                new Exception("Unable to read migration record db"));
        }

        final TargetRecord record = migrations.getTarget(partitionId);

        /*
         * If no one here by that name, return unknown. If there is no record,
         * then:
         *  1) no migration request was recorded,
         *  2) the request failed and the record was removed, or (most likely)
         *  3) we are between the record removed and the partition DB updated.
         */
        if (record == null) {
            return new MigrationState(PartitionMigrationState.UNKNOWN,
                new Exception("Migration record for " + partitionId +
                              " not found"));
        }

        /*
         * If here, there is a target record but no MigrationTarget so try to
         * submit the migration.
         */
        return submitTarget(record);
    }

    synchronized void removeTarget(PartitionId partitionId) {
        targets.remove(partitionId);
    }

    /**
     * Attempts to cancel the migration for the specified partition. Returns
     * the migration state if there was a migration in progress, otherwise
     * null is returned. If the returned state is
     * PartitionMigrationState.ERROR the cancel was successful (or the
     * migration was already canceled). If the returned state is
     * PartitionMigrationState.SUCCEEDED then the migration has completed and
     * cannot be canceled. All other states indicate that the cancel should be
     * retried.
     *
     * If the cancel is successful (PartitionMigrationState.ERROR is returned)
     * then canceled(PartitionId, RepGroupId) must be invoked on the source
     * node.
     *
     * @param partitionId a partition ID
     * @return a migration state or null
     */
    public synchronized MigrationState canCancel(PartitionId partitionId) {

        if (!isMaster()) {
            final String message =
                "Request to cancel migration of " + partitionId +
                " but node shutdown or not master";
            logger.fine(message);
            return new MigrationState(PartitionMigrationState.UNKNOWN,
                                      new IllegalStateException(message));
        }
        logger.log(Level.INFO,
                   "Request to cancel migration of {0}", partitionId);

        final MigrationTarget target = targets.get(partitionId);

        if (target != null) {

            /* If there is an active migration, and its not cancelable - fail */
            if (!target.cancel(false)) {
                logger.log(Level.INFO, "Unable to cancel {0}", target);
                assert target.getState().
                        equals(PartitionMigrationState.SUCCEEDED);
                return new MigrationState(PartitionMigrationState.SUCCEEDED);
            }

            /*
             * There was an active target and it could be
             * canceled, try removing the record for it.
             */
            try {
                removeRecord(partitionId, target.getRecordId(), false);
                final MigrationState ret = target.getState();
                logger.log(Level.INFO,
                           "Request to cancel migration of {0}, removed {1}",
                           new Object[] {partitionId, target});
                return ret;

            } catch (DatabaseException de) {
                final String message =
                    "Exception attempting to remove migration record for " +
                    partitionId;
                logger.log(Level.INFO, message, de);
                return new MigrationState(PartitionMigrationState.UNKNOWN,
                                          new Exception(message, de));
            }
        }

        /* No target, check the db for the record of a past request */

        final PartitionMigrations migrations = getMigrations();

        if (migrations == null) {
            return new MigrationState(PartitionMigrationState.UNKNOWN,
                new Exception("Unable to read migration record db"));
        }

        final TargetRecord record = migrations.getTarget(partitionId);
        final MigrationState ret = (record == null) ? null :
            new MigrationState(record.getState());
        logger.log(Level.INFO,
                   "Request to cancel migration of {0} returning {1}",
                   new Object[] {partitionId, ret});
        return ret;
    }

    /**
     * Cleans up the source stream after a cancel or error. The method should
     * be invoked on the source node whenever PartitionMigrationState.ERROR is
     * returned from a call to getMigrationState(PartitionId) or
     * cancel(PartitionId).
     *
     * @param partitionId a partition ID
     * @param targetRGId the target RG (for confirmation)
     * @return true if cleanup was successful
     */
    public synchronized boolean canceled(PartitionId partitionId,
                                         RepGroupId targetRGId) {

        /* Can't do anything if not the master */
        if (!isMaster()) {
            return false;
        }
        logger.log(Level.INFO, "Canceling source migration of {0} to {1}",
                   new Object[]{partitionId, targetRGId});

        /* Stops the ongoing source if there is one */
        if (migrationService != null) {
            migrationService.cancel(partitionId, targetRGId);
        }

        final PartitionMigrations migrations = getMigrations();

        /* If can't get object, then something is wrong. */
        if (migrations == null) {
            return false;
        }

        final MigrationRecord record = migrations.get(partitionId);
        if (record == null) {
            return true;
        }

        /*
         * If the migration is complete, and the source is this node, the cancel
         * is to cleanup a failure after EOD was sent.
         */
        if (record.isCompleted() &&
            (record.getTargetRGId().equals(targetRGId)) &&
            (record.getSourceRGId().getGroupId() ==
             repNode.getRepNodeId().getGroupId())) {
            logger.log(Level.INFO, "Removing {0}", record);
            try {
                removeRecord(record, true);
            } catch (DatabaseException de) {
                logger.log(Level.WARNING, "Exception removing " + record, de);
                return false;
            }
        }

        return true;
    }

    /**
     * Returns a localized topology. The returned topology may have been
     * updated with partition changes due to completed transfers. The topology
     * returned should NEVER be passed on to other nodes. If there have been no
     * completed transfers, then the input topology is returned.  Null is
     * returned if the topology could not be localized due to the input
     * topology not sufficiently up-to-date to be localized, or the migration
     * db is not accessible.
     *
     * @param topology Topology to localize
     * @return a localized topology or null
     */
    @Override
    public Topology localizeTopology(Topology topology) {

        /* If topology is null, then called before things are initialized. */
        if (topology == null) {
            return null;
        }

        final ReplicatedEnvironment repEnv = repNode.getEnv(1 /* ms */);

        /* No env, then can't get to the db */
        if (repEnv == null) {
            return null;
        }
        openMigrationDb(repEnv);
        final PartitionMigrations migrations = getMigrations();

        /* Punt if the db is not available. */
        if (migrations == null) {
            return null;
        }

        final int topoSeqNum = topology.getSequenceNumber();

        /*
         * If the topology that the migration db is based on is newer
         * than the topology to be modified we are in trouble. In this case
         * return null, which will cause some exceptions further down the
         * road, but this should be temporary.
         */
        if (migrations.getTopoSequenceNum() > topoSeqNum) {
            logger.log(Level.INFO,
                       "Cannot localize topology seq#: {0} because it is < " +
                       "migration topology seq#: {1}",
                       new Object[] { topoSeqNum,
                                      migrations.getTopoSequenceNum() });
            return null;
        }

        logger.log(Level.FINE, "Localizing topology seq#: {0}", topoSeqNum);

        final Topology copy = topology.getCopy();
        boolean modified = false;

        final Iterator<MigrationRecord> itr = migrations.completed();

        while (itr.hasNext()) {
            final MigrationRecord record = itr.next();

            logger.log(Level.FINE, "Checking {0}", record);

            final PartitionId partitionId = record.getPartitionId();
            final RepGroupId targetRGId = record.getTargetRGId();

            /*
             * If the partition's group of a completed transfer matches
             * what is in the topology, then the ToO is complete. In
             * this case the element can be removed - ToO #10.
             */
            if (targetRGId.equals(copy.get(partitionId).getRepGroupId())) {

                logger.log(Level.INFO,
                           "ToO completed for {0} by topology seq#: {1}",
                           new Object[] { partitionId, topoSeqNum });

                if (repEnv.getState().isMaster()) {
                    try {

                        /*
                         * The topology sequence number must be updated before
                         * the partition DB is removed so that a replica does
                         * not attempt to re-open the DB.
                         */
                        if (updateTopoSeqNum(topoSeqNum)) {

                            /*
                             * If the moved partition's source is this node then
                             * we can remove the old partition's database.
                             */
                            if (record.getSourceRGId().equals
                                (new RepGroupId(repNode.getRepNodeId().
                                                getGroupId()))) {

                                /*
                                 * Let the RepNode know this partition is
                                 * officially removed from its care.
                                 */
                                repNode.getTableManager().
                                        notifyRemoval(partitionId);

                                /*
                                 * Remove the record once the partition db has
                                 * been successfully removed.
                                 */
                                removePartitionDb(partitionId, repEnv);
                                removeRecord(record, false);

                            } else {

                                /* This was a target record, just remove it */
                                removeRecord(record, false);
                            }
                        }
                    } catch (LockConflictException lce) {
                        /* Common - reduce the noise, log at fine */
                        logger.log(Level.FINE, "Lock conflict removing " +
                                   record, lce);
                    } catch (DatabaseException de) {

                        /*
                         * Since this is not a topology change, we can continue
                         * if the update fails. Better luck the next time.
                         */
                        logger.log(Level.INFO, "Exception removing " + record,
                                   de);
                    }
                }
            } else {
                logger.log(Level.INFO, "Moving {0} to {1} locally",
                           new Object[] { partitionId, targetRGId });

                /*
                 * Replace the partition object with our own "special"
                 * one which points to its new location. This call will
                 * cause the copy's sequence number to be incremented
                 * allowing requests for this partition to be forwarded.
                 * (See  RequestHandlerImpl.handleException)
                 */
                copy.updatePartition(partitionId, targetRGId);
                modified = true;
            }
        }
        return modified ? copy : topology;
    }

    /**
     * Writes the topology sequence number to the db.
     *
     * @param seqNum
     */
    private boolean updateTopoSeqNum(final int seqNum) {
        final TransactionConfig txnConfig =
            new TransactionConfig().
            setConsistencyPolicy
            (NoConsistencyRequiredPolicy.NO_CONSISTENCY).
            setDurability
            (new Durability(Durability.SyncPolicy.SYNC,
                            Durability.SyncPolicy.SYNC,
                            Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

        final Boolean success = tryDBOperation(new DBOperation<Boolean>() {

                @Override
                public Boolean call(Database db) {

                    Transaction txn = null;
                    try {
                        txn = db.getEnvironment().
                            beginTransaction(null, txnConfig);

                        final PartitionMigrations pMigrations =
                            PartitionMigrations.fetch(db, txn);

                        pMigrations.setTopoSequenceNum(seqNum);
                        pMigrations.persist(db, txn, false);
                        txn.commit();
                        txn = null;
                        /*
                         * Return a Boolean object, rather than depending on
                         * autoboxing to convert a boolean primitive, in order
                         * to work around an obscure linking problem that may
                         * involve the Eclipse incremental compiler.  Same with
                         * similar cases below.
                         */
                        return Boolean.TRUE;
                    } finally {
                        TxnUtil.abort(txn);
                    }
                }
        }, false);

        return (success == null) ? Boolean.FALSE : success;
    }

    /**
     * Removes the partition DB.
     */
    private void removePartitionDb(PartitionId partitionId,
                                   ReplicatedEnvironment repEnv) {

        final String dbName = partitionId.getPartitionName();

        logger.log(Level.INFO,
                   "Removing database {0} for moved {1}",
                   new Object[]{dbName, partitionId});

        /*
         * This is not done in tryDBOperation() as retrying the removeDatabase
         * can create significant lock conflicts in the presence of heavy client
         * activity. TODO - Figure out why?
         */
        try {
            repEnv.removeDatabase(null, dbName);
        } catch (DatabaseNotFoundException ignore) {
            /* Already gone */
        }
    }

    /**
     * Opens the partition migration db. Wait indefinitely for access.
     */
    private synchronized void openMigrationDb(ReplicatedEnvironment repEnv) {

        while ((migrationDb == null) && (!shutdown)) {

            logger.log(Level.FINE, "Open partition migration DB: {0}", this);

            final DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);

            try {

                /*
                 * Replicas depend on DB triggers to track topo changes due to
                 * migrations completing.
                 */
                if (repEnv.getState().isReplica()) {
                    dbConfig.getTriggers().add(new CompletionTrigger());
                }

                migrationDb = PartitionMigrations.openDb(repEnv, dbConfig);
                assert migrationDb != null;
                return;
            } catch (DatabaseException de) {
                /* retry unless the env. is bad */
                if (!repEnv.isValid()) {
                    return;
                }

            } catch (IllegalStateException ise) {
                /* If the env. went bad, exit, otherwise rethrow the ise */
                if (!repEnv.isValid()) {
                    return;
                }
                throw ise;
            }

            /* Wait to retry */
            try {
                wait(PartitionManager.DB_OPEN_RETRY_MS);
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }

    /**
     * Closes the partition migration db. Likely from a env. change
     */
    private synchronized void closeMigrationDb() {

        if (migrationDb == null) {
            return;
        }
        logger.fine("Close partition migration db");

        TxnUtil.close(logger, migrationDb, "migration");

        migrationDb = null;
    }

    /**
     * Gets the migrations object from the db for read-only use. If there is
     * an error or in shutdown null is returned.
     *
     * @return the migration object or null.
     */
    PartitionMigrations getMigrations() {
        /* If the DB is not open, just make a quick exit. */
        if (migrationDb == null) {
            return null;
        }
        try {
            return tryDBOperation(new DBOperation<PartitionMigrations>() {

                @Override
                public PartitionMigrations call(Database db) {
                    return PartitionMigrations.fetch(db);
                }
            }, false);
        } catch (DatabaseException de) {
            logger.log(Level.INFO,
                       "Exception accessing the migration db {0}", de);
            return null;
        }
    }

    /**
     * Removes the specified migration record from the db.
     *
     * @param record the record to remove
     * @param affectsTopo true if removing the record affects the topology
     */
    void removeRecord(MigrationRecord record, boolean affectsTopo) {
        removeRecord(record.getPartitionId(), record.getId(), affectsTopo);
    }

    /**
     * Removes the migration record for the specified partition and record ID.
     *
     * @param partitionId a partition ID
     * @param recordId a migration record ID
     * @param affectsTopo true if removing the record affects the topology
     */
    void removeRecord(final PartitionId partitionId,
                      final long recordId,
                      final boolean affectsTopo) {

        final TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setConsistencyPolicy
            (NoConsistencyRequiredPolicy.NO_CONSISTENCY);
        if (affectsTopo) {
            txnConfig.setDurability(
                new Durability(Durability.SyncPolicy.SYNC,
                               Durability.SyncPolicy.SYNC,
                               Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));
        }

        Boolean removed = tryDBOperation(new DBOperation<Boolean>() {

            @Override
            public Boolean call(Database db) {

                Transaction txn = null;
                try {
                    txn = db.getEnvironment().beginTransaction(null, txnConfig);
                    final PartitionMigrations pMigrations =
                        PartitionMigrations.fetch(db, txn);
                    final MigrationRecord record =
                            pMigrations.remove(partitionId);

                    if (record == null) {
                        logger.log(Level.FINE,
                                   "removeRecord: No record for {0}",
                                   partitionId);
                        return Boolean.FALSE;
                    }

                    if (record.getId() != recordId) {
                        return Boolean.FALSE;
                    }
                    pMigrations.persist(db, txn, affectsTopo);
                    txn.commit();
                    txn = null;
                    return Boolean.TRUE;
                } finally {
                    TxnUtil.abort(txn);
                }
            }
        }, affectsTopo);

        if (removed == null) {
            /* In shutdown or op timed out. */
            return;
        }
        if (removed && affectsTopo) {
            updateLocalTopology();
        }
    }

    /**
     * Updates the local topology.
     */
    boolean updateLocalTopology() {

        Boolean success = null;
        try {

            /*
             * This is wrapped in a DB operation because
             * repNode.updateLocalTopology will attempt to close the DB handle
             * for the moved partition. If there is an outstanding client
             * operation this will fail and should be retried.
             *
             * Early in that call, the topology will be updated which will
             * prevent further client operations.
             */
            success = tryDBOperation(new DBOperation<Boolean>() {

                @Override
                public Boolean call(Database db) {
                    return repNode.updateLocalTopology();
                }
            }, true);
        } catch (DatabaseException de) {

            /*
             * A DB exception here is not critical. It means there may have
             * been an issue closing a partition DB. That will be retried the
             * next time updateLocalTopology() is called. So just log it.
             */
            logger.log(Level.INFO,
                       "Exception updating local topology: {0}", de);
        }
        return (success == null) ? false : success;
    }

    /**
     * Updates the local topology in a critical situation. If the update fails
     * for any reason the node will be shutdown.
     */
    void criticalUpdate() {
        try {
            if (!updateLocalTopology()) {
                throw new IllegalStateException("Unable to update local " +
                                                "topology in critical section");
            }
        } catch (Exception ex) {
            if (!shutdown) {
                repNode.getExceptionHandler().
                            uncaughtException(Thread.currentThread(), ex);
            }
        }
    }

    /**
     * Starts a thread which will monitor targets of completed migrations.
     */
    synchronized void monitorTarget() {
        if (!isMaster()) {
            return;
        }

        if (targetMonitorExecutor == null) {
            targetMonitorExecutor =
                new TargetMonitorExecutor(this, repNode, logger);
        }
        targetMonitorExecutor.monitorTarget();
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
     * @param <T> type of the return value
     * @param op the operation
     * @param retryIAE true if InsufficientAcksException should be retried
     * @return the value returned by op.call() or null
     */
    <T> T tryDBOperation(DBOperation<T> op, boolean retryIAE) {
        int retryCount = NUM_DB_OP_RETRIES;

        while (!shutdown) {

            try {
                final Database db = migrationDb;

                if (db != null) {
                    return op.call(db);
                }
                if (retryCount <= 0) {
                    return null;
                }
                retrySleep(retryCount, LONG_RETRY_TIME, null);
            } catch (InsufficientAcksException iae) {
                if (!retryIAE) {
                    throw iae;
                }
                retrySleep(retryCount, LONG_RETRY_TIME, iae);
            } catch (InsufficientReplicasException ire) {
                retrySleep(retryCount, LONG_RETRY_TIME, ire);
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

    void setLastMigrationDuration(long duration) {
        lastMigrationDuration = duration;
    }

    /**
     * Return whether existing feeders feeding subscribe stream.
     */
    boolean isStreaming() {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);
        /* No feeder when ReplicatedEnvironment is not initialized */
        if (repEnv == null) {
            return false;
        }

        com.sleepycat.je.rep.impl.node.RepNode jeRepNode =
                            RepInternal.getRepImpl(repEnv).getRepNode();

        /*
         * Only master repnode needs to check whether feeding external
         * nodes
         */
        if (jeRepNode == null || !jeRepNode.isMaster()) {
            return false;
        }

        final FeederManager feederManager = jeRepNode.feederManager();
        /* No feeding when feederManager is not initialized */
        if (feederManager == null) {
            return false;
        }

        for (Feeder feeder : feederManager.activeReplicasMap().values()) {
            if(feeder.getReplicaNode().getType().isExternal()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the specified key belongs to a table which has been
     * dropped. We want to filter records of dropped tables on both the source
     * and target.
     * 
     * @return true if the key belongs to a dropped table
     */
    static boolean isForDroppedTable(RepNode repNode, byte[] key) {
        try {
            repNode.getTableManager().getTable(key);
            return false;
        } catch (DroppedTableException dte) {
            return true;
        }
    }
    
    /**
     * Adds the names of the partition DBs that are targets of migration on
     * this node to the specified set.
     */
    public synchronized void getTargetPartitionDbNames(Set<String> names) {
        for (PartitionId partition : targets.keySet()) {
            names.add(partition.getPartitionName());
        }
    }
    
    /* -- Unit test -- */

    public void setReadHook(TestHook<DatabaseEntry> hook) {
        migrationService.setReadHook(hook);
    }

    public void setResponseHook(TestHook<AtomicReference<Response>> hook) {
        migrationService.setResponseHook(hook);
    }

    @Override
    public String toString() {
        return "MigrationManager[" + repNode.getRepNodeId() +
                ", " + isMaster + ", " + completedSequenceNum + "]";
    }

    /**
     * A database operation that returns a result and may throw an exception.
     *
     * @param <V>
     */
    interface DBOperation<V> {

        /**
         * Invokes the operation. This method may be called multiple times
         * in the course of retrying in the face of failures.
         *
         * @param db the migration db
         * @return the result
         */
        V call(Database db);
    }

    /**
     * Executor for partition migration target threads. Migration targets are
     * queued and run as threads become available. When a target completes
     * it is checked for whether it should be retried, and if so is put back
     * on the queue.
     */
    private class TargetExecutor extends ScheduledThreadPoolExecutor {

        private RepGroupId lastSource = null;
        private long adjustment = 0;

        TargetExecutor() {
            super(concurrentTargetLimit,
                  new KVThreadFactory(" partition migration target",
                                      logger));
            setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }

        /**
         * Submits a new migration target for execution, giving order
         * preference to migrations from different sources.
         *
         * @param target new migration target
         */
        synchronized void submitNew(MigrationTarget target) {

            long delay = 0;

            /*
             * If the source is the same as the last submitted target, schedule
             * it with a small delay, otherwise run it as soon as a thread
             * is available (delay == 0).
             */
            if (target.getSource().equals(lastSource)) {
                delay = MINIMUM_DELAY + adjustment;
                adjustment += MINIMUM_DELAY;
            } else {
                lastSource = target.getSource();
                adjustment = 0;
            }
            schedule(target, delay, "start");
        }

        /**
         * Retries a migration target after a failed execution.
         *
         * @param r the completed target runnable (wrapped in a Future)
         * @param t the exception that caused termination, or null if execution
         * completed normally
         */
        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);

            if (t != null) {
                logger.log(Level.INFO, "Target execution failed", t);
                return;
            }

            if (isShutdown()) {
                return;
            }
            @SuppressWarnings("unchecked")
            final Future<MigrationTarget> f = (Future<MigrationTarget>)r;

            MigrationTarget target = null;
            try {
                target = f.get();
            } catch (Exception ex) {
                logger.log(Level.WARNING, "Exception getting target", ex);
            }

            /* If the target was returned, then it should be re-run. */
            if (target == null) {
                return;
            }

            /*
             * First check to see if we can run. If there is secondary
             * cleaning, exit.
             *
             * Periodic calls by the admin to getStatus() will restart any
             * targets that can't be restarted here.
             */
            if (repNode.getTableManager().busySecondaryCleaning()) {
                logger.log(Level.FINE,
                          "Unable to restart {0}, secondary cleaning " +
                          "operations in progress",
                          target);
                removeTarget(target.getPartitionId());
                return;
            }

            long delay = lastMigrationDuration;

            /*
             * Check for minimum here instead of after getRetryWait()
             * because the configuration parameters may have been set with
             * a time < the minimum.
             */
            if (delay < MINIMUM_DELAY) {
                delay = MINIMUM_DELAY;
            }

            /*
             * If the last migration took less time than the delay then use
             * that time to schedule the next target start.
             */
            if (delay > target.getRetryWait()) {
                delay = target.getRetryWait();
            }

            if (delay < 0) {
                return;
            }
            schedule(target, delay, "restart");
        }

        private void schedule(MigrationTarget target, long delay, String msg) {
            logger.log(Level.FINE, "Scheduling {0} to {1} in {2}ms",
                       new Object[]{target, msg, delay});

            try {
                schedule(target, delay, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                /* Could be due to shutdown, if so, just ignore */
                if (isShutdown()) {
                    logger.log(Level.FINE,
                               "Failed to {0} {1}, executor shutdown",
                               new Object[]{msg, target});
                } else {
                    logger.log(Level.WARNING,
                               "Failed to " + msg + " " + target, ree);
                }
                removeTarget(target.getPartitionId());
            }
        }
    }

    /**
     * Thread to manage replicated environment state changes.
     */
    private class MigrationStateTracker extends StateTracker {

        MigrationStateTracker(Logger logger) {
            super(MigrationStateTracker.class.getSimpleName(),
                  repNode.getRepNodeId(), logger,
                  repNode.getExceptionHandler());
        }

        /**
         * Updates the migration services based on the replicated environment
         * state change. Called sequentially, once per state change. If the
         * change is to something other than a master all services are stopped.
         * If the master, attempt to start all services, retrying if there is a
         * failure. Since this method is not called until the previous call
         * returns, we cannot use the value of isMaster to detect master
         * changes, but instead must check the environment directly.
         */
        @Override
        protected void doNotify(StateChangeEvent sce) {
            if (shutdown.get()) {
                return;
            }
            logger.log(Level.INFO, "Migration manager change state to {0}.",
                       sce.getState());

            synchronized (MigrationManager.this) {
                isMaster = sce.getState().isMaster();
                if (!isMaster) {
                    stopServices(false);
                    return;
                }
            }

            RateLimitingLogger<String> rll = null;

            /*
             * While not shutdown attempt to start the migration services.
             */
            while (!shutdown.get()) {
                final String failureReason = startServices();
                if (failureReason == null) {
                    break;
                }

                if (rll == null) {
                    rll = new RateLimitingLogger<>(60 * 1000, 4, logger);
                }
                rll.log(failureReason, Level.INFO,
                        "Failed to start migration service, " + failureReason +
                        ". Retrying");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    /* Should not happen. */
                    throw new IllegalStateException(ie);
                }
            }
        }
    }

    /**
     * Database trigger registered with the migration DB.
     */
    private class CompletionTrigger implements TransactionTrigger,
                                               ReplicatedDatabaseTrigger {
        private String dbName;

        @Override
        public void repeatTransaction(Transaction t) { }

        @Override
        public void repeatAddTrigger(Transaction t) { }

        @Override
        public void repeatRemoveTrigger(Transaction t) { }

        @Override
        public void repeatCreate(Transaction t) { }

        @Override
        public void repeatRemove(Transaction t) { }

        @Override
        public void repeatTruncate(Transaction t) { }

        @Override
        public void repeatRename(Transaction t, String string) { }

        @Override
        public void repeatPut(Transaction t, DatabaseEntry key,
                              DatabaseEntry newData) { }

        @Override
        public void repeatDelete(Transaction t, DatabaseEntry key) { }

        @Override
        public String getName() {
            return "CompletionTrigger";
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
        public void addTrigger(Transaction t) { }

        @Override
        public void removeTrigger(Transaction t) { }

        @Override
        public void put(Transaction t, DatabaseEntry key, DatabaseEntry oldData,
                        DatabaseEntry newData) { }

        @Override
        public void delete(Transaction t, DatabaseEntry key,
                           DatabaseEntry oldData) { }

        @Override
        public void commit(Transaction t) {
            if (shutdown) {
                return;
            }
            logger.fine("Received commit trigger");

            /* Don't wait, we just care about the state: replica or not */
            final ReplicatedEnvironment env = repNode.getEnv(0);
            try {
                if ((env == null) || !env.getState().isReplica()) {
                    logger.info("Environment changed, ignoring trigger");
                    return;
                }
            } catch (EnvironmentFailureException efe) {
                /* It's in the process of being re-established. */
                logger.info("Environment changing, ignoring trigger");
                return;
            } catch (IllegalStateException ise) {
                /* A closed environment. */
                logger.info("Environment closed, ignoring trigger");
                return;
            }

            /**
             * If an update is required (the completedSequenceNum has been
             * incremented) then the update must succeed. If it does not,
             * an exception should be thrown which will invalidate and
             * restart environment. The failure case this prevents has to do
             * with a read on the replica with a time consistency. In this case
             * if the replica does not know the partition has moved (i.e. the
             * local topology is out of date) the read will wait until time
             * catches up with the master and will then finish the operation
             * using the local partition DB. However, the local partition DB
             * will be out-of-date due to the master having stopped sending
             * updates for that partition because the partition is no longer
             * in the group.
             */
            final PartitionMigrations migrations = getMigrations();

            if (migrations == null) {
                throw new IllegalStateException("unable to access migration " +
                                                "db from commit trigger");
            }
            final long seqNum = migrations.getChangeNumber();

            if (seqNum != completedSequenceNum) {
                logger.info("Partition migration db has been " +
                            "modified, updating local topology");

                if (!updateLocalTopology()) {
                    throw new IllegalStateException("update of local " +
                                                    "topology failed from " +
                                                    "commit trigger");
                }
                completedSequenceNum = seqNum;
            }
        }

        @Override
        public void abort(Transaction t) { }
    }
}

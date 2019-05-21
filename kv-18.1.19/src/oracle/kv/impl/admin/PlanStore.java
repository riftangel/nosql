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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

import oracle.kv.impl.admin.Admin.RunTransaction;
import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;
import oracle.kv.impl.admin.AdminStores.AdminStore;
import oracle.kv.impl.admin.AdminStores.AdminStoreCursor;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.util.SerializationUtil;

/**
 * A base class for implementations of the plan store in Admin.
 */
public abstract class PlanStore extends AdminStore {

    /**
     * Maximin number of plans returned from the multi-plan get methods.
     */
    private static final int MAX_PLANS = 20;
    
    /*
     * Limit on the number of plans maintained in the store.
     *
     * Public for testing.
     */
    public static final int PLAN_LIMIT = 1000;
    
    /*
     * The number of plans which will cause a pruning run. It is > PLAN_LIMIT
     * to keep pruning from happening every time a new plan is written.
     */
    private static final int PRUNE_TRIGGER = PLAN_LIMIT + 10;
    
    /*
     * We use non-sticky cursors here to obtain a slight performance advantage
     * and to run in a deadlock-free mode.
     */
    static final CursorConfig CURSOR_READ_COMMITTED =
        new CursorConfig().setNonSticky(true).setReadCommitted(true);

    public static PlanStore getReadOnlyInstance(Logger logger,
                                                 Environment env) {
        return new PlanDatabaseStore(logger, env, true);
    }

    /* Used for testing only */
    static PlanStore getWritableInstance(Logger logger,
            Environment env) {
    	return new PlanDatabaseStore(logger, env, false);
    }

    /**
     * Creates a PlanStore instance according to the schema version.  If the
     * schema version is earlier than V4, a DPL-based plan store will be
     * returned for compatibility. Otherwise, a JE HA database-based plan
     * store will be returned.
     */
    static PlanStore getStoreByVersion(int schemaVersion,
                                       Admin admin, EntityStore eStore) {
        /*
         * The plan store was migrated back at version 4.
         */
        if (schemaVersion < AdminSchemaVersion.SCHEMA_VERSION_4) {
            assert eStore != null;
            return new PlanDPLStore(admin.getLogger(), eStore);
        }
        return new PlanDatabaseStore(admin.getLogger(), admin.getEnv(),
                                     false /* read only */);
    }

    private PlanStore(Logger logger) {
        super(logger);
    }

    /**
     * Persists a plan into the store. Callers are responsible for
     * exception handling.
     *
     * The method must be called while synchronized on the plan instance
     * to ensure that the plan instance is not modified while the object is
     * being serialized into bytes before being stored into the database. Note
     * that the synchronization hierarchy requires that no other JE locks are
     * held before the mutex is acquired, so the caller to this method must be
     * careful. The synchronization is done explicitly by the caller, rather
     * than making this method synchronized, to provide more flexibility for
     * obeying the synchronization hierarchy.
     *
     * @param txn the transaction in progress
     */
    abstract void put(Transaction txn, Plan plan);

    /**
     * Fetches a plan from the store. Callers are responsible for
     * exception handling.
     */
    abstract Plan get(Transaction txn, int planId);

    /**
     * Returns a cursor for iterating all plans in the store.  Callers are
     * responsible for exception handling, and should close the cursor via
     * {@link PlanCursor#close} after use.
     */
    public abstract PlanCursor getPlanCursor(Transaction txn,
                                             Integer startPlanId);

    /**
     * Fetches all non-terminal Plans as a Map. System plans are not included
     * in the map.
     */
    Map<Integer, Plan> getActivePlans(Transaction txn,
                                      Planner planner,
                                      AdminServiceParams aServiceParams) {
        final Map<Integer, Plan> activePlans = new HashMap<>();

        try (final PlanCursor cursor = getPlanCursor(txn, null)) {
            for (Plan p = cursor.first();
                 p != null;
                 p = cursor.next()) {

                if (p.isSystemPlan()) {
                    continue;
                }

                if (!p.getState().isTerminal()) {
                    p.initializePlan(planner, aServiceParams);
                    activePlans.put(p.getId(), p);
                }
            }
        }
        return activePlans;
    }

    /**
     * Retrieve the beginning plan id and number of plans that satisfy the
     * request.
     *
     * Returns an array of two integers indicating a range of plan id
     * numbers. [0] is the first id in the range, and [1] number of
     * plan ids in the range.
     *
     * Operates in three modes:
     *
     *    mode A requests howMany plans ids following startTime
     *    mode B requests howMany plans ids preceding endTime
     *    mode C requests a range of plan ids from startTime to endTime.
     *
     *    mode A is signified by endTime == 0
     *    mode B is signified by startTime == 0
     *    mode C is signified by neither startTime nor endTime being == 0.
     *        howMany is ignored in mode C.
     *
     * If the owner is not null, only plans with the specified owner will be
     * returned.  System plans are not included.
     */
    int[] getPlanIdRange(Transaction txn,
                         long startTime,
                         long endTime,
                         int howMany,
                         String ownerId) {
        final int[] range = {0, 0};
        final PlanCursor cursor = getPlanCursor(txn, null /* startPlanId */);

        int n = 0;
        try {
            if (startTime == 0L) {
                /* This is mode B. */
                for (Plan p = cursor.last();
                     p != null && n < howMany;
                     p = cursor.prev()) {

                    if (p.isSystemPlan()) {
                        continue;
                    }
                    if (ownerId != null) {
                        final String planOwnerId =
                            p.getOwner() == null ? null : p.getOwner().id();
                        if (!ownerId.equals(planOwnerId)) {
                            continue;
                        }
                    }

                    long creation = p.getCreateTime().getTime();
                    if (creation < endTime) {
                        n++;
                        range[0] = p.getId();
                    }
                }
                range[1] = n;
            } else {
                for (Plan p = cursor.first();
                     p != null;
                     p = cursor.next()) {

                    if (p.isSystemPlan()) {
                        continue;
                    }

                    if (ownerId != null) {
                        final String planOwnerId =
                            p.getOwner() == null ? null : p.getOwner().id();
                        if (!ownerId.equals(planOwnerId)) {
                            continue;
                        }
                    }

                    long creation = p.getCreateTime().getTime();
                    if (creation >= startTime) {
                        if (range[0] == 0) {
                            range[0] = p.getId();
                        }
                        if (endTime != 0L && creation > endTime) {
                            /* Mode C */
                            break;
                        }
                        if (howMany != 0 && n >= howMany) {
                            /* Mode A */
                            break;
                        }
                        n++;
                    }
                }
                range[1] = n;
            }
        } finally {
            cursor.close();
        }

        return range;
    }

    /**
     * Returns a map of plans starting at firstPlanId.  The number of plans in
     * the map is the lesser of howMany, MAXPLANS, or the number of extant
     * plans with id numbers following firstPlanId.  The range is not
     * necessarily fully populated; while plan ids are mostly sequential, it is
     * possible for values to be skipped. If the owner is not null, only plans
     * with the specified owner will be returned. System plans are not included
     * in the map.
     */
    Map<Integer, Plan> getPlanRange(Transaction txn,
                                    Planner planner,
                                    AdminServiceParams aServiceParams,
                                    int firstPlanId,
                                    int howMany,
                                    String ownerId) {
        if (howMany > MAX_PLANS) {
            howMany = MAX_PLANS;
        }

        final Map<Integer, Plan> fetchedPlans = new HashMap<>();

        try (final PlanCursor cursor = getPlanCursor(txn, firstPlanId)) {
            for (Plan p = cursor.first();
                 p != null && howMany > 0;
                 p = cursor.next()) {

                if (p.isSystemPlan()) {
                    continue;
                }
                if (ownerId != null) {
                    final String planOwnerId =
                        p.getOwner() == null ? null : p.getOwner().id();
                    if (!ownerId.equals(planOwnerId)) {
                        continue;
                    }
                }

                p.initializePlan(planner, aServiceParams);
                p.stripForDisplay();
                fetchedPlans.put(p.getId(), p);
                howMany--;
            }
        }
        return fetchedPlans;
    }

    /**
     * Returns the Plan corresponding to the given id,
     * fetched from the database; or null if there is no corresponding plan.
     */
    Plan getPlanById(int id,
                     Transaction txn,
                     Planner planner,
                     AdminServiceParams aServiceParams) {
        final Plan p = get(txn, id);

        if (p != null) {
            p.initializePlan(planner, aServiceParams);
        }
        return p;
    }

    /**
     * Returns the <i>howMany</i> most recent plans in the plan history.
     * The plan instance returned will be stripped of memory intensive
     * components and will not be executable. System plans are not included
     * in the map.
     */
    @Deprecated
    Map<Integer, Plan> getRecentPlansForDisplay(
                                            int howMany,
                                            Transaction txn,
                                            Planner planner,
                                            AdminServiceParams aServiceParams) {
        final Map<Integer, Plan> fetchedPlans = new HashMap<>();

        try (final PlanCursor cursor = getPlanCursor(txn, null)) {
            int n = 0;
            for (Plan p = cursor.last();
                 p != null && n < howMany;
                 p = cursor.prev(), n++) {

                if (p.isSystemPlan()) {
                    continue;
                }
                p.initializePlan(planner, aServiceParams);
                p.stripForDisplay();
                fetchedPlans.put(p.getId(), p);
            }
        }
        return fetchedPlans;
    }

    protected void logFetching(int planId) {
        logger.log(Level.FINE, "Fetching plan using id {0}", planId);
    }

    /**
     * A plan store using the non-DPL
     * {@link oracle.kv.impl.admin.AdminPlanDatabase} as the underlying storage.
     */
    private static class PlanDatabaseStore extends PlanStore {
        
        /* Thread to asynchronously prune plans. */
        private StoppableThread pruner = null;

        /* Highest plan ID seen while writing. */
        private int highestIdSeen = 0;

        /*
         * Estimated number of plans in the store. Initialized to PRUNE_TRIGGER
         * so that pruning is run on the first write after the Admin starts.
         */
        private final AtomicInteger estNumPlans =
                                            new AtomicInteger(PRUNE_TRIGGER);

        private final AdminDatabase<Integer, Plan> planDb;

        private PlanDatabaseStore(Logger logger, Environment env,
                                  boolean readOnly) {
            super(logger);
            planDb = new AdminDatabase<Integer, Plan>(DB_TYPE.PLAN, logger,
                                                      env, readOnly) {
                @Override
                protected DatabaseEntry keyToEntry(Integer key) {
                    final DatabaseEntry keyEntry = new DatabaseEntry();
                    IntegerBinding.intToEntry(key, keyEntry);
                    return keyEntry;
                }};
        }

        @Override
        void put(Transaction txn, Plan plan) {
            final int planId = plan.getId();

            /* The plan ID is the primary key. */
            planDb.put(txn, planId, plan, false);
            logger.log(Level.FINE, "Storing plan {0}", planId);

            /*
             * If writing a new plan, record the new ID and increment the
             * estimated plan count. If over PRUNE_TRIGGER attempt to prune.
             * Note that since highestIdSeen is initialized to 0 and
             * estNumPlan is initialized to PRUNE_TRIGGER, pruning will occur
             * on the fist write (of any kind) after the Admin starts.
             * Also note that plans may be written in non-sequential order (by
             * plan ID). If that happens the count will be off, hence the
             * "estimated" number of plans. Eventually pruning will be
             * triggered which is what is important.
             */
            if (planId > highestIdSeen) {
                highestIdSeen = planId;
                final int numPlans = estNumPlans.incrementAndGet();
                if (numPlans > PRUNE_TRIGGER) {
                    /* Planner can be null during unit testing */
                    final Planner planner = plan.getPlanner();
                    if (planner != null) {
                        /* prunePlans will reset estNumPlans */
                        prunePlans(planner.getAdmin().getEnv());
                    }
                }
            }
        }

        @Override
        Plan get(Transaction txn, int planId) {
            logFetching(planId);
            return planDb.get(txn, planId, LockMode.READ_COMMITTED, Plan.class);
        }

        @Override
        public PlanCursor getPlanCursor(Transaction txn, Integer startPlanId) {
            final Cursor cursor = planDb.openCursor(txn);
            return new PlanCursor(cursor, startPlanId) {

                @Override
                protected Plan entryToObject(DatabaseEntry key,
                                             DatabaseEntry value) {
                    return SerializationUtil.getObject(value.getData(),
                                                       Plan.class);
                }
            };
        }

        @Override
        public void close() {
            planDb.close();
        }

        @Override
        protected void convertTo(int existingVersion, AdminStore newStore,
                                 Transaction txn) {
            /*
             * Plans were converted at schema version 4, which means that
             * this method may be called when the rest of the data is moved
             * over. If not V4 then call super which will throw an exception.
             */
            if (existingVersion != AdminSchemaVersion.SCHEMA_VERSION_4) {
                super.convertTo(existingVersion, newStore, txn);
            }
        }
        
        /**
         * Prunes plans from the store. Plans are pruned in two passes, first
         * system plans are removed, independent of the number of plans. Once
         * system plans are removed, user plans are pruned back to the
         * PLAN_LIMIT. Once pruning is complete, estNumPlans is set to the
         * number of plans found duing scans.
         * 
         * System plans are pruned first in an attempt to keep as many user
         * plans as possible, for as long as possible.
         */
        private synchronized void prunePlans(ReplicatedEnvironment env) {
            if ((pruner != null) && pruner.isAlive()) {
                return;
            }
            pruner = new PlanPrunningThread(env);
            pruner.start();
        }
    
        private class PlanPrunningThread extends StoppableThread {

            private final ReplicatedEnvironment env;

            PlanPrunningThread(ReplicatedEnvironment env) {
                super("PlanPruningThread");
                this.env = env;
            }

            @Override
            public void run() {
                final long startTime = System.currentTimeMillis();
                logger.log(Level.FINE, "Starting {0}", this);

                /*
                 * Prune system plans first. By passing in MAX_VALUE as the
                 * target, the scan will read all plans, and result.numPlans
                 * will be the actual number of plans in the store.
                 */
                final PruneResult result = prunePlans(Integer.MAX_VALUE, true);
                
                /* If the # of plans is over the limit prune user plans */
                final int excess = result.numPlans - PLAN_LIMIT;
                if (excess > 0) {
                    final PruneResult user = prunePlans(excess, false);
                    
                    /* Adjust the previous result */
                    result.numPlans -= user.numDeleted;
                    result.numDeleted += user.numDeleted;
                    
                }
                estNumPlans.set(result.numPlans);
                final long duration = System.currentTimeMillis() - startTime;
                logger.log(Level.FINE,
                           "Plan store contains {0} plans, scan took {1}ms," +
                           " {2} plans were deleted",
                           new Object[] {result.numPlans, duration,
                                         result.numDeleted});
            }

            /**
             * Scans the plan DB and removes plans which can be removed. Up to
             * target number of plans will be removed. The number of plans
             * removed and the number found before the target was reached
             * is returned in PruneResult.
             * If pruneSystem is true, only system plans will be removed,
             * otherwise any plans will be removed.
             */
            private PruneResult prunePlans(int target, boolean pruneSystem) {
                final PruneResult result = new PruneResult();
                
                final String type = pruneSystem ? "system" : "user";
                
                /* ID of last plan scanned */
                final AtomicInteger lastPlanId = new AtomicInteger();

                /*
                 * Loop reading the plans from the DB. Each iteration will
                 * scan up to 100 plans in order to keep the transaction short.
                 */
                Boolean cont = true;
                while (cont) {
                    cont = new RunTransaction<Boolean>(env,
                                                       RunTransaction.sync,
                                                       logger) {
                        @Override
                        Boolean doTransaction(Transaction txn) {
                            final int start = lastPlanId.incrementAndGet();
                            try (final PlanCursor cursor =
                                                    getPlanCursor(txn, start)) {
                                /* per-pass counts */
                                int nPlans = 0;
                                int nDeletedPlans = 0;

                                for (Plan p = cursor.first();
                                     p != null;
                                     p = cursor.next()) {

                                    lastPlanId.set(p.getId());

                                    if (canPrune(p, pruneSystem)) {
                                        cursor.delete();
                                        nDeletedPlans++;
                                        result.numDeleted++;
                                        
                                        /* Done if the target is reached */
                                        if (result.numDeleted >= target) {
                                            /* Will return false */
                                            break;
                                        }
                                    } else {
                                        result.numPlans++;
                                    }

                                    /* Cap this pass at 100 */
                                    nPlans++;
                                    if (nPlans >= 100) {
                                        logger.log(Level.FINE,
                                                   "Scanned from plan {0} to" +
                                                   " {1}, removed {2} {3}" +
                                                   " plans",
                                                   new Object[] {start,
                                                                 lastPlanId,
                                                                 nDeletedPlans,
                                                                 type});  
                                        return true; /* continue */
                                    }
                                }
                                /* Done scanning */
                                logger.log(Level.FINE,
                                           "Final scan from plan {0} to" +
                                           " {1}, removed {2} {3} plans",
                                           new Object[] {start, lastPlanId,
                                                         nDeletedPlans, type});  
                                return false;
                            }
                        }
                    }.run();
                }
                return result;
            }

            /**
             * Returns true if the specified plan can be pruned (in a terminal
             * state). If pruneSystem is true then the plan can be pruned if
             * it is a system plan and is in the terminal, ERROR, or
             * INTERRUPTED state.
             */
            private boolean canPrune(Plan p, boolean pruneSystem) {
                final Plan.State state = p.getState();
                return pruneSystem ? (p.isSystemPlan() &&
                                      (state.isTerminal() ||
                                       state.equals(Plan.State.ERROR) ||
                                       state.equals(Plan.State.INTERRUPTED))) :
                                     state.isTerminal();
            }
            
            @Override
            protected Logger getLogger() {
                return logger;
            }
            
            private class PruneResult {
                int numPlans = 0;
                int numDeleted = 0;
            }
        }
    }

    /**
     * A provisional plan store used when not all the nodes in the store have
     * been upgraded to R3.1.0 or later.  The underlying storage is the
     * EntityStore.
     */
    private static class PlanDPLStore extends PlanStore {
        private final EntityStore eStore;

        private PlanDPLStore(Logger logger, EntityStore eStore) {
            super(logger);
            this.eStore = eStore;
        }

        @Override
        public void put(Transaction txn, Plan plan) {
            readOnly();
        }

        @Override
        public Plan get(Transaction txn, int planId) {
            logFetching(planId);
            final PrimaryIndex<Integer, AbstractPlan> pi =
                eStore.getPrimaryIndex(Integer.class, AbstractPlan.class);
            return pi.get(txn, planId, LockMode.READ_COMMITTED);
        }

        @Override
        public PlanCursor getPlanCursor(Transaction txn, Integer startPlanId) {
            final PrimaryIndex<Integer, AbstractPlan> pi;
            pi = eStore.getPrimaryIndex(Integer.class, AbstractPlan.class);

            final Cursor cursor =
                pi.getDatabase().openCursor(txn, CURSOR_READ_COMMITTED);

            return new PlanCursor(cursor, startPlanId) {

                /*
                 * We need to use EntityBinding to convert a database entry
                 * to a plan object in DPL store.
                 */
                private final EntityBinding<AbstractPlan> planBinding =
                    eStore.getPrimaryIndex(Integer.class, AbstractPlan.class).
                        getEntityBinding();

                @Override
                protected Plan entryToObject(DatabaseEntry key,
                                             DatabaseEntry value) {
                    return planBinding.entryToObject(key, value);
                }
            };
        }

        @Override
        protected void convertTo(int existingVersion, AdminStore newStore,
                                 Transaction txn) {
            final PlanStore newPlanStore = (PlanStore)newStore;
            final PrimaryIndex<Integer, AbstractPlan> pi =
                eStore.getPrimaryIndex(Integer.class, AbstractPlan.class);

            try (final EntityCursor<AbstractPlan> cursor =
                                                    pi.entities(txn, null)) {
                for (AbstractPlan p : cursor) {
                    if (existingVersion < AdminSchemaVersion.SCHEMA_VERSION_3) {
                        p.upgradeToV3();
                    }
                    newPlanStore.put(txn, p);
                }
            }
        }
    }

    /**
     * A cursor class to facilitate the scan of the plan store.
     */
    public abstract static class PlanCursor
            extends AdminStoreCursor<Integer, Plan> {

        private PlanCursor(Cursor cursor,  Integer startKey) {
            super(cursor, startKey);
        }

        @Override
        protected void keyToEntry(Integer key, DatabaseEntry entry) {
            IntegerBinding.intToEntry(key, entry);
        }
    }
}

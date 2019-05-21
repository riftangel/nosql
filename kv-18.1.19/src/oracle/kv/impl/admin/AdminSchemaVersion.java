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

import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.VersionUtil;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;

/**
 * The Admin service stores an integer which represents the version of the
 * schema of the Admin database. It's intentionally held within a non-DPL
 * database separate from the database that holds plans, params and the memo,
 * so that it can safely be used to check the version of that DPL store.
 *
 * Admin services can only be run on databases which have equal or older
 * schemas. They cannot run on newer schemas. To read older schemas, newer
 * software may not need to do anything at all (for example, new plans were
 * added) or may have to do some conversion if there have been semantic
 * changes. DPL changes may also require writing explicit converters, as
 * required by DPL rules.
 */
public class AdminSchemaVersion {

    /*
     * The first version of NoSQL DB did not have this mechanism. It is
     * implicitly schema version 1.
     *
     * changes in schema version 2:
     *  - add removeSNPlan
     * changes in schema version 3:
     *  - add repfactor field to Datacenter Component
     *  - add new plans and tasks: DeployTopoPlan, DeployShard,
     *    DeployNewRN, MigratePartition
     *  - store topologies in their own store, keyed by name.
     *  - TaskRun stores task
     * changes in schema version 4:
     *  - move all plan data from entity store to non-DPL database
     * changes in schema version 5:
     *  - move all Admin persistent data from Entity store to non-DPL database
     */
    static final int SCHEMA_VERSION_3 = 3;
    public static final int SCHEMA_VERSION_4 = 4;
    public static final int SCHEMA_VERSION_5 = 5;
    public static final int CURRENT_SCHEMA = SCHEMA_VERSION_5;

    /*
     * There are two records in the version db. These two initial record types
     * cannot ever be deleted. If we need more metadata in the future, we can
     * add records, but would have to use the schema version to know to read
     * them.
     *   key="schemaVersion", data=<number>
     *   key="softwareVersion", data=<kvstore version>
     * The software version is used mainly to help construct an informative
     * error message.
     */
    private static final String DB_NAME = "AdminSchemaVersion";
    private static final String SCHEMA_VERSION_KEY = "schemaVersion";
    private static final String SOFTWARE_VERSION_KEY = "softwareVersion";

    /*
     * If no schema DB is present, or there is no version in the DB, we
     * assume R1 (1.2.123).
     */
    private static final KVVersion FIRST_KV_VERSION = KVVersion.R1_2_123;

    private final Admin admin;
    private final ReplicatedEnvironment repEnv;
    private final Logger logger;

    public AdminSchemaVersion(Admin admin, Logger logger) {
        this.logger = logger;
        this.admin = admin;
        this.repEnv = admin.getEnv();
    }

    /**
     * Throws IllegalStateException if the on-disk schema version is newer than
     * the version supported by this software package. If this node is the
     * master, update the schema version if the version is older.
     */
    void checkAndUpdateVersion(Transaction txn, boolean isMaster,
                               AdminStores stores) {

        /*
         * First check to see if the environment is empty.  This is how we know
         * whether we are starting from scratch or upgrading an existing
         * environment.
         */
        final List<String> dbNames = repEnv.getDatabaseNames();
        if (dbNames.isEmpty()) {
            if (isMaster) {
                initSchemaDB(txn);
            }
            return;
        }

        /*
         * If the environment contains some databases, then it might or might
         * not contain a database named AdminSchemaVersion.  If it does not,
         * then we are dealing with a version 1 environment.  Otherwise, we'll
         * read the version number from the AdminSchemaVersion database.
         */
        int existingVersion;
        KVVersion existingKVVersion;

        if (!dbNames.contains(DB_NAME)) {
            existingVersion = 1;
            existingKVVersion = FIRST_KV_VERSION;
        } else {
            final DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(false);
            dbConfig.setTransactional(true);
            dbConfig.setReadOnly(true);
            try (final Database versionDb =
                                 repEnv.openDatabase(null, DB_NAME, dbConfig)) {
                existingVersion = readSchemaVersion(txn, versionDb);
                existingKVVersion = readSoftwareVersion(txn, versionDb);

                if (existingVersion > CURRENT_SCHEMA) {

                    throw new IllegalStateException
                        ("This Admin Service software is at " +
                         KVVersion.CURRENT_VERSION.getNumericVersionString() +
                         ", schema version " + CURRENT_SCHEMA +
                         " but the stored schema is at version " +
                         existingVersion + "/" +
                         existingKVVersion.getNumericVersionString() +
                         ". Please upgrade this node's NoSQL Database" +
                         " software version to "
                         + existingKVVersion.getNumericVersionString() +
                         " or higher.");
                }
            }
        }

        /* If the version has not changed, nothing else to do */
        if (existingKVVersion.equals(KVVersion.CURRENT_VERSION)) {
            assert existingVersion == CURRENT_SCHEMA;
            return;
        }

        /*
         * This is an upgrade (or downgrade) situation so make sure it is
         * legal. (The SNA should prevent improper upgrades)
         */
        VersionUtil.checkUpgrade(existingKVVersion);

        /* Attempt to upgrade the db only if the version is newer */
        if (existingKVVersion.compareTo(KVVersion.CURRENT_VERSION) >= 0) {
            return;
        }

        /* Further operations need the store. */
        stores.init(existingVersion, false);

        /* Get the software versions of the other Admins */
        final Map<AdminId, KVVersion> adminVersions =
                                                admin.getOtherAdminVersions();
        if (adminVersions == null) {
            logger.info("Unable to confirm the versions of all Admins, " +
                        "not ready for version DB update now.");
            monitorUpgrade(isMaster, existingVersion);
            return;
        }

        /* Potential targets for master transfer */
        final Set<String> targets = new HashSet<>();

        if (isMaster) {
            /*
             * When upgrading from a DPL version the update will depend on
             * whether the other Admins have also been upgraded.
             */
            if (updateSchemaVersion(existingVersion, existingKVVersion,
                                    txn, stores, adminVersions)) {
                monitorUpgrade(isMaster, existingVersion);
                return;
            }
            assert existingVersion < SCHEMA_VERSION_5;

            /*
             * Upgrade from DPL failed. This will result in the Admin being in
             * read-only mode. Try to transfer master to a non-upgraded
             * Admin so that write operations can happen.
             */
            for (Entry<AdminId, KVVersion> e : adminVersions.entrySet()) {
                if (e.getValue().compareTo(AdminStores.DPL_CONVERSION) < 0) {
                    targets.add(e.getKey().getFullName());
                }
            }
            assert !targets.isEmpty();
        } else {

            /*
             * A replica. Check to see if this node should become the master
             * in order to support new features.
             *
             * If any other Admins are at the pre DPL-conversion version don't
             * attempt to become the master as this would result in the Admin
             * being read-only, and then doing another MT (see above).
             */
            for (KVVersion kv : adminVersions.values()) {
                if (kv.compareTo(AdminStores.DPL_CONVERSION) < 0) {
                    return;
                }
            }

            try {
                /*
                 * If the master is already upgraded, don't bother transfering
                 */
                if (admin.checkAdminMasterVersion(KVVersion.CURRENT_VERSION)) {
                    return;
                }
            } catch (AdminFaultException afe) {
                /*
                 * Returns true if could not determine, so as not to initiate
                 * master transfer to avoid the possible infinite MT loop issue
                 */
                logger.info("Unable to confirm the version of current admin " +
                            "master.");
                return;
            }

            logger.log(Level.INFO,
                       "Admin master has not upgraded to current version of " +
                     "{0}. Try to become master to support upgrade operations.",
                       KVVersion.CURRENT_VERSION.getNumericVersionString());
            targets.add(repEnv.getNodeName());
        }

        try {
            final ReplicationGroupAdmin repGroupAdmin =
                new ReplicationGroupAdmin(
                    repEnv.getGroup().getName(),
                    repEnv.getRepConfig().getHelperSockets(),
                    admin.getRepNetConfig());
            repGroupAdmin.transferMaster(targets,
                                         1, TimeUnit.MINUTES,
                                         false /* Don't preempt an ongoing
                                                  MT operation. */);
            /*
             * Success. There will eventually be a master transition
             * to or from this node.
             */
            logger.log(Level.INFO,
                       "Master transfer initiated due to upgrade to {0}",
                       KVVersion.CURRENT_VERSION.getNumericVersionString());
            return;
        } catch (MasterTransferFailureException mtfe) {
            /*
             * This could be because some other replica beat us to it.
             * Failing to transfer is not fatal.
             */
            logger.log(Level.INFO, "Attempt to transfer master failed: {0}",
                       mtfe.getMessage());
        } catch (Exception ex) {
            /* Failing to transfer is not fatal. */
            logger.log(Level.INFO, "Attempt to transfer master failed", ex);
        }
        /*
         * There was some problem with MT, so monitor the upgrade if necessary
         */
        monitorUpgrade(isMaster, existingVersion);
    }

    /**
     * Create the version database. Should only be called the first time the
     * admin database is created, at bootstrap; will fail if the version
     * database already exists.
     */
    private void initSchemaDB(Transaction txn) {
        logger.log(Level.INFO,
                 "Initializing Admin Schema to schema version {0}/NoSQL DB {1}",
                 new Object[]{CURRENT_SCHEMA, KVVersion.CURRENT_VERSION});

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        dbConfig.setTransactional(true);
        try (Database versionDb = repEnv.openDatabase(txn, DB_NAME, dbConfig)) {
            writeSchemaVersion(CURRENT_SCHEMA, txn, versionDb);
            writeSoftwareVersion(KVVersion.CURRENT_VERSION, txn, versionDb);
        }
    }

    /**
     * Update the version database, creating it if necessary. Returns true
     * if the upgrade was successful. The upgrade can fail if the node is
     * being upgraded from a DPL version but not all nodes have been
     * upgraded.
     */
    private boolean updateSchemaVersion(int existingVersion,
                                        KVVersion existingKVVersion,
                                        Transaction txn,
                                        AdminStores stores,
                                        Map<AdminId, KVVersion> adminVersions) {
        /* Determine the minimum version of the other Admins in the group */
        KVVersion groupKVVersion = KVVersion.CURRENT_VERSION;
        for (KVVersion kv : adminVersions.values()) {
            if (kv.compareTo(groupKVVersion) < 0) {
                groupKVVersion = kv;
            }
        }

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        try (Database versionDb = repEnv.openDatabase(txn, DB_NAME, dbConfig)) {

            /*
             * If we are converting from DPL, check to make sure all other
             * Admins have been upgraded. If not, then the conversion to
             * non-DPL stores can't be done.
             */
            if (existingVersion < SCHEMA_VERSION_5) {

                if (groupKVVersion.compareTo(AdminStores.DPL_CONVERSION) < 0) {
                    logger.log(Level.INFO,
                               "Unable to upgrade to Admin DB version {0} " +
                               "while not all Admins in the store have " +
                               "version of {1} or later.",
                               new Object[]{CURRENT_SCHEMA,
                                            AdminStores.DPL_CONVERSION.
                                                    getNumericVersionString()});
                    return false;
                }
                logger.log(Level.INFO,
                           "Converting stores from schema version {0} to {1}",
                           new Object[] {existingVersion, CURRENT_SCHEMA});

                stores.convertTo(existingVersion, txn);
            }
            logger.log(Level.INFO,
                       "Updating Admin Schema version from schema version " +
                       "{0}/NoSQL DB {1} to schema version {2}/NoSQL DB {3}",
                       new Object[]{existingVersion,
                                    existingKVVersion.getNumericVersionString(),
                                    CURRENT_SCHEMA, groupKVVersion});

            writeSchemaVersion(CURRENT_SCHEMA, txn, versionDb);
            writeSoftwareVersion(groupKVVersion, txn, versionDb);
        }
        return true;
    }

    /**
     * Opens and reads the schema version
     */
    public int openAndReadSchemaVersion() {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(false);
        dbConfig.setTransactional(true);
        dbConfig.setReadOnly(true);
        Database versionDb = null;
        Transaction txn = null;
        int version = 0;
        try {
            txn = repEnv.beginTransaction
                (null,
                 new TransactionConfig().setDurability(Durability.COMMIT_SYNC));
            versionDb = repEnv.openDatabase(txn, DB_NAME, dbConfig);
            version = readSchemaVersion(txn, versionDb);
            txn.commit();
        } finally {
            if (versionDb != null) {
                versionDb.close();
            }
            TxnUtil.abort(txn);
        }
        return version;
    }

    /*
     * Reads the software version.
     */
    private KVVersion readSoftwareVersion(Transaction txn, Database versionDb) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();

        StringBinding.stringToEntry(SOFTWARE_VERSION_KEY, key);
        OperationStatus status =
                versionDb.get(txn, key, value, LockMode.DEFAULT);
        if (status == OperationStatus.SUCCESS) {
            return KVVersion.parseVersion(StringBinding.entryToString(value));
        }

        /* If no version record exists, this was version 1.2.124 */
        return FIRST_KV_VERSION;
    }

    /*
     * Writes the software version.
     */
    private void writeSoftwareVersion(KVVersion kvVersion,
                                      Transaction txn, Database versionDb) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();

        StringBinding.stringToEntry(SOFTWARE_VERSION_KEY, key);
        StringBinding.stringToEntry(kvVersion.toString(), value);
        versionDb.put(txn, key, value);
    }
    /*
     * Reads the schema version.
     */
    private int readSchemaVersion(Transaction txn, Database versionDb) {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();

        StringBinding.stringToEntry(SCHEMA_VERSION_KEY, key);
        OperationStatus status =
                versionDb.get(txn, key, value, LockMode.DEFAULT);
        if (status == OperationStatus.SUCCESS) {
            return IntegerBinding.entryToInt(value);
        }

        /*
         * If no version record exists, this was schema version 1, which did
         * not have this mechanism.
         */
        return 1;
    }

    /*
     * Writes the schema version to a new value in db.
     */
    private void writeSchemaVersion(int newVersion,
                                    Transaction txn,
                                    Database versionDb) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();

        StringBinding.stringToEntry(SCHEMA_VERSION_KEY, key);
        IntegerBinding.intToEntry(newVersion, value);
        versionDb.put(txn, key, value);
    }

    /*
     * Conditionaly requests the Admin to monitor the upgrade process. In the
     * case that this is a master and we are converting from DPL, we must
     * wait until all Admins have been upgraded before doing the conversion.
     * This avoids getting into a loop doing master transfers hoping someone
     * does the conversion.
     */
    private void monitorUpgrade(boolean isMaster, int existingVersion) {
        /*
         * Only monitor upgrade if we are the master and the DPL conversion
         * needs to be done.
         */
        if (isMaster && (existingVersion < SCHEMA_VERSION_5)) {
            admin.monitorUpgrade();
        }
    }
}

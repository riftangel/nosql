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

package oracle.kv.impl.util;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.plan.VerifyDataPlan;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.ResourceId;

import com.sleepycat.je.CorruptSecondariesException;
import com.sleepycat.je.Database;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.util.DbVerifyLog;

/**
 * Collection of utilities for JE Database operations
 */
public class DatabaseUtils {

    /**
     * Prevent instantiation.
     */
    private DatabaseUtils() {
    }

    /**
     * Handles an exception opening a replicated DB. Returns
     * true if the open should be retried otherwise the exception is
     * re-thrown.
     *
     * @param re the exception from the open
     * @param logger a logger
     * @param dbName name of DB that was opened
     * @return true if the open should be retried
     */
    public static boolean handleException(RuntimeException re,
                                          Logger logger,
                                          String dbName) {
        try {
            throw re;
        } catch (ReplicaWriteException | UnknownMasterException de) {

            /*
             * Master has not had a chance to create the database as
             * yet, or the current environment (in the replica, or
             * unknown) state is lagging or the node has become a
             * replica. Wait, giving the environment
             * time to catch up and become current.
             */
            logger.log(Level.FINE,
                       "Failed to open database for {0}. {1}",
                       new Object[] {dbName, de.getMessage()});
            return true;
        } catch (InsufficientReplicasException ire) {
            logger.log(Level.FINE,
                       "Insufficient replicas when creating " +
                       "database {0}. {1}",
                       new Object[] {dbName, ire.getMessage()});
            return true;
        } catch (InsufficientAcksException iae) {
            logger.log(Level.FINE,
                       "Insufficient acks when creating database {0}. {1}",
                       new Object[] {dbName, iae.getMessage()});
            /*
             * Database has already been created locally, ignore
             * the exception.
             */
            return false;
        } catch (IllegalStateException ise) {
            logger.log(Level.FINE,
                       "Problem accessing database {0}. {1}",
                       new Object[] {dbName, ise.getMessage()});
            return true;
        } catch (LockTimeoutException lte) {
            logger.log(Level.FINE, "Failed to open database for {0}. {1}",
                       new Object[] {dbName, lte.getMessage()});
            return true;
        } catch (LockNotAvailableException lna) {
            logger.log(Level.FINE, "Failed to open database for {0}. {1}",
                       new Object[] {dbName, lna.getMessage()});
            return true;
        }
    }

    /*
     * Resets the members of the JE replication group, replacing the group
     * members with the single member associated with the specified
     * environment.  This method does what DbResetRepGroup.reset does, but
     * using the specified configuration properties rather reading the
     * configuration from the environment directory.  Note that the
     * configuration arguments will be modified.
     *
     * @param envDir the node's replicated environment directory
     * @param envConfig the environment configuration
     * @param repConfig the replicated environment configuration
     * @see com.sleepycat.je.rep.util.DbResetRepGroup#reset
     */
    /* TODO: Consider creating a JE entrypoint to do this */
    public static void resetRepGroup(File envDir,
                                     EnvironmentConfig envConfig,
                                     ReplicationConfig repConfig) {
        final Durability durability =
            new Durability(Durability.SyncPolicy.SYNC,
                           Durability.SyncPolicy.SYNC,
                           Durability.ReplicaAckPolicy.NONE);

        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(durability);
        repConfig.setHelperHosts(repConfig.getNodeHostPort());

        /* Force the re-initialization upon open. */
        repConfig.setConfigParam(RepParams.RESET_REP_GROUP.getName(), "true");

        /* Open the environment, thus replacing the group. */
        final ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envDir, repConfig, envConfig);

        repEnv.close();
    }

    /**
     * Returns true if the specified database handle needs to be refreshed.
     * Specifically, true is returned if db is null or the database's
     * environment is null or does not match the current environment.
     *
     * @param db the database to check, or null
     * @param current the current environment
     * @return true if the specified database handle needs refreshing
     */
    public static boolean needsRefresh(Database db, Environment current) {
        if (db == null) {
            return true;
        }

        final Environment dbEnv = db.getEnvironment();
        if (dbEnv == null) {
            return true;
        }

        /* If the old and current envs match, no need to refresh */
        if (dbEnv == current) {
            return false;
        }

        /* The old and current env are different, the old should be invalid */
        if (dbEnv.isValid()) {
            throw new IllegalStateException("Database needs refreshing, but " +
                                            "references a valid environment");
        }
        return true;
    }

    /**
     * Verify data for a node.
     *
     * @param env the environment for the node
     * @param verifyBtree verifies the btree of databases
     * @param verifyLog verifies the log files
     * @param verifyIndex verifies the index
     * @param verifyRecord verifies the data records in disk
     * @param btreeDelay delay between batches for btree verification
     * @param logDelay delay between log file reads
     * @param logger
     * @throws IOException
     */
    public static void verifyData(ReplicatedEnvironment env,
                                  ResourceId id,
                                  boolean verifyBtree,
                                  boolean verifyLog,
                                  boolean verifyIndex,
                                  boolean verifyRecord,
                                  long btreeDelay,
                                  long logDelay,
                                  Logger logger)
        throws IOException {
        logger.info("Stop running scheduled verification.");
        EnvironmentMutableConfig mutableConfig = env.getMutableConfig();
        String oldScheduledConfig =
            mutableConfig.getConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER);
        try {
            /* cancel any running scheduled verification */
            mutableConfig.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER,
                                         "false");

            /* btree verification */
            if (verifyBtree) {
                VerifyConfig config = new VerifyConfig();
                if (btreeDelay >= 0) {
                    config.setBatchDelay(btreeDelay, TimeUnit.MILLISECONDS);
                }
                config.setVerifyDataRecords(verifyRecord);
                config.setVerifySecondaries(verifyIndex);

                logger.info("Start JE btree verification.");
                /* for testing */
                assert TestHookExecute.doHookIfSet(
                    VerifyDataPlan.VERIFY_HOOK,
                    new Object[] { id, env });
                /* Start JE verifying */
                env.verify(config, null /* out */);

            }

            /* log file verification */
            if (verifyLog) {

                DbVerifyLog logVerify = new DbVerifyLog(env);
                if (logDelay >= 0) {
                    logVerify.setReadDelay(logDelay, TimeUnit.MILLISECONDS);
                }

                logger.info("Start JE log verification.");
                /* for testing */
                assert TestHookExecute.doHookIfSet(
                    VerifyDataPlan.VERIFY_HOOK,
                    new Object[] { id, env });
                /* Start JE verifying */
                logVerify.verifyAll();
            }

        } catch (EnvironmentFailureException e) {
            /* persistent corruption exists */
            if (e.isCorrupted()) {
                logger.severe("Persistent corrupiton detected. " +
                              "This node will be shut down. " +
                              "Please do the network restore");

            } else {
                /*
                 * Transient corruption happens.
                 */
                logger.severe("Transient corruption detected. " +
                              "This node will restart automatically");
            }
            throw e;
        } catch (CorruptSecondariesException e) {
            /*
             * Index corruption exists.
             */
            logger.severe("Index corruption detected. " +
                          "This node will restart automatically. " +
                          "Please try the network restore to rebuild indexes.");
            throw e;

        } catch (IOException e) {
            throw e;
        } finally {
            /*
             * set the configuration for scheduled verification back to previous
             * value.
             */
            mutableConfig.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER,
                                         oldScheduledConfig);
        }
    }
}

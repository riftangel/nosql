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

package oracle.kv.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import oracle.kv.BulkWriteOptions;
import oracle.kv.EntryStream;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValue;
import oracle.kv.LoginCredentials;
import oracle.kv.Value;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.SecurityStore;
import oracle.kv.impl.admin.TableStore;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.login.KVSessionManager;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.KVStoreLogin.CredentialsProvider;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.Table;
import oracle.kv.util.shell.ShellException;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.DiskOrderedCursorProducerException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * Load from a snapshot directory (source) to a target store. For a discussion
 * of how snapshots are created, please refer to
 *
 * <a href="https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/18.1&id=NSADM-GUID-31470932-7DEE-4D2A-ACCB-B33C352E7C10">
 * Backing Up the Store</a>
 *
 * Note that due to changes in the Admin's persistent store, this utility is
 * not compatible with releases prior to R4.0.
 *
 * This utility can be invoked in one of two ways:
 * <ol>
 * <li>To load just the metadata</li> from the admin database
 * <li>To load the contents of the store itself</li>
 * </ol>
 *
 * <p>
 * In a typical scenario, an empty store is first initialized with the admin
 * metadata which contains the users, tables and indexes and is followed by the
 * actual load of the data, which populates the tables and indexes. If the new
 * store is secure, the admin metadata should be loaded before security is
 * enabled on the store.
 * <p>
 * <h1>Load Admin Metadata Command Line Usage:</h1>
 *
 * <p>
 * Load -store &LTstorename&GT -host &LThostname&GT -port &LTport&GT
 * -load-admin -source &LTadmin-backup-dir&GT [-force] [-username &LTuser&GT]
 * [-security &LTsecurity-file-path&GT] [-verbose]
 * </p>
 *
 * <h2>Arguments:</h2>
 *
 * <p>
 * -store &LTstorename&GT identifies the new store which is the target of the
 * load
 * <p>
 * -host &LThostname&GT identifies a host in the store
 * <p>
 * -port &LTport&GT identifies the port associated with the host used by NoSQL
 * <p>
 * -username &LTuser&GT the username to log in as.
 * <p>
 * -security-file &LTsecurity-file-path&GT denotes the file that specifies the
 * properties to be used for store login, if &LTstorename&GT denotes a secure
 * store.
 * <p>
 * -load-admin indicates that it's only admin metadata that is to be loaded.
 * <p>
 * -source &LTadmin-backup-dir&GT the admin snapshot directory containing the
 * contents of the admin metadata that is to be loaded into &LTstorename&GT.
 * <p>
 * The -force flag will cause the -load-admin flag to overwrite existing
 * metadata, which it will not do by default.
 *
 * <p>
 * <h1>Load Store Data Command Line Usage:</h1>
 * <p>
 *
 * The following invocation is used to load data into a store that has already
 * been initialized with schema that matches the data to be loaded:
 *
 * <p>
 * Load -store &LTstorename&GT -host &LThostname&GT -port &LTport&GT -source
 * &LTshard-backup-dir&GT[, &LTshard-backup-dir&GT]* [-checkpoint
 * &LTcheckpoint-files-directory&GT] [-username &LTuser&GT]
 * [-security &LTsecurity-file-path&GT] [-verbose]
 * </p>
 * <h2>Arguments:</h2>
 * <p>
 * The -store, -host, -port, -username and -security-file arguments are as
 * described earlier.
 * <p>
 * -source &LTshard-backup-dir&GT[, &LTshard-backup-dir&GT]* lists the snapshot
 * directories associated with each shard in the store. There's typically one
 * entry for each shard in the old store. These backup directories typically
 * represent the contents of snapshots created using the snapshot commands
 * described at: <a href="https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/18.1&id=NSADM-GUID-2B5A8F1D-9647-4F9E-8477-B1BC283C6E69">
 * Taking a Snapshot</a>
 * <p>
 * -checkpoint &LTcheckpoint-files-directory&GT The utility used this directory
 * to checkpoint its progress on a periodic basis. If the load process is
 * interrupted for some reason, e.g. the machine running the load fails, the
 * progress checkpoint information is used to skip data that had already been
 * loaded when the load utility is subsequently re-executed with the same
 * arguments. If the -checkpoint flag is not specified, progress will not be
 * checkpointed and all the data in the partitions that were already loaded
 * will be reread. The &LTcheckpoint-files-directory&GT, if specified must
 * exist. *
 * <p>
 * The {@link oracle.kv.table.TableAPI#put bulk put API} is used by this
 * utility to load data into the target store. An input EntryStream of bulk put
 * supplies the records of the databases in a backup directory, if there are
 * multiple backup directories specified, then there are multiple concurrent
 * EntryStreams, one for each shard directory. To recreate the complete
 * contents of the store, you must specify one directory per shard for each
 * shard associated with the store.
 * <p>
 * Note that bulk put API internally uses a cache to sort and assemble entries
 * into batches to load, the cache size is calculated base on the max Java heap
 * size of the load process, so it's best to set max heap size using -Xmx when
 * running the load process. The value for -Xmx must typically be 20% greater
 * than the size of the heap used by the RN that generated the snapshot, so
 * that the Load utility itself has the memory it needs for batching intermediate
 * results for good load performance.
 *
 * <p>
 * The load utility is highly parallelized and utilizes thread parallelism as
 * described earlier to make maximum use of the resources available on a single
 * machine. To further boost load performance, you can choose to run multiple
 * concurrent invocations of the load utility on different machines, and assign
 * each invocation a non-overlapping subset of the shard directories, via the
 * -source argument. The use of these additional machine resources could
 * significantly decrease overall elapsed load times.
 * <p>
 * Note that creating multiple processes on the same machine is unlikely to be
 * beneficial and could be detrimental, since the two processes are likely to
 * be contending for the same CPU and network resources. The main reason for
 * having multiple processes on a single machine is to reduce the java heap
 * size and resulting GC overheads.
 *
 */
public class Load implements CredentialsProvider {

    /* External commands, for "java -jar" usage. */
    public static final String COMMAND_NAME = "load";

    public static final String COMMAND_DESC =
        "Loads Admin metadata or data into a store from backup directories.";

    private static final String LOAD_ADMIN_COMMAND_DESC=
        "Loads Admin metadata into a store from backup directory.";

    private static final String LOAD_ADMIN_COMMAND_ARGS =
        CommandParser.getStoreUsage() + " " +
        CommandParser.getHostUsage() + " " +
        CommandParser.getPortUsage() + "\n\t" +
        "-load-admin " + "-source <admin-backup-dir> " +
        CommandParser.optional("-force") + "\n\t" +
        CommandParser.optional(CommandParser.getUserUsage()) + " " +
        CommandParser.optional(CommandParser.getSecurityUsage());

    private static final String LOAD_DATA_COMMAND_DESC=
        "Loads data into a store from backup directories.";

    private static final String LOAD_DATA_COMMAND_ARGS =
        CommandParser.getStoreUsage() + " " +
        CommandParser.getHostUsage() + " " +
        CommandParser.getPortUsage() + "\n\t" +
        "-source <shard-backup-dir>[, <shard-backup-dir>]*" + "\n\t"  +
        CommandParser.optional("-checkpoint <checkpoint-files-directory>") +
        "\n\t" + CommandParser.optional(CommandParser.getUserUsage()) + " " +
        CommandParser.optional(CommandParser.getSecurityUsage());

    public static final String COMMAND_ARGS =
        LOAD_ADMIN_COMMAND_ARGS + "\n\t" + LOAD_ADMIN_COMMAND_DESC + "\n\n" +
        CommandParser.KVSTORE_USAGE_PREFIX + COMMAND_NAME + " " +
        CommandParser.optional(CommandParser.VERBOSE_FLAG) + "\n\t" +
        LOAD_DATA_COMMAND_ARGS + "\n\t" + LOAD_DATA_COMMAND_DESC;

    /* The name of lock file in checkpoint directory */
    private static final String LOCK_FILE = "cp.lck";

    /*
     * The default stream parallelism when using multiple database disk ordered
     * cursor to read records from multiple databases.
     */
    private static final int STREAM_PARALLELISM_ENV_DOC = 1;

    /*
     * The default stream parallelism when using single database disk ordered
     * cursor to read records from each database.
     */
    private static final int STREAM_PARALLELISM_DB_DOC = 3;

    /**
     * Timestamp format, using the UTC timezone:
     * 2012-01-31 13:22:31.137 UTC
     */
    private static final SimpleDateFormat utcDateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
    static {
        utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /* Parent key of session record keys */
    private static final Key SESSION_PARENT_KEY =
        Key.fromString("///" + KVSessionManager.INTERNAL_SESSION_KEY);

    private final List<SnapshotEnvironment> environments;
    private PrintStream output;
    private boolean verboseOutput;

    /* Use for authentication */
    private KVStoreLogin storeLogin;
    private LoginCredentials loginCreds;

    /*
     * Use an internal store handle for writing any K/V pair, including those
     * in the internal keyspace (e.g., schemas).  KVStore.close should only be
     * called on the user store handle.
     */
    private final KVStore userStore;
    private final KVStore internalStore;

    /* The target store name */
    private final String storeName;

    /* The host name. */
    private final String hostname;

    /* The total count of entries loaded to store. */
    private AtomicLong loadedCount;

    /* Use for locking checkpoint files directory */
    private RandomAccessFile checkpointLockFile;
    private FileChannel checkpointLockChannel;
    private FileLock checkpointEnvLock;

    /* Use for configure bulkput write options */
    private final int bulkputStreamParallelism;
    private final int bulkputHeapPercent;
    private final int bulkputPerShardParallelism;
    private final int bulkputMaxRequestSize;
    private final long requestTimeoutMs;

    /**
     * Heap organization in Load utility:
     * +---------------------------------------+
     * |       Reserve for GC Overhead         |
     * |    so CMS collector can keep up       |
     * +---------------------------------------+ --------------------+---------
     * |                                       |                     |
     * |          BulkPut Memory               |                     |
     * | As determined by bulkput-heap-percent |           bulkput-heap-percent
     * |                                       |                     |
     * +---------------------------------------+ ---+----------------+---------
     * |      DiskOrderedCursor Memory         |    |                |
     * |           (Stream 1)                  |    |                |
     * +---------------------------------------+ stream-cache-percent/N
     * |    JE Snapshot Preloaded Env Ins      |    |                |
     * |           (Stream 1)                  |    |                |
     * +---------------------------------------+ ---+---             |
     * |                                       |                     |
     * |     Memory for Streams 2..N-1         |           stream-cache-percent
     * |                                       |                     |
     * +---------------------------------------+                     |
     * |      DiskOrderedCursor Memory         |                     |
     * |           (Stream N)                  |                     |
     * +---------------------------------------+                     |
     * |    JE Snapshot Preloaded Env Ins      |                     |
     * |           (Stream N)                  |                     |
     * +---------------------------------------+ --------------------+---------
     */

    /*
     * Use to configure the max memory size of JE Snapshot Preloaded Env cache
     * and DiskOrderedCursor cache in each stream.
     */
    private final static int BULKPUT_HEAP_PERCENT_DEF = 20;
    private final static int STREAM_CACHE_PERCENT_DEF = 60;
    private final static int ENV_CACHE_PERCENT_DEF = 66;
    private final int streamCachePercent;
    private final int envCachePercent;
    private long envCacheSize;
    private long docCacheSize;

    /* Flag indicates if use multiple databases disk ordered cursor */
    private final boolean useEnvDOC;

    /* The configuration properties for JE environment */
    private final Map<String, String> envConfigProps;

    /*
     * A list of system table IDs used to filter out the records from
     * system tables.
     */
    private final Set<String> sysTableIds;

    /* The test hook of LoadStream.getNext() */
    private TestHook<LoadStream> streamGetNext;

    public Load(File[] envDirs,
                String storeName,
                String targetHost,
                int targetPort,
                String user,
                String securityFile,
                String checkpointDir,
                String statsDir,
                boolean useEnvDOC,
                Map<String, String> envConfigProps,
                int bulkputStreamParallelism,
                int bulkputHeapPercent,
                int bulkputPerShardParallelism,
                int bulkputMaxRequestSize,
                long requestTimeoutMs,
                int streamCachePercent,
                int envCachePercent,
                boolean verboseOutput,
                PrintStream output)
        throws Exception {

        this.verboseOutput = verboseOutput;
        this.output = output;
        this.storeName = storeName;
        this.hostname = getLocalHostName();

        this.useEnvDOC = useEnvDOC;
        this.envConfigProps = envConfigProps;

        this.bulkputStreamParallelism =
            (bulkputStreamParallelism == 0) ?
                (useEnvDOC ? STREAM_PARALLELISM_ENV_DOC :
                             STREAM_PARALLELISM_DB_DOC) :
                bulkputStreamParallelism;
        this.bulkputHeapPercent = (bulkputHeapPercent == 0) ?
                BULKPUT_HEAP_PERCENT_DEF : bulkputHeapPercent;
        this.bulkputPerShardParallelism = bulkputPerShardParallelism;
        this.bulkputMaxRequestSize = bulkputMaxRequestSize;
        this.requestTimeoutMs = requestTimeoutMs;

        this.streamCachePercent = (streamCachePercent == 0) ?
                STREAM_CACHE_PERCENT_DEF : streamCachePercent;
        this.envCachePercent = (envCachePercent == 0) ?
                ENV_CACHE_PERCENT_DEF : envCachePercent;

        if ((this.bulkputHeapPercent + this.streamCachePercent) > 85) {
            throw new IllegalArgumentException
            ("The sum of bulkput-heap-percent and stream-cache-percent must " +
             "typically be less than 85, leaving 15% as room for the GC to " +
             "collect objects. -bulkput-heap-percent: " +
             this.bulkputHeapPercent + ", -stream-cache-percent: " +
             this.streamCachePercent);
        }

        verbose(String.format("\nArguments:\n\tstoreName: %s\n" +
                "\thelper-host: %s\n\tbulkputStreamParallelism: %d\n"+
                "\tbulkputHeapPercent: %d\n\tbulkputPerShardParallelism: %d\n"+
                "\tbulkputMaxRequestSize: %d\n\trequestTimeoutMs: %d\n" +
                "\tstreamCachePercent: %d\n\tenvCachePercent: %d",
                this.storeName, targetHost + ":" + targetPort,
                this.bulkputStreamParallelism,
                this.bulkputHeapPercent,
                this.bulkputPerShardParallelism,
                this.bulkputMaxRequestSize,
                this.requestTimeoutMs,
                this.streamCachePercent,
                this.envCachePercent));

        computeStreamCacheSizes();

        if (checkpointDir != null) {
            lockCheckpointDir(checkpointDir);
        }

        environments = new ArrayList<SnapshotEnvironment>(envDirs.length);
        for (File envDir : envDirs) {
            environments.add(new SnapshotEnvironment(envDir,
                                                     checkpointDir,
                                                     statsDir));
        }

        String[] hosts = new String[1];
        hosts[0] = targetHost + ":" + targetPort;

        prepareAuthentication(user, securityFile);

        KVStoreConfig kvConfig = new KVStoreConfig(storeName, hosts[0]);
        kvConfig.setSecurityProperties(storeLogin.getSecurityProperties());

        userStore = KVStoreFactory.getStore(
            kvConfig, loginCreds, KVStoreLogin.makeReauthenticateHandler(this));
        internalStore = KVStoreImpl.makeInternalHandle(userStore);
        sysTableIds = getSysTableIds();
        verbose("Opened store " + storeName);
    }

    public Load(File[] envDirs,
                String storeName,
                String targetHost,
                int targetPort,
                String user,
                String securityFile,
                String checkpointDir,
                boolean verboseOutput,
                PrintStream output)
        throws Exception {

        this(envDirs, storeName, targetHost, targetPort, user, securityFile,
             checkpointDir, null /* statsDir */,
             false /* useEnvDOC */, null /* envConfigProps */,
             0 /* maxStreamsPerShard */,
             0 /* bulkputHeapPercent */,
             0 /* bulkputPerShardParallelism */,
             0 /* bulkputMaxRequestSize */,
             0 /* requestTimeoutMs */,
             0 /* streamCachePercent */,
             0 /* envCachePercent */,
             verboseOutput, output);
    }

    public Load(File envDir,
                String storeName,
                String targetHost,
                int targetPort,
                String user,
                String securityFile,
                String checkpointDir,
                boolean verboseOutput,
                PrintStream output)
        throws Exception {

        this(new File[]{envDir}, storeName, targetHost, targetPort,
             user, securityFile, checkpointDir, verboseOutput, output);
    }

    @Override
    public LoginCredentials getCredentials() {
        return loginCreds;
    }

    public void setOutput(PrintStream output) {
        this.output = output;
    }

    public PrintStream getOutput() {
        return output;
    }

    public void setVerbose(boolean verboseOutput) {
        this.verboseOutput = verboseOutput;
    }

    public boolean getVerbose() {
        return verboseOutput;
    }

    private void message(String msg) {
        if (output != null) {
            String now = utcDateFormat.format(new Date());
            output.println(String.format("%s [LOAD] %s", now, msg));
        }
    }

    private void verbose(String msg) {
        if (verboseOutput) {
            message(msg);
        }
    }

    public long run()
        throws Exception {

        loadedCount = new AtomicLong();
        List<EntryStream<KeyValue>> streams = null;
        try {
            streams = createLoadStreams();
            if (streams.isEmpty()) {
                message("No more database to load.");
                return 0;
            }

            final BulkWriteOptions wro = new BulkWriteOptions(null, 0, null);
            wro.setStreamParallelism(bulkputStreamParallelism);
            if (bulkputHeapPercent > 0) {
                wro.setBulkHeapPercent(bulkputHeapPercent);
            }
            if (bulkputPerShardParallelism > 0) {
                wro.setPerShardParallelism(bulkputPerShardParallelism);
            }
            if (bulkputMaxRequestSize > 0) {
                wro.setMaxRequestSize(bulkputMaxRequestSize);
            }
            if (requestTimeoutMs > 0) {
                wro.setTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS);
            }
            verbose(String.format("Bulk put write options: " +
                    "%d streamParallelism; %d heapPercent; " +
                    "%d perShardParallelism; %d maxRequestSize; " +
                    "%,d requestTimeoutMs\n",
                    wro.getStreamParallelism(), wro.getBulkHeapPercent(),
                    wro.getPerShardParallelism(), wro.getMaxRequestSize(),
                    (wro.getTimeoutUnit() == null) ?
                     ((KVStoreImpl)internalStore).getDefaultRequestTimeoutMs() :
                     wro.getTimeout()));

            try {
                verbose("Starting loading..");
                internalStore.put(streams, wro);
                verbose("Loading is done.");
            } catch (RuntimeException re) {
                message("Loading failed: " + re.getMessage());
                throw re;
            }

        } finally {
            if (streams != null) {
                for (EntryStream<KeyValue> stream : streams) {
                    ((LoadStream)stream).close();
                }
            }
            close();
        }
        return loadedCount.get();
    }

    private void computeStreamCacheSizes() {
        long streamCacheSize = JVMSystemUtils.getRuntimeMaxMemory() *
            streamCachePercent / 100 / bulkputStreamParallelism;
        if (useEnvDOC) {
            envCacheSize = streamCacheSize * envCachePercent / 100;
            docCacheSize = streamCacheSize - envCacheSize;
            verbose("JE Snapshot preloaded env internal memory limit: " +
                    toMB(envCacheSize) + "MB");
            verbose("DiskOrderedCursor internal memory limit: " +
                    toMB(docCacheSize) + "MB");
        } else {
            docCacheSize = streamCacheSize;
            verbose("DiskOrderedCursor internal memory limit: " +
                    toMB(docCacheSize) + "MB");
        }
    }

    private long getStreamEnvCacheSize() {
        return envCacheSize;
    }

    private long getStreamDocCacheSize() {
        return docCacheSize;
    }

    /*
     * Check and set SSL connection
     */
    private void prepareAuthentication(final String user,
                                       final String securityFile)
        throws Exception {

        storeLogin = new KVStoreLogin(user, securityFile);
        try {
            storeLogin.loadSecurityProperties();
        } catch (IllegalArgumentException iae) {
            message(iae.getMessage());
        }

        /* Needs authentication */
        if (storeLogin.foundSSLTransport()) {
            loginCreds = storeLogin.makeShellLoginCredentials();
        }
    }

    private void close() {
        if (environments != null) {
            for (SnapshotEnvironment se : environments) {
                se.close();
            }
        }

        if (userStore != null) {
            userStore.close();
        }

        releaseCheckpointDirLock();
    }

    /**
     * Create a list of EntryStream, each EntryStream represents a partition
     * from a shard backup directory, the streams are ordered to make sure they
     * alternate different shards.
     */
    private List<EntryStream<KeyValue>> createLoadStreams() {
        final List<EntryStream<KeyValue>> streams =
            new ArrayList<EntryStream<KeyValue>>();

        if (useEnvDOC) {
            for (SnapshotEnvironment se : environments) {
                streams.add(new SnapshotStream(se));
            }
        } else {
            List<SnapshotEnvironment> envs =
                new ArrayList<SnapshotEnvironment>();
            for (SnapshotEnvironment se : environments) {
                verbose("Opened source backup directory " + se.getEnvDir());
                final List<String> dbToLoads = se.getDatabasesToLoad();
                if (dbToLoads.isEmpty()) {
                    message("No more database to load in: " + se.getName());
                    continue;
                }
                envs.add(se);
            }

            int ind = 0;
            while (!envs.isEmpty()) {
                Iterator<SnapshotEnvironment> envIter = envs.iterator();
                while (envIter.hasNext()) {
                    SnapshotEnvironment se = envIter.next();
                    List<String> dbsToLoad = se.getDatabasesToLoad();
                    if (ind < dbsToLoad.size()) {
                        streams.add(new DatabaseStream(se, dbsToLoad.get(ind)));
                    } else {
                        envIter.remove();
                    }
                }
                ind++;
            }
        }
        return streams;
    }

    /**
     * Tally the total count of loaded entries.
     */
    private void tallyLoadedCount(long count) {
        loadedCount.addAndGet(count);
    }

    /**
     * Displays the loading information of a database.
     */
    private synchronized void displayLoadProgress(String envDir,
                                                  String database,
                                                  long count,
                                                  long skipCount,
                                                  long dupCount,
                                                  long elapseTimeMs) {

        String info;
        if (database == null) {
            String fmt = "Load %,d records, %,d records skipped, %,d " +
                "pre-existing records from %s: ";
            info = String.format(fmt, count, skipCount, dupCount, envDir);
        } else {
            String fmt = "Load %,d records, %,d records skipped, %,d " +
                    "pre-existing records from %s of %s: ";
            info = String.format(fmt, count, skipCount, dupCount,
                                 database, envDir);
        }
        if (elapseTimeMs > 60000) {
            info += String.format("%,dm%.3fs", elapseTimeMs/60000,
                                  (float)(elapseTimeMs % 60000)/1000);
        } else {
            info += String.format("%.3fs", (float)elapseTimeMs/1000);
        }
        message(info);
    }

    /**
     * Acquires an exclusive lock on checkpoint files directory.
     */
    private void lockCheckpointDir(String checkpointDir) {
        try {
            checkpointLockFile =
                new RandomAccessFile(new File(checkpointDir, LOCK_FILE), "rwd");

            checkpointLockChannel = checkpointLockFile.getChannel();

            final String msg =
                "Another load is already running. Failed to acquire " +
                    "a lock on checkpoint files directory: " +
                    checkpointDir;
            try {
                checkpointEnvLock = checkpointLockChannel.tryLock(1, 1, false);
                if (checkpointEnvLock == null) {
                    throw new IllegalStateException(msg);
                }
            } catch (OverlappingFileLockException ofle) {
                throw new IllegalStateException(msg, ofle);
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Failed to open " +
                "checkpoint files directory: " + checkpointDir, ioe);
        } catch (SecurityException se) {
            throw new IllegalArgumentException("Failed to open " +
                 "checkpoint files directory: " + checkpointDir, se);
        }
    }

    /**
     * Releases the lock on checkpoint files directory.
     */
    private void releaseCheckpointDirLock() {
        try {
            if (checkpointEnvLock != null) {
                checkpointEnvLock.release();
            }

            if (checkpointLockChannel != null) {
                checkpointLockChannel.close();
            }

            if (checkpointLockFile != null) {
                checkpointLockFile.close();
            }
        } catch (IOException ignored) {
        }
    }

    /**
     * Returns the name of local host.
     */
    private String getLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    /**
     * Set test hook for LoadStream.getNext().
     */
    public void setLoadStreamGetNext(TestHook<LoadStream> testHook) {
        streamGetNext = testHook;
    }

    /**
     * A class that loads metadata from an admin database environment in a
     * snapshot.  At this time it only loads table metadata, and it does this
     * via a single serialized object.  An instance of the CommandServiceAPI
     * is required to put the new metadata.  It is read directly from the
     * admin environment.
     */
    private static class LoadAdmin {

        private final Environment env;
        private final boolean verboseOutput;
        private final PrintStream output;
        private final String securityFile;
        private final String user;
        private final String targetHost;
        private final int targetPort;
        private final boolean forceLoad;

        public LoadAdmin(File envDir,
                         String targetHost,
                         int targetPort,
                         String user,
                         String securityFile,
                         boolean verboseOutput,
                         boolean forceLoad,
                         PrintStream output) {

            env = openEnvironment(envDir, null);
            this.output = output;
            this.verboseOutput = verboseOutput;
            this.securityFile = securityFile;
            this.user = user;
            this.targetHost = targetHost;
            this.targetPort = targetPort;
            this.forceLoad = forceLoad;
            verbose("Opened environment for admin load: " + envDir);
        }

        /**
         * Loads both table and security metadata, if present.
         */
        public void loadMetadata() {
            final Logger logger = Logger.getLogger(LoadAdmin.class.getName());
            final TableStore ts = TableStore.getReadOnlyInstance(logger, env);
            final TableMetadata tmd = ts.getTableMetadata(null);
            ts.close();

            if (tmd != null) {
                verbose("Found table metadata");
                if (verboseOutput) {
                    List<String> tablesToLoad = tmd.listTables(null, true);
                    if (!tablesToLoad.isEmpty()) {
                        message("Writing tables:");
                        for (String s : tablesToLoad) {
                            message("\t" + s);
                        }
                    }
                }
            } else {
                message("No tables to write");
            }


            final SecurityStore ss =
                               SecurityStore.getReadOnlyInstance(logger, env);
            final SecurityMetadata smd = ss.getSecurityMetadata(null);
            ss.close();

            if (smd != null) {
                verbose("Found security metadata");
            }

            if (tmd != null || smd != null) {
                writeMetadata(tmd, smd);
            }
            env.close();
        }

        /**
         * Gets an instance of CommandServiceAPI from a CommandShell and uses
         * it to push the metadata to the store admin.
         */
        private void writeMetadata(TableMetadata tmd,
                                   SecurityMetadata smd) {

            ArrayList<String> argList = new ArrayList<String>();
            argList.add(CommandParser.HOST_FLAG);
            argList.add(targetHost);
            argList.add(CommandParser.PORT_FLAG);
            argList.add(Integer.toString(targetPort));
            if (securityFile != null) {
                argList.add(CommandParser.ADMIN_SECURITY_FLAG);
                argList.add(securityFile);
            }
            if (user != null) {
                argList.add(CommandParser.ADMIN_USER_FLAG);
                argList.add(user);
            }
            CommandShell shell = new CommandShell(null, output);
            shell.parseArgs(argList.toArray(new String[argList.size()]));
            shell.init();
            try {
                CommandServiceAPI cs = shell.getAdmin();
                boolean tableMDExist = false;
                boolean secMDExist = false;

                /*
                 * Every store has the system tables in the table metadata,
                 * check if any other tables exists. If so, warns users to
                 * specify -force flag.
                 */
                if (tmd != null) {
                    TableMetadata existingTableMD = cs.getMetadata(
                        TableMetadata.class, MetadataType.TABLE);
                    if (existingTableMD != null) {
                        for (Table table :
                            existingTableMD.getTables().values()) {
                            if (!((TableImpl)table).isSystemTable()) {
                                tableMDExist = true;
                            }
                        }
                    }
                }

                /*
                 * Check if security metadata exists, if so, warns users to
                 * specify -force flag overwrite. This check is relied on the
                 * CommandService.getUsersDescription method return value.
                 *
                 * For a secure store, as the bootstrap admin, when anonymous
                 * user invokes this method, it returns null only if security
                 * metadata is null. When other users having SYSVIEW invoke this
                 * method, it returns all users created. User who does not have
                 * that privilege invokes this method, it will return user
                 * itself. For a non-secure store, it always return all users
                 * if security metadata exists and users have been created. So
                 * this method can be used to indirectly examine if security
                 * metadata exists in the restore store.
                 */
                secMDExist = (cs.getUsersDescription() != null);
                if ((tableMDExist || secMDExist) &&
                    !forceLoad) {
                        message("Metadata exists, use -force flag"
                                + " to overwrite");
                        return;
                }

                if (tmd != null) {
                    cs.putMetadata(tmd);
                    verbose("Wrote table metadata");
                }
                if (smd != null) {
                    cs.putMetadata(smd);
                    verbose("Wrote security metadata");
                }
            } catch (RemoteException re) {
                message("Failed to acquire admin interface or write" +
                        " metadata: " + re.getMessage());
            } catch (ShellException se) {
                message("Failed to acquire admin interface or write" +
                        " metadata: " + se.getMessage());
            }
        }

        private void message(String msg) {
            output.println(msg);
        }

        private void verbose(String msg) {
            if (verboseOutput) {
                message(msg);
            }
        }
    }

    /**
     * Parser for the Load command line.
     */
    public static class LoadParser extends CommandParser {
        private static final String SOURCE_FLAG = "-source";
        private static final String CHECKPOINT_FLAG = "-checkpoint";
        private static final String ADMIN_LOAD_FLAG = "-load-admin";
        private static final String FORCE_ADMIN_FLAG = "-force";

        /*
         * The hidden flags.
         */
        /* Flags to configure write options of bulk put */
        private static final String BULKPUT_STREAM_PARALLELISM =
            "-bulkput-stream-parallelism";
        private static final String BULKPUT_HEAP_PERCENT =
            "-bulkput-heap-percent";
        private static final String BULKPUT_PER_SHARD_PARALLELISM =
            "-bulkput-per-shard-parallelism";
        private static final String BULKPUT_MAX_REQUEST_SIZE =
            "-bulkput-max-request-size";
        private static final String REQUEST_TIMEOUT_MS =
            "-request-timeout-ms";

        /*
         * The percentage of max memory used across all concurrently active
         * streams, as defined by the stream parallelism.
         */
        private static final String STREAM_CACHE_PERCENT =
            "-stream-cache-percent";
        /*
         * The percentage of the stream cache(max-memory * stream-cache-percent)
         * used for holding the pre fetched environment. The remaining
         * (100 - env-cache-percent) * (max-memory * stream-cache-percent) is
         * used for the dbcursor cache.
         */
        private static final String ENV_CACHE_PERCENT =
            "-env-cache-percent";

        /* Used to collect JE statistics of snapshot environment */
        private static final String STATS_FLAG = "-collect-stats";

        /* Used to set JE environment configure parameters */
        private static final String JE_PROP_FLAG_PREFIX = "-je.";

        /* Flag to use single database cursor */
        private static final String DB_DOC_FLAG = "-single-database-cursor";

        private File[] sourceDirs = null;
        private String checkpointDir = null;
        private boolean loadAdmin = false;
        private boolean forceLoadAdmin = false;
        private int bulkputStreamParallelism = 0;
        private int bulkputHeapPercent = 0;
        private int bulkputPerShardParallelism = 0;
        private int bulkputMaxRequestSize = 0;
        private long requestTimeoutMs = 0;
        private String statsDir = null;
        private boolean useEnvDOC = true;
        private Map<String, String> jeProps = null;
        private int streamCachePercent = 0;
        private int envCachePercent = 0;

        LoadParser(String[] args) {
            super(args);
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME + "\n\t" +
                               COMMAND_ARGS);
            System.exit(-1);
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(SOURCE_FLAG)) {
                String source = nextArg(arg);
                String[] paths = source.split(",");
                sourceDirs = new File[paths.length];
                for (int i = 0; i < paths.length; i++) {
                    sourceDirs[i] = new File(paths[i]);
                    if (!sourceDirs[i].exists() ||
                        !sourceDirs[i].isDirectory()) {
                        throw new IllegalArgumentException("Could not access " +
                            "backup source directory: " + sourceDirs[i]);
                    }
                }
                return true;
            }
            if (arg.equals(CHECKPOINT_FLAG)) {
                checkpointDir = nextArg(arg);
                File file = new File(checkpointDir);
                if (!file.exists() || !file.isDirectory()) {
                    throw new IllegalArgumentException("Could not access " +
                        "checkpoint files directory: " + checkpointDir);
                }
                return true;
            }
            if (arg.equals(ADMIN_LOAD_FLAG)) {
                loadAdmin = true;
                return true;
            }
            if (arg.equals(FORCE_ADMIN_FLAG)) {
                forceLoadAdmin = true;
                return true;
            }
            if (arg.equals(BULKPUT_STREAM_PARALLELISM)) {
                bulkputStreamParallelism = nextIntArg(arg);
                if (bulkputStreamParallelism < 1) {
                    throw new IllegalArgumentException
                    (BULKPUT_STREAM_PARALLELISM + " requires a positive " +
                     "integer: " + bulkputStreamParallelism);
                }
                return true;
            }
            if (arg.equals(BULKPUT_HEAP_PERCENT)) {
                bulkputHeapPercent = checkPercentArg(arg);
                return true;
            }
            if (arg.equals(BULKPUT_PER_SHARD_PARALLELISM)) {
                bulkputPerShardParallelism = nextIntArg(arg);
                if (bulkputPerShardParallelism < 1) {
                    throw new IllegalArgumentException
                    (BULKPUT_PER_SHARD_PARALLELISM + " requires a positive " +
                     "integer: " + bulkputPerShardParallelism);
                }
                return true;
            }
            if (arg.equals(BULKPUT_MAX_REQUEST_SIZE)) {
                bulkputMaxRequestSize = nextIntArg(arg);
                if (bulkputMaxRequestSize < 1) {
                    throw new IllegalArgumentException
                    (BULKPUT_MAX_REQUEST_SIZE + " requires a positive " +
                     "integer: " + bulkputMaxRequestSize);
                }
                return true;
            }
            if (arg.equals(STATS_FLAG)) {
                statsDir = nextArg(arg);
                File file = new File(statsDir);
                if (!file.exists() || !file.isDirectory()) {
                    throw new IllegalArgumentException("Could not access " +
                        "stats files directory: " + statsDir);
                }
                return true;
            }
            if (arg.equals(DB_DOC_FLAG)) {
                useEnvDOC = false;
                return true;
            }
            if (arg.startsWith(JE_PROP_FLAG_PREFIX) &&
                arg.length() > JE_PROP_FLAG_PREFIX.length()) {
                if (jeProps == null) {
                    jeProps = new HashMap<String, String>();
                }
                jeProps.put(arg.substring(1), nextArg(arg));
                return true;
            }
            if (arg.equals(REQUEST_TIMEOUT_MS)) {
                requestTimeoutMs = nextLongArg(arg);
                if (requestTimeoutMs < 1) {
                    throw new IllegalArgumentException
                    (REQUEST_TIMEOUT_MS + " requires a positive " +
                     "integer: " + requestTimeoutMs);
                }
                return true;
            }
            if (arg.equals(STREAM_CACHE_PERCENT)) {
                streamCachePercent = checkPercentArg(arg);
                return true;
            }
            if (arg.equals(ENV_CACHE_PERCENT)) {
                envCachePercent = checkPercentArg(arg);
                return true;
            }
            return false;
        }


        /**
         * Get the next percent argument and convert it into an int.
         *
         * @param argName name of the argument being checked
         * @param percent the value being checked
         *
         * @return the integer percentage a value in the range 1..99
         */
        private int checkPercentArg(String arg) {
            final int percent = nextIntArg(arg);
            if (percent < 1 || percent > 99) {
                throw new IllegalArgumentException
                    (arg +
                     " is a percentage and must be in the range 1 ~ 99: " +
                     percent);
            }
            return percent;
        }

        @Override
        protected void verifyArgs() {
            if (getHostname() == null) {
                missingArg(HOST_FLAG);
            }
            if (getRegistryPort() == 0) {
                missingArg(PORT_FLAG);
            }
            if (getStoreName() == null && !loadAdmin) {
                missingArg(STORE_FLAG);
            }
            if (getSourceDirs() == null) {
                missingArg(SOURCE_FLAG);
            } else {
                if (loadAdmin && getSourceDirs().length > 1) {
                    throw new IllegalArgumentException("There must be " +
                        "exactly one source dir if loading Admin metadata");
                }
            }
        }

        public String getCheckpointDir() {
            return checkpointDir;
        }

        public File[] getSourceDirs() {
            return sourceDirs;
        }

        public boolean getLoadAdmin() {
            return loadAdmin;
        }

        public boolean getForceLoadAdmin() {
            return forceLoadAdmin;
        }

        public int getBulkputStreamParallelism() {
            return bulkputStreamParallelism;
        }

        public int getBulkputHeapPercent() {
            return bulkputHeapPercent;
        }

        public int getBulkputPerShardParallelism() {
            return bulkputPerShardParallelism;
        }

        public int getBulkputMaxRequestSize() {
            return bulkputMaxRequestSize;
        }

        public long getRequestTimeoutMs() {
            return requestTimeoutMs;
        }

        public String getStatsDir() {
            return statsDir;
        }

        public boolean useEnvDOC() {
            return useEnvDOC;
        }

        public Map<String, String> getJEProps() {
            return jeProps;
        }

        public int getStreamCachePercent() {
            return streamCachePercent;
        }

        public int getEnvCachePercent() {
            return envCachePercent;
        }
     }

    public static void main(String[] args)
        throws Exception {

        LoadParser lp = new LoadParser(args);
        lp.parseArgs();
        if (lp.getLoadAdmin()) {
            loadAdmin(lp);
            return;
        }

        try {
            Load load = new Load(lp.getSourceDirs(),
                                 lp.getStoreName(),
                                 lp.getHostname(),
                                 lp.getRegistryPort(),
                                 lp.getUserName(),
                                 lp.getSecurityFile(),
                                 lp.getCheckpointDir(),
                                 lp.getStatsDir(),
                                 lp.useEnvDOC(),
                                 lp.getJEProps(),
                                 lp.getBulkputStreamParallelism(),
                                 lp.getBulkputHeapPercent(),
                                 lp.getBulkputPerShardParallelism(),
                                 lp.getBulkputMaxRequestSize(),
                                 lp.getRequestTimeoutMs(),
                                 lp.getStreamCachePercent(),
                                 lp.getEnvCachePercent(),
                                 lp.getVerbose(),
                                 System.out);

            long total = load.run();
            System.out.println("Load succeeded, wrote " + total + " records");
        } catch (Exception e) {
            System.err.println("Load operation failed with exception: " +
                               LoggerUtils.getStackTrace(e));
            System.exit(-1);
        }
    }

    /**
     * Loads metadata from the admin rather than store data.  In this case
     * the source directory must be an Admin's environment.  The store name,
     * if provided, is ignored.
     *
     * This loads both table metadata and security metadata.
     */
    private static void loadAdmin(LoadParser lp) {

        loadAdmin(lp.getSourceDirs()[0],
                  lp.getHostname(),
                  lp.getRegistryPort(),
                  lp.getUserName(),
                  lp.getSecurityFile(),
                  lp.getVerbose(),
                  lp.getForceLoadAdmin(),
                  System.out);
    }

    public static void loadAdmin(File envDir,
                                 String targetHost,
                                 int targetPort,
                                 String user,
                                 String securityFile,
                                 boolean verboseOutput,
                                 boolean forceLoad,
                                 PrintStream output) {
        try {
            LoadAdmin load =
                new LoadAdmin(envDir, targetHost, targetPort, user,
                              securityFile, verboseOutput, forceLoad, output);
            load.loadMetadata();
        } catch (Exception e) {
            System.err.println("Admin load operation failed with exception: " +
                               LoggerUtils.getStackTrace(e));
        }
    }

    /**
     * Open the JE environment and if present.
     */
    private static Environment
        openEnvironment(File envDir, Map<String, String> configProps) {

        if (!envDir.isDirectory()) {
            System.err.println("Environment path is not a directory or " +
                "does not exist: " + envDir);
            throw new IllegalArgumentException("Bad environment " +
                "directory: " + envDir);
        }

        final EnvironmentConfig envConfig = new EnvironmentConfig();

        if (configProps != null) {
            for (Entry<String, String> prop : configProps.entrySet()) {
                try {
                    envConfig.setConfigParam(prop.getKey(), prop.getValue());
                } catch (IllegalArgumentException iae) {
                    throw new IllegalArgumentException("Invalid environment " +
                        "configuration parameter '-" + prop.getKey() + " " +
                        prop.getValue() + "': " + iae.getMessage());
                }
            }
        }
        envConfig.setTransactional(false);
        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(true);
        envConfig.setSharedCache(true);
        try {
            return new Environment(envDir, envConfig);
        } catch (EnvironmentNotFoundException e) {
            throw new IllegalArgumentException("Cannot find valid " +
                "Environment in directory: " + envDir);
        }
    }

    /* Returns a list of system table IDs. */
    private Set<String> getSysTableIds() {
        Set<String> tableIds = new HashSet<String>();
        for (TableImpl table :
                 ((TableAPIImpl)internalStore.getTableAPI()).getSystemTables()){
            tableIds.add(table.getIdString());
        }
        return tableIds;
    }

    /* Checks if the given key belongs to a system table */
    private boolean isSysTableRecord(Key key) {
        return sysTableIds.contains(key.getMajorPath().get(0));
    }

    /* Checks if the given key belongs to a session record */
    private boolean isSessionRecord(Key key) {
        return SESSION_PARENT_KEY.isPrefix(key);
    }

    /**
     * A interface encapsulates the methods for checkpointing.
     */
    interface LoadCheckpoint {
        /**
         * Marks the loaded status of a partition or snapshot.
         */
        void setLoaded(String name, long timestampMs);

        /**
         * Checks if the partition or snapshot is loaded.
         */
        boolean isLoaded(String name);
    }

    /**
     * The class represents a snapshot environment.
     *
     * If checkpointDir is provided, the LoadCheckpoint is initialized with an
     * underlying file, the file name is
     * <checkpointDir>/<SnapshotEnvironment-name>.status
     */
    private class SnapshotEnvironment {

        private final File envDir;
        private Environment jeEnv;
        private final LoadCheckpoint checkpoint;
        private List<String> dbsToLoad;

        /* The suffix of statistic file */
        private final static String SUFFIX_STATS_FILE = ".stats";

        /* The statistic file of the JE envrionment */
        private final File envStatsFile;

        SnapshotEnvironment(File sourceDir,
                            String checkpointDir,
                            String statsDir) {
            envDir = sourceDir;

            if (checkpointDir != null) {
                if (useEnvDOC) {
                    if (!envDir.exists() || ! envDir.isDirectory()) {
                        final String msg = envDir +
                            " is not a valid snapshot directory.";
                        throw new IllegalArgumentException(msg);
                    }
                    checkpoint = new SnapshotCheckpoint(new File(checkpointDir),
                                                        getName());
                } else {
                    File cpDir = new File(new File(checkpointDir), getName());
                    checkpoint = new DatabaseCheckpoint(cpDir);
                }
            } else {
                checkpoint = null;
            }

            if (statsDir != null) {
                envStatsFile = new File(statsDir, getName() + SUFFIX_STATS_FILE);
            } else {
                envStatsFile = null;
            }
        }

        String getName() {
            return "snapshot" + envDir.getAbsolutePath().replace("/", "_")
                    .replace(".", "");
        }

        File getEnvDir() {
            return envDir;
        }

        Environment getJEEnv() {
            if (jeEnv == null) {
                jeEnv = openJEEnvironment();
            }
            return jeEnv;
        }

        private Environment openJEEnvironment() {
            long startMs = System.currentTimeMillis();
            final Environment env = openEnvironment(envDir, envConfigProps);
            verbose(String.format("Environment open time:%,d ms",
                                  (System.currentTimeMillis() - startMs)));

            if (envConfigProps != null && !envConfigProps.isEmpty()) {
                String fmt = "Opened JE envrionement '%s' with properties: %s";
                verbose(String.format(fmt, envDir.getAbsolutePath(),
                                      envConfigProps));
            }
            return env;
        }

        /**
         * Returns the list of databases to load, skip non-partition databases
         * and those databases are already loaded according to the checkpoint
         * file.
         */
        List<String> getDatabasesToLoad() {
            if (dbsToLoad != null) {
                return dbsToLoad;
            }

            final List<String> dbs = getJEEnv().getDatabaseNames();
            dbsToLoad = new ArrayList<String>();
            if (useEnvDOC) {
                if (isLoaded(getName())) {
                    dbsToLoad = Collections.emptyList();
                    message("Skipping already loaded snapshot: " + getName());
                } else {
                    for (String db : dbs) {
                        if (!PartitionId.isPartitionName(db)) {
                            verbose("Skipping non-partition database: " + db);
                            continue;
                        }
                        dbsToLoad.add(db);
                    }
                }
            } else {
                for (String db : dbs) {
                    if (!PartitionId.isPartitionName(db)) {
                        verbose("Skipping non-partition database: " + db);
                        continue;
                    }
                    if (isLoaded(db)) {
                        verbose("Skipping already loaded database: " + db);
                        continue;
                    }
                    dbsToLoad.add(db);
                }
                if (!dbsToLoad.isEmpty()) {
                    Collections.sort(dbsToLoad);
                }
            }
            return dbsToLoad;
        }

        void setLoaded(LoadStream stream) {
            if (checkpoint != null) {
                String name = useEnvDOC? getName() : stream.getDatabase();
                checkpoint.setLoaded(name, stream.getElapsedTime());
            }
            displayLoadProgress(envDir.toString(),
                                stream.getDatabase(),
                                stream.getReadCount(),
                                stream.getSkippedCount(),
                                stream.getKeyExistCount(),
                                stream.getElapsedTime());
        }

        boolean isLoaded(String name) {
            return (checkpoint != null) ? checkpoint.isLoaded(name) : false;
        }

        void close() {
            if (jeEnv != null) {
                if (!jeEnv.isClosed()) {
                    if (envStatsFile != null) {
                        dumpEnvStatsToFile();
                    }
                    jeEnv.close();
                    verbose("Environment closed: " + getName());
                }
                jeEnv = null;
            }
        }

        /* Dumps the statistic information of JE environment to file */
        private void dumpEnvStatsToFile() {
            if (!envStatsFile.exists()) {
                try {
                    envStatsFile.createNewFile();
                } catch (IOException ioe) {
                    throw new IllegalStateException("Failed to create " +
                        "environment stats file: " + envStatsFile, ioe);
                }
            }

            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(envStatsFile);
                String info = jeEnv.getStats(StatsConfig.DEFAULT).toString();
                fos.write(info.getBytes());
                fos.flush();
            } catch (IOException ioe) {
                throw new IllegalStateException("Failed to write " +
                    "stats information to " + envStatsFile, ioe);
            } catch (DatabaseException de) {
                throw new IllegalStateException("Failed to get " +
                    "stats information:" + de.getMessage(), de);
            } finally {
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (IOException ignored) {
                    }
                }
            }

            verbose("Dump environment statistic to " +
                    envStatsFile.getAbsolutePath());
        }
    }

    /**
     * The LoadCheckpoint class manages the loading progress of a shard
     * in partition granularity.
     *
     * Once all entries of a partition are loaded to store, a checkpoint
     * file with the name of partition is created in the checkpoint
     * directory, the layout of the checkpoint directory like below:
     *  <checkpoint-dir>
     *      \_ p1
     *      \_ p2
     *      ...
     *      \_ p<n>
     *
     * The checkpoint file contains below information:
     *  store=<storeName>
     *  machine=<hostname>
     *  loadTime=<timestamp>
     *
     * During initialization, it scans the checkpoint files and collects all
     * partitions loaded.
     */
    class DatabaseCheckpoint extends Checkpoint {

        /*
         * Use to store name of loaded partition or snapshot that loaded
         * from check point file(s during initialization.
         */
        private final Set<String> loaded = new HashSet<String>();

        DatabaseCheckpoint(File baseDir) {
            super(baseDir);
            if (!baseDir.exists()) {
                if (!baseDir.mkdirs()) {
                    throw new IllegalStateException("Failed to " +
                        "create checkpoint directory: " + baseDir);
                }
            } else {
                if (!baseDir.isDirectory()) {
                    throw new IllegalArgumentException("A file but " +
                        "not a directory exists :" +  baseDir);
                }
                loadCheckpointFiles();
            }
        }

        /**
         * Returns true if the given partition is loaded, otherwise return
         * false.
         */
        @Override
        public boolean isLoaded(String name) {
            return loaded.contains(name);
        }

        /**
         * Collects the name of loaded partitions from checkpoint files.
         */
        private void loadCheckpointFiles() {
            File[] files = baseDir.listFiles();
            for (File file : files) {
                if (file.getName().equals(LOCK_FILE)) {
                    continue;
                }
                if (!file.isFile()) {
                    message("Skip an invalid checkpoint file: " + file);
                    continue;
                }
                if (isValidCheckpointFile(file)) {
                    loaded.add(file.getName());
                    continue;
                }
                message("The checkpoint file is not for store \"" +
                        storeName + "\" or not a valid checkpoint file: " +
                        file.getAbsolutePath());
            }
        }
    }

    /**
     * The LoadCheckpoint class manages the loading progress in entire
     * snapshot granularity
     *
     * Once all entries of the snapshot are loaded to store, a checkpoint
     * file with the name: SnapshotEnvironement.getName() is created.
     *
     * The checkpoint file contains below information:
     *  store=<storeName>
     *  machine=<hostname>
     *  loadTime=<timestamp>
     *
     * During initialization, it tries to load the checkpoint file,
     * verify its content, and mark its loaded status accordingly
     */
    class SnapshotCheckpoint extends Checkpoint {

        /* Used to indicate if the snapshot is loaded or not */
        private boolean isLoaded;

        SnapshotCheckpoint(File baseDir, String name) {
            super(baseDir);
            if (!baseDir.isDirectory()) {
                throw new IllegalArgumentException("A file but " +
                    "not a directory exists :" +  baseDir);
            }

            File cpFile = new File(baseDir, name);
            if (cpFile.exists() && cpFile.isFile()) {
                isLoaded = isValidCheckpointFile(cpFile);
            } else {
                isLoaded = false;
            }
        }

        /**
         * Returns true if the given snapshot is loaded, otherwise return
         * false.
         */
        @Override
        public boolean isLoaded(String name) {
            return isLoaded;
        }
    }

    /**
     * A base class that implements LoadCheckPoint interface and also
     * encapsulates some facility methods.
     */
    abstract class Checkpoint implements LoadCheckpoint {

        private final static String STORE = "store";
        private final static String MACHINE = "machine";
        private final static String LOADTIME = "loadTime";

        /* The folder of checkpoint files */
        final File baseDir;

        Checkpoint(File baseDir) {
            assert(baseDir != null);
            this.baseDir = baseDir;
        }

        /**
         * Sets the given partition is loaded.
         */
        @Override
        public void setLoaded(String name, long timestampMs) {
            writeToCheckpointFile(name, timestampMs);
        }

        boolean isValidCheckpointFile(File file) {
            try {
                Properties props = new Properties();
                props.load(new FileReader(file));
                String store = props.getProperty(STORE);
                if (store != null && store.equalsIgnoreCase(storeName)) {
                    String loadedTime = (String)props.get(LOADTIME);
                    if (loadedTime != null) {
                        verbose("Loaded checkpoint information from " +
                                file + ": " + props.toString());
                        return true;
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load from " +
                    "checkpoint file " + file, e);
            }
            return false;
        }

        /**
         * Writes the checkpoint information to the underlying file
         */
        private void writeToCheckpointFile(String name, long timestampMs) {
            File file = new File(baseDir, name);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException ioe) {
                    throw new IllegalStateException("Failed to create " +
                        "checkpoint file: " + file, ioe);
                }
            }

            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(file);
                Properties props = new Properties();
                props.put(STORE, storeName);
                props.put(MACHINE, hostname);
                props.put(LOADTIME,
                          utcDateFormat.format(new Date(timestampMs)));
                props.store(fos, null);
            } catch (IOException ioe) {
                throw new IllegalStateException("Failed to write " +
                    "checkpoint information to " + file, ioe);
            } finally {
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }


    /**
     * A DatabaseStream represents a database, it supplies the KeyValue entries
     * of a database from snapshot environment.
     */
    class DatabaseStream extends LoadStream {

        /* The database to load */
        private final String database;
        private Database db;

        DatabaseStream(SnapshotEnvironment env, String database) {
            super(env);
            this.database = database;
        }

        @Override
        public String name() {
            return "LoadStream_" + database;
        }

        /**
         * Open JE database and its cursor.
         */
        @Override
        DiskOrderedCursor openCursor() {
            db = openDatabase(database);
            /*
             * TODO: We may need to make provisions for configuring this cursor
             * if the performance tests show we need it. Let's just keep this in
             * mind for now.
             */
            DiskOrderedCursorConfig docConfig = new DiskOrderedCursorConfig()
                    .setInternalMemoryLimit(getStreamDocCacheSize());
            return db.openCursor(docConfig);
        }

        @Override
        String getDatabase() {
            return database;
        }

        /**
         * Close JE database and its cursor.
         */
        @Override
        void close() {
            super.close();
            if (db != null) {
                try {
                    db.close();
                } catch (ThreadInterruptedException ignored) {
                }
                db = null;
            }
        }
    }

    /**
     * A SnapshotStream represents a snapshot, it supplies the KeyValue entries
     * from multiple databases of a snapshot environment.
     */
    class SnapshotStream extends LoadStream {

        /* The Databases to load */
        private Database[] dbsToLoad;

        SnapshotStream(SnapshotEnvironment env) {
            super(env);
        }

        @Override
        public String name() {
            return "LoadStream_" + env.getName();
        }

        /**
         * Open the disk ordered cursor of the environment.
         */
        @Override
        DiskOrderedCursor openCursor() {

            final List<String> dbNames = env.getDatabasesToLoad();
            if (dbNames.isEmpty()) {
                return null;
            }

            long startMs = System.currentTimeMillis();
            dbsToLoad = openDatabases(dbNames);
            PreloadConfig preConfig = new PreloadConfig().setLoadLNs(false)
                    .setInternalMemoryLimit(getStreamEnvCacheSize());
            env.getJEEnv().preload(dbsToLoad, preConfig);
            verbose(String.format("Environment preload time:%,d ms",
                                  (System.currentTimeMillis() - startMs)));

            DiskOrderedCursorConfig docConfig = new DiskOrderedCursorConfig()
                    .setInternalMemoryLimit(getStreamDocCacheSize());
            return env.getJEEnv().openDiskOrderedCursor(dbsToLoad, docConfig);
        }

        private Database[] openDatabases(List<String> dbNames) {
            ArrayList<Database> dbs = new ArrayList<Database>();
            for (String name : dbNames) {
                Database db = openDatabase(name);
                dbs.add(db);
            }
            return dbs.toArray(new Database[dbs.size()]);
        }

        @Override
        String getDatabase() {
            final List<String> dbs = env.getDatabasesToLoad();
            return dbs.isEmpty() ? null : dbs.toString();
        }

        /**
         * Close JE database and its cursor.
         */
        @Override
        void close() {
            super.close();
            if (dbsToLoad != null) {
                for (Database db : dbsToLoad) {
                    try {
                        db.close();
                    } catch (ThreadInterruptedException ignored) {
                    }
                }
                dbsToLoad = null;
            }
            env.close();
        }
    }

    /**
     * A basic class that implementation of EntryStream<KeyValue>, the input of
     * bulk put API.
     */
    abstract class LoadStream implements EntryStream<KeyValue> {

        /* The number of entries read from database. */
        private long readCount;

        /* The number of entries skipped */
        private long skipCount;

        /* The number of duplicated keys */
        private final AtomicLong keyExists;

        /* The snapshot environment represented by this stream. */
        final SnapshotEnvironment env;

        /* The cursor of environment */
        private ForwardCursor cursor;

        private final DatabaseEntry key;
        private final DatabaseEntry data;

        /* The time stamp to start the loading */
        private long startTimeMs;
        private long endTimeMs;

        LoadStream(SnapshotEnvironment env) {
            this.env = env;

            cursor = null;
            key = new DatabaseEntry();
            data = new DatabaseEntry();

            readCount = 0;
            skipCount = 0;
            keyExists = new AtomicLong();
            startTimeMs = 0;
        }

        /*
         * Open the cursor to iterate the records, return null if no more
         * databases to read.
         */
        abstract DiskOrderedCursor openCursor();

        /* Returns the name of database(s) to load */
        abstract String getDatabase();

        @Override
        public String toString() {
            final String fmt = "%s %,d rows read; %,d row skipped; %,d " +
                "pre-existing rows.";
            return String.format(fmt, name(), readCount, skipCount,
                                 keyExists.get());
        }

        @Override
        public KeyValue getNext() {
            assert TestHookExecute.doHookIfSet(streamGetNext, this);

            if (startTimeMs == 0) {
                startTimeMs = System.currentTimeMillis();
            }

            if (cursor == null) {
                cursor = openCursor();
                if (cursor == null) {
                    return null;
                }
            }

            KeyValue entry = readNextEntry();
            if (entry == null)  {
                close();
                verbose(name() + " read EOS: " + readCount);
                return null;
            }
            return entry;
        }

        @Override
        public void completed() {
            endTimeMs = System.currentTimeMillis();
            long nLoaded = readCount - keyExists.get() - skipCount;
            tallyLoadedCount(nLoaded);
            env.setLoaded(this);
        }

        @Override
        public void keyExists(KeyValue entry) {
            keyExists.incrementAndGet();
        }

        @Override
        public void catchException(RuntimeException rte, KeyValue entry) {
            throw rte;
        }

        Database openDatabase(String name) {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);
            dbConfig.setCacheMode(CacheMode.EVICT_LN);
            return env.getJEEnv().openDatabase(null, name, dbConfig);
        }

        long getReadCount() {
            return readCount;
        }

        long getSkippedCount() {
            return skipCount;
        }

        long getKeyExistCount() {
            return keyExists.get();
        }

        long getElapsedTime() {
            return (endTimeMs == 0) ? 0 : endTimeMs - startTimeMs;
        }

        private KeyValue readNextEntry() {

            try {
                while (true) {
                    if (cursor.getNext(key, data, null) ==
                            OperationStatus.SUCCESS) {

                        readCount++;
                        Key k = Key.fromByteArray(key.getData());
                        if (isSysTableRecord(k) || isSessionRecord(k)) {
                            skipCount++;
                            continue;
                        }

                        Value v = (data.getData().length == 0) ?
                                   Value.EMPTY_VALUE :
                                   Value.fromByteArray(data.getData());
                        return new KeyValue(k, v);
                    }
                    break;
                }
            } catch (DiskOrderedCursorProducerException dcpe) {
                message("Failed to read entry from cursor:" + dcpe.getMessage());
                throw dcpe;
            } catch (ThreadInterruptedException tie) {
                message("Failed to read entry from cursor:" + tie.getMessage());
                throw tie;
            }
            return null;
        }

        /**
         * Close JE database and its cursor.
         */
        void close() {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (ThreadInterruptedException ignored) {
                }
                cursor = null;
            }
        }
    }

    private static String toMB(long size) {
        return String.format("%.2f", (double)size / (1024 * 1024));
    }
}

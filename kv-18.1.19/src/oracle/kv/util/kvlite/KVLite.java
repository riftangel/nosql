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

package oracle.kv.util.kvlite;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.SecurityConfigCreator;
import oracle.kv.impl.util.SecurityConfigCreator.GenericIOHelper;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A simple standalone KVStore instance.
 * Usage:
 * ...KVLite -root <kvroot> \
 *  [-store <storename> -host <hostname> -port <port>] \
 *  [-noadmin] \
 *  [-nothreads] [-shutdown] [-partitions n] [-harange start,end] \
 *  [-servicerange start,end] [-secure-config <enable|disable>] -jmx \
 *  [-restore-from-snapshot <name of snapshot>]
 *
 *   -nothreads -- means no threads and a separate process will be used for the
 *      RN.
 *   -shutdown -- will attempt to shutdown a running KVLite service, cleanly.
 *      e.g. KVLite -root <kvroot> -shutdown
 *   -noadmin -- do not start up bootstrap admin service.
 *   -secure-config <enable|disable> -- this option specifies whether to create
 *      a secure KVLite service, which is the default. When security is
 *      enabled, "-noadmin" option will be ignored, meaning that a bootstrap
 *      admin and regular will be created.
 *   -restore-from-snapshot <name of snapshot> -- When restart KVLite from
 *      existing root directory. If previous KVLite had been backup, snapshot
 *      directories will be created with snapshot name. Restore KVLite data
 *      and configurations using the specified snapshot. If no snapshot found,
 *      restart with snapshot will fail.
 *
 * -partitions, -jmx -harange and -servicerange are "hidden" arguments in that
 * they are not part of the usage message at this time.
 *
 * Kvroot is created if it does not exist.  If not provided, hostname defaults
 * to "localhost."  The -store, -host, -port, and -admin arguments only apply
 * when initially creating the store.  They are ignored for existing stores.
 *
 * This class will create and/or start a single RepNode in a single RepGroup.
 * If the adminHttpPort is used a bootstrap admin instance will be created and
 * deployed, accessible via that port; otherwise there is no admin.
 *
 * The services are created as threads inside a single process unless the
 * -nothreads option is passed, which results in independent processes.
 *
 * This program depends only on kvstore.jar and je.jar unless the
 * adminHttpPort is passed.
 */
public class KVLite {

    public final static String CONFIG_NAME="config.xml";
    public final static int DEFAULT_NUM_PARTITIONS = 10;
    private static final String DEFAULT_ROOT = "./kvroot";
    private static final String DEFAULT_STORE = "kvstore";
    private static final int DEFAULT_PORT = 5000;
    private static final String DEFAULT_STORAGE_DIR_SIZE = "1 GB";
    private static final String SECURITY_ENABLE = "enable";
    private static final String SECURITY_DISABLE = "disable";
    private LoginHandle loginHandle;
    private LoginManager loginManager;

    /* External commands, for "java -jar" usage. */
    public static final String COMMAND_NAME = "kvlite";
    public static final String COMMAND_DESC =
        "start KVLite; note all args (-host, -port, etc) have defaults";
    public static final String COMMAND_ARGS =
       mkArgLine(CommandParser.getRootUsage(), DEFAULT_ROOT) + "\n\t" +
       mkArgLine(CommandParser.getStoreUsage(), DEFAULT_STORE) + "\n\t" +
       mkArgLine(CommandParser.getHostUsage(), "local host name") + "\n\t" +
       mkArgLine(CommandParser.getPortUsage(),
                 String.valueOf(DEFAULT_PORT)) + "\n\t" +
       mkArgLine(CommandParser.getNoAdminUsage(), "false") + "\n\t" +
       mkArgLine(KVLiteParser.SECURE_CONFIG_FLAG + " <" + SECURITY_ENABLE +
                 "|" + SECURITY_DISABLE + ">", SECURITY_ENABLE) + "\n\t" +
       mkArgLine(KVLiteParser.RESTORE_FROM_SNAPSHOT + " <name of snapshot>",
                 "no restore");

    /*
     * Hidden: -shutdown, -partitions, -nothreads, -harange, -servicerange
     * -jmx, -storagedir and -storagedirsize
     */

    private String haPortRange;
    private String servicePortRange = null;
    private String host;
    private String kvroot;
    private String kvstore;
    private String mountPoint;
    private String mountPointSize = DEFAULT_STORAGE_DIR_SIZE;
    private int port;
    private boolean runBootAdmin;
    private int numPartitions;
    private StorageNodeAgentImpl sna;
    private StorageNodeAgentAPI snaAPI;
    private boolean useThreads;
    private boolean verbose;
    private boolean enableJmx;
    private ParameterMap policyMap;
    private boolean isSecure;
    private String restoreSnapshotName;
    private int memoryMB; /* optional */

    /**
     * Makes an arg usage line for an optional arg with a default value.  Adds
     * padding so default values line up neatly.  Looks like this:
     *      [argUsage]        # defaults to "defaultValue"
     */
    private static String mkArgLine(String argUsage, String defaultValue) {
       final StringBuilder builder = new StringBuilder();
       builder.append(CommandParser.optional(argUsage));
       while (builder.length() < 30) {
           builder.append(' ');
       }
       builder.append("# defaults to: ");
       builder.append(defaultValue);
       return builder.toString();
    }

    public KVLite(String kvroot,
                  String kvstore,
                  int registryPort,
                  boolean runBootAdmin,
                  String hostname,
                  String haPortRange,
                  String servicePortRange,
                  int numPartitions,
                  String mountPoint,
                  boolean useThreads,
                  boolean isSecure,
                  String restoreSnapshotName) {
        this.kvroot = kvroot;
        this.kvstore = kvstore;
        this.port = registryPort;
        this.runBootAdmin = runBootAdmin;
        this.host = hostname;
        this.haPortRange = haPortRange;
        this.servicePortRange = servicePortRange;
        this.useThreads = useThreads;
        this.mountPoint = mountPoint;
        sna = null;
        policyMap = null;
        verbose = true;
        enableJmx = false;
        this.numPartitions = numPartitions;
        this.isSecure = isSecure;
        this.restoreSnapshotName = restoreSnapshotName;
    }

    private KVLite() {
        this(null, null, 0, true, "localhost", null, null,
             DEFAULT_NUM_PARTITIONS, null, true, true, null);
    }

    public ParameterMap getPolicyMap() {
        return policyMap;
    }

    public void setPolicyMap(ParameterMap map) {
        policyMap = map;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean getVerbose() {
        return verbose;
    }

    public void setEnableJmx(boolean enableJmx) {
        this.enableJmx = enableJmx;
    }

    public boolean getEnableJmx() {
        return enableJmx;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public File getMountPoint() {
        if (mountPoint != null) {
            return new File(mountPoint);
        }
        return null;
    }

    public int getMemoryMB() {
        return memoryMB;
    }

    public void setMemoryMB(int memoryMB) {
        this.memoryMB = memoryMB;
    }

    public StorageNodeId getStorageNodeId() {
        return new StorageNodeId(1);
    }

    private BootstrapParams generateBootstrapDir()
        throws Exception {

        File rootDir = new File(kvroot);
        rootDir.mkdir();
        File configfile = new File(kvroot + File.separator + CONFIG_NAME);
        File secfile = new File
            (kvroot + File.separator + FileNames.JAVA_SECURITY_POLICY_FILE);
        if (configfile.exists()) {
            BootstrapParams bp = ConfigUtils.getBootstrapParams(configfile);
            mountPoint = getMountPointFromBP(bp);
            return bp;
        }
        if (kvstore == null || port == 0) {
            System.err.println("Store does not exist and there are " +
                               "insufficient arguments to create it.");
            new KVLiteParser(new String[0]).usage(null);
        }

        if (mountPoint == null) {
            mountPoint = getDefaultMountPoint();
        }

        ArrayList<String> mountPoints = new ArrayList<String>(1),
            sizes = new ArrayList<String>(1);
        mountPoints.add(mountPoint);
        sizes.add(mountPointSize);

        if (haPortRange == null) {
            /*
             * This is somewhat arbitrary, but if not specified, just add 5 to
             * the port to start the port range.
             */
            haPortRange = (port + 5) + "," + (port + 7);
        }

        BootstrapParams bp =
            new BootstrapParams(kvroot, host, host, haPortRange,
                                servicePortRange, null /*storeName*/, port,
                                1 /*capacity*/,
                                isSecure ? generateSecurityDir() :
                                    null /*securityDir*/,
                                isSecure || runBootAdmin /*hostingAdmin*/,
                                (enableJmx ?
                                 "oracle.kv.impl.mgmt.jmx.JmxAgent" :
                                 null));
        bp.setStorgeDirs(mountPoints, sizes);
        if (memoryMB != 0) {
            bp.setMemoryMB(memoryMB);
        }

        ConfigUtils.createBootstrapConfig(bp, configfile.toString());

        if (!secfile.exists()) {
            ConfigUtils.createSecurityPolicyFile(secfile);
        }
        return bp;
    }

    private static String getMountPointFromBP(BootstrapParams bp) {
        final ParameterMap mountMap = bp.getStorageDirMap();
        /* For backward compatibility with existing installation */
        if (mountMap == null) {
            return null;
        }
        final Iterator<Parameter> it = mountMap.iterator();
        if (!it.hasNext()) {
            return null;
        }
        /* We assume only one entry in the map for KVLite */
        return it.next().getName();
    }

    private String getDefaultMountPoint() {
        /* For kvlite, use storage node directory as storage directory */
        return FileNames.getStorageNodeDir(
                kvroot,
                kvstore,
                getStorageNodeId()).getAbsolutePath();
    }

    private void startSNA()
        throws Exception {

        final List<String> snaArgs = new ArrayList<String>();
        snaArgs.add(CommandParser.ROOT_FLAG);
        snaArgs.add(kvroot);
        snaArgs.add(StorageNodeAgent.CONFIG_FLAG);
        snaArgs.add(CONFIG_NAME);

        if (useThreads) {
            snaArgs.add(StorageNodeAgent.THREADS_FLAG);
        }

        if (restoreSnapshotName != null) {
            snaArgs.add(StorageNodeAgent.RESTORE_FROM_SNAPSHOT);
            snaArgs.add(restoreSnapshotName);
            snaArgs.add(StorageNodeAgent.UPDATE_CONFIG_FLAG);
            snaArgs.add("true");
        }

        /**
         * Tell the SNA to not start a bootstrap admin service if adminPort is
         * 0. If it is security mode, force SNA to start up bootstrap admin.
         */
        sna = new StorageNodeAgentImpl(isSecure || runBootAdmin);
        try {
            sna.parseArgs(snaArgs.toArray(new String[snaArgs.size()]));
            sna.start();
            if (!useThreads) {
                sna.addShutdownHook();
            }
        } catch (Exception e) {
            sna = null;
            throw e;
        }
        if (isSecure) {
            if (!sna.isRegistered()) {
                waitForSecurityStartUp(host, port);
            }
            loginManager = new InternalLoginManager(null);
            loginHandle = loginManager.getHandle(
               new HostPort(host, port), ResourceType.STORAGE_NODE);
        }

        snaAPI = StorageNodeAgentAPI.wrap(sna, loginHandle);
    }

    static AdminLoginManager waitForSecurityStartUp(String host, int port) {
        final AdminLoginManager alm = new AdminLoginManager(null, true);
        final int MAX_RETRY = 10;
        Exception lastException = null;
        for (int i=0; i < MAX_RETRY; i++) {
            try {
                if (!alm.bootstrap(host, port, null)) {
                    Thread.sleep(1000);
                } else {
                    return alm;
                }
            } catch (Exception e) {
                lastException = e;
            }
        }
        throw new RuntimeException(
            "Wait for admin login service fail", lastException);
    }

    private void showVersion() {
        System.out.println(KVVersion.CURRENT_VERSION);
        System.exit(0);
    }

    class KVLiteParser extends CommandParser {

        private static final String NOTHREADS_FLAG = "-nothreads";
        private static final String VERSION_FLAG = "-version";
        private static final String PARTITION_FLAG = "-partitions";
        private static final String HARANGE_FLAG = "-harange";
        private static final String SERVICERANGE_FLAG = "-servicerange";
        private static final String MOUNT_FLAG = "-storagedir";
        private static final String OLD_MOUNT_FLAG = "-mount";
        private static final String MOUNT_SIZE_FLAG = "-storagedirsize";
        private static final String JMX_FLAG = "-jmx";
        private static final String SECURE_CONFIG_FLAG = "-secure-config";
        private static final String RESTORE_FROM_SNAPSHOT =
            "-restore-from-snapshot";
        private boolean shutdown;

        public KVLiteParser(String[] args) {
            super(args);
            shutdown = false;
        }

        public boolean getShutdown() {
            return shutdown;
        }

        @Override
        protected void verifyArgs() {
            if (getRootDir() == null) {
                missingArg(ROOT_FLAG);
            }
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(StorageNodeAgent.SHUTDOWN_FLAG)) {
                shutdown = true;
                return true;
            }
            if (arg.equals(NOTHREADS_FLAG)) {
                useThreads = false;
                return true;
            }
            if (arg.equals(JMX_FLAG)) {
                enableJmx = true;
                return true;
            }
            if (arg.equals(VERSION_FLAG)) {
                showVersion();
                return true;
            }
            if (arg.equals(HARANGE_FLAG)) {
                haPortRange = nextArg(arg);
                return true;
            }
            if (arg.equals(SERVICERANGE_FLAG)) {
                servicePortRange = nextArg(arg);
                return true;
            }
            if (arg.equals(PARTITION_FLAG)) {
                numPartitions = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(MOUNT_FLAG)) {
                mountPoint = nextArg(arg);
                return true;
            }
            /* [#21880] -mount is deprecated, replaced by -storagedir */
            if (arg.equals(OLD_MOUNT_FLAG)) {
                mountPoint = nextArg(arg);
                return true;
            }
            if (arg.equals(MOUNT_SIZE_FLAG)) {
                mountPointSize = nextArg(arg);
                return true;
            }
            if (arg.equals(SECURE_CONFIG_FLAG)) {
                final String security = nextArg(arg);
                if (security.equals(SECURITY_ENABLE)) {
                    isSecure = true;
                    runBootAdmin = true;
                } else if (security.equals(SECURITY_DISABLE)) {
                    isSecure = false;
                } else {
                    usage("Unexpected value for " + SECURE_CONFIG_FLAG +
                        ": " + security);
                }
                return true;
            }
            if (arg.equals(RESTORE_FROM_SNAPSHOT)) {
                restoreSnapshotName = nextArg(arg);
                return true;
            }
            return false;
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME + "\n\t" +
                               COMMAND_ARGS);
            System.exit(1);
        }
    }

    private boolean parseArgs(String[] args) {
        String localHostname = "localhost";
        try {
            localHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            /* Use "localhost" default. */
        }
        KVLiteParser kp = new KVLiteParser(args);
        kp.setDefaults(DEFAULT_ROOT, DEFAULT_STORE, localHostname,
                       DEFAULT_PORT);
        kp.parseArgs();

        /*
         * Note we do not call setVerbose(kp.getVerbose()) because the verbose
         * option in this class is meant to be always on, or at least on by
         * default.  The verbose option in CommandParser, OTOH, is off by
         * default.
         */

        /* TODO: consider passing kp to KVLite for direct use */
        kvroot = kp.getRootDir();
        kvstore = kp.getStoreName();
        port = kp.getRegistryPort();
        runBootAdmin = kp.isRunBootAdmin();
        host = kp.getHostname();
        return kp.getShutdown();
    }

    /**
     * Tell a running KVLite instance to shut down.
     */
    public void shutdownStore(boolean force) {

        File configfile = new File(kvroot + File.separator + CONFIG_NAME);
        if (!configfile.exists()) {
            System.err.println("Cannot find configuration file for root: " +
                               kvroot);
            return;
        }
        try {
            BootstrapParams bp = ConfigUtils.getBootstrapParams(configfile);
            LoginManager login = null;
            if (bp.getSecurityDir() != null) {
                File securityDir =
                    new File(bp.getRootdir(), bp.getSecurityDir());
                File securityConfig = new File(securityDir,
                    StorageNodeAgent.DEFAULT_SECURITY_FILE);
                if (securityConfig.exists()) {
                    SecurityParams sp =
                        ConfigUtils.getSecurityParams(securityConfig);
                    sp.initRMISocketPolicies();
                    BootstrapParams.initRegistryCSF(sp);
                }
                login = new InternalLoginManager(null);
            }
            String name =
                RegistryUtils.bindingName
                (bp.getStoreName(), getStorageNodeId().getFullName(),
                 RegistryUtils.InterfaceType.MAIN);
            StorageNodeAgentAPI snai =
                RegistryUtils.getStorageNodeAgent
                (bp.getHostname(), bp.getRegistryPort(), name, login);
            System.err.println("Shutting down store " + bp.getStoreName() +
                               " in kvroot: " + kvroot);
            snai.shutdown(true, force);
        } catch (Exception e) {
            System.err.println
                ("Exception in shutdown, maybe the service is not running: " +
                 e.getMessage());
        }
    }

    /**
     * Start without waiting for services.
     */
    public void start() {
        start(false);
    }

    /**
     * Start the store, optionally waiting for the services to be in status
     * RUNNING.
     */
    public void start(boolean waitForServices) {

        try {
            BootstrapParams bp = generateBootstrapDir();
            startSNA();
            if (sna.isRegistered()) {
                if (verbose) {
                    final boolean isHostingAdmin =
                        sna.getStorageNodeAgent()
                        .getBootstrapParams()
                        .isHostingAdmin();
                    System.out.println
                        ("Opened existing kvlite store with config:\n" +
                         CommandParser.ROOT_FLAG + " " + kvroot + " " +
                         CommandParser.STORE_FLAG + " " +
                         sna.getStoreName() + " " +
                         CommandParser.HOST_FLAG + " " +
                         sna.getStorageNodeAgent().getHostname() + " " +
                         CommandParser.PORT_FLAG + " " +
                         sna.getRegistryPort() + " " +
                         (isHostingAdmin ?
                          "" :
                          CommandParser.NO_ADMIN_FLAG + " ") +
                         KVLiteParser.SECURE_CONFIG_FLAG + " " +
                         ((bp.getSecurityDir() != null) ?
                          SECURITY_ENABLE :
                          SECURITY_DISABLE) + " " +
                         StorageNodeAgent.RESTORE_FROM_SNAPSHOT + " " +
                         restoreSnapshotName);
                }
                return;
            }

            if (numPartitions == 0) {
                numPartitions = DEFAULT_NUM_PARTITIONS;
            }
            if (isSecure || runBootAdmin) {
                new KVLiteAdmin(kvstore, bp, policyMap, numPartitions).run();
            } else {
                new KVLiteRepNode(kvstore, snaAPI, bp, numPartitions).run();
            }
            if (verbose) {
                System.err.println
                    ("Created new kvlite store with args:\n" +
                     CommandParser.ROOT_FLAG + " " + kvroot + " " +
                     CommandParser.STORE_FLAG + " " + kvstore + " " +
                     CommandParser.HOST_FLAG + " " + host + " " +
                     CommandParser.PORT_FLAG + " " + port + " " +
                     (runBootAdmin ? "" : CommandParser.NO_ADMIN_FLAG + " ") +
                     KVLiteParser.SECURE_CONFIG_FLAG + " " +
                         (isSecure ? SECURITY_ENABLE : SECURITY_DISABLE) + " " +
                     ((restoreSnapshotName == null) ?
                         "" :
                         (StorageNodeAgent.RESTORE_FROM_SNAPSHOT + " " +
                         restoreSnapshotName)));
            }
            if (waitForServices) {
                if (verbose) {
                    System.out.println("Waiting for services to start");
                }
                if (isSecure || runBootAdmin) {
                    if (verbose) {
                        System.out.println
                            ("Waiting for admin at " + host + ":" + port);
                    }
                    ServiceUtils.waitForAdmin
                        (host, port, loginManager, 10, ServiceStatus.RUNNING);
                }
                ServiceStatus[] target = {ServiceStatus.RUNNING};
                if (verbose) {
                    System.out.println
                        ("Waiting for RepNode for store " + kvstore + " at " +
                         host + ":" + port);
                }
                ServiceUtils.waitForRepNodeAdmin
                    (kvstore, host, port, new RepNodeId(1,1),
                     getStorageNodeId(), loginManager, 10, target);
            }
        } catch (Exception e) {
            String trace = LoggerUtils.getStackTrace(e);
            System.err.println("KVLite: exception in start: " + trace);
        }
    }

    public void stop(boolean force) {
        if (verbose) {
            System.out.println("Stopping KVLite store " + kvstore);
        }
        if (sna == null) {
            return;
        }
        try {
            snaAPI.shutdown(true, force);
        } catch (Exception e) {
            System.err.println("Exception in stop: " + e.getMessage());
        }
    }

    public StorageNodeAgentImpl getSNA() {
        return sna;
    }

    public StorageNodeAgentAPI getSNAPI() {
        return snaAPI;
    }

    private String generateSecurityDir() {
        final ParsedConfig config = new ParsedConfig();
        /* Default to length 12 key store password */
        config.setKeystorePassword(
            SecurityUtils.generateKeyStorePassword(12));
        config.setPrintCreatedFiles(false);
        final SecurityConfigCreator scCreator =
            new SecurityConfigCreator(kvroot,
                                      config,
                                      new GenericIOHelper(System.out));
        try {
            scCreator.createConfig();
        } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception when creating " +
                "security configurations", e);
        }
        return config.getSecurityDir();
    }

    public static void main(String[] args) {

        KVLite store = new KVLite();
        boolean shutdown = store.parseArgs(args);
        if (shutdown) {
            store.shutdownStore(false);
        } else {
            store.start();
        }
    }
}

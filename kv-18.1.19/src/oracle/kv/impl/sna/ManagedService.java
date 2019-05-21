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

package oracle.kv.impl.sna;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A class to implement a service managed by the Storage Node Agent.  It has a
 * main() and expects specific arguments which are used by the SNA to identify
 * the process as one it manages.  It's main() method should be called in the
 * execution context of the service to be started, which is either a new
 * process or thread.  See the usage() method for usage.
 *
 * This class can be used in either the context of the Storage Node Agent or
 * the target service.  In the SNA it provides a consistent way to create and
 * pass required arguments to the service.
 *
 * It has a convenience method, createArgs(), which can be used to construct
 * the arguments to main() from the current state of the object.
 *
 * TODO: absolute paths for jps, kill, taskkill
 */
public abstract class ManagedService {

    /**
     * These are protected so they can be accessed by sub-classes.  They are
     * "final" for the most part but not set as such to allow
     * ManagedBootstrapAdmin to reset them if necessary.
     */
    protected File kvRootDir;
    protected File kvSecDir;
    protected File kvSNDir;
    protected String kvName;
    protected String serviceName;
    final protected String serviceClass;
    protected ParameterMap params;
    protected Logger logger;
    protected StringBuilder startupBuffer;
    protected static boolean usingThreads;

    public static final String REP_NODE_NAME = "RepNode";
    public static final String ADMIN_NAME = "Admin";
    public static final String BOOTSTRAP_ADMIN_NAME = "BootstrapAdmin";
    public static final String ARB_NODE_NAME = "ArbNode";
    public static final String LOG_CONFIG_PREFIX = "config.";

    /* Flags used for exec args */
    public static final String ROOT_FLAG = CommandParser.ROOT_FLAG;
    public static final String SECDIR_FLAG = "-secdir";
    public static final String STORE_FLAG = CommandParser.STORE_FLAG;
    public static final String CLASS_FLAG = "-class";
    public static final String SERVICE_FLAG = "-service";
    public static final String THREADS_FLAG = StorageNodeAgent.THREADS_FLAG;
    public static final String STARTUP_OK = "ManagedServiceStarted";

    private static final String JAVA_VERSION_KEY = "java.version";

    private static final String IBM_VENDOR_PREFIX = "IBM";

    public ManagedService(File kvRootDir,
                          File kvSecDir,
                          File kvSNDir,
                          String kvName,
                          String serviceClass,
                          String serviceName,
                          ParameterMap params) {
        this.kvRootDir = kvRootDir;
        this.kvSecDir = kvSecDir;
        this.kvSNDir = kvSNDir;
        this.kvName = kvName;
        this.serviceClass = serviceClass;
        this.serviceName = serviceName;
        this.params = params;
        logger = null;
    }

    /**
     * This method must be run in the execution context of the service.
     */
    public abstract void start(boolean usingThreads1);

    /**
     * Whether a request has been made to stop this service.
     *
     * @return whether a stop has been requested
     */
    public abstract boolean stopRequested();

    /* Log InetAddress dns properties. */
    protected void logInetAddressProperties() {
        if (logger != null) {
            logger.info
                ("JVM java.net.InetAddress properties: cache.ttl=" +
                 java.security.Security.getProperty
                 ("networkaddress.cache.ttl") +
                 " cache.negative.ttl=" +
                 java.security.Security.getProperty
                 ("networkaddress.cache.negative.ttl"));
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getKvName() {
        return kvName;
    }

    public Logger getLogger() {
        return logger;
    }

    public String getJVMArgs() {
        if (params != null) {
            return params.get(ParameterState.JVM_MISC).asString();
        }
        return null;
    }

    /**
     * Returns the environment variables and values associated with the
     * service process.
     */
    public Map<String, String> getEnvironment() {
        return null;
    }

    public String getLoggingConfig() {
        if (params != null) {
            return params.get(ParameterState.JVM_LOGGING).asString();
        }
        return null;
    }

    /**
     * Returns the JVM parameter string used to configure GC logging
     */
    String getGCLoggingArgs(final Parameter gcLogFiles,
                            final Parameter gcLogFileSize,
                            final String resourceName,
                            final RepNodeParams rnp) {

        final int javaVersion = getJavaMajorVersion();
        if (javaVersion < 7) {

            /* Versions before 1.7 do not support log rotation. */
            return "";
        }

        final String jvmVendor = System.getProperty("java.vendor");
        if (jvmVendor != null && jvmVendor.startsWith(IBM_VENDOR_PREFIX)) {
            /* Do not use log rotation for IBM java vendor. */
            return "";
        }

        /*
         * Support for rgx-rny.gc logs in RN log directory
         */
        final String gcFileName;
        if (rnp != null && rnp.getLogDirectoryFile()!= null) {
            gcFileName =
                FileNames.getRNGCLoggingDir
                    (rnp.getLogDirectoryFile(), resourceName).toString() +
                    ".gc";
        } else {
            gcFileName =
                new File(FileNames.getLoggingDir(kvRootDir, kvName),
                    resourceName).toString() + ".gc";
        }

        if (javaVersion >= 9) {
            return "-Xlog:gc=info:file=" + gcFileName +
                ":time:filecount=" + gcLogFiles.asString() +
                ",filesize=" + gcLogFileSize.asString();
        }

        return " -XX:+PrintGCDetails -XX:+PrintGCDateStamps "
            + " -XX:+PrintGCApplicationStoppedTime"
            + " -XX:+UseGCLogFileRotation"
            + " -XX:NumberOfGCLogFiles=" + gcLogFiles.asString()
            + " -XX:GCLogFileSize=" + gcLogFileSize.asString()
            + " -Xloggc:" + gcFileName + " " ;
    }

    /**
     * Returns the default args to be associated with the JVM, given the
     * specific args supplied for the service. Knowledge of the specific
     * args is used to customize the default args so that there are not
     * conflicting arguments, eg. use of both the CMS and G1 GC.
     *
     * @param overrideJvmArgs the overriding jvm args
     */
    public String getDefaultJavaArgs(String overrideJvmArgs) {
        return null;
    }

    /**
     * Returns the major version number of the current Java VM as an integer,
     * returning -1 if the version cannot be determined.  Handles three version
     * styles:
     * <ul>
     * <li> 1.8.0_151 returns 8 (for versions 1.8 and earlier)
     * <li> 9.0.4 returns 9 (for release versions 9 and later)
     * <li> 10-ea returns 10 (for early access versions)
     * </ul>
     */
    public static int getJavaMajorVersion() {
        return getJavaMajorVersion(System.getProperty(JAVA_VERSION_KEY));
    }

    /** Provide version as an argument, for testing. */
    static int getJavaMajorVersion(String version) {
        if (version != null) {
            if (version.endsWith("-ea")) {

                /* major-ea */
                final String v1 = version.substring(0, version.length() - 3);
                try {
                    return Integer.parseInt(v1);
                } catch (NumberFormatException e) {
                }
            } else {
                final int dot1 = version.indexOf('.');
                if (dot1 > 0) {
                    final String v1 = version.substring(0, dot1);
                    if ("1".equals(v1)) {

                        /* 1.major.xxx */
                        final int dot2 = version.indexOf('.', dot1 + 1);
                        if (dot2 > 0) {
                            final String v2 =
                                version.substring(dot1 + 1, dot2);
                            try {
                                return Integer.parseInt(v2);
                            } catch (NumberFormatException e) {
                            }
                        }
                    } else {

                        /* major.xxx */
                        try {
                            return Integer.parseInt(v1);
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            }
        }

        /* Bad Java version */
        return -1;
    }

    public synchronized void setStartupBuffer(StringBuilder buf) {
        startupBuffer = buf;
    }

    public synchronized StringBuilder getStartupBuffer() {
        return startupBuffer;
    }

    public static void setUsingThreads(boolean value) {
        usingThreads = value;
    }

    public abstract ResourceId getResourceId();

    public abstract void resetHandles();

    public abstract void resetParameters(boolean inTarget);

    /**
     * Does this service need to reset its command line argument on restart?
     * Default to no.
     */
    public boolean resetOnRestart() {
        return false;
    }

    /**
     * Start logging, which is only done in the execution context.
     */
    protected void startLogger(Class<?> cl,
                               ResourceId rid,
                               LoadParameters lp) {

        GlobalParams globalParams =
            new GlobalParams(lp.getMapByType(ParameterState.GLOBAL_TYPE));
        StorageNodeParams storageNodeParams =
            new StorageNodeParams
            (lp.getMapByType(ParameterState.SNA_TYPE));
        logger = LoggerUtils.getLogger(cl,
                                       rid.toString(),
                                       rid,
                                       globalParams,
                                       storageNodeParams);

        /* Log the JVM command line if using processes */
        if (!usingThreads) {
            RuntimeMXBean runtimeBean =
                ManagementFactory.getRuntimeMXBean();
            logger.info("Starting service process: " + rid.toString() +
                        ", Java command line arguments: " +
                        runtimeBean.getInputArguments());
        }
    }

    /**
     * Start RN logging, which is only done in the execution context.
     */
    protected void startRNLogger(Class<?> cl,
                               RepNodeParams rnp,
                               ResourceId rid,
                               LoadParameters lp) {

        GlobalParams globalParams =
            new GlobalParams(lp.getMapByType(ParameterState.GLOBAL_TYPE));
        StorageNodeParams storageNodeParams =
            new StorageNodeParams(lp.getMapByType(ParameterState.SNA_TYPE));
        RepNodeParams repNodeParams = rnp;
        logger = LoggerUtils.getLogger(cl,
                                       rid.toString(),
                                       rid,
                                       globalParams,
                                       storageNodeParams,
                                       repNodeParams);

        /* Log the JVM command line if using processes */
        if (!usingThreads) {
            RuntimeMXBean runtimeBean =
                ManagementFactory.getRuntimeMXBean();
            logger.info("Starting service process: " + rid.toString() +
                        ", Java command line arguments: " +
                        runtimeBean.getInputArguments());
        }
    }

    /**
     * Get the PropertySheet for this instance.  This is called in the
     * execution context.
     */
    protected LoadParameters getParameters() {

        File kvConfigPath = new File(kvSNDir, FileNames.SNA_CONFIG_FILE);
        LoadParameters lp = LoadParameters.getParameters(kvConfigPath, logger);
        return lp;
    }

    /**
     * Get the SecurityParams for this instance.  This is called in the
     * execution context.
     */
    protected SecurityParams getSecurityParameters() {

        if (kvSecDir == null) {
            return SecurityParams.makeDefault();
        }

        File securityConfigPath = new File(kvSecDir,
                                           FileNames.SECURITY_CONFIG_FILE);
        if (!securityConfigPath.exists()) {
            throw new IllegalStateException(
                "The security configuraton file " + securityConfigPath +
                " does not exist.");
        }

        LoadParameters lp = LoadParameters.getParameters(securityConfigPath,
                                                         logger);
        SecurityParams sp = new SecurityParams(lp, securityConfigPath);
        return sp;
    }

    /**
     * Create a logging config file based on the properties string.  Exceptions
     * in the function are logged but are not fatal.
     */
    public String createLoggingConfigFile(String properties) {

        String pathToFile = null;
        try {

            /**
             * Logging config files are per-service and kept in the store's log
             * directory, named as "config.servicename."
             */
            File logConfigFile =
                new File(FileNames.getLoggingDir(kvRootDir, kvName),
                         LOG_CONFIG_PREFIX + serviceName);

            /**
             * Save properties to the file.
             */
            Properties props = new Properties();
            String header = "Logging properties for " + serviceName +
                ". DO NOT EDIT!";
            props.load(ConfigUtils.getPropertiesStream(properties));
            props.store(new FileOutputStream(logConfigFile), header);
            pathToFile = logConfigFile.toString();
        } catch (Exception e) {
            if (logger != null) {
                logger.warning("Could not configure logging config file for " +
                               serviceName + ": " + e.getMessage());
            }
        }
        return pathToFile;
    }

    /**
     * Kill a process.  This is somewhat complicated by the need to handle
     * Windows as well.  TODO: consider absolute paths to kill, taskkill.
     */
    public static void killProcess(Integer pid) {

        boolean isWindows;
        String os = System.getProperty("os.name");
        if (os.indexOf("Windows") != -1) {
            isWindows = true;
        } else {
            isWindows = false;
        }
        String[] command;
        int i = 0;
        if (isWindows) {
            command = new String[4];
            command[i++] = "taskkill";
            command[i++] = "/f";
            command[i++] = "/pid";
        } else {
            command = new String[3];
            command[i++] = "kill";
            command[i++] = "-9";
        }
        command[i] = pid.toString();
        try {
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
        } catch (Exception ignored) {
        }
    }

    /**
     * Kill any processes that match the pattern.
     *
     * @param storeName the KVStore name to find
     *
     * @param serviceName if non-null the service name to find
     *
     * @param logger if non-null, log the kills
     *
     */
    public static void killManagedProcesses(String storeName,
                                            String serviceName,
                                            Logger logger) {

        List<Integer> list =
            findManagedProcesses(storeName, serviceName, logger);
        for (Integer pid : list) {
            if (logger != null) {
                logger.info("Killing managed process " + pid + " for " +
                            "store, serviceName: "+ storeName + ", " +
                            serviceName);
            }
            killProcess(pid);
        }
    }

    protected static void killManagedProcesses(String className,
                                               String storeName,
                                               String serviceName,
                                               Logger logger) {

        List<Integer> list =
            findManagedProcesses(className, storeName, serviceName, logger);
        for (Integer pid : list) {
            if (logger != null) {
                logger.info("Killing managed process " + pid + " matching " +
                            "these fields: "+ storeName + ", " +
                            serviceName);
            }
            killProcess(pid);
        }
    }

    /**
     * Return a list of process IDs for processes that are managed by
     * a particular SNA.
     *
     * @param storeName the KVStore name to find (can be null)
     *
     * @param serviceName if non-null the service name to find
     *
     * @param logger if non-null, log failure information
     *
     * @return list of process ids that match the parameters
     */
    public static List<Integer> findManagedProcesses(String storeName,
                                                     String serviceName,
                                                     Logger logger) {

        return findManagedProcesses("ManagedService", storeName, serviceName,
                                    logger);
    }

    /**
     * "Internal" version of findManagedProcesses that also takes the class
     * name.
     */
    protected static List<Integer> findManagedProcesses(String className,
                                                        String storeName,
                                                        String serviceName,
                                                        Logger logger) {

        List<Integer> list = new ArrayList<Integer>();
        try {
            List<String> command = new ArrayList<String>();
            command.add("jps");
            command.add("-m");
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.redirectErrorStream(true);
            Process process = builder.start();
            BufferedReader reader = new BufferedReader
                (new InputStreamReader(process.getInputStream()));
            for (String line = reader.readLine();
                 line != null;
                 line = reader.readLine()) {
                if ((line.indexOf(className) >= 0) &&
                    (storeName == null ||
                     line.indexOf(storeName) >= 0) &&
                    (serviceName == null ||
                     line.indexOf(serviceName) >= 0) &&
                    /* exclude -shutdown -- it is probably this process */
                    line.indexOf(StorageNodeAgent.SHUTDOWN_FLAG) < 0) {
                    String[] args = line.split(" ");
                    list.add(new Integer(args[0]));
                }
            }
        } catch (Exception e) {
            if (logger != null) {
                logger.info
                    ("findManagedProcesses exception: " + e.getMessage());
            }
        }
        return list;
    }

    public static void usage()
        throws IllegalArgumentException {

        /**
         * At this point there is no log file.  Stderr will be captured by the
         * managing SNA process and put into its log.
         */
        System.err.println("Usage: ...ManagedService " +
                           ROOT_FLAG + " <rootdir> " +
                           STORE_FLAG + " <storename> " +
                           CLASS_FLAG + " <serviceClass> " +
                           SERVICE_FLAG + " <serviceName>");
        throw new IllegalArgumentException
            ("Could not parse ManagedService args");
    }

    /**
     * Construct the arguments expected by this class from its
     * state.  This called when using threads to create the service.
     */
    public String[] createArgs() {
        String[] args;
        final int kvNameCount = (null == kvName) ? 0 : 2;
        final int secDirCount = (null == kvSecDir) ? 0 : 2;
        args = new String[11 + kvNameCount + secDirCount];
        int i = 0;
        args[i++] = ROOT_FLAG;
        args[i++] = kvSNDir.toString();
        if (kvSecDir != null) {
            args[i++] = SECDIR_FLAG;
            args[i++] = kvSecDir.toString();
        }
        if (kvName != null) {
            args[i++] = STORE_FLAG;
            args[i++] = kvName;
        }
        args[i++] = CLASS_FLAG;
        args[i++] = serviceClass;
        args[i++] = SERVICE_FLAG;
        args[i++] = serviceName;
        args[i++] = THREADS_FLAG;
        additionalArgs(args, i);
        return args;
    }

    public List<String> addExecArgs(List<String> command) {

        /**
         * Use ManagedService here rather than getClass().getName() so that
         * findManagedProcesses() can work more simply.
         */
        command.add("oracle.kv.impl.sna.ManagedService");
        command.add(ROOT_FLAG);
        command.add(kvSNDir.toString());
        if (kvSecDir != null) {
            command.add(SECDIR_FLAG);
            command.add(kvSecDir.toString());
        }
        if (kvName != null) {
            command.add(STORE_FLAG);
            command.add(kvName);
        }
        command.add(CLASS_FLAG);
        command.add(serviceClass);
        command.add(SERVICE_FLAG);
        command.add(serviceName);
        additionalExecArgs(command);
        return command;
    }

    /**
     * Allow subclasses to add additional arguments.
     */
    public void additionalExecArgs
        (@SuppressWarnings("unused") List<String> command) {
    }

    public int additionalArgs(@SuppressWarnings("unused") String[] args,
                              int index) {
        return index;
    }

    public static void main(String[] args) {
        String kvSecDir = null;
        String kvSNDir = null;
        String kvName = null;
        String serviceClass = null;
        String serviceName = null;
        String bootstrapConfigFile = null;
        boolean usingThreads1 = false;
        int argc = 0;
        int nArgs = args.length;

        while (argc < nArgs) {
            String thisArg = args[argc++];
            if (thisArg == null) {
                continue;
            }
            if (thisArg.equals(ROOT_FLAG)) {
                if (argc < nArgs) {
                    kvSNDir = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals(SECDIR_FLAG)) {
                if (argc < nArgs) {
                    kvSecDir = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals(StorageNodeAgent.CONFIG_FLAG)) {
                if (argc < nArgs) {
                    bootstrapConfigFile = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals(STORE_FLAG)) {
                if (argc < nArgs) {
                    kvName = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals(CLASS_FLAG)) {
                if (argc < nArgs) {
                    serviceClass = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals(SERVICE_FLAG)) {
                if (argc < nArgs) {
                    serviceName = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals(THREADS_FLAG)) {
                usingThreads1 = true;
            } else {
                usage();
            }
        }

        /**
         * kvName can be null in the case of a bootstrap admin instance.
         */
        if (kvSNDir == null ||
            serviceClass == null ||
            serviceName == null ) {
            usage();
        }

        /**
         * Set up dns cache ttl parameter for java.net.InetAddress;
         */
        String cacheTTL = System.getProperty
            ("kvdns.networkaddress.cache.ttl");
        if (cacheTTL != null) {
            java.security.Security.setProperty
                ("networkaddress.cache.ttl" , cacheTTL);
            java.security.Security.setProperty
                ("networkaddress.cache.negative.ttl", cacheTTL);
        }

        ManagedService.setUsingThreads(usingThreads1);
        ManagedService ms = null;
        try {
            if (REP_NODE_NAME.equals(serviceClass)) {
                ms = new ManagedRepNode(kvSecDir, kvSNDir, kvName,
                                        serviceClass, serviceName);
            } else if (ADMIN_NAME.equals(serviceClass) &&
                       serviceName != null) {
                if (serviceName.indexOf(BOOTSTRAP_ADMIN_NAME) >= 0) {
                    if (bootstrapConfigFile == null) {
                        usage();
                    }
                    ms = new ManagedBootstrapAdmin(kvSNDir,
                                                   kvSecDir,
                                                   bootstrapConfigFile,
                                                   serviceName);
                } else {
                    ms = new ManagedAdmin(kvSecDir, kvSNDir, kvName,
                                          serviceClass, serviceName);
                }
            } else if (ARB_NODE_NAME.equals(serviceClass)) {
                ms = new ManagedArbNode(kvSecDir, kvSNDir, kvName,
                                        serviceClass, serviceName);
            } else {
                throw new IllegalArgumentException
                    ("Unknown service name " + serviceClass);
            }
            ms.start(usingThreads1);

            /*
             * This tells the SNA that the service got this far.  This allows
             * the SNA to isolate JVM and service startup problems from
             * "runtime" issues.
             */
            if (!usingThreads1) {
                System.err.println(STARTUP_OK + ": " + serviceName);
            }
        } catch (Exception e) {
            String msg = "Exception creating service " +
                serviceName + ": " + e.getMessage() + ": " +
                LoggerUtils.getStackTrace(e);
            if (ms != null && ms.getLogger() != null) {
                ms.getLogger().log(
                    ms.stopRequested() ? Level.INFO : Level.SEVERE,
                    msg);
            } else {
                System.err.println(msg);
            }

            /**
             * Also print to System.err in the event the logger isn't yet
             * initialized.
             */
            System.err.println(msg);
            LoggerUtils.closeAllHandlers();
        }
        System.err.flush();
    }

    /**
     * Build a File from a fileName, if not null.
     */
    static File nullableFile(String fileName) {
        return (fileName == null) ? null : new File(fileName);
    }

}

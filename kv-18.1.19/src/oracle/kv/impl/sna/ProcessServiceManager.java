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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.mgmt.MgmtAgent;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.test.TwoArgTestHook;
import oracle.kv.impl.test.TwoArgTestHookExecute;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * Implementation of ServiceManager that uses processes.
 */
public class ProcessServiceManager extends ServiceManager {
    private AgentRepository agentRepository;
    private ProcessMonitor monitor;
    private MgmtAgent mgmtAgent;
    private static String pathToJava;
    private final StorageNodeAgent sna;
    private boolean stopExplicitly = false;

    /**
     * Test hook that is called to permit reading or updating the exec
     * arguments on startup or restart.  The first argument is the list of
     * command line arguments, and the second is this instance.
     */
    private static TwoArgTestHook<List<String>, ProcessServiceManager>
        createExecArgsHook;

    public ProcessServiceManager(StorageNodeAgent sna,
                                 ManagedService service) {
        super(sna, service);
        this.sna = sna;
        this.mgmtAgent = sna.getMgmtAgent();
        this.agentRepository = null;
        monitor = null;
        registered(sna);
    }

    class ServiceProcessMonitor extends ProcessMonitor {
        private final ServiceManager mgr;

        public ServiceProcessMonitor(ServiceManager mgr,
                                     List<String> command,
                                     Map<String, String> env) {
            super(command, env, -1, service.getServiceName(), logger);
            this.mgr = mgr;
        }

        private void generateStatusChange(ServiceStatus status) {
            if (agentRepository != null) {
                ResourceId rid = getService().getResourceId();
                ProxiedServiceStatusChange sc =
                    new ProxiedServiceStatusChange(rid, status);
                logger.info("generate status change: " + sc);
                agentRepository.add(sc);
                mgmtAgent.proxiedStatusChange(sc);
            }
        }

        @Override
        protected void onExit() {

            service.setStartupBuffer(startupBuffer);
            final ResourceId rId = service.getResourceId();
            if (rId instanceof RepNodeId) {
                if (sna.getMasterBalanceManager() != null) {
                    sna.getMasterBalanceManager().noteExit((RepNodeId)rId);
                }
            }

            if (stopExplicitly) {
                /*
                 * If stop explicitly, proxy a STOPPED state because that
                 * happens on admin-generated shutdown and may not have been
                 * picked up by the monitor.
                 */
                generateStatusChange(ServiceStatus.STOPPED);
            } else {
                /*
                 * exceed limited restart count or exit code indicates that the
                 * process don't need to be restarted.
                 */
                generateStatusChange(ServiceStatus.ERROR_NO_RESTART);
            }
        }

        @Override
        protected void onRestart() {

            final ResourceId rId = service.getResourceId();
            if (rId instanceof RepNodeId) {
               sna.getMasterBalanceManager().noteExit((RepNodeId)rId);
            }

            if (getExitCode() == 0) {
                generateStatusChange(ServiceStatus.EXPECTED_RESTARTING);
            } else {
                generateStatusChange(ServiceStatus.ERROR_RESTARTING);
            }
            service.resetHandles();
            service.resetParameters(false);
            if (service.resetOnRestart() || (createExecArgsHook != null)) {
                mgr.reset();
            }
        }

        @Override
        protected void afterStart() {
            mgr.notifyStarted();
        }
    }

    @Override
    public void registered(StorageNodeAgent sna1) {
        if (agentRepository == null) {
            agentRepository = (sna1.getMonitorAgent() != null) ?
                sna1.getMonitorAgent().getAgentRepository() : null;
        }
    }

    @Override
    public void start()
        throws Exception {

        List<String> command = createExecArgs();
        Map<String, String> env = service.getEnvironment();
        if (logger != null) {
            logger.info("Executing process with arguments: " + command + 
                        " with environment: " + env);
        }
        monitor = new ServiceProcessMonitor(this, command, env);
        monitor.startProcess();
    }

    /**
     * Terminate managed process with prejudice.
     */
    @Override
    public void stop() {

        try {
            if (monitor != null) {
                monitor.stopProcess(false);
            }
        } catch (InterruptedException ie) {
        } finally {
            monitor.destroyProcess();
            try {
                monitor.waitProcess(0);
            } catch (InterruptedException ignored) {
                /* nothing to do if this was interrupted */
            }
        }
    }

    @Override
    public void waitFor(int millis) {

        try {
            if (monitor != null) {
                if (!monitor.waitProcess(millis)) {
                    logger.info("Service did not exiting cleanly in " + millis
                                + " milliseconds, it will be killed");
                    stop();
                }
            }
        } catch (InterruptedException ie) {
        }
    }

    @Override
    public void dontRestart() {

        /* User stop service explicitly. */
        stopExplicitly = true;
        if (monitor != null) {
            monitor.dontRestart();
        }
    }

    @Override
    public boolean isRunning() {
        if (monitor != null) {
            return monitor.isRunning();
        }
        return false;
    }

    /**
     * Reset ProcessMonitor command using the current service info.
     */
    @Override
    public void reset() {
        List<String> command = createExecArgs();
        monitor.reset(command, service.getServiceName());
    }

    /**
     * Force is OK with processes.
     */
    @Override
    public boolean forceOK(boolean force) {
        return force;
    }

    @Override
    public void resetLogger(Logger logger1) {
        this.logger = logger1;
        monitor.resetLogger(logger);
    }

    @Override
    public void reloadSNParams() {
        mgmtAgent = sna.getMgmtAgent();
    }

    /* For testing */
    public int getExitCode() {
        return monitor.getExitCode();
    }

    /**
     * Terminate managed process without any bookkeeping -- this simulates
     * a random exit.  The process should be restarted by the ProcessMonitor.
     */
    void destroy() {
        if (monitor != null) {
            monitor.destroyProcess();
            try {
                monitor.waitProcess(0);
            } catch (InterruptedException ignored) {
                /* nothing to do if this was interrupted */
            }
        }
    }

    /**
     * Tiny test class to test exec of java.
     */
    public static class TestJavaExec {
        public static final int EXITCODE = 75;
        public static void main(String args[]) {
            System.exit(EXITCODE);
        }
    }

    /**
     * Try exec'ing "java.home"/bin/java.  If it works, use it.  If not,
     * return "java" and rely on the PATH environment variable.
     */
    private String findJava() {
        String home = System.getProperty("java.home");
        String cp = System.getProperty("java.class.path");
        String path = home + File.separator + "bin" +
            File.separator + "java";
        String execString = path;
        if (cp != null) {
            execString = execString + " -cp " + cp;
        }
        execString = execString + " " + getClass().getName() + "$TestJavaExec";
        try {
            Process process = Runtime.getRuntime().exec(execString);
            process.waitFor();
            if (process.exitValue() == TestJavaExec.EXITCODE) {
                return path;
            }
        } catch (Exception e) {
            logger.info("Unable to exec test process: " +
                        execString + ", exception: " + e);
        }
        return "java";
    }

    /**
     * Determine the path to the JVM used to execute this process and use it
     * for execution of new JVMs.  Test it out with a tiny program.
     */
    private synchronized String getPathToJava() {
        if (pathToJava == null) {
            pathToJava = findJava();
            logger.info("Using java program: " + pathToJava +
                        " to execute managed processes");
        }
        return pathToJava;
    }

    /**
     * TODO: think about inferred arguments that could be added based on
     * heap size.  E.g.:
     * < 4G -d32
     * > 4G -d64 if available, -XX:+UseCompressedOops
     * > 32G -- they should just figure it out.
     */
    private void addJavaMiscArgs(List<String> command,
                                 String jvmArgs) {

        String miscParams = "";
        /* A service may add its own default arguments */
        if (service.getDefaultJavaArgs(jvmArgs) != null) {
            miscParams = service.getDefaultJavaArgs(jvmArgs);
        }
        if (jvmArgs != null) {
            miscParams += " " + jvmArgs;
        }
        if (miscParams.length() != 0) {
            String[] args = miscParams.trim().split("\\s+");
            for (String arg : args) {

                /**
                 * Replace leading/trailing quotes that may have ended up in
                 * the command.  These will cause problems.
                 */
                arg = arg.replaceAll("^\"|\"$", "");
                arg = arg.replaceAll("^\'|\'$", "");
                command.add(arg);
            }
        }
    }

    private void addJavaExtraArgs(List<String> command) {
        final String jvmExtraArgs =
            System.getProperty("oracle.kv.jvm.extraargs");
        if (jvmExtraArgs != null) {
            for (String arg : splitExtraArgs(jvmExtraArgs)) {
                command.add(arg);
            }
            command.add("-Doracle.kv.jvm.extraargs=" + jvmExtraArgs);
        }
    }

    /* Add args that needs to be inherited from this process. */
    private void addInheritedArgs(List<String> command) {
        String cacheTTL = System.getProperty
            ("kvdns.networkaddress.cache.ttl");
        if (cacheTTL != null) {
             command.add("-Dkvdns.networkaddress.cache.ttl=" +
                         cacheTTL);
        }
    }

    private static String[] splitExtraArgs(String extraArgs) {
        return extraArgs.split(";");
    }

    private void addLoggingArgs(List<String> command,
                                String loggingConfig) {

        if (loggingConfig == null || loggingConfig.length() == 0) {
            return;
        }
        String logConfigFile = service.createLoggingConfigFile(loggingConfig);
        if (logConfigFile != null) {
            command.add("-Djava.util.logging.config.file=" + logConfigFile);
        }
    }

    private boolean addAssertions(List<String> command) {
        command.add("-ea");
	return true;
    }

    private List<String> createExecArgs() {

        List<String> command = new ArrayList<String>();

        String customStartupPrefix = sna.getCustomProcessStartupPrefix();
        if ((customStartupPrefix != null) && !customStartupPrefix.isEmpty()){
            command.add(customStartupPrefix);
        }

        command.add(getPathToJava());
        JVMSystemUtils.addZingJVMArgs(command);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));

        /**
         * If Java arguments and/or logging configuration are present, add
         * them.
         */
        String jvmArgs = service.getJVMArgs();
        String loggingConfig = service.getLoggingConfig();
        addJavaMiscArgs(command, jvmArgs);
        TwoArgTestHookExecute.doHookIfSet(createExecArgsHook, command, this);
        addJavaExtraArgs(command);
        addInheritedArgs(command);
        assert(addAssertions(command));
        if (loggingConfig != null) {
            /*
             * Logging configuration.  This will create the logging config file.
             */
            addLoggingArgs(command, loggingConfig);
        } else {
            String logConfig =
                System.getProperty("java.util.logging.config.file");
            if (logConfig != null) {
                command.add("-Djava.util.logging.config.file=" + logConfig);
            }
        }
        return service.addExecArgs(command);
    }

    /*
     * Set the test hook to read or modify the command line arguments.
     */
    public static void setCreateExecArgsHook(
        TwoArgTestHook<List<String>, ProcessServiceManager> hook) {
        createExecArgsHook = hook;
    }
}

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

package oracle.kv.impl.diagnostic.ssh;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.diagnostic.JavaVersionVerifier;
import oracle.kv.impl.diagnostic.SNAInfo;
import oracle.kv.impl.security.util.ShellPasswordReader;
import oracle.kv.impl.util.ConfigUtils;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

/**
 * A SSH client is to connect with SSH server and it executes all remote
 * commands in SSH server to exchange info, generate files and get files.
 */
public class SSHClient {
    /* SSH port */
    private int SSH_PORT = 22;
    private String EXE_CMD = "exec";
    private String SFTP_CMD = "sftp";

    /* Files to ignore, using shell file pattern syntax */
    private String[] FILTERS = new String[] {"*.bup",
                                             "*.jdb",
                                             "*/admin*/webapp/*"};
    private int BUFFER_SZIE = 4 * 1024;
    private int INTERVAL_TIME = 500;
    private String UNDER_LINE = "_";
    private String LINUX_HOME_SIGN = "~";
    private long begin;
    private long end;

    private String USER_HOME = System.getProperty("user.home");

    private JSch jsch = null;
    private Session jschSession = null;
    private String host = null;
    private String username = null;
    private boolean openStatus = false;
    private String errorMessage;

    public SSHClient(String host, String username) {
        jsch = new JSch();
        this.host = host;
        this.username = username;
    }

    /**
     * Check whether client is open
     *
     * @return true when client is open, or false
     */
    public boolean isOpen() {
        return openStatus;
    }

    /**
     * Get error message indicating the reason causing client is not open
     * correctly
     *
     * @return error message
     */
    public String getErrorMessage() {
        if (openStatus)
            return null;
        return errorMessage;
    }

    /**
     * Open client
     */
    public void open() {
        /* Create session by public authenticated file */
        jschSession = createSessionByAuthFile();
        if (jschSession == null || !jschSession.isConnected()) {
            /* create session by password */
            jschSession = createSessionByPassword();
        }

        setStatus();
    }

    /**
     * Open client for unit test
     */
    public void open(String password) {
        /* Create session by public authenticated file */
        jschSession = createSessionByAuthFile();
        if (jschSession == null || !jschSession.isConnected()) {
            /* create session by password */
            jschSession = createSessionByPassword(password);
        }

        setStatus();
    }

    /**
     * Open client by public authenticated file
     */
    public void openByAuthenticatedFile() {
        jschSession = createSessionByAuthFile();
        setStatus();
    }

    /**
     * Open client by password
     */
    public void openByPassword() {
        jschSession = createSessionByPassword();
        setStatus();
    }

    /**
     * Open client by password for unit test
     */
    public void openByPassword(String password) {
        jschSession = createSessionByPassword(password);
        setStatus();
    }

    /**
     * Close client
     */
    public void close() {
        if (jschSession != null && jschSession.isConnected()) {
            jschSession.disconnect();
        }
    }

    /**
     * Set status of client
     */
    private void setStatus() {
        if (jschSession != null && jschSession.isConnected()) {
            errorMessage = null;
            openStatus = true;
        } else {
            openStatus = false;
        }
    }

    /**
     * Create a session for SSH to connect SSH server by authenticated file.
     *
     * @return session of SSH
     * @throws Exception
     */
    private Session createSessionByAuthFile() {

        /*
         * return null when can not find public authentication key directory
         */
        File authKeyDirectory = new File(USER_HOME + "/.ssh");
        if (!authKeyDirectory.exists() || authKeyDirectory.isFile()) {
            return null;
        }

        File[] files = authKeyDirectory.listFiles();
        Session session = null;

        /* Try all authentication file in the .ssh directory in parallel */
        int numberSSHThread = files.length;
        ThreadPoolExecutor threadExecutor =
                new ThreadPoolExecutor(numberSSHThread,
                                       numberSSHThread,
                                       0L,
                                       TimeUnit.MILLISECONDS,
                                       new LinkedBlockingQueue<Runnable>());
        try {
            /*
             * Create session by authenticated files in parallel to improve
             * performance
             */
            List<Future<Session>> sessionFutureList =
                    new ArrayList<Future<Session>>();
            for (final File f : files) {
                if (f.isFile()) {
                    Callable<Session> clientOpenCallable =
                            new Callable<Session>() {

                        @Override
                        public Session call() {
                            try {
                                /* Create session by a authenticated file */
                                return createSessionByOneAuthFile(
                                    f.getAbsolutePath());
                            } catch (Exception ex) {
                                errorMessage = ex.getMessage();
                                return null;
                            }
                        }
                    };
                    sessionFutureList.
                        add(threadExecutor.submit(clientOpenCallable));
                }
            }

            /* Check returns of all threads to get valid session */
            for (Future<Session> fs : sessionFutureList) {
                session = fs.get();
                if (session != null && session.isConnected()) {
                    break;
                }
            }
        } catch (Exception ex) {
            if (session == null || !session.isConnected()) {
                errorMessage = ex.getMessage();
            }
        } finally {
            threadExecutor.shutdown();
        }
        return session;
    }

    /**
     * Create SSH session to connect SSH server by a public authentication key
     * file
     *
     * @param authFilePath public authentication key file
     * @return
     * @throws JSchException
     */
    private Session createSessionByOneAuthFile(String authFilePath)
        throws Exception {
        Session session = null;
        try {
            /* Add authenticated file for jsch to connect remote machine */
            jsch.addIdentity(authFilePath);
            session = jsch.getSession(username, host, SSH_PORT);
            UserInfo ui = new AuthUserInfo();
            session.setUserInfo(ui);
            session.setDaemonThread(true);

            session.connect();
        } catch (JSchException ex) {
            String msg = ex.getMessage();
            if (msg.contains("java.net.UnknownHostException")) {
                throw new Exception("Unreachable host " + host);
            } else if (msg.contains("Connection refused")) {
                throw new Exception("Cannot establish SSH to connect " + host);
            } else {
                errorMessage = null;
            }
        }
        return session;
    }

    /**
     * Create SSH session using prompt password
     *
     * @return JSchSession
     */
    private Session createSessionByPassword() {
    	return createSessionByPassword(null);
    }


    /**
     * Create SSH session using password when password is not null, or using
     * prompt password
     *
     * @param password
     * @return JSchSession
     */
    private Session createSessionByPassword(String password) {
        Session session = null;
        try {
            session = jsch.getSession(username, host, SSH_PORT);
            UserInfo ui = new PasswordUserInfo(password);
            session.setUserInfo(ui);
            session.setDaemonThread(true);

            session.connect();
        } catch (JSchException ex) {
            String msg = ex.getMessage();
            if (msg.equals("Auth cancel")) {
                errorMessage = "Authentication cancel for " + username
                        + " to connect " + host;
            } else if (msg.equals("Auth fail")) {
                errorMessage = "Authentication fail for " + username
                        + " to connect " + host;
            } else if (msg.contains("authentication failures")) {
                errorMessage = "Incorrect password for " + username
                        + " to connect " + host;
            } else if (msg.contains("java.net.UnknownHostException")) {
                errorMessage = "Unreachable host " + host;
            } else if (msg.contains("Connection refused")) {
                errorMessage = "Cannot establish SSH to connect " + host;
            } else {
                errorMessage = ex.getMessage();
            }
        }
        return session;
    }

    /**
     * Check whether a file exists in remote machine via system commands
     *
     * @param filePath file path in remote machine
     * @return true when the file exist; or return false
     * @throws Exception
     */
    public boolean checkFile(String filePath) throws Exception {
        /* Generate command of checking */
        String command = "ls " + filePath;

        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        boolean status = executeCommand(command, out, err);

        /*
         * The file does exist in remote machine, when the status of execution
         * of "ls <filePath>"
         */
        if (status) {
            return true;
        }

        return false;
    }

    /**
     * Get the absolute path of the a file in the remote host
     * @param remotePath the path in the remote host
     * @return the absolute path in the remote host
     * @throws Exception
     */
    private String getRemoteAbsolutePath(String remotePath) throws Exception {
        ChannelSftp channel = null;
        try {
            /* Get sftp channel */
            channel = (ChannelSftp) jschSession.openChannel(SFTP_CMD);
            /* Connect channel */
            channel.connect();
            if (remotePath.startsWith(LINUX_HOME_SIGN)) {
                String homePath = channel.pwd();
                remotePath = remotePath.replaceAll(LINUX_HOME_SIGN,
                                                       homePath);
            }
        } catch (Exception ex) {
            throw new Exception("Problem get absolute path for file : " +
                        remotePath,
                        ex.getCause());
        } finally {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
            }
        }
        return remotePath;
    }

    /**
     * Copy a file or a directory from remote machine to local machine via SFTP
     *
     * @param remoteSource the file path in remote machine
     * @param localTarget the destination directory in local machine
     * @param isRecursive indicate whether copy files recursively or not
     * @throws Exception
     */
    private void sftp(String remoteSource,
                      File localTarget,
                      boolean isRecursive) throws Exception {

        ChannelSftp channel = null;
        try {
            /* Get sftp channel */
            channel = (ChannelSftp) jschSession.openChannel(SFTP_CMD);
            /* Connect channel */
            channel.connect();

            if (remoteSource.startsWith(LINUX_HOME_SIGN)) {
                String homePath = channel.pwd();
                remoteSource = remoteSource.replaceAll(LINUX_HOME_SIGN,
                                                       homePath);
            }

            /* Get attribute of remote file */
            SftpATTRS attrs = channel.stat(remoteSource);
            if (attrs.isDir()) {
                /* Iterate the remote file when it is a directory */
                String topFileName = null;
                channel.cd(remoteSource);
                remoteSource = channel.pwd();
                topFileName = getDirectoryLastName(remoteSource);

                /*
                 * Iterate the remote file path and get all files under this
                 * directory, except jdb files
                 */
                if (topFileName == null) {
                    iterateDirectory(channel, remoteSource, localTarget,
                                     FILTERS, isRecursive);
                } else {
                    iterateDirectory(channel, remoteSource,
                                     new File(localTarget, topFileName),
                                     FILTERS, isRecursive);
                }
            } else {
                /* Get the file when the remote file path is a file */
                int index = remoteSource.lastIndexOf("/");
                String parent = ".";
                if (index > 0) {
                    parent = remoteSource.substring(0, index);
                }
                channel.cd(parent);
                Collection<?> files = channel.ls(remoteSource);
                LsEntry entry = (LsEntry) files.toArray()[0];
                /* Get file from remote machine to local machine */
                getFile(channel, entry.getFilename(), localTarget);
            }
        } catch (Exception ex) {
            throw new Exception("Problem sftp file : " + remoteSource,
                                ex.getCause());
        } finally {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
            }
        }
    }

    /**
     * Get last name of the specified full directory path of remote machine
     *
     * @param directory the specified full directory path
     * @return last name
     */
    private String getDirectoryLastName(String directory) {
        String lastName = null;
        if (directory == null || directory.equals("")) {
            return null;
        }

        /* root directory return it directly */
        if (directory.equals("/")) {
            return directory;
        }

        if (directory.endsWith("/")) {
            directory = directory.substring(0, directory.length() - 1);
        }

        int index = directory.lastIndexOf("/");
        if (index > 0) {
            lastName = directory.substring(index + 1);
        }

        return lastName;
    }

    /**
     * Iterate all files under a specified directory from remote machine to
     * local machine.
     *
     * @param channel Sftp channel
     * @param directory remote directory path
     * @param targetFile local directory
     * @param filterPatterns filename patterns for files that should be excluded
     * @param isRecursive indicates whether to iterate the directory recursively
     * and to iterate its children directories or not
     * @throws SftpException
     */
    private void iterateDirectory(ChannelSftp channel, String directory,
                                  File targetFile, String[] filteredPatterns,
                                  boolean isRecursive)
                                        throws SftpException {
        /* cd to directory in remote machine */
        channel.cd(directory);

        /* create new directory when it does not exist */
        if (!targetFile.exists()) {
            targetFile.mkdirs();
        }

        /* List all files under directory in remote machine */
        Collection<?> entries = channel.ls(directory);

        /*
         * Iterate all files under directory. If the listed file is a directory
         * and call iterateDirectory recursively; if the listed file is a file,
         * call getFile to get file from remote machine
         */
        for (Object object : entries) {
            if (object instanceof LsEntry) {
                LsEntry entry = (LsEntry) object;
                String name = entry.getFilename();
                String fullName = directory + "/" + name;
                if (entry.getAttrs().isDir()) {
                    /* Add separator for the path of the directory */
                    fullName += "/";
                    /* Iterate the children directories */
                    if (isRecursive) {
                        /*
                         * Filter current directory and parent directory and
                         * the path contain the filtered patterns
                         */
                        if (name.equals(".") || name.equals("..") ||
                                isFiltered(fullName)) {
                            continue;
                        }
                        iterateDirectory(channel,
                               channel.pwd() + "/" + name + "/",
                               new File(targetFile, entry.getFilename()),
                               filteredPatterns, isRecursive);
                    }
                } else {
                    /*
                     * Get file from remote machine when the path of the file
                     * does not match filtered pattern
                     */
                    if (!isFiltered(fullName)) {
                        getFile(channel, entry.getFilename(), targetFile);
                    }
                }
            }
        }
        channel.cd("..");
    }

    /**
     * Check whether a path should be filtered or not.
     *
     * @return true when the path contain any one filter pattern; or return
     * false
     */
    private boolean isFiltered(String path) {
        for (String pattern : FILTERS) {
            /*
             * Convert the pattern to be fit for java regular expressions.
             * (.) sign mean any characters, so it should be replaced by (\.).
             * replaceAll accepts regular expressions, so (.) should be escaped
             * as (\\.) and (\.) should be escaped as (\\\\.).
             *
             * (*) in the pattern also should be converted as (.*) which is fit
             * for java regular expressions. replaceAll accepts regular
             * expressions, so (*) should be escaped as (\\*).
             */
            pattern = pattern.replaceAll("\\.", "\\\\.");
            pattern = pattern.replaceAll("\\*", ".\\*");
            if (path.matches(pattern)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get a file from remote machine
     *
     * @param channel SFTP channel
     * @param fileName the file name in remote machine
     * @param targetFile the destination directory in local machine
     * @throws SftpException
     */
    private void getFile(ChannelSftp channel, String fileName, File targetFile)
        throws SftpException {
        /* Create a new folder if it does not exist */
        if (!targetFile.exists()) {
            targetFile.mkdirs();
        }

        /* Invoke get API to fetch file from remote machine */
        channel.get(fileName, targetFile.getAbsolutePath());
    }

    /**
     * Get all log files of a SNA from remote machine
     *
     * @param snaInfo the info of a SNA
     * @param outputdir directory to store the log files
     * @param isCompress determine whether to compress the log files
     * @return the directory contains the log files
     * @throws Exception
     */
    public File getLogFiles(SNAInfo snaInfo,
                            String outputdir,
                            boolean isCompress) throws Exception {
        String localSNDir = snaInfo.getStoreName() + UNDER_LINE +
                snaInfo.getStorageNodeName();
        File resultDir = new File(outputdir, localSNDir);
        String rootdir = snaInfo.getRootdir();
        String localKVRootDir = getDirectoryLastName(rootdir);


        getFileFromRemoteDirectory(rootdir,
                                   resultDir,
                                   resultDir.getParentFile(),
                                   localSNDir,
                                   isCompress);
        /*
         * Iterate all files in specified root directory and determine which
         * one is the configuration file
         */
        Set<String> set = new HashSet<String>();
        File localKVRoot = new File(resultDir, localKVRootDir);
        File[] files = localKVRoot.listFiles();
        for (File file : files) {
            try {
                if (file.isDirectory()) {
                    continue;
                }
                BootstrapParams bp = ConfigUtils.
                    getBootstrapParams(file, false /* check read only*/);

                /* Check whether explicit storage directories are set for SNs */
                for (String path : bp.getStorageDirPaths()) {
                    set.add(path);
                }
                /* Check whether explicit rnlog directories are set for SNs */
                for (String path : bp.getRNLogDirPaths()) {
                    set.add(path);
                }
                /* Check whether explicit admin directory is set for SNs */
                for (String path : bp.getAdminDirPath()) {
                    set.add(path);
                }
            } catch (Exception ignore) {
                /*
                 * Ignore this exception. This exception is used to check
                 * whether the files under kvroot are configuration XML files
                 */
            }
        }

        /*
         * Copy the log files in the mount points
         */
        for (String mountPoint : set) {
            String localStorageDirName = mountPoint.replace('/', '_');
            getFileFromRemoteDirectory(mountPoint,
                                       resultDir,
                                       resultDir,
                                       localStorageDirName,
                                       isCompress);

            /* Rename the storage directory to avoid overwrite */
            File originalDir = new File(resultDir,
                                        getDirectoryLastName(mountPoint));
            File targetDir = new File(resultDir, localStorageDirName);
            originalDir.renameTo(targetDir);
        }

        return resultDir;
    }

    private void getFileFromRemoteDirectory(String remotedir,
                                            File resultDir,
                                            File targetDir,
                                            String targetDirName,
                                            boolean isCompress)
                                                    throws Exception {
        String remotedirName = getDirectoryLastName(remotedir);

        if (isCompress) {
            /* Generate command to zip log files in remote machine */
            String zipfileName = targetDirName + ".zip";
            String remoteZipPath = "/tmp/" + zipfileName;
            String command = "cd " + remotedir + "/.. && zip -r " +
                                remoteZipPath + " " + remotedirName;
            String filterPatterns = getFilterPatterns();
            if (!filterPatterns.isEmpty()) {
                command += " -x " + filterPatterns;
            }

            /* Zip log files in remote machine */
            StringBuffer out = new StringBuffer();
            StringBuffer err = new StringBuffer();
            boolean status = executeCommand(command, out, err);

            if (!status) {
                throw new Exception("Generate zip file failed. " +
                		"Please specify -nocompress flag to retry.");
            }

            /*
             * Get all log files need copy the whole directory and its
             * children directories
             */
            sftp(remoteZipPath, targetDir, true);

            /* Clear standard output and err buffer */
            out.setLength(0);
            err.setLength(0);

            /* Delete generated zip file in remote machine */
            executeCommand("rm -rf " + remoteZipPath, out, err);

            unzip(targetDir + File.separator + zipfileName,
                  resultDir.getAbsolutePath());

        } else {
            /* Call sftp method to get all log file from a remote machine */
            sftp(remotedir, resultDir, true);
        }
    }

    /**
     * Get the latency between the client and a remote host. The algorithm is
     * as follows:
     * 1. client records the begin time
     * 2. client executes a command to show the date in remote host
     * 3. client gets the result from remote host and record the end time
     * 4. Assumed that the time taken to send command to and get result from
     * remote host are same, so the latency follow the formula:
     * latency = (remote time - begin time) - delta time
     * delta time = (end time - begin time) / 2
     * @return the latency; return Long.MIN_VALUE when the remote host can not
     * support date +%s%N command.
     * @throws Exception
     */
    public long getTimeLatency() throws Exception {
        /* To get accurate result, to several times */
        long minLatency = Long.MAX_VALUE;

        for (int i=0; i<5; i++) {
            StringBuffer out = new StringBuffer();
            StringBuffer err = new StringBuffer();
            boolean status = executeCommand("date +%s%N", out, err);
            if (!status) {
                return 0;
            }

            long remoteTime = 0;
            try {
                remoteTime = Long.parseLong(out.toString().trim()) /
                        (1000 * 1000);
            } catch (NumberFormatException nfe) {
                return Long.MIN_VALUE;
            }

            long latency = remoteTime - (end - begin) / 2 - begin;
            if (latency < minLatency) {
                minLatency = latency;
            }
        }

        return minLatency;
    }

    /**
     * Get the JDK version information from remote hosts
     * @return the JDK version and JDK vendor
     * @throws Exception
     */
    public JavaVersionVerifier getJavaVersion() throws Exception {
        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        executeCommand("java -XshowSettings:properties", out, err);

        String settings = err.toString().trim();
        List<String> settingList = Arrays.asList(settings.split("\n"));
        String vendor = null;
        String version = null;
        for (String item : settingList) {
            if (item.contains("java.vendor = ")) {
                vendor = item.replace("java.vendor = ", "").trim();
            }

            if (item.contains("java.version = ")) {
                version = item.replace("java.version = ", "").trim();
            }
        }

        JavaVersionVerifier verifier = new JavaVersionVerifier();
        verifier.setJKDVersionInfo(vendor, version);
        return verifier;
    }

    /**
     * Get the network connectivity status from the connected host to others
     * in the list
     * @param list the list contains the others hosts
     * @return the map contains the network connectivity status
     * @throws Exception
     */
    public Map<SNAInfo, Boolean> getNetWorkStatus(List<SNAInfo> list)
            throws Exception {
        /* Distinct host in SNA info list */
        Map<InetAddress, SNAInfo> ipSNAMap =
                new HashMap<InetAddress, SNAInfo>();

        /* Get IP Address of other hosts */
        for (SNAInfo si : list) {
            ipSNAMap.put(si.getIP(), si);
        }

        /*
         * Execute ping command in the connected host to ping other hosts to
         * determine whether the network is connected between them
         */
        Map<SNAInfo, Boolean> map = new HashMap<SNAInfo, Boolean>();
        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        for (Map.Entry<InetAddress, SNAInfo> entry : ipSNAMap.entrySet()) {
            String command = "ping -c 1 " + entry.getValue().getHost();
            boolean status = executeCommand(command, out, err);
            map.put(entry.getValue(), status);
        }
        return map;
    }

    /**
     * Get all configuration files in the specified root directory of the
     * connected host.
     * @param snaInfo
     * @param saveFolder
     * @return the list of the local path of the configuration files
     * @throws Exception
     */
    public List<File> getConfigFile(SNAInfo snaInfo, File saveFolder)
            throws Exception {
        if (!saveFolder.exists()) {
            saveFolder.mkdirs();
        }

        String rootdir = snaInfo.getRootdir();
        String zipfileName = saveFolder.getName() + ".zip";
        String remoteZipPath = "/tmp/" + zipfileName;
        String command = "cd " + rootdir + " && zip " + remoteZipPath + " ./*";
        String filterPatterns = getFilterPatterns();
        if (!filterPatterns.isEmpty()) {
            command += " -x " + filterPatterns;
        }

        /* Zip log files in remote machine */
        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        boolean status = executeCommand(command, out, err);

        if (!status) {
            throw new Exception("Generate zip file failed");
        }

        String remoteRoot = getRemoteAbsolutePath(snaInfo.getRootdir());
        /* Fetch generated zip file to local machine */
        sftp(remoteZipPath, saveFolder, true);

        /* Clear standard output and err buffer */
        out.setLength(0);
        err.setLength(0);

        /* Delete generated zip file in remote machine */
        executeCommand("rm -rf " + remoteZipPath, out, err);

        unzip(saveFolder + File.separator + zipfileName,
              saveFolder.getAbsolutePath());

        new File(saveFolder + File.separator + zipfileName).delete();

        snaInfo.setRemoteRootdir(remoteRoot);

        /*
         * Iterate all files in specified root directory and determine which
         * one is the configuration file
         */
        List<File> list = new ArrayList<File>();
        File[] files = saveFolder.listFiles();
        Map<String, String> securityDirMap = new HashMap<String, String>();
        for (File file : files) {
            try {
                if (file.isDirectory()) {
                    continue;
                }
                BootstrapParams bp = ConfigUtils.
                    getBootstrapParams(file, false /* check read only*/);

                /*
                 * Check whether the configuration file indicates to enable
                 * security or not
                 */
                if (bp.getSecurityDir() != null &&
                        !bp.getSecurityDir().isEmpty()) {
                    /*
                     * Copy security files when configuration file indicates to
                     * enable security. If the security folder has existed in
                     * local, no need to copy it
                     */
                    String dir = securityDirMap.get(bp.getSecurityDir());
                    if (dir == null) {
                        getSecurityFile(file.getParentFile(),
                                        snaInfo.getRootdir(),
                                        bp.getSecurityDir());
                        securityDirMap.put(bp.getSecurityDir(),
                                           bp.getSecurityDir());
                    }
                }
                list.add(file);
            } catch (Exception ignore) {
                /*
                 * Ignore this exception. This exception is used to check
                 * whether the files under kvroot are configuration XML files
                 */
            }
        }
        return list;
    }

    /**
     * Copy security folder from remote host
     * @param targetFile
     * @param source
     * @param securityFolder
     * @throws Exception
     */
    private void getSecurityFile(File targetFile, String source,
                                String securityFolder) throws Exception {
        String remoteSource;
        if (source.endsWith("/")) {
            remoteSource = source + securityFolder;
        } else {
            remoteSource = source + "/" + securityFolder;
        }

        String zipfileName = targetFile.getName() + "_" + securityFolder +
                ".zip";
        String remoteZipPath = "/tmp/" + zipfileName;
        String command = "cd " + remoteSource + " && zip -r " + remoteZipPath +
                " ./*";

        String filterPatterns = getFilterPatterns();
        if (!filterPatterns.isEmpty()) {
            command += " -x " + filterPatterns;
        }

        /* Zip log files in remote machine */
        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        boolean status = executeCommand(command, out, err);

        if (!status) {
            throw new Exception("Generate zip file failed");
        }

        File saveFolder = new File(targetFile, securityFolder);

        /* Fetch generated zip file to local machine */
        sftp(remoteZipPath, saveFolder, true);

        /* Clear standard output and err buffer */
        out.setLength(0);
        err.setLength(0);

        /* Delete generated zip file in remote machine */
        executeCommand("rm -rf " + remoteZipPath, out, err);

        unzip(saveFolder + File.separator + zipfileName,
              saveFolder.getAbsolutePath());

        new File(saveFolder + File.separator + zipfileName).delete();
    }

    /**
     * Generate the filter patterns for zip command.
     */
    private String getFilterPatterns() {
        String patterns = "";
        for (String pattern : FILTERS) {
            patterns += pattern + " ";
        }
        return patterns.trim();
    }

    /**
     * Unzip a zip file
     *
     * @param zipPath the path of the source zip file
     * @param outputdir the destination directory
     * @throws Exception
     */
    private void unzip(String zipPath, String outputdir) throws Exception {
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(zipPath);
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            /* Iterate all files in zip file */
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();

                /* Create a new directory when the entry is directory in zip */
                if (entry.isDirectory()) {
                    new File(outputdir, entry.getName()).mkdirs();
                    continue;
                }

                /*
                 * Read the file from zip and write in local machine when it is
                 * a file
                 */
                BufferedInputStream bis = null;
                FileOutputStream fos = null;
                BufferedOutputStream bos = null;
                try {
                    bis = new BufferedInputStream(zipFile.
                                                  getInputStream(entry));
                    File file = new File(outputdir, entry.getName());

                    File parent = file.getParentFile();
                    if (parent != null && (!parent.exists())) {
                        parent.mkdirs();
                    }
                    fos = new FileOutputStream(file);
                    bos = new BufferedOutputStream(fos, BUFFER_SZIE);

                    int count;
                    byte data[] = new byte[BUFFER_SZIE];
                    while ((count = bis.read(data, 0, BUFFER_SZIE)) != -1) {
                        bos.write(data, 0, count);
                    }
                } catch (Exception ex) {
                    throw new Exception("Problem unzipping file: " + zipPath);
                } finally {
                    if (bos != null) {
                        bos.flush();
                        bos.close();
                    }

                    if (fos != null) {
                        fos.flush();
                        fos.close();
                    }

                    if (bis != null) {
                        bis.close();
                    }
                }
            }
        } catch (Exception ex) {
            throw new Exception("Problem unzipping file: " + zipPath);
        } finally {
            if (zipFile != null) {
                zipFile.close();
            }
        }
    }

    /**
     * Execute command in remote machine via SSH
     *
     * @param command the command to be executed via SSH
     * @param out is the standard output of executed command
     * @param err is the standard error message of executed command
     * @return the status of execution, true if command is executed
     * successfully; or false
     * @throws IOException
     */
    private boolean executeCommand(String command,
                                   StringBuffer out,
                                   StringBuffer err) throws Exception {
        InputStream outStream = null;
        InputStream errStream = null;
        boolean status = false;

        ChannelExec channel = null;
        try {
            /* Open execution channel */
            channel = (ChannelExec) jschSession.openChannel(EXE_CMD);
            /* Set command */
            channel.setCommand(command);
            outStream = channel.getInputStream();
            errStream = channel.getErrStream();

            /*
             * Connect and execute command and record the begin time and
             * end time
             */
            begin = System.currentTimeMillis();
            channel.connect();
            end = System.currentTimeMillis();

            byte[] buffer = new byte[BUFFER_SZIE];
            while (true) {
                /* Read the standard output of the command executed */
                while (outStream.available() > 0) {
                    int len = outStream.read(buffer, 0, BUFFER_SZIE);
                    out.append(new String(buffer, 0, len));
                }

                /* Read the standard err of the command executed */
                while (errStream.available() > 0) {
                    int len = errStream.read(buffer, 0, BUFFER_SZIE);
                    err.append(new String(buffer, 0, len));
                }

                /* Continue read when there is still available data existing */
                if (channel.isClosed()) {
                    if (outStream.available() > 0 ||
                            errStream.available() > 0) {
                        continue;
                    }
                    break;
                }

                /*
                 * Sleep some time and let JSch get std out and std err from
                 * command executed in remote host
                 */
                try {
                    Thread.sleep(INTERVAL_TIME);
                } catch (Exception ignore) {
                }
            }

            /* The execution is successful when status code is 0 */
            if (channel.getExitStatus() == 0) {
                status = true;
            }
            return status;
        } catch (JSchException ex) {
            throw new Exception("Problem executing command : " + command,
                                ex.getCause());
        } finally {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
            }
        }
    }

    /**
     * An abstract class which derives from UserInfo
     */
    private abstract class SSHUserInfo implements UserInfo,
            UIKeyboardInteractive {
        @Override
        public boolean promptYesNo(String str) {
            return true;
        }

        @Override
        public String getPassphrase() {
            return null;
        }

        @Override
        public boolean promptPassword(String message) {
            return true;
        }

        @Override
        public void showMessage(String message) {
            System.out.println(message);
        }

        @Override
        public String[] promptKeyboardInteractive(String destination,
                                                  String name,
                                                  String instruction,
                                                  String[] prompt,
                                                  boolean[] echo) {
            return new String[] { getPassword() };
        }
    }

    /**
     * An implementation of UserInfo, it is used for authentication when
     * connect SSH server
     */
    private class PasswordUserInfo extends SSHUserInfo {
    	String password = null;
    	PasswordUserInfo(String password) {
    		this.password = password;
    	}

        @Override
        public String getPassword() {
            /* Prompt users to enter password */

            String prompt = username + "@" + host + "'s password: ";
            if (password != null) {
            	return password;
            }
            try {
            	ShellPasswordReader passwordReader = new ShellPasswordReader();
                password = new String(passwordReader.readPassword(prompt));
            } catch (IOException e) {

            }
            return password;
        }

        @Override
        public boolean promptPassphrase(String message) {
            return true;
        }
    }

    /**
     * An implementation of UserInfo, it is used for authentication when
     * connect SSH server
     */
    private class AuthUserInfo extends SSHUserInfo {
        @Override
        public String getPassword() {
            return null;
        }

        @Override
        public boolean promptPassphrase(String message) {
            System.out.println(message);
            return true;
        }

    }
}

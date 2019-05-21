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

package oracle.kv.util.expimp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;

import oracle.kv.impl.util.CommandParser;
import oracle.kv.util.expimp.ExitHandler.ExitCode;

/**
 * This class contains the implementaion for export and import commands.
 * KVToolMain class delegates the execution of the export/import commands
 * to this class.
 */
public class ExportImportMain {

    public static final String REGEX = "\\s*,\\s*";
    public static final String EXPORT_TYPE = "export-type";
    public static final String CONFIG_FILE = "-config";
    public static boolean expimpJson = false;

    /*
     * Parameters in config file specific to local file system
     */
    public static final String
        EXPORT_PACKAGE_PATH_PARAM = "export-package-path";

    public static String[] localStoreParams = {EXPORT_PACKAGE_PATH_PARAM};

    /*
     * Parameters in config file specific to oracle storage cloud service
     */
    public static final String CONTAINER_NAME_PARAM = "container-name";
    public static final String SERVICE_NAME_PARAM = "service-name";
    public static final String USER_NAME_PARAM = "user-name";
    public static final String PASSWORD_PARAM = "password";
    public static final String SERVICE_URL_PARAM = "service-url";

    public static String[] objectStoreParams =
        {CONTAINER_NAME_PARAM, SERVICE_NAME_PARAM,
        USER_NAME_PARAM, PASSWORD_PARAM, SERVICE_URL_PARAM};

    /*
     * Parameters in config file specific to import
     */

    /*
     * TTL parameters
     */
    public static final String TTL_PARAM = "ttl";
    public static final String TTL_RELATIVE_DATE_PARAM = "ttl-relative-date";

    /*
     * Bulk put tuning parameters
     */
    public static final String STREAM_PARALLELISM_PARAM ="stream-parallelism";
    public static final String
        PER_SHARD_PARALLELISM_PARAM = "per-shard-parallelism";
    public static final String BULK_HEAP_PERCENT_PARAM = "bulk-heap-percent";

    public static String[] importSpecificParams =
        {TTL_PARAM, TTL_RELATIVE_DATE_PARAM, STREAM_PARALLELISM_PARAM,
         PER_SHARD_PARALLELISM_PARAM, BULK_HEAP_PERCENT_PARAM};

    public static Map<String, String> importSpecificParamMap =
        new HashMap<String, String>();

    public static final String relativeFlagValue = "RELATIVE";
    public static final String absoluteFlagValue = "ABSOLUTE";

    /*
     * Parameters in config file specific to export
     */
    public static final String CONSISTENCY_PARAM = "consistency";
    public static final String TIME_LAG_PARAM = "permissible-lag";

    public static String[] exportSpecificParams =
        {CONSISTENCY_PARAM, TIME_LAG_PARAM};

    public static Map<String, String> exportSpecificParamMap =
            new HashMap<String, String>();

    public static final String timeConsistencyFlagValue = "TIME";
    public static final String absoluteConsistencyFlagValue = "ABSOLUTE";
    public static final String noConsistencyFlagValue = "NONE";

    /*
     * Supported export stores
     */
    public static enum ExportStoreType {
        LOCAL,
        OBJECT_STORE
    }

    /**
     * Argument parser for export/import commands
     */
    public static abstract class ExportImportParser extends CommandParser {

        private String tableNames = null;
        private String helperHosts = null;

        ExportImportParser(String[] args) {
            super(args);
        }

        @Override
        protected boolean checkArg(String arg) {

            if (arg.equals(HELPER_HOSTS_FLAG)) {
                helperHosts = nextArg(arg);
                return true;
            }

            if (arg.equals(TABLE_FLAG)) {
                tableNames = nextArg(arg);
                return true;
            }

            return false;
        }

        @Override
        protected void verifyArgs() {
            if (getStoreName() == null) {
                missingArg(STORE_FLAG);
            }

            if (helperHosts == null) {
                missingArg(HELPER_HOSTS_FLAG);
            }
        }

        public String getHelperHosts() {
            return helperHosts;
        }

        public String getTableNames() {
            return tableNames;
        }

        public static String getHelperHostsUsage() {
            return HELPER_HOSTS_FLAG + " <helper_hosts>";
        }

        public static String getTableUsage() {
            return TABLE_FLAG + " <table_names>";
        }

        public static String getConfigFileUsage() {
            return CONFIG_FILE + " <config_file_name>";
        }
    }

    /**
     * Argument parser specific to import
     */
    public static class ImportParser extends ExportImportParser {

        public static final String IMPORT_ALL_FLAG = "-import-all";
        public static final String STATUS_FLAG = "-status";

        private Boolean importAll = false;
        private String status = null;

        @Override
        protected boolean checkArg(String arg) {

            if (arg.equals(IMPORT_ALL_FLAG)) {
                importAll = true;
                return true;
            }

            if (arg.equals(STATUS_FLAG)) {
                status = nextArg(arg);
                return true;
            }

            return super.checkArg(arg);
        }

        public ImportParser(String[] args) {
            super(args);
        }

        @Override
        public void usage(String errorMsg) {

            exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
        }

        @Override
        protected void verifyArgs() {

            if (!importAll() && getTableNames() == null) {
                String errorMessage = "Missing flags: "  + IMPORT_ALL_FLAG +
                    " | " + TABLE_FLAG + ". Please use either one of the " +
                    "flags to perform the import";
                usage(errorMessage);
            }

            if (importAll() && getTableNames() != null) {
                String errorMessage = "Found flags: " + IMPORT_ALL_FLAG +
                    " | " + TABLE_FLAG + ". Please use either one of the " +
                    "flags to perform the import";
                usage(errorMessage);
            }

            super.verifyArgs();
        }

        public Boolean importAll() {
            return importAll;
        }

        public String getStatus() {
            return status;
        }

        public static String getImportAllUsage() {
            return IMPORT_ALL_FLAG;
        }

        public static String getStatusFlagUsage() {
            return STATUS_FLAG + " <status_file>";
        }
    }

    /**
     * Argument parser specific to export
     */
    public static class ExportParser extends ExportImportParser {

        public static final String EXPORT_ALL_FLAG = "-export-all";

        private Boolean exportAll = false;

        public ExportParser(String[] args) {
            super(args);
        }

        @Override
        protected boolean checkArg(String arg) {

            if (arg.equals(EXPORT_ALL_FLAG)) {
                exportAll = true;
                return true;
            }

            return super.checkArg(arg);
        }

        @Override
        public void usage(String errorMsg) {

            exit(ExitCode.EXIT_USAGE, errorMsg, Export.COMMAND_NAME);
        }

        @Override
        protected void verifyArgs() {

            if (!exportAll() && getTableNames() == null) {
                String errorMessage = "Missing flags: "  + EXPORT_ALL_FLAG +
                    " | " + TABLE_FLAG + ". Please use either one of the " +
                    "flags to perform the export";
                usage(errorMessage);
            }

            if (exportAll() && getTableNames() != null) {
                String errorMessage = "Found flags: " + EXPORT_ALL_FLAG +
                    " | " + TABLE_FLAG + ". Please use either one of the " +
                    "flags to perform the export";
                usage(errorMessage);
            }

            super.verifyArgs();
        }

        public Boolean exportAll() {
            return exportAll;
        }

        public static String getExportAllUsage() {
            return EXPORT_ALL_FLAG;
        }
    }

    /**
     * Returns value for a given parameter from the list of parameters in the
     * command line arguments
     */
    private static String getParam(String args[],
                                   String param,
                                   String operation) {

        int argC;
        for (argC = 0; argC < args.length; argC++) {
            if (args[argC].equals(param)) {
                break;
            }
        }

        if (argC == args.length) {

            String errorMsg = "Flag " + param + " is required";
            printToConsole(ExitCode.EXIT_USAGE, errorMsg, operation);
            return null;
        }

        if (argC == args.length - 1) {

            String errorMsg = "Flag " + param + " requires an argument";
            printToConsole(ExitCode.EXIT_USAGE, errorMsg, operation);
            return null;
        }

        return args[++argC];
    }

    /**
     * Parses config file containing parameters for a given export store
     */
    private static Map<String, String> parseConfigFile(File file,
                                                       String operation) {

        /*
         * Map that holds all the config file parameters
         */
        Map<String, String> configParameters = new HashMap<String, String>();

        try {
            Properties prop = new Properties();
            InputStream inputStream = new FileInputStream(file);

            prop.load(inputStream);

            for (Entry<Object, Object> entry : prop.entrySet()) {
                String name = (String)entry.getKey();
                String value = (String)entry.getValue();

                configParameters.put(name, value);
            }

            /*
             * Move the import specific params from configParameters to
             * a separate map. Import specific params will be validated
             * separately later.
             */
            for (String impSpecificParam : importSpecificParams) {
                String value = configParameters.get(impSpecificParam);

                if (value != null) {
                    importSpecificParamMap.put(impSpecificParam, value);
                    configParameters.remove(impSpecificParam);
                }
            }

            /*
             * Move the export specific params from configParameters to
             * a separate map. Export specific params will be validated
             * separately later.
             */
            for (String expSpecifigParam : exportSpecificParams) {
                String value = configParameters.get(expSpecifigParam);

                if (value != null) {
                    exportSpecificParamMap.put(expSpecifigParam, value);
                    configParameters.remove(expSpecifigParam);
                }
            }
        } catch (FileNotFoundException e) {
            exit(ExitCode.EXIT_USAGE, "Config file: " + file.getName() +
                 " not found in the path provided.", operation);
        } catch (IOException ioe) {
            exit(ExitCode.EXIT_UNEXPECTED, "Exception reading config file: " +
                 file.getName(), operation);
        }

        String exportType = configParameters.get(EXPORT_TYPE);
        if (exportType == null) {
            String errorMsg = EXPORT_TYPE + " must be specified in the " +
                              "config file specified by -config";
            exit(ExitCode.EXIT_USAGE, errorMsg, operation);
        } else {

            switch (exportType.toUpperCase()) {

                case "LOCAL" :
                    validateConfigParams(configParameters,
                        localStoreParams, operation);
                    break;

                case "OBJECT_STORE" :
                    validateConfigParams(configParameters,
                        objectStoreParams, operation);
                    break;

                default :
                    String errorMsg = EXPORT_TYPE + " parameter specified in " +
                        "the config file currently only supports " +
                        "LOCAL and OBJECT_STORE as valid values";
                    exit(ExitCode.EXIT_INVALID_EXPORT_STORE,
                         errorMsg, operation);
                    break;
            }
        }

        return configParameters;
    }

    /**
     * Validate the parameters defined in the config file
     *
     * @param configParams parameters in the config file
     * @param validParams valid parameters for the given export type
     * @param operation export or import operation
     */
    public static void validateConfigParams(Map<String, String> configParams,
                                            String[] validParams,
                                            String operation) {

        /*
         * All the validParams must be present in the config params
         */
        for (String param : validParams) {
            if (!configParams.containsKey(param)) {
                String errorMsg = param + " must be specified in the " +
                    "config file specified by -config";
                exit(ExitCode.EXIT_USAGE, errorMsg, operation);
            } else if (configParams.get(param).trim().equals("")) {
                String errorMsg = param + " specified in the config " +
                    "cannot have an empty value";
                exit(ExitCode.EXIT_USAGE, errorMsg, operation);
            }
        }

        /*
         * Config params must have only those parameters that are defined in
         * valid params. Additional unknown parameters must not be present in
         * the config file
         */
        for (String param : configParams.keySet()) {
            if (param.equals(EXPORT_TYPE) ||
                parameterInArray(validParams,param)) {
                continue;
            }

            String errorMsg = "Invalid parameter " + param + " specified " +
                "in the config file";
            exit(ExitCode.EXIT_USAGE, errorMsg, operation);
        }
    }

    private static boolean parameterInArray(String[] array, String param) {
        for (String str : array) {
            if (str.equals(param)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Print the message to console
     */
    private static void printToConsole(ExitCode exitCode,
                                       String errorMsg,
                                       String operation) {
        if (expimpJson) {
            /*
             * Display the message in json format
             */
            ExitHandler
                .displayExitJson(System.err, exitCode, errorMsg, operation);
        } else {
            /*
             * Display the error message
             */
            System.err.println(errorMsg);

            String commandArgs = "";

            if (operation.equals(Export.COMMAND_NAME)) {
                commandArgs = Export.COMMAND_ARGS;
            } else {
                commandArgs = Import.COMMAND_ARGS;
            }

            /*
             * Display the command usage
             */
            System.err.println(CommandParser.KVTOOL_USAGE_PREFIX + operation +
                               "\n\t" + commandArgs);
        }
    }

    private static void exit(ExitCode exitCode,
                             String errorMsg,
                             String operation) {

        printToConsole(exitCode, errorMsg, operation);
        System.exit(exitCode.value());
    }

    /**
     * Removes the arg from the args arrays. Returns a new array without arg.
     */
    public static String[] removeArg(String[] args, String arg) {

        String[] allArgs = new String[args.length - 2];
        int index = 0;

        for (int i = 0; i < args.length; i++) {

            /*
             * Ignore arg
             */
            if (args[i].equals(arg)) {
                i++;
                continue;
            }

            allArgs[index++] = args[i];
        }

        return allArgs;
    }

    /**
     * Check if -json flag is present in the cli args
     */
    private static boolean jsonOutput(String[] args) {

        int argC;
        for (argC = 0; argC < args.length; argC++) {
            if (args[argC].equals("-json")) {
                return true;
            }
        }

        return false;
    }

    /**
     * Implementation class for Export command
     */
    public static class Export {
        public static final String COMMAND_NAME = "export";
        public static final String COMMAND_DESC = "Exports the entire " +
            "kvstore or a given table to a export store. Use config file " +
            "to specify export store parameters.";

        public static final String COMMAND_ARGS =
            ExportParser.getExportAllUsage() + " | " +
            ExportImportParser.getTableUsage() + "\n\t" +
            CommandParser.getStoreUsage() + " " +
            ExportImportParser.getHelperHostsUsage() + "\n\t" +
            ExportImportParser.getConfigFileUsage() + " " +
            CommandParser.optional(ExportImportParser.getUserUsage()) + "\n\t" +
            CommandParser.optional(ExportImportParser.getSecurityUsage()) +
            "\n\n";

        private static void validateExportSpecificParams() {
            String consistency = exportSpecificParamMap.get(CONSISTENCY_PARAM);
            String timeLag =
                exportSpecificParamMap.get(TIME_LAG_PARAM);

            if (consistency != null &&
                !(consistency.toUpperCase()
                    .equals(absoluteConsistencyFlagValue) ||
                   consistency.toUpperCase().equals(timeConsistencyFlagValue) ||
                   consistency.toUpperCase()
                    .equals(noConsistencyFlagValue))) {

                String errorMsg = CONSISTENCY_PARAM + " parameter specified " +
                    "in the config file currently only supports " +
                    "ABSOLUTE, TIME and NONE as valid values";
                exit(ExitCode.EXIT_USAGE, errorMsg, Export.COMMAND_NAME);
            }

            if ((consistency == null ||
                 !consistency.toUpperCase()
                     .equals(timeConsistencyFlagValue)) && (timeLag != null)) {

                String errorMsg = TIME_LAG_PARAM + " parameter can be " +
                    "specified in the config file only if " +
                    CONSISTENCY_PARAM + " is " + timeConsistencyFlagValue +
                    ". Please add " + TIME_LAG_PARAM + " = " +
                    timeConsistencyFlagValue + " in the config file.";
                exit(ExitCode.EXIT_USAGE, errorMsg, Export.COMMAND_NAME);
            }

            if (consistency != null && consistency
                    .toUpperCase().equals(timeConsistencyFlagValue) &&
                    timeLag == null) {

                String errorMsg = TIME_LAG_PARAM + " parameter " +
                    "not specified for TIME consistency.";
                exit(ExitCode.EXIT_USAGE, errorMsg, Export.COMMAND_NAME);
            }

            if (timeLag != null) {

                String errorMsg = "Invalid value " + timeLag + " specified for"
                    + " param " + TIME_LAG_PARAM + " in the config file.";

                try {
                    Integer.parseInt(timeLag);
                } catch (NumberFormatException e) {
                    exit(ExitCode.EXIT_USAGE, errorMsg, Export.COMMAND_NAME);
                }
            }
        }

        public static void main(String[] args) {

            expimpJson = jsonOutput(args);

            /*
             * Get the configFile from the command line arguments
             */
            String configFile =
                getParam(args, CONFIG_FILE, Export.COMMAND_NAME);

            if (configFile == null) {
                System.exit(ExitCode.EXIT_USAGE.value());
                return;
            }

            /*
             * Parse config file and retrieve all the parameters
             */
            Map<String, String> configParams =
                parseConfigFile(new File(configFile), Export.COMMAND_NAME);

            /*
             * Validate the export specific parameters separately
             */
            validateExportSpecificParams();

            /*
             * Export config file should not contain import specific config
             * parameters
             */
            for (String param : importSpecificParams) {
                if (importSpecificParamMap.containsKey(param)) {
                    String errorMsg = "Invalid parameter " + param +
                        " specified in the config file";
                    exit(ExitCode.EXIT_USAGE, errorMsg, Export.COMMAND_NAME);
                }
            }

            String[] cliArgs = removeArg(args, CONFIG_FILE);

            ExportParser exportParser = new ExportParser(cliArgs);
            exportParser.parseArgs();

            String storeName = exportParser.getStoreName();
            String[] helperHosts = exportParser.getHelperHosts().split(REGEX);
            String userName = exportParser.getUserName();
            String securityFile = exportParser.getSecurityFile();

            if (securityFile != null) {
                File file = new File(securityFile);

                if (!file.exists()) {
                    String errorMsg = "Security file " +
                        securityFile + " not found";
                    exit(ExitCode.EXIT_SECURITY_ERROR, errorMsg,
                         Export.COMMAND_NAME);
                }
            }

            String exportType = configParams.get(EXPORT_TYPE).toUpperCase();
            String consistencyType =
                exportSpecificParamMap.get(CONSISTENCY_PARAM);
            String timeLag = exportSpecificParamMap.get(TIME_LAG_PARAM);

            if (exportType.equals(ExportStoreType.LOCAL.name())) {

                String exportPackagePath =
                    configParams.get(EXPORT_PACKAGE_PATH_PARAM);

                /*
                 * Instance of LocalStoreExport used to export contents from
                 * Oracle NoSql store to local file system
                 */
                AbstractStoreExport storeExport =
                    new LocalStoreExport(storeName,
                                         helperHosts,
                                         userName,
                                         securityFile,
                                         exportPackagePath,
                                         !exportParser.exportAll(),
                                         exportParser.getJson());
                if (consistencyType != null) {
                    storeExport.setConsistencyType(consistencyType);
                }

                if (timeLag != null) {
                    storeExport.setTimeLag(Integer.parseInt(timeLag));
                }

                if (exportParser.exportAll()) {
                    /*
                     * Exports all contents in kvstore to local file system
                     */
                    storeExport.export();
                } else {
                    /*
                     * Exports specified tables in kvstore to local file
                     * system
                     */
                    storeExport
                        .exportTable(exportParser.getTableNames()
                        .split(REGEX));
                }
            } else if (exportType.equals(ExportStoreType.OBJECT_STORE.name())) {

                String containerName = configParams.get(CONTAINER_NAME_PARAM);
                String serviceName = configParams.get(SERVICE_NAME_PARAM);
                String objectStoreUserName = configParams.get(USER_NAME_PARAM);
                String objectStorePassword = configParams.get(PASSWORD_PARAM);
                String serviceUrl = configParams.get(SERVICE_URL_PARAM);

                final ObjectStoreFactory objectStoreFactory =
                    createObjectStoreFactory(Export.COMMAND_NAME);

                /*
                 * Instance of ObjectStoreExport used to export contents from
                 * Oracle NoSql store to Oracle Storage Cloud Service
                 */
                AbstractStoreExport storeExport =
                    objectStoreFactory.createObjectStoreExport(
                        storeName,
                        helperHosts,
                        userName,
                        securityFile,
                        containerName,
                        serviceName,
                        objectStoreUserName,
                        objectStorePassword,
                        serviceUrl,
                        exportParser.getJson());

                if (consistencyType != null) {
                    storeExport.setConsistencyType(consistencyType);
                }

                if (timeLag != null) {
                    storeExport.setTimeLag(Integer.parseInt(timeLag));
                }

                if (exportParser.exportAll()) {
                    /*
                     * Exports all contents in kvstore to Oracle Storage
                     * Cloud Service
                     */
                    storeExport.export();
                } else {
                    /*
                     * Exports specified tables in kvstore to Oracle Storage
                     * Cloud Service
                     */
                    storeExport
                        .exportTable(exportParser.getTableNames()
                                     .split(REGEX));
                }
            }
        }
    }

    private static final ObjectStoreFactory createObjectStoreFactory(
        String commandName) {

        ObjectStoreFactory factory = null;
        try {
            factory = ObjectStoreFactory.createFactory();
        } catch (RuntimeException e) {
            exit(ExitCode.EXIT_UNEXPECTED, e.getMessage(), commandName);
        }

        if (factory == null) {
            final String errorMsg =
                "To use Oracle Storage Cloud as the export store, please " +
                "install the following jars:\n" +
                "1. oracle.cloud.storage.api\n" +
                "2. jersey-core\n" +
                "3. jersey-client\n" +
                "4. jettison\n" +
                "Instructions to install the above jars can be found in the " +
                "documentation.";
            exit(ExitCode.EXIT_USAGE, errorMsg, commandName);
        }

        return factory;
    }

    /**
     * Implementation class for Import command
     */
    public static class Import {

        public static final String COMMAND_NAME = "import";
        public static final String COMMAND_DESC = "Imports the exported " +
            "store contents to a new nosql store. Also used to import " +
            "individual tables. Use config file to specify export store " +
            "parameters.";

        public static final String COMMAND_ARGS =
            ImportParser.getImportAllUsage() + " | " +
            ExportImportParser.getTableUsage() + "\n\t" +
            CommandParser.getStoreUsage() + " " +
            ExportImportParser.getHelperHostsUsage() + "\n\t" +
            ExportImportParser.getConfigFileUsage() + " " +
            CommandParser.optional(ImportParser.getStatusFlagUsage()) + "\n\t" +
            CommandParser.optional(ExportImportParser.getUserUsage()) + " " +
            CommandParser.optional(ExportImportParser.getSecurityUsage()) +
            "\n\n";

        private static void validateImportSpecificParams() {
            String ttl = importSpecificParamMap.get(TTL_PARAM);
            String ttlRelativeDate =
                importSpecificParamMap.get(TTL_RELATIVE_DATE_PARAM);

            String streamParallelism =
                importSpecificParamMap.get(STREAM_PARALLELISM_PARAM);
            String perShardParallelism =
                importSpecificParamMap.get(PER_SHARD_PARALLELISM_PARAM);
            String bulkHeapPercent =
                importSpecificParamMap.get(BULK_HEAP_PERCENT_PARAM);

            if (ttl != null &&
                !(ttl.toUpperCase().equals(relativeFlagValue) ||
                  ttl.toUpperCase().equals(absoluteFlagValue))) {

                String errorMsg = TTL_PARAM + " parameter specified in " +
                    "the config file currently only supports " +
                    "RELATIVE and ABSOLUTE as valid values";
                exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
            }

            if ((ttl == null || !ttl.toUpperCase().equals(relativeFlagValue))
                   && (ttlRelativeDate != null)) {

                String errorMsg = TTL_RELATIVE_DATE_PARAM + " parameter " +
                    "can be specified in the config file only if " +
                    TTL_PARAM + " is " + relativeFlagValue + ". Please " +
                    "add " + TTL_PARAM + " = " + relativeFlagValue + " in " +
                    "the config file.";
                exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
            }

            if (ttl != null && ttl.toUpperCase().equals(relativeFlagValue)
                    && ttlRelativeDate == null) {

                String errorMsg = TTL_RELATIVE_DATE_PARAM + " parameter " +
                    "not specified for RELATIVE ttl.";
                exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
            }

            if (ttlRelativeDate != null) {

                String errorMsg = "Invalid date " + ttlRelativeDate
                    + " specified for param " + TTL_RELATIVE_DATE_PARAM
                    + " in the config file.";

                SimpleDateFormat formatter =
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
                formatter.setLenient(false);

                try {
                    formatter.parse(ttlRelativeDate);
                } catch (ParseException e) {
                    exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
                }

                String year = ttlRelativeDate.split(" ")[0].split("-")[0];

                if (year.length() > 4) {
                    exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
                }
            }

            if (streamParallelism != null) {

                String errorMsg = "Invalid value " + streamParallelism +
                    " specified for param " + STREAM_PARALLELISM_PARAM +
                    " in the config file.";

                try {
                    int num = Integer.parseInt(streamParallelism);

                    if (num < 1) {
                        exit(ExitCode.EXIT_USAGE,
                             errorMsg, Import.COMMAND_NAME);
                    }

                } catch (NumberFormatException e) {
                    exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
                }
            }

            if (perShardParallelism != null) {

                String errorMsg = "Invalid value " + perShardParallelism +
                    " specified for param " + PER_SHARD_PARALLELISM_PARAM +
                    " in the config file.";

                try {
                    int num = Integer.parseInt(perShardParallelism);

                    if (num < 1) {
                        exit(ExitCode.EXIT_USAGE,
                             errorMsg, Import.COMMAND_NAME);
                    }

                } catch (NumberFormatException e) {
                    exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
                }
            }

            if (bulkHeapPercent != null) {

                String errorMsg = "Invalid value " + bulkHeapPercent +
                    " specified for param " + BULK_HEAP_PERCENT_PARAM +
                    " in the config file.";

                try {
                    int num = Integer.parseInt(bulkHeapPercent);

                    if (num < 1 || num > 100) {
                        exit(ExitCode.EXIT_USAGE,
                             errorMsg, Import.COMMAND_NAME);
                    }

                } catch (NumberFormatException e) {
                    exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
                }
            }
        }

        public static void main(String[] args) {

            expimpJson = jsonOutput(args);

            /*
             * Get the configFile from the command line arguments
             */
            String configFile =
                getParam(args, CONFIG_FILE, Import.COMMAND_NAME);

            if (configFile == null) {
                System.exit(ExitCode.EXIT_USAGE.value());
                return;
            }

            /*
             * Parse config file. Validate and retrieve the parameters
             */
            Map<String, String> configParams =
                parseConfigFile(new File(configFile), Import.COMMAND_NAME);

            /*
             * Validate the import specific parameters separately
             */
            validateImportSpecificParams();

            /*
             * Import config file should not contain export specific config
             * parameters
             */
            for (String param : exportSpecificParams) {
                if (exportSpecificParamMap.containsKey(param)) {
                    String errorMsg = "Invalid parameter " + param +
                        " specified in the config file";
                    exit(ExitCode.EXIT_USAGE, errorMsg, Import.COMMAND_NAME);
                }
            }

            String[] cliArgs = removeArg(args, CONFIG_FILE);

            ImportParser importParser = new ImportParser(cliArgs);
            importParser.parseArgs();

            String storeName = importParser.getStoreName();
            String[] helperHosts = importParser.getHelperHosts().split(REGEX);
            String userName = importParser.getUserName();
            String securityFile = importParser.getSecurityFile();

            if (securityFile != null) {
                File file = new File(securityFile);

                if (!file.exists()) {
                    String errorMsg = "Security file " +
                        securityFile + " not found";
                    exit(ExitCode.EXIT_SECURITY_ERROR, errorMsg,
                         Import.COMMAND_NAME);
                }
            }

            String status = importParser.getStatus();
            String exportType = configParams.get(EXPORT_TYPE).toUpperCase();
            String ttlRelativeDate =
                importSpecificParamMap.get(TTL_RELATIVE_DATE_PARAM);
            String streamParallelism =
                importSpecificParamMap.get(STREAM_PARALLELISM_PARAM);
            String perShardParallelism =
                importSpecificParamMap.get(PER_SHARD_PARALLELISM_PARAM);
            String bulkHeapPercent =
                importSpecificParamMap.get(BULK_HEAP_PERCENT_PARAM);

            if (exportType.equals(ExportStoreType.LOCAL.name())) {

                String exportPackagePath =
                    configParams.get(EXPORT_PACKAGE_PATH_PARAM);

                /*
                 * Instance of LocalStoreImport used to import contents from
                 * local file system to Oracle NoSql store
                 */
                AbstractStoreImport storeImport =
                    new LocalStoreImport(storeName,
                                         helperHosts,
                                         userName,
                                         securityFile,
                                         exportPackagePath,
                                         status,
                                         importParser.getJson());

                storeImport.setImportTtlRelativeDate(ttlRelativeDate);

                if (streamParallelism != null) {
                    storeImport.setStreamParallelism(
                        Integer.parseInt(streamParallelism));
                }

                if (perShardParallelism != null) {
                    storeImport.setPerShardParallelism(
                        Integer.parseInt(perShardParallelism));
                }

                if (bulkHeapPercent != null) {
                    storeImport.setBulkHeapPercent(
                        Integer.parseInt(bulkHeapPercent));
                }

                if (importParser.importAll()) {
                    /*
                     * Imports all contents from the export package in local
                     * file system to kvstore
                     */
                    storeImport.doImport();
                } else {
                    /*
                     * Imports the specified tables from the export package
                     * in local file system to kvstore
                     */
                    storeImport
                        .doTableImport(importParser.getTableNames()
                                       .split(REGEX));
                }
            } else if (exportType.equals(ExportStoreType.OBJECT_STORE.name())) {

                String containerName = configParams.get(CONTAINER_NAME_PARAM);
                String serviceName = configParams.get(SERVICE_NAME_PARAM);
                String objectStoreUserName = configParams.get(USER_NAME_PARAM);
                String objectStorePassword = configParams.get(PASSWORD_PARAM);
                String serviceUrl = configParams.get(SERVICE_URL_PARAM);

                final ObjectStoreFactory objectStoreFactory =
                    createObjectStoreFactory(Import.COMMAND_NAME);

                /*
                 * Instance of ObjectStoreImport used to import contents
                 * from Oracle Storage Cloud Service to Oracle NoSql store
                 */
                AbstractStoreImport storeImport =
                    objectStoreFactory.createObjectStoreImport(
                        storeName,
                        helperHosts,
                        userName,
                        securityFile,
                        containerName,
                        serviceName,
                        objectStoreUserName,
                        objectStorePassword,
                        serviceUrl,
                        status,
                        importParser.getJson());

                storeImport.setImportTtlRelativeDate(ttlRelativeDate);

                if (streamParallelism != null) {
                    storeImport.setStreamParallelism(
                        Integer.parseInt(streamParallelism));
                }

                if (perShardParallelism != null) {
                    storeImport.setPerShardParallelism(
                        Integer.parseInt(perShardParallelism));
                }

                if (bulkHeapPercent != null) {
                    storeImport.setBulkHeapPercent(
                        Integer.parseInt(bulkHeapPercent));
                }

                if (importParser.importAll()) {
                    /*
                     * Imports all contents from the container in Oracle
                     * Storage Cloud Service to kvstore
                     */
                    storeImport.doImport();
                } else {
                    /*
                     * Imports the specified table from the container in
                     * Oracle Storage Cloud Service to kvstore
                     */
                    storeImport
                        .doTableImport(importParser.getTableNames()
                                       .split(REGEX));
                }
            }
        }
    }
}

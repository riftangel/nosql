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

import static oracle.kv.impl.util.JsonUtils.createObjectNode;
import static oracle.kv.impl.util.JsonUtils.createWriter;

import java.io.IOException;
import java.io.PrintStream;

import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandResult;

import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;

public class ExitHandler {

    static final String EXIT_CODE_FIELD = "exit_code";

    /*
     * The possible return codes for the export/import utility, obeying the Unix
     * convention that a non-zero value is an abnormal termination.
     */
    public static enum ExitCode {

        EXIT_OK(0, "Operation completed"),
        EXIT_USAGE(100, "Usage error"),
        EXIT_NOPERM(101, "Unauthorized access to Oracle Cloud Storage Service"),
        EXIT_EXPSTR_NOCONNECT(102, "The Oracle Cloud Storage Service could " +
                              "not be accessed using the service " +
                              "connection parameters"),
        EXIT_NOCONNECT(103, "The source NoSql store could not be connected " +
                       "using the given store-name and helper hosts."),
        EXIT_UNEXPECTED(104, "The utility experienced an unexpected error"),
        EXIT_NOWRITE(105, "The export package has no write permissions."),
        EXIT_NOREAD(106, "No permission to read the files in the " +
                    "export package"),
        EXIT_CONTAINER_EXISTS(107, "The container already exists in the " +
                              "Object Store. Delete the container or " +
                              "use another container name."),
        EXIT_NO_EXPORT_FOLDER(108, "Export folder with the given name does " +
                              "not exist."),
        EXIT_INVALID_EXPORT_STORE(109, "Invalid export store type. " +
                                  "Valid export stores are local and " +
                                  "object_store"),
        EXIT_SECURITY_ERROR(110, "Error loading security file"),
        EXIT_NOSQL_NOPERM(111, "User has no read permissions on the object"),
        EXIT_NOEXPPACKAGE(112, "The export package needed for import not " +
                          "found in the path provided. For Oracle Cloud " +
                          "Storage Service, this means the required " +
                          "container not found");

        /* A value that is a valid Unix return code, in the range of 0-127 */
        private final int returnCode;
        private final String description;

        ExitCode(int returnCode, String description) {
            this.returnCode = returnCode;
            this.description = description;
        }

        public int value(){
            return this.returnCode;
        }

        public String getDescription() {
            return description;
        }
    }

    private static class ExpImpResult implements CommandResult {

        private final ExitCode exitCode;
        private final String errorMsg;

        ExpImpResult(ExitCode exitCode, String errorMsg) {
            this.exitCode = exitCode;
            this.errorMsg = errorMsg;
        }

        ExitCode getExitCode() {
            return exitCode;
        }

        @Override
        public String getReturnValue() {
            /* No values are returned by this command */
            return null;
        }

        @Override
        public String getDescription() {
            if (errorMsg == null) {
                return exitCode.getDescription();
            }

            return exitCode.getDescription() + " - " + errorMsg;
        }

        @Override
        public int getErrorCode() {
            return exitCode.value();
        }

        @Override
        public String[] getCleanupJobs() {
            return null;
        }
    }

    public static void displayExitJson(PrintStream ps,
                                       ExitCode exitCode,
                                       String errorMsg,
                                       String operation) {
        /* Package up the exit results in a json node */
        final ObjectNode exitStatus = createObjectNode();
        ExpImpResult exportResult = new ExpImpResult(exitCode, errorMsg);
        createResultsJson(exitStatus, exportResult, operation);

        /* print the json node. */
        final ObjectWriter writer = createWriter(true /* pretty */);
        try {
            ps.println(writer.writeValueAsString(exitStatus));
        } catch (IOException e) {
            ps.println(e);
        }
    }

    /**
     * Generate the results in json format
     */
    private static void createResultsJson(ObjectNode on,
                                          ExpImpResult result,
                                          String operation) {
        if (result == null) {
            return;
        }
        on.put(CommandJsonUtils.FIELD_OPERATION, operation);
        on.put(CommandJsonUtils.FIELD_DESCRIPTION, result.getDescription());
        on.put(EXIT_CODE_FIELD, result.getExitCode().value());
        return;
    }
}

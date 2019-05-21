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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import oracle.kv.impl.util.JsonUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.ShellException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * This class provides utilities for interaction with Jackson JSON processing
 * libraries, as well as helpful JSON operations, for creating command result
 * JSON output.
 */
public class CommandJsonUtils extends JsonUtils {
    /*
     * These string constants are used for construction of command result
     * JSON fields.
     */
    public static final String FIELD_OPERATION = "operation";
    public static final String FIELD_RETURN_CODE = "return_code";
    public static final String FIELD_DESCRIPTION = "description";
    public static final String FIELD_RETURN_VALUE = "return_value";
    public static final String FIELD_CLEANUP_JOB = "cmd_cleanup_job";

    /**
     * Return a command result JSON output based on the operation and
     * CommandResult. A sample JSON output:
     * {
     *   "operation" : "configure",
     *   "return_code" : 5400,
     *   "return_value" : "",
     *   "description" : "Deploy failed: ConnectionIOException ......" ,
     *   "cmd_cleanup_job" : [ "clean-store.kvs -config store.config" ]
     * }
     * @param operation the name set in "operation" JSON field
     * @param result contains the information set in other JSON fields
     * @return return the created JSON output string
     * @throws IOException if Jackson create JSON output string error
     */
    public static String getJsonResultString(String operation,
                                             CommandResult result)
        throws IOException {

        final ObjectNode jsonTop = createObjectNode();
        updateNodeWithResult(jsonTop, operation, result);
        return toJsonString(jsonTop);
    }

    /**
     * Add command result JSON fields to jsonTop node.
     * <p>
     * For example:
     * jsonTop is:
     * {
     *   "name" : <i>plan_name</i>,
     *   "id" : <i>plan_id</i>,
     *   "state" : <i>plan_state</i>
     * }
     * It will be updated as following: 
     * {
     *   "name" : <i>plan_name</i>,
     *   "id" : <i>plan_id</i>,
     *   "state" : <i>plan_state</i>,
     *   "operation" : "plan deploy-admin",
     *   "return_code" : 5400,
     *   "return_value" : "",
     *   "description" : "Deploy failed: ConnectionIOException ......" ,
     *   "cmd_cleanup_job" : [ "clean-store.kvs -config store.config" ]
     * }
     * @param jsonTop the JSON node to be updated
     * @param operation the name set in "operation" JSON field
     * @param result the created JSON output string
     * @throws IOException if Jackson update JSON node error
     */
    public static void updateNodeWithResult(ObjectNode jsonTop,
                                            String operation,
                                            CommandResult result)
        throws IOException {

        if (result == null) {
            return;
        }
        jsonTop.put(FIELD_OPERATION, operation);
        jsonTop.put(FIELD_RETURN_CODE, result.getErrorCode());
        jsonTop.put(FIELD_DESCRIPTION, result.getDescription());
        final String returnValueJsonStr = result.getReturnValue();
        if (returnValueJsonStr != null) {
            final JsonParser valueParser = JsonUtils.createJsonParser(
                new ByteArrayInputStream(returnValueJsonStr.getBytes()));
            JsonNode returnValueNode = valueParser.readValueAsTree();
            jsonTop.put(FIELD_RETURN_VALUE, returnValueNode);
        }
        if (result.getCleanupJobs() != null) {
            ArrayNode cleanupJobNodes = jsonTop.putArray(FIELD_CLEANUP_JOB);
            for(String job: result.getCleanupJobs()) {
                cleanupJobNodes.add(job);
            }
        }
    }

    /**
     * Return the JSON string to present the jsonTop node.
     */
    public static String toJsonString(ObjectNode jsonTop)
        throws IOException {

        final ObjectWriter writer = createWriter(true /* pretty */);
        return writer.writeValueAsString(jsonTop);
    }

    /*
     * Convert string to object node
     */
    public static ObjectNode readObjectValue(String input)
        throws IOException {
        return mapper.readValue(input, ObjectNode.class);
    }

    /*
     * Common method to handle failure of conversion between JSON object and
     * JSON string. Return 5500 code when there is failure.
     */
    public static <T> T handleConversionFailure(JsonConversionTask<T> task)
        throws ShellException {
        try {
            return task.execute();
        } catch (IOException e) {
            throw new ShellException(e.getMessage(),
                                     ErrorMessage.NOSQL_5500,
                                     new String[] {});
        }
    }

    /*
     * Define the JSON conversion work, it may be JSON object convert to
     * JSON string, or JSON string convert to JSON object
     */
    public static interface JsonConversionTask<R> {
        abstract R execute() throws IOException;
    }
}

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

package oracle.kv.util.shell;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.util.ErrorMessage;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * <p>
 * This is the POJO class for all the shell command execution result. In admin
 * CLI, the output of a command will be turn into this class before
 * display in the admin console. ShellCommandResult will then handle the
 * conversion of the command result to meaningful JSON string. The fields in
 * the JSON are mapping to fields in ShellCommandResult. ShellCommandResult
 * consists of following fields:
 * <p>
 * operation - The name of the executing operation, in most cases it will be
 * the same as command name. If there is internal level operation failure, the
 * operation field may be the name to indicate the internal operation.
 * <p>
 * returnCode - The number to indicate the command result return to caller.
 * For admin CLI result, 5000 return code indicate the successful execution of
 * operation, return code greater than 5000 indicate a operation failure.
 * <p>
 * description - This field store the string description of command result.
 * The description field store information that human readable. It is used
 * when some command results require manual operation on the JSON result. If
 * the command is failed, the message information will be stored in description
 * field. It is not recommended to use description field to do any automation,
 * programmer should instead use returnCode and returnValue fields for admin
 * CLI automation purpose.
 * <p>
 * returnValue - This field stores a JSON object. The JSON object field has
 * no fixed schema. Most of useful information retrieved from server should be
 * stored in returnValue field. The mapping class of returnValue field is
 * ObjectNode from "org.codehaus.jackson" implementation of JSON. Below is an
 * example of converting between ObjectNode and string:
 * <pre>
 * String jsonString = "{\"jsonExmaple\":\"field value\"}"
 * ObjectMapper mapper = new ObjectMapper();
 * ObjectNode on = mapper.readValue(jsonString, ObjectMapper.class);
 *
 * String convertBackToString = mapper.writeValueAsString(on);
 * <p>
 * The following will show an example that run an admin CLI command with JSON
 * flag, the output will map to fields in ShellCommandResult:
 *
 * <pre>
 * kv-> show users -json
 * {
 *  "operation" : "show user",
 *  "returnCode" : 5000,
 *  "description" : "Operation ends successfully",
 *  "returnValue" : {
 *    "users" : [ {
 *      "user" : "id=u1 name=root"
 *    } ]
 *  }
 *}
 *
 */
public class ShellCommandResult {

    public static final String SUCCESS_MESSAGE =
        "Operation ends successfully";

    public static final String CONVERSION_FAILURE_MESSAGE =
        "Fail to convert JSON string";

    public static final String UNSUPPORTED_MESSAGE =
        "JSON output does not suppport.";

    private String operation;

    private int returnCode;

    private String description;

    private ObjectNode returnValue;

    public ShellCommandResult() {}

    public ShellCommandResult(String operation,
                              int returnCode,
                              String description,
                              ObjectNode returnValue) {
        this.operation = operation;
        this.returnCode = returnCode;
        this.description = description;
        this.returnValue = returnValue;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public ObjectNode getReturnValue() {
        return returnValue;
    }

    public void setReturnValue(ObjectNode returnValue) {
        this.returnValue = returnValue;
    }

    /**
     * Convert this object to JSON string.
     */
    public String convertToJson() throws IOException {
        final ObjectWriter ow = JsonUtils.createWriter(true);
        return ow.writeValueAsString(this);
    }

    /**
     * Get the JSON output for conversion failure.
     */
    public String getConversionErrorJsonResult(Exception e) {
        return "{" + Shell.eolt +
               "\"operation\" : \"" + getOperation() + "\"," +
               Shell.eolt +
               "\"returnCode\" : 5500," + Shell.eolt +
               "\"description\" : " +
               "\"Exception in generating JSON format result: " +
               e.getMessage() + "\"," + Shell.eolt +
               "\"returnValue\" : " + null +
               Shell.eolt +
               "}";
    }

    /**
     * Return default instance of this class. By default, the return code is
     * 5000 indicate a successful operation.
     */
    public static ShellCommandResult getDefault(String operaionName) {
        final ShellCommandResult scr = new ShellCommandResult();
        scr.setOperation(operaionName);
        scr.setReturnCode(ErrorMessage.NOSQL_5000.getValue());
        scr.setDescription(SUCCESS_MESSAGE);
        return scr;
    }

    /**
     * <p>
     * Convert the JSON v1 admin CLI result string to ShellCommandResult.
     * Specifically, the following fields will be parsed to map fields in
     * ShellCommandResult:
     * <pre>
     * operation -> operation
     * return_code -> returnCode
     * description -> description
     * return_value -> returnValue
     * <p>
     * cmd_cleanup_job will be removed from previous JSON output.
     * <p>
     * returnValue field is an ObjectNode which can host other JSON structures.
     * For all other fields from JSON v1 admin CLI output, ShellCommandResult
     * will move those fields to be wrapped by returnValue field. If any field
     * name is detected as name_with_underscore, it will convert the
     * name_with_underscore to nameWithUnderscore to meet the new convention.
     */
    public static ShellCommandResult filterJsonV1Result(String input)
        throws IOException {
        final ShellCommandResult scr = new ShellCommandResult();
        final ObjectNode v1Result = CommandJsonUtils.readObjectValue(input);
        final JsonNode operation =
            v1Result.remove(CommandJsonUtils.FIELD_OPERATION);
        final JsonNode returnCode =
            v1Result.remove(CommandJsonUtils.FIELD_RETURN_CODE);
        final JsonNode description =
            v1Result.remove(CommandJsonUtils.FIELD_DESCRIPTION);
        final JsonNode returnValue =
            v1Result.remove(CommandJsonUtils.FIELD_RETURN_VALUE);
        v1Result.remove(CommandJsonUtils.FIELD_CLEANUP_JOB);
        if (operation == null || returnCode == null || description == null) {
            throw new IOException("Fail to convert JSON result, " +
                "one of following fields is null: " +
                CommandJsonUtils.FIELD_OPERATION + ", " +
                CommandJsonUtils.FIELD_RETURN_CODE + ", " +
                CommandJsonUtils.FIELD_DESCRIPTION);
        }
        scr.setOperation(operation.asText());
        scr.setReturnCode(returnCode.asInt());
        scr.setDescription(description.asText());
        if (returnValue != null) {
            if (!(returnValue instanceof ObjectNode)) {
                throw new IOException(
                    "Fail to convert return value, " +
                    "return value is not instance of ObjectNode");
            }
            final ObjectNode v1ReturnValue = (ObjectNode)returnValue;
            final ObjectNode convertedNode = convertFields(v1ReturnValue);
            final Iterator<Entry<String, JsonNode>> iter =
                convertedNode.getFields();
            /* merge return value fields with v1 result fields */
            while(iter.hasNext()) {
                final Entry<String, JsonNode> entry = iter.next();
                v1Result.put(entry.getKey(), entry.getValue());
            }
        }
        scr.setReturnValue(convertFields(v1Result));
        return scr;
    }

    private static ObjectNode
        convertFields(ObjectNode v1Node) throws IOException {
        final ObjectNode result = JsonUtils.createObjectNode();
        final Iterator<Entry<String, JsonNode>> iter = v1Node.getFields();
        while(iter.hasNext()) {
            final Entry<String, JsonNode> entry = iter.next();
            final String key = entry.getKey();
            final String resultKey = translateV1Key(key);
            if (entry.getValue() instanceof ObjectNode) {
                final ObjectNode innerNode = (ObjectNode)entry.getValue();
                result.put(resultKey, convertFields(innerNode));
            } else if (entry.getValue() instanceof ArrayNode) {
                final ArrayNode innerNode = (ArrayNode)entry.getValue();
                result.put(resultKey, convertFields(innerNode));
            } else {
                result.put(resultKey, entry.getValue());
            }
        }
        return result;
    }

    private static ArrayNode
        convertFields(ArrayNode v1Node) throws IOException {
        final ArrayNode resultNode = JsonUtils.createArrayNode();
        final Iterator<JsonNode> iter = v1Node.getElements();
        while(iter.hasNext()) {
            final JsonNode element = iter.next();
            if (element instanceof ObjectNode) {
                final ObjectNode innerNode = (ObjectNode)element;
                resultNode.add(convertFields(innerNode));
            } else if (element instanceof ArrayNode) {
                final ArrayNode innerNode = (ArrayNode)element;
                resultNode.add(convertFields(innerNode));
            } else {
                resultNode.add(element);
            }
        }
        return resultNode;
    }

    /*
     * Convert the field_with_underscore to camelCase.
     */
    private static String translateV1Key(String key) throws IOException {
        final int index = key.indexOf("_");
        if (index == -1) {
            return key;
        }
        if (key.startsWith("_") || key.endsWith("_")) {
            throw new IOException(
                "Unexpected result. Fail to convert key: " + key);
        }
        String firstComp = key.substring(0, index);
        String upperCase =
            key.substring(index + 1, index + 2).toUpperCase();
        String lastComp = upperCase + key.substring(index + 2);
        return firstComp + translateV1Key(lastComp);
    }

    /*
     * Convert the previous CommandResult plus name to map the JSON output of
     * ShellCommandResult. This method is mostly used in exception handling
     * part of shell command execution. Previously, the JSON output for shell
     * exception handling has been implemented in along the CommandResult,
     * all the related exeception contains a CommandResult. This method is to
     * convert the information in CommandResult to match the new JSON output
     * format.
     */
    public static String toJsonReport(String command,
                                      CommandResult cmdResult) {
        try {
            final ShellCommandResult scr = new ShellCommandResult();
            scr.setOperation(command);
            scr.setDescription(cmdResult.getDescription());
            scr.setReturnCode(cmdResult.getErrorCode());
            final String returnValue = cmdResult.getReturnValue();
            if (returnValue != null) {
                scr.setReturnValue(
                    CommandJsonUtils.readObjectValue(returnValue));
            }
            return scr.convertToJson();
        } catch (IOException e) {
            /* the final resolve is to return the string manually */
            return "{" + Shell.eolt +
                   "\"operation\" : \"" + command + "\"," + Shell.eolt +
                   "\"returnCode\" : 5500," + Shell.eolt +
                   "\"description\" : " +
                   "\"Exception in generating JSON format result: " +
                   e.getMessage() + "\"," + Shell.eolt +
                   "\"returnValue\" : " + cmdResult.getReturnValue() +
                   Shell.eolt +
                   "}";
        }
    }
}

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

import java.io.IOException;

import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.client.DdlJsonFormat;
import oracle.kv.impl.client.admin.ClientAdminServiceAPI;
import oracle.kv.impl.util.JsonUtils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Defines the display and formatting of status info and results for Ddl
 * operation output.
 */
public class DdlResultsReport {

    private static int CURRENT_OUTPUT_VERSION = 2;

    /**
     * {
     *   "version" : "2",
     *   "comment" : "Statement did not require execution"
     * }
     */
    public static String NOOP_STATUS_JSON =
        "{\n  \"" + DdlJsonFormat.VERSION_TAG + "\" : \"" +
        CURRENT_OUTPUT_VERSION + "\",\n  \"comment\" : \"" +
        DdlJsonFormat.NOOP_STATUS + "\"\n}";

    private static String R3_2_5_NOOP_JSON =
        "{\n  \"type\" : \"noop\",\n  \"comment\" : \"Statement did not require execution\"\n}";

    /**
     * {
     *   "version" : "2",
     *   "comment" : "Statement completed"
     * }
     */
    public static final String STATEMENT_COMPLETED = "Statement completed.";
    public static String STATEMENT_COMPLETED_JSON =
        "{\n  \"" + DdlJsonFormat.VERSION_TAG + "\" : \"" +
        CURRENT_OUTPUT_VERSION + "\",\n  \"comment\" : \"" +
        STATEMENT_COMPLETED + "\"\n}";

    private final String status;
    private final String statusAsJson;
    private final String result;

    /**
     * Returns the human-readable format.
     */
    String getStatus() {
        return status;
    }

    /**
     * Returns the JSON format.
     */
    String getStatusAsJson() {
        return statusAsJson;
    }

    /**
     * Generate status and save results for non-plan DDL operations -- noops,
     * show, describe
     * @param serialVersion
     */
    DdlResultsReport(DdlHandler handler, short serialVersion) {
        result = handler.getResultString();
        if (serialVersion <
            ClientAdminServiceAPI.STATEMENT_RESULT_VERSION) {
            /*
             * This statement was issued by an old client, which will not have
             * a StatementResult.result field AND will also expect "type" :
             * {noop, plan, show, describe} in the json output.  Put the
             * pertinent result into the info field. The transformation is not
             * perfect because it's possible that statusAsJson will get text,
             * or status will get Json. But if the caller expects json, they'll
             * have issued a "as Json" statement, and will use the infoAsJson
             * field. Also, this will only happen for a R3.2 client.
             */
            if (result == null) {
                status = DdlJsonFormat.NOOP_STATUS; /* this is text, not json */
                statusAsJson = R3_2_5_NOOP_JSON;
            } else {
                status = result;
                statusAsJson = make_R_3_2_5_showResult(handler);
            }
        } else {
            /* No upgrading required, server and client are compatible. */
            if (result == null) {
                status = DdlJsonFormat.NOOP_STATUS; /* this is text, not json */
                statusAsJson = NOOP_STATUS_JSON;
            } else {
                status = STATEMENT_COMPLETED;
                statusAsJson = STATEMENT_COMPLETED_JSON;
            }
        }
    }

    /**
     * Generate results for plan DDL operations - the status is a plan history,
     * formatted either for human readability, or as JSON.
     */
    DdlResultsReport(Plan p, short serialVersion) {
        /*
         * The planRun has information about the last execution of this plan.
         * Hang onto this planRun in case another run starts.
         */
        StatusReport statusReport =
            new StatusReport(p, StatusReport.VERBOSE_BIT);
        status = statusReport.display();

        /**
         * {
         *   "version" : "2",
         *   "info" : JSON from plan status report
         * }
         */
        ObjectWriter writer = JsonUtils.createWriter(true);
        ObjectNode o = JsonUtils.createObjectNode();
        if (serialVersion <
            ClientAdminServiceAPI.STATEMENT_RESULT_VERSION) {
            o.put("type", "plan");
        } else {
            o.put(DdlJsonFormat.VERSION_TAG, CURRENT_OUTPUT_VERSION);
        }
        JsonNode reportAsJson = statusReport.displayAsJson();
        o.put("planInfo", reportAsJson);
        try {
            statusAsJson = writer.writeValueAsString(o);
        } catch (IOException e) {
            throw new NonfatalAssertionException
            ("Problem trying to construct JSON for " + reportAsJson +
            ": " + e);
        }
        result = null;
    }

    String getResult() {
        return result;
    }

    /**
     * Generate results for non-plan DDL operations -- noops, show, describe
     */
    private String make_R_3_2_5_showResult(DdlHandler handler) {
        /**
         * JSON version:
         * {
         *   "type" : "describe" | "show",
         *   "results" : JSON from describe or show
         * }
         */
        StringBuilder sb = new StringBuilder();
        sb.append("{").append("\n");
        sb.append("  \"").append("type").append("\" : \"");
        if (handler.isDescribe()) {
            sb.append("describe");
        } else if (handler.isShow()) {
            sb.append("show");
        } else {
            assert false : "handler should be show or describe";
        }
        sb.append("\",\n");
        sb.append("  \"result\" : ");
        sb.append(handler.getResultString()).append("\n").append("}");
        return sb.toString();
    }
}

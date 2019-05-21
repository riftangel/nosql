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
package oracle.kv.impl.client.admin;

import java.io.Serializable;

import oracle.kv.StatementResult;

/**
 * See ExecutionInfo
 */
public class ExecutionInfoImpl implements ExecutionInfo, Serializable {

    private static final long serialVersionUID = 1L;
    private static final int CURRENT_VERSION = 1;
    private final int planId;
    private final boolean isTerminated;
    private final String info;
    private final String infoAsJson;
    private final boolean isSuccess;
    private final String errorMessage;
    private final boolean isCancelled;
    private final boolean needsCancel;
    private final String results;

    public ExecutionInfoImpl(int planId,
                             boolean isTerminated,
                             String info,
                             String infoAsJson,
                             boolean isSuccess,
                             boolean isCancelled,
                             String errorMessage,
                             boolean needsCancel,
                             String results) {
        this.planId = planId;
        this.isTerminated = isTerminated;
        this.info = info;
        this.infoAsJson = infoAsJson;
        this.isSuccess = isSuccess;
        this.isCancelled = isCancelled;
        this.errorMessage = errorMessage;
        this.needsCancel = needsCancel;
        this.results = results;
    }

    ExecutionInfoImpl(StatementResult pastResult) {
        this.planId = pastResult.getPlanId();
        this.isTerminated = pastResult.isDone();
        this.info = pastResult.getInfo();
        this.infoAsJson = pastResult.getInfoAsJson();
        this.isSuccess = pastResult.isSuccessful();
        this.errorMessage = pastResult.getErrorMessage();
        this.isCancelled = pastResult.isCancelled();

        /*
         * Needs cancel is only set when an ExecutionInfo is generated
         * by the Admin service. This constructor is used when creating new
         * DDLFutures for the proxy server.
         */
        this.needsCancel = false;
        this.results = pastResult.getResult();
    }

    /**
     * Lets the client check that it knows how to parse this ExecutionInfo.
     */
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public int getPlanId() {
        return planId;
    }

    @Override
    public boolean isTerminated() {
        return isTerminated;
    }

    @Override
    public String getJSONInfo() {
        return infoAsJson;
    }

    @Override
    public String getInfo() {
        return info;
    }

    @Override
    public boolean isSuccess() {
        return isSuccess;
    }

    @Override
    public String toString() {
        return "ExecutionInfoImpl [planId=" + planId + ", isTerminated="
            + isTerminated + ", info=" + info
            + ", infoAsJson=" + infoAsJson + "]";
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public boolean needsTermination() {
        return needsCancel;
    }

    @Override
    public String getResult() {
        return results;
    }
}

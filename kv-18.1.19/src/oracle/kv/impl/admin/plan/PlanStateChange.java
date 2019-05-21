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

package oracle.kv.impl.admin.plan;

import java.io.Serializable;

import oracle.kv.impl.admin.plan.Plan.State;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.JsonUtils;

import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Information about a change in plan status, for monitoring.
 */
public class PlanStateChange implements Measurement, Serializable {
    private static final long serialVersionUID = 1L;
   
    private final int planId;
    private final String planName;
    private final Plan.State status;
    private final long time;
    private final int attemptNumber;
    private final boolean needsAlert;
    private final String msg;
    
    public PlanStateChange(int planId,
                           String planName,
                           State state,
                           int attemptNumber,
                           String msg) {
        this.planId = planId;
        this.planName = planName;
        this.status = state;
        time = System.currentTimeMillis();
        this.attemptNumber = attemptNumber;
        this.msg = msg;
        needsAlert = (status == Plan.State.ERROR);
    }

    @Override
    public long getStart() {
        return time;
    }
    
    @Override
    public long getEnd() {
        return time;
    }
    @Override
    public int getId() {
        return Metrics.PLAN_STATE.getId();
    }  

    /**
     * @return the planId
     */
    public int getPlanId() {
        return planId;
    }

    /**
     * @return the status
     */
    public Plan.State getStatus() {
        return status;
    }

    /**
     * @return the time
     */
    public long getTime() {
        return time;
    }

    /**
     * @return the attemptNumber
     */
    public int getAttemptNumber() {
        return attemptNumber;
    }

    /**
     * @return the needsAlert
     */
    public boolean isNeedsAlert() {
        return needsAlert;
    }

    /**
     * @return the msg
     */
    public String getMsg() {
        return msg;
    }

    /* 
     */
    @Override
    public String toString() {
        String show = "PlanStateChange [id=" + planId +
            " name=" + planName +
            " state=" + status + 
            " at " + FormatUtils.formatDateAndTime(time) + 
            " numAttempts=" + attemptNumber;
        if (needsAlert) {
            show += " needsAlert=true";
        }

        if (msg != null) {
            show += " : " +  msg;
        } 
        
        show +="]";
        return show;
    }

    public String toJsonString() {
        try {
            ObjectNode jsonRoot = JsonUtils.createObjectNode();
            jsonRoot.put("planId", planId);
            jsonRoot.put("planName", planName);
            jsonRoot.put("reportTime", time);
            jsonRoot.put("state", status.toString());
            jsonRoot.put("attemptNumber", attemptNumber);
            if (msg != null) {
                jsonRoot.put("message", msg);
            }
            ObjectWriter writer = JsonUtils.createWriter(false);
            return writer.writeValueAsString(jsonRoot);
        } catch (Exception e) {
            return "";
        }
    }
}

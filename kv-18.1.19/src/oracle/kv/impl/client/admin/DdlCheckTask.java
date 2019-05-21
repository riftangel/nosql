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

import java.rmi.RemoteException;
import java.util.logging.Logger;

/**
 * DDLCheckTask contacts the Admin service to obtain new status for a given
 * plan. The new status is sent on to the DdlStatementExecutor to update
 * any waiting targets.
 */
class DdlCheckTask implements Runnable {

    private final Logger logger;

    private final int planId;
    private final DdlStatementExecutor statementExec;

    private final int maxRetries;
    private int numRetries;

    DdlCheckTask(int planId,
                 DdlStatementExecutor statementExec,
                 int maxRetries,
                 Logger logger) {
        this.planId = planId;
        this.statementExec = statementExec;
        this.maxRetries = maxRetries;
        this.logger = logger;
    }

    @Override
    public void run() {

        /* Call to the server side to get up to date status. */
        try {
            ExecutionInfo newInfo = statementExec.getClientAdminService().
                getExecutionStatus(planId);
            newInfo = DdlFuture.checkForNeedsCancel(newInfo, statementExec,
                                                    planId);
            statementExec.updateWaiters(newInfo);
        } catch (RemoteException e) {
            numRetries++;
            logger.fine("Got " + e + ", " + numRetries + "th retry" +
                        " maxRetries = "  + maxRetries);
            if (numRetries > maxRetries) {
                statementExec.shutdownWaitersDueToError(planId, e);
            }
        } catch (Throwable t) {
            logger.info("DDL polling task for plan " + planId +
                        " shut down due to " + t);
            statementExec.shutdownWaitersDueToError(planId, t);
        }
    }
}

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.persist.model.Persistent;

/**
 * A single plan execution run attempt. A plan consists of multiple tasks.  In
 * some cases a task failure halts the plan. In other cases, the plan plows on
 * ahead,and a planRun may contain multiple success and failure task statuses.
 *
 * Note that any caller who modifies the PlanRun must synchronize against its
 * owning plan. This coordinates between threads that are:
 *  - concurrently executing different tasks and are adding taskRuns, changing
 *    plan state, and changing start and end information to the PlanRun
 *  - PlanExecutor threads that are saving the plan instance to the persistent
 *    DPL store.
 *
 * Readers of the planRun may not necessarily need to synchronize on the
 * plan. It may not be important that the reader get the latest plan
 * information, if it's only needed for logging and reporting.

 */
@Persistent(version=1)
public class PlanRun implements Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * The current state of this run. Note that state modification should be
     * synchronized on the owning plan, since different threads may want to
     * modify state. For example, the user may attempt to mark plans as
     * approved, interrupted, or canceled, and that action comes via the admin
     * thread. A running plan has executor threads that may attempt to change
     * state.
     */
    private Plan.State state;

    /* Status for each task in the plan. */
    private List<TaskRun> taskRuns;

    /* The time this attempt was started. */
    private long startTime;

    /* The time an interrupt request was made for this plan */
    private long interruptTime;

    /* The time this plan was ended. */
    private long endTime;

    /* A count of attempt numbers, for display purposes. */
    private int attemptNumber;

    /*
     * Keeps track of any exception incurred at the plan rather than task
     * level.  This object is the keeper of exception information for this
     * PlanRun; and it is also used in transferring across thread boundaries to
     * the PlanWaiter.
     * @deprecated in favor of transferList but kept in order to maintain
     * DPL consistency
     */
    @Deprecated
    private ExceptionTransfer transfer;

    private List<ExceptionTransfer> transferList;

    /*
     * The execution state that owns this PlanRun.
     */
    private ExecutionState executionState;

    /*
     * A failed task does not necessarily stop plan execution, so we keep
     * a count of failed tasks to figure out if the run succeeded. Since
     * it's only needed when the plan is actually executing, it doesn't
     * need to be saved persistently.
     */
    private transient AtomicInteger interruptedTasks;
    private transient AtomicInteger errorTasks;
    private transient AtomicInteger finishedTasks;

    /*
     * Plans may consist of multiple instances of the same tasks, so the task
     * counter provides a way to label them distinctly.
     */
    private transient AtomicInteger taskNumCounter;

    /*
     * Access to the cleanup flags must be synchronized against each other.
     * If the user has requested that the plan be halted, interruptRequested
     * will be true. If the user issues additional interrupts after task
     * cleanups start, cleanupInterrupted will be true.
     */
    private transient boolean interruptRequested;
    private transient boolean cleanupInterrupted;
    private transient boolean cleanupStarted;

    PlanRun(int attemptNumber, ExecutionState executionState) {
        startTime = System.currentTimeMillis();
        taskRuns = new ArrayList<>();
        this.attemptNumber = attemptNumber;
        errorTasks = new AtomicInteger(0);
        interruptedTasks = new AtomicInteger(0);
        finishedTasks = new AtomicInteger(0);
        taskNumCounter = new AtomicInteger(0);

        this.executionState = executionState;

        /*
         * Note that the plan state is set to RUNNING when execution
         * starts, and a proper PlanStateChange is sent at that time.
         */
        state = Plan.State.APPROVED;

        transferList = new CopyOnWriteArrayList<>();
    }

    public long getEndTime() {
        return endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    /** Empty, for DPL */
    PlanRun() {
    }

    public boolean isTerminated() {
        return state.isTerminal();
    }

    void requestInterrupt() {
        if (interruptTime == 0) {
            interruptTime = System.currentTimeMillis();
        }

        interruptRequested = true;
        if (cleanupStarted) {
            cleanupInterrupted = true;
        }
    }

    boolean isInterruptRequested() {
        return interruptRequested;
    }

    void setCleanupStarted() {
        cleanupStarted = true;
    }

    /**
     * @return true if an interrupt request has been made since the cleanup
     * started.
     */
    boolean cleanupInterrupted() {
        return cleanupInterrupted;
    }

    void setState(Planner planner,
                  Plan plan,
                  Plan.State newState,
                  String msg) {
        state = executionState.changeState(planner, plan,
                                           state, newState,
                                           attemptNumber, msg);
    }
    
    /* for testing only */
    public void forciblySetState(Plan.State newState) {
    	state = newState;
    }

    void saveFailure(Throwable t,
                     String problem,
                     ErrorMessage errorMsg,
                     String[] cleanupJobs,
                     Logger logger) {

        final ExceptionTransfer failure = ExceptionTransfer.newInstance(
            t, problem, errorMsg, cleanupJobs);
        transferList.add(failure);

        /*
         * Log additional information for non-command failures, for help
         * in troubleshooting
         */
        if (!(t instanceof IllegalCommandException)) {
            logger.log(Level.SEVERE, "Plan [{0}] failed. {1}",
                       new Object[]{executionState.getPlanName(), this});
        }
    }

    public Plan.State getState() {
        return state;
    }

    /*
     * Start a task.
     */
    synchronized TaskRun startTask(Task task, Logger logger) {
        final TaskRun run = new TaskRun(task, logger,
                                        taskNumCounter.incrementAndGet());
        taskRuns.add(run);
        return run;
    }

    void setEndTime() {
        final long now = System.currentTimeMillis();
        if (now > endTime) {
            endTime = now;
        }
    }

    /**
     * Keep track of how many tasks failed or were interrupted, so we can
     * decide what the end state should be for this run.
     */
    void incrementEndCount(Task.State tState) {
        /*
         * A task in the running state was left there when it returned RESTART.
         * It is not counted as ended so that it can be re-run.
         */
        if (tState == Task.State.RUNNING) {
            return;
        }
        finishedTasks.incrementAndGet();
        if (tState == Task.State.ERROR) {
            errorTasks.incrementAndGet();
        } else if (tState == Task.State.INTERRUPTED) {
            interruptedTasks.incrementAndGet();
        }
    }

    int getNumErrorTasks() {
        return errorTasks.get();
    }

    int getNumInterruptedTasks() {
        return interruptedTasks.get();
    }

    int getNumFinishedTasks() {
        return finishedTasks.get();
    }

    /**
     * This plan incurred an exception. Transfer this exception to the thread
     * that is synchronously waiting for plan finish, so that it can propagate
     * the exception upward.
     */
    synchronized ExceptionTransfer getExceptionTransfer() {
        if (transferList.size() > 0 && transferList.get(0) != null) {
            /* The plan incurred an exception above the task level.*/
            return transferList.get(0);
        }

        /*
         * See if any of the tasks hit an exception. Send the first task
         * exception upwards.
         */
        for (TaskRun oneTask : taskRuns) {
            if (oneTask.getTransfer() != null) {
                return oneTask.getTransfer();
            }
        }
        return null;
    }

    /**
     * Get a description of the most recent plan failure, return null if no
     * failure.
     * @param verbose if true, any stack traces are appended, if false, stack
     * traces are omitted.
     */
    public String getFailureDescription(boolean verbose) {

        if (transferList.isEmpty()) {
            return null;
        }

        final StringBuilder sb = new StringBuilder();
        int i = 1;
        for (ExceptionTransfer et: transferList) {
            sb.append("\n\tFailure ").append(i).append(": ");
            sb.append(et.getDescription());
            if (verbose && (et.getStackTrace() != null)) {
                sb.append("\n").append(et.getStackTrace());
            }
            i++;
        }

        return sb.toString();
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Attempt ").append(attemptNumber);
        sb.append(" [").append(state).append("]");
        sb.append(" start=").append(FormatUtils.formatDateAndTime(startTime));
        sb.append(" end=").append(FormatUtils.formatDateAndTime(endTime));
        final String failure = getFailureDescription(true);
        if (failure != null) {
            sb.append(" ").append(failure);
        }
        return sb.toString();
    }

    /**
     * AdminSchemaVersion 3 changed the single ExceptionTransfer into a list
     * of transfers, in order to preserve failures from multiple tasks.
     */
    void upgradeToV3(List<Task> tasks) {

        if (transferList == null) {
            transferList = new ArrayList<>();
        }

        /* Convert the single transfer into a list. */
        if (transfer != null) {
            transferList.add(transfer);
        }

        /* Upgrade each TaskRun with tasknum, and task. */
        int tasknum = 0;
        for (TaskRun oneTask : taskRuns) {
            tasknum++;
            oneTask.upgradeToV3(tasknum, tasks.get(tasknum - 1));
        }
    }

    int getAttempt() {
        return attemptNumber;
    }

    long getInterruptTime() {
        return interruptTime;
    }

    synchronized List<TaskRun> getTaskRuns() {
        /* Return a copy to avoid a CME due to being modified in startTask() */
        return new ArrayList<>(taskRuns);
    }

    public boolean isSuccess() {
        return getState().equals(Plan.State.SUCCEEDED);
    }

    public boolean isCancelled() {
        return getState().equals(Plan.State.CANCELED);
    }
}

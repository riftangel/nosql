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

package oracle.kv.impl.admin.plan.task;

import java.util.concurrent.Callable;

import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.PlanExecutor.ParallelTaskRunner;
import oracle.kv.impl.admin.plan.task.TaskList.ExecutionStrategy;

import com.sleepycat.persist.model.Persistent;

/**
 * Groups together a set of nested tasks for parallel execution.
 */
@Persistent
public class ParallelBundle extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private TaskList taskList;

    public ParallelBundle() {
        taskList = new TaskList(ExecutionStrategy.PARALLEL);
    }

    @Override
    protected Plan getPlan() {
        /* TODO - We could figure this out */
        return null;
    }

    @Override
    public TaskList getNestedTasks() {
        return taskList;
    }

    public void addTask(Task task) {
        taskList.add(task);
    }

    /**
     * Return the number of nested tasks.
     */
    @Override
    public int getTotalTaskCount() {
        return taskList.getTotalTaskCount();
    }

    /**
     * Returns true if this bundle is empty.
     * @return true if this bundle is empty
     */
    public boolean isEmpty() {
        return taskList.isEmpty();
    }

    /**
     * No work is done in this task. Its only purpose is to shelter its nested
     * tasks.
     */
    @Override
    public Callable<Task.State> getFirstJob(int taskId,
                                            ParallelTaskRunner runner)
        throws Exception {

        throw new UnsupportedOperationException
           ("Should be no work of its own in the parent task");
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}

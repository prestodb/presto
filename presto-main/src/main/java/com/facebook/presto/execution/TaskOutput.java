/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.FailureInfo.toFailures;

@ThreadSafe
public class TaskOutput
{
    private static final Logger log = Logger.get(TaskOutput.class);

    private final String queryId;
    private final String stageId;
    private final String taskId;
    private final URI location;
    private final SharedBuffer<Page> sharedBuffer;

    private final ExecutionStats stats = new ExecutionStats();
    private final AtomicReference<TaskState> taskState = new AtomicReference<>(TaskState.RUNNING);
    private final AtomicLong taskInfoVersion = new AtomicLong();

    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    @GuardedBy("this")
    private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

    public TaskOutput(String queryId, String stageId, String taskId, URI location, List<String> initialOutputIds, int pageBufferMax)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(initialOutputIds, "initialOutputIds is null");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");

        this.queryId = queryId;
        this.stageId = stageId;
        this.taskId = taskId;
        this.location = location;
        sharedBuffer = new SharedBuffer<>(pageBufferMax);
        for (String outputId : initialOutputIds) {
            sharedBuffer.addQueue(outputId);
        }
    }

    public String getTaskId()
    {
        return taskId;
    }

    public TaskState getState()
    {
        return taskState.get();
    }

    public ExecutionStats getStats()
    {
        return stats;
    }

    public void addResultQueue(String outputIds)
    {
        sharedBuffer.addQueue(outputIds);
    }

    public boolean addPage(Page page)
            throws InterruptedException
    {
        long start = System.nanoTime();
        try {
            return sharedBuffer.add(page);
        }
        finally {
            stats.addSinkBufferWaitTime(Duration.nanosSince(start));
        }
    }

    public void noMoreResultQueues()
    {
        sharedBuffer.noMoreQueues();
    }

    public synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        this.noMoreSplits.add(sourceId);
    }

    public synchronized Set<PlanNodeId> getNoMoreSplits()
    {
        return ImmutableSet.copyOf(noMoreSplits);
    }

    public List<Page> getResults(String outputId, int maxPageCount, Duration maxWait)
            throws InterruptedException
    {
        return sharedBuffer.get(outputId, maxPageCount, maxWait);
    }

    public void abortResults(String outputId)
    {
        sharedBuffer.abort(outputId);
    }

    /**
     * Marks the output as complete.  After this method is called no more data can be added but there may still be buffered output pages.
     */
    public void finish()
    {
        sharedBuffer.finish();

        // the output will only transition to finished if it isn't already marked as failed or cancel
        updateFinishedState();
    }

    public void cancel()
    {
        transitionToDoneState(TaskState.CANCELED);
    }

    public void queryFailed(Throwable cause)
    {
        if (taskState.get() == TaskState.FAILED || transitionToDoneState(TaskState.FAILED)) {
            failureCauses.add(cause);
        }
    }

    private void updateFinishedState()
    {
        TaskState overallState = taskState.get();
        if (overallState.isDone()) {
            return;
        }

        // if all buffers are finished, transition to finished
        if (sharedBuffer.isFinished()) {
            transitionToDoneState(TaskState.FINISHED);
        }
    }

    private boolean transitionToDoneState(TaskState doneState)
    {
        Preconditions.checkNotNull(doneState, "doneState is null");
        Preconditions.checkArgument(doneState.isDone(), "doneState %s is not a done state", doneState);

        while (true) {
            TaskState taskState = this.taskState.get();
            if (taskState.isDone()) {
                return false;
            }

            if (this.taskState.compareAndSet(taskState, doneState)) {

                stats.recordEnd();

                sharedBuffer.destroy();

                log.debug("Task %s is %s", taskId, doneState);
                return true;
            }
        }
    }

    public TaskInfo getTaskInfo()
    {
        updateFinishedState();
        return new TaskInfo(queryId,
                stageId,
                taskId,
                taskInfoVersion.incrementAndGet(),
                getState(),
                location,
                sharedBuffer.getInfo(),
                getNoMoreSplits(),
                stats,
                toFailures(failureCauses));
    }
}

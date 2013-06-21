/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OperatorStats.SplitExecutionStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class TaskOutput
{
    private static final Logger log = Logger.get(TaskOutput.class);

    private final TaskId taskId;
    private final URI location;
    private final SharedBuffer sharedBuffer;

    private final ExecutionStats stats;
    private final StateMachine<TaskState> taskState;
    private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    @GuardedBy("this")
    private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

    @GuardedBy("this")
    private final Map<PlanNodeId, Set<?>> outputs = new HashMap<>();

    private final Set<OperatorStats> activeSplits = Sets.newSetFromMap(new ConcurrentHashMap<OperatorStats, Boolean>());

    public TaskOutput(TaskId taskId, URI location, DataSize maxBufferSize, Executor executor, SqlTaskManagerStats taskManagerStats)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        Preconditions.checkNotNull(executor, "executor is null");

        this.taskId = taskId;
        this.location = location;
        sharedBuffer = new SharedBuffer(maxBufferSize);
        taskState = new StateMachine<>("task " + taskId, executor, TaskState.RUNNING);
        taskState.addStateChangeListener(new StateChangeListener<TaskState>() {
            @Override
            public void stateChanged(TaskState newValue)
            {
                log.debug("Task %s is %s", TaskOutput.this.taskId, newValue);
            }
        });

        stats  = new ExecutionStats(taskManagerStats);
    }

    public TaskId getTaskId()
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

    public synchronized void addOutput(PlanNodeId id, Set<?> output)
    {
        checkNotNull(id, "id is null");
        checkNotNull(output, "output is null");

        ImmutableSet.Builder<Object> builder = ImmutableSet.builder();
        Set<?> current = this.outputs.get(id);
        if (current != null) {
            builder.addAll(current);
        }
        builder.addAll(output);
        this.outputs.put(id, builder.build());
    }

    public synchronized Map<PlanNodeId, Set<?>> getOutputs()
    {
        return ImmutableMap.copyOf(outputs);
    }

    public void noMoreResultQueues()
    {
        sharedBuffer.noMoreQueues();
    }

    public synchronized boolean noMoreSplits(PlanNodeId sourceId)
    {
        return this.noMoreSplits.add(sourceId);
    }

    public synchronized Set<PlanNodeId> getNoMoreSplits()
    {
        return ImmutableSet.copyOf(noMoreSplits);
    }

    public BufferResult getResults(String outputId, long startingSequenceId, DataSize maxSize, Duration maxWait)
            throws InterruptedException
    {
        Preconditions.checkNotNull(outputId, "outputId is null");
        Preconditions.checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");
        Preconditions.checkNotNull(maxWait, "maxWait is null");

        return sharedBuffer.get(outputId, startingSequenceId, maxSize, maxWait);
    }

    public void abortResults(String outputId)
    {
        sharedBuffer.abort(outputId);
    }

    public void addActiveSplit(OperatorStats operatorStats)
    {
        activeSplits.add(operatorStats);
    }

    public void removeActiveSplit(OperatorStats operatorStats)
    {
        activeSplits.remove(operatorStats);
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
        failureCauses.add(cause);
        transitionToDoneState(TaskState.FAILED);
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
            TaskState currentTaskState = this.taskState.get();
            if (currentTaskState.isDone()) {
                return false;
            }

            stats.recordEnd();

            // If someone changed the state while we were working, start
            // the whole process over again.  This assures that a final
            // state can not be changed.
            if (taskState.compareAndSet(currentTaskState, doneState)) {

                sharedBuffer.destroy();

                return true;
            }
        }
    }

    public void waitForStateChange(TaskState currentState, Duration maxWait)
            throws InterruptedException
    {
        taskState.waitForStateChange(currentState, maxWait);
    }

    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskState.addStateChangeListener(stateChangeListener);
    }

    public TaskInfo getTaskInfo(boolean full)
    {
        updateFinishedState();

        List<SplitExecutionStats> splitStats = null;
        if (full) {
            ImmutableList.Builder<SplitExecutionStats> builder = ImmutableList.builder();
            for (OperatorStats activeSplit : activeSplits) {
                builder.add(activeSplit.snapshot());
            }
            splitStats = builder.build();
        }

        SharedBufferInfo sharedBufferInfo = sharedBuffer.getInfo();
        synchronized (this) {
            TaskState state = getState();

            List<FailureInfo> failures = ImmutableList.of();
            if (state == TaskState.FAILED) {
                failures = toFailures(failureCauses);
            }

            return new TaskInfo(taskId,
                    nextTaskInfoVersion.getAndIncrement(),
                    state,
                    location,
                    sharedBufferInfo,
                    getNoMoreSplits(),
                    stats.snapshot(full),
                    splitStats,
                    failures,
                    getOutputs());
        }
    }
}

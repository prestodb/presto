/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.execution.PageBuffer.BufferState;
import com.facebook.presto.operator.Page;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.FailureInfo.toFailures;
import static com.facebook.presto.execution.PageBuffer.infoGetter;
import static com.facebook.presto.execution.PageBuffer.stateGetter;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class TaskOutput
{
    private final String queryId;
    private final String stageId;
    private final String taskId;
    private final URI location;
    private final Map<String, PageBuffer> outputBuffers;

    private final ExecutionStats stats = new ExecutionStats();
    private final AtomicReference<TaskState> taskState = new AtomicReference<>(TaskState.RUNNING);

    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    public TaskOutput(String queryId, String stageId, String taskId, URI location, List<String> outputIds, int pageBufferMax, int splits)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");
        Preconditions.checkArgument(!outputIds.isEmpty(), "outputIds is empty");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");
        Preconditions.checkArgument(splits >= 0, "splits is negative");

        this.queryId = queryId;
        this.stageId = stageId;
        this.taskId = taskId;
        this.location = location;
        stats.addSplits(splits);
        ImmutableMap.Builder<String, PageBuffer> builder = ImmutableMap.builder();
        for (String outputId : outputIds) {
            builder.put(outputId, new PageBuffer(outputId, 1, pageBufferMax));
        }
        outputBuffers = builder.build();
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

    public List<PageBufferInfo> getBufferInfos()
    {
        return ImmutableList.copyOf(transform(outputBuffers.values(), infoGetter()));
    }

    private void updateState()
    {
        TaskState overallState = taskState.get();
        if (!overallState.isDone()) {
            ImmutableList<BufferState> bufferStates = ImmutableList.copyOf(transform(outputBuffers.values(), stateGetter()));

            if (Iterables.any(bufferStates, equalTo(BufferState.FAILED))) {
                taskState.set(TaskState.FAILED);
                stats.recordEnd();
                // this shouldn't be necessary, but be safe
                finishAllBuffers();
            }
            else if (Iterables.all(bufferStates, equalTo(BufferState.FINISHED))) {
                taskState.set(TaskState.FINISHED);
                stats.recordEnd();
            }
        }
    }

    /**
     * Marks the output as complete.  After this method is called no more data can be added but there may still be buffered output pages.
     */
    public void finish()
    {
        // finish all buffers
        for (PageBuffer outputBuffer : outputBuffers.values()) {
            outputBuffer.sourceFinished();
        }
        // the output will only transition to finished if it isn't already marked as failed or cancel
        updateState();
    }

    public void cancel()
    {
        while (true) {
            TaskState taskState = this.taskState.get();
            if (taskState.isDone()) {
                return;
            }
            if (this.taskState.compareAndSet(taskState, TaskState.CANCELED)) {
                stats.recordEnd();
                break;
            }
        }

        // cancel all buffers
        finishAllBuffers();
        // the output will only transition to cancel if it isn't already marked as failed
        updateState();
    }

    private void finishAllBuffers()
    {
        for (PageBuffer outputBuffer : outputBuffers.values()) {
            outputBuffer.finish();
        }
    }

    public void queryFailed(Throwable cause)
    {
        failureCauses.add(cause);
        taskState.set(TaskState.FAILED);
        stats.recordEnd();
        for (PageBuffer outputBuffer : outputBuffers.values()) {
            outputBuffer.queryFailed(cause);
        }
    }

    public boolean addPage(Page page)
            throws InterruptedException
    {
        for (PageBuffer outputBuffer : outputBuffers.values()) {
            if (!outputBuffer.addPage(page)) {
                updateState();
                TaskState state = getState();
                Preconditions.checkState(state.isDone(), "Expected a done state but state is %s", state);
                return false;
            }
        }
        return true;
    }

    public List<Page> getResults(String outputId, int maxPageCount, Duration maxWait)
            throws InterruptedException
    {
        PageBuffer outputBuffer = outputBuffers.get(outputId);
        Preconditions.checkArgument(outputBuffer != null, "Unknown output %s: available outputs %s", outputId, outputBuffers.keySet());
        return outputBuffer.getNextPages(maxPageCount, maxWait);
    }

    public void abortResults(String outputId)
    {
        PageBuffer outputBuffer = outputBuffers.get(outputId);
        Preconditions.checkArgument(outputBuffer != null, "Unknown output %s: available outputs %s", outputId, outputBuffers.keySet());
        outputBuffer.finish();
    }

    public TaskInfo getTaskInfo()
    {
        updateState();
        return new TaskInfo(queryId,
                stageId,
                taskId,
                getState(),
                location,
                getBufferInfos(),
                stats,
                toFailures(failureCauses));
    }
}

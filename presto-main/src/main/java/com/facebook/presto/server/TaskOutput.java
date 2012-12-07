/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.server.PageBuffer.BufferState;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.server.PageBuffer.stateGetter;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Maps.transformValues;

public class TaskOutput
{
    private final String taskId;
    private final URI location;
    private final List<TupleInfo> tupleInfos;
    private final Map<String, PageBuffer> outputBuffers;

    private final AtomicReference<TaskState> taskState = new AtomicReference<>(TaskState.RUNNING);

    private final int splits;
    private final AtomicInteger startedSplits = new AtomicInteger();
    private final AtomicInteger completedSplits = new AtomicInteger();

    private final AtomicLong splitCpuMillis = new AtomicLong();

    private final AtomicLong inputDataSize = new AtomicLong();
    private final AtomicLong inputPositions = new AtomicLong();

    private final AtomicLong completedDataSize = new AtomicLong();
    private final AtomicLong completedPositions = new AtomicLong();

    public TaskOutput(String taskId, URI location, List<String> outputIds, List<TupleInfo> tupleInfos, int pageBufferMax, int splits)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");
        Preconditions.checkArgument(!outputIds.isEmpty(), "outputIds is empty");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");
        Preconditions.checkArgument(splits >= 0, "splits is negative");

        this.taskId = taskId;
        this.location = location;
        this.tupleInfos = tupleInfos;
        this.splits = splits;
        ImmutableMap.Builder<String, PageBuffer> builder = ImmutableMap.builder();
        for (String outputId : outputIds) {
            builder.put(outputId, new PageBuffer(tupleInfos, 1, pageBufferMax));
        }
        outputBuffers = builder.build();
    }

    public String getTaskId()
    {
        return taskId;
    }

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    public TaskState getState()
    {
        return taskState.get();
    }

    public Map<String, BufferState> getOutputBufferStates()
    {
        return ImmutableMap.copyOf(transformValues(outputBuffers, stateGetter()));
    }

    private void updateState()
    {
        TaskState overallState = taskState.get();
        if (!overallState.isDone()) {
            Map<String, BufferState> outputBufferStates = getOutputBufferStates();

            if (Iterables.any(outputBufferStates.values(), equalTo(BufferState.FAILED))) {
                taskState.set(TaskState.FAILED);
                // this shouldn't be necessary, but be safe
                finishAllBuffers();
            }
            else if (Iterables.all(outputBufferStates.values(), equalTo(BufferState.FINISHED))) {
                taskState.set(TaskState.FINISHED);
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
        taskState.set(TaskState.FAILED);
        for (PageBuffer outputBuffer : outputBuffers.values()) {
            outputBuffer.queryFailed(cause);
        }
    }

    public int getBufferedPageCount()
    {
        int bufferedPageCount = 0;
        for (PageBuffer outputBuffer : outputBuffers.values()) {
            bufferedPageCount = Math.max(outputBuffer.getBufferedPageCount(), bufferedPageCount);
        }
        return bufferedPageCount;
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
        return new TaskInfo(taskId,
                location,
                getOutputBufferStates(),
                getTupleInfos(),
                getState(),
                getBufferedPageCount(),
                getSplits(),
                getStartedSplits(),
                getCompletedSplits(),
                (long) getSplitCpu().toMillis(),
                getInputDataSize().toBytes(),
                getInputPositions(),
                getCompletedDataSize().toBytes(),
                getCompletedPositions(),
                0,
                0);
    }

    public int getSplits()
    {
        return splits;
    }

    public int getStartedSplits()
    {
        return Ints.min(startedSplits.get(), splits);
    }

    public int getCompletedSplits()
    {
        return Ints.min(completedSplits.get(), startedSplits.get(), splits);
    }

    public Duration getSplitCpu()
    {
        return new Duration(splitCpuMillis.get(), TimeUnit.MILLISECONDS);
    }

    public DataSize getInputDataSize()
    {
        return new DataSize(inputDataSize.get(), Unit.BYTE);
    }

    public long getInputPositions()
    {
        return inputPositions.get();
    }

    public DataSize getCompletedDataSize()
    {
        return new DataSize(completedDataSize.get(), Unit.BYTE);
    }

    public long getCompletedPositions()
    {
        return completedPositions.get();
    }

    public void splitStarted()
    {
        startedSplits.incrementAndGet();
    }

    public void splitCompleted()
    {
        completedSplits.incrementAndGet();
    }

    public void addSplitCpuTime(Duration duration)
    {
        splitCpuMillis.addAndGet((long) duration.toMillis());
    }

    public void addInputPositions(long inputPositions)
    {
        this.inputPositions.addAndGet(inputPositions);
    }

    public void addInputDataSize(DataSize inputDataSize)
    {
        this.inputDataSize.addAndGet(inputDataSize.toBytes());
    }

    public void addCompletedPositions(long completedPositions)
    {
        this.completedPositions.addAndGet(completedPositions);
    }

    public void addCompletedDataSize(DataSize completedDataSize)
    {
        this.completedDataSize.addAndGet(completedDataSize.toBytes());
    }
}

/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ExecutionStats
{
    private final AtomicInteger splits;
    private final AtomicInteger startedSplits;
    private final AtomicInteger completedSplits;

    private final AtomicLong splitCpuTime;

    private final AtomicLong inputDataSize;
    private final AtomicLong completedDataSize;

    private final AtomicLong inputPositionCount;
    private final AtomicLong completedPositionCount;

    private final AtomicLong outputDataSize;
    private final AtomicLong outputPositionCount;

    public ExecutionStats()
    {
        this(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    @JsonCreator
    public ExecutionStats(
            @JsonProperty("splits") int splits,
            @JsonProperty("startedSplits") int startedSplits,
            @JsonProperty("completedSplits") int completedSplits,
            @JsonProperty("splitCpuTime") long splitCpuTime,
            @JsonProperty("inputDataSize") long inputDataSize,
            @JsonProperty("completedDataSize") long completedDataSize,
            @JsonProperty("inputPositionCount") long inputPositionCount,
            @JsonProperty("completedPositionCount") long completedPositionCount,
            @JsonProperty("outputDataSize") long outputDataSize,
            @JsonProperty("outputPositionCount") long outputPositionCount)
    {
        this.splits = new AtomicInteger(splits);
        this.startedSplits = new AtomicInteger(startedSplits);
        this.completedSplits = new AtomicInteger(completedSplits);
        this.splitCpuTime = new AtomicLong(splitCpuTime);
        this.inputDataSize = new AtomicLong(inputDataSize);
        this.inputPositionCount = new AtomicLong(inputPositionCount);
        this.completedDataSize = new AtomicLong(completedDataSize);
        this.completedPositionCount = new AtomicLong(completedPositionCount);
        this.outputDataSize = new AtomicLong(outputDataSize);
        this.outputPositionCount = new AtomicLong(outputPositionCount);
    }

    @JsonProperty
    public int getSplits()
    {
        return splits.get();
    }

    @JsonProperty
    public int getStartedSplits()
    {
        return startedSplits.get();
    }

    @JsonProperty
    public int getCompletedSplits()
    {
        return completedSplits.get();
    }

    @JsonProperty
    public long getSplitCpuTime()
    {
        return splitCpuTime.get();
    }

    @JsonProperty
    public long getInputDataSize()
    {
        return inputDataSize.get();
    }

    @JsonProperty
    public long getInputPositionCount()
    {
        return inputPositionCount.get();
    }

    @JsonProperty
    public long getCompletedDataSize()
    {
        return completedDataSize.get();
    }

    @JsonProperty
    public long getCompletedPositionCount()
    {
        return completedPositionCount.get();
    }

    @JsonProperty
    public long getOutputDataSize()
    {
        return outputDataSize.get();
    }

    @JsonProperty
    public long getOutputPositionCount()
    {
        return outputPositionCount.get();
    }

    public void addSplits(int splits)
    {
        this.splits.addAndGet(splits);
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
        splitCpuTime.addAndGet((long) duration.toMillis());
    }

    public void addInputPositions(long inputPositions)
    {
        this.inputPositionCount.addAndGet(inputPositions);
    }

    public void addInputDataSize(DataSize inputDataSize)
    {
        this.inputDataSize.addAndGet(inputDataSize.toBytes());
    }

    public void addCompletedPositions(long completedPositions)
    {
        this.completedPositionCount.addAndGet(completedPositions);
    }

    public void addCompletedDataSize(DataSize completedDataSize)
    {
        this.completedDataSize.addAndGet(completedDataSize.toBytes());
    }

    public void add(ExecutionStats stats) {
        splits.addAndGet(stats.getSplits());
        startedSplits.addAndGet(stats.getStartedSplits());
        completedSplits.addAndGet(stats.getCompletedSplits());
        splitCpuTime.addAndGet(stats.getSplitCpuTime());
        inputDataSize.addAndGet(stats.getInputDataSize());
        inputPositionCount.addAndGet(stats.getInputPositionCount());
        completedDataSize.addAndGet(stats.getCompletedDataSize());
        completedPositionCount.addAndGet(stats.getCompletedPositionCount());
        outputDataSize.addAndGet(stats.getOutputDataSize());
        outputPositionCount.addAndGet(stats.getOutputPositionCount());

    }
}

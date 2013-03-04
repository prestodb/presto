/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.ExchangeOperator.ExchangeClientStatus;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@ThreadSafe
public class ExecutionStats
{
    private static final Duration ZERO_DURATION = new Duration(0, TimeUnit.SECONDS);
    private static final DataSize ZERO_SIZE = new DataSize(0, DataSize.Unit.BYTE);

    private final DateTime createTime;
    @GuardedBy("this")
    private DateTime executionStartTime;
    @GuardedBy("this")
    private DateTime lastHeartBeat;
    @GuardedBy("this")
    private DateTime endTime;

    private final AtomicInteger splits = new AtomicInteger();
    private final AtomicInteger startedSplits = new AtomicInteger();
    private final AtomicInteger completedSplits = new AtomicInteger();

    @GuardedBy("this")
    private Duration splitWallTime = ZERO_DURATION;
    @GuardedBy("this")
    private Duration splitCpuTime = ZERO_DURATION;
    @GuardedBy("this")
    private Duration splitUserTime = ZERO_DURATION;

    @GuardedBy("this")
    private Duration exchangeWaitTime = ZERO_DURATION;

    @GuardedBy("this")
    private DataSize inputDataSize = ZERO_SIZE;
    @GuardedBy("this")
    private DataSize completedDataSize = ZERO_SIZE;

    private final AtomicLong inputPositionCount = new AtomicLong();
    private final AtomicLong completedPositionCount = new AtomicLong();

    @GuardedBy("this")
    private DataSize outputDataSize = ZERO_SIZE;
    private final AtomicLong outputPositionCount = new AtomicLong();

    // todo this assumes that there is only one exchange in a plan
    private final AtomicReference<List<ExchangeClientStatus>> exchangeStatus = new AtomicReference<List<ExchangeClientStatus>>(ImmutableList.<ExchangeClientStatus>of());

    public ExecutionStats()
    {
        createTime = DateTime.now();
        lastHeartBeat = DateTime.now();
    }

    @JsonCreator
    public ExecutionStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("executionStartTime") DateTime executionStartTime,
            @JsonProperty("lastHeartBeat") DateTime lastHeartBeat,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("splits") int splits,
            @JsonProperty("startedSplits") int startedSplits,
            @JsonProperty("completedSplits") int completedSplits,
            @JsonProperty("splitWallTime") Duration splitWallTime,
            @JsonProperty("splitCpuTime") Duration splitCpuTime,
            @JsonProperty("splitUserTime") Duration splitUserTime,
            @JsonProperty("exchangeStatus") List<ExchangeClientStatus> exchangeStatus,
            @JsonProperty("exchangeWaitTime") Duration exchangeWaitTime,
            @JsonProperty("inputDataSize") DataSize inputDataSize,
            @JsonProperty("completedDataSize") DataSize completedDataSize,
            @JsonProperty("inputPositionCount") long inputPositionCount,
            @JsonProperty("completedPositionCount") long completedPositionCount,
            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositionCount") long outputPositionCount)
    {
        this.createTime = createTime;
        this.executionStartTime = executionStartTime;
        this.lastHeartBeat = lastHeartBeat;
        this.endTime = endTime;
        this.splits.addAndGet(splits);
        this.startedSplits.addAndGet(startedSplits);
        this.completedSplits.addAndGet(completedSplits);
        this.splitWallTime = splitWallTime;
        this.splitCpuTime = splitCpuTime;
        this.splitUserTime = splitUserTime;
        this.exchangeStatus.set(ImmutableList.copyOf(exchangeStatus));
        this.exchangeWaitTime = exchangeWaitTime;
        this.inputDataSize = inputDataSize;
        this.inputPositionCount.addAndGet(inputPositionCount);
        this.completedDataSize = completedDataSize;
        this.completedPositionCount.addAndGet(completedPositionCount);
        this.outputDataSize = outputDataSize;
        this.outputPositionCount.addAndGet(outputPositionCount);
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public synchronized DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    @JsonProperty
    public synchronized DateTime getLastHeartBeat()
    {
        return lastHeartBeat;
    }

    @JsonProperty
    public synchronized DateTime getEndTime()
    {
        return endTime;
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

    public int getPendingSplits()
    {
        return Math.max(0, getSplits() - Math.max(getStartedSplits(), getCompletedSplits()));
    }

    @JsonProperty
    public synchronized Duration getSplitCpuTime()
    {
        return splitCpuTime;
    }

    @JsonProperty
    public synchronized Duration getSplitWallTime()
    {
        return splitWallTime;
    }

    @JsonProperty
    public synchronized Duration getSplitUserTime()
    {
        return splitUserTime;
    }

    @JsonProperty
    public List<ExchangeClientStatus> getExchangeStatus()
    {
        return exchangeStatus.get();
    }

    @JsonProperty
    public synchronized Duration getExchangeWaitTime()
    {
        return exchangeWaitTime;
    }

    @JsonProperty
    public synchronized DataSize getInputDataSize()
    {
        return inputDataSize;
    }

    @JsonProperty
    public long getInputPositionCount()
    {
        return inputPositionCount.get();
    }

    @JsonProperty
    public synchronized DataSize getCompletedDataSize()
    {
        return completedDataSize;
    }

    @JsonProperty
    public long getCompletedPositionCount()
    {
        return completedPositionCount.get();
    }

    @JsonProperty
    public synchronized DataSize getOutputDataSize()
    {
        return outputDataSize;
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

    public void setExchangeStatus(List<ExchangeClientStatus> exchangeStatus)
    {
        this.exchangeStatus.set(ImmutableList.copyOf(exchangeStatus));
    }

    public void splitStarted()
    {
        startedSplits.incrementAndGet();
    }

    public void splitCompleted()
    {
        completedSplits.incrementAndGet();
    }

    public synchronized void addSplitCpuTime(Duration duration)
    {
        splitCpuTime = new Duration(splitCpuTime.toMillis() + duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public synchronized void addSplitWallTime(Duration duration)
    {
        splitWallTime = new Duration(splitWallTime.toMillis() + duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public synchronized void addSplitUserTime(Duration duration)
    {
        splitUserTime = new Duration(splitUserTime.toMillis() + duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public synchronized void addExchangeWaitTime(Duration duration)
    {
        exchangeWaitTime = new Duration(exchangeWaitTime.toMillis() + duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void addInputPositions(long inputPositions)
    {
        this.inputPositionCount.addAndGet(inputPositions);
    }

    public synchronized void addInputDataSize(DataSize addedDataSize)
    {
        inputDataSize = new DataSize(inputDataSize.toBytes() + addedDataSize.toBytes(), DataSize.Unit.BYTE);
    }

    public void addCompletedPositions(long completedPositions)
    {
        this.completedPositionCount.addAndGet(completedPositions);
    }

    public synchronized void addCompletedDataSize(DataSize addedDataSize)
    {
        completedDataSize = new DataSize(completedDataSize.toBytes() + addedDataSize.toBytes(), DataSize.Unit.BYTE);
    }

    public void addOutputPositions(long outputPositions)
    {
        this.outputPositionCount.addAndGet(outputPositions);
    }

    public synchronized void addOutputDataSize(DataSize addedDataSize)
    {
        outputDataSize = new DataSize(outputDataSize.toBytes() + addedDataSize.toBytes(), DataSize.Unit.BYTE);
    }

    public synchronized void recordExecutionStart()
    {
        this.executionStartTime = DateTime.now();
    }

    public synchronized void recordHeartBeat()
    {
        this.lastHeartBeat = DateTime.now();
    }

    public synchronized void recordEnd()
    {
        if (endTime == null) {
            endTime = DateTime.now();
        }
    }

    public void add(ExecutionStats stats) {
        splits.addAndGet(stats.getSplits());
        startedSplits.addAndGet(stats.getStartedSplits());
        completedSplits.addAndGet(stats.getCompletedSplits());
        addSplitWallTime(stats.getSplitWallTime());
        addSplitCpuTime(stats.getSplitCpuTime());
        addSplitUserTime(stats.getSplitUserTime());
        addExchangeWaitTime(stats.getExchangeWaitTime());
        addInputDataSize(stats.getInputDataSize());
        inputPositionCount.addAndGet(stats.getInputPositionCount());
        addCompletedDataSize(stats.getCompletedDataSize());
        completedPositionCount.addAndGet(stats.getCompletedPositionCount());
        addOutputDataSize(stats.getOutputDataSize());
        outputPositionCount.addAndGet(stats.getOutputPositionCount());
    }
}

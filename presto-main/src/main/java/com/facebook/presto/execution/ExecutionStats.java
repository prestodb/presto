/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.ExchangeClientStatus;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.DistributionStat.DistributionStatSnapshot;
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
    private DateTime lastHeartbeat;
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
    private Duration sinkBufferWaitTime = ZERO_DURATION;

    @GuardedBy("this")
    private Duration exchangeWaitTime = ZERO_DURATION;

    @GuardedBy("this")
    private DataSize inputDataSize = ZERO_SIZE;
    @GuardedBy("this")
    private DataSize completedDataSize = ZERO_SIZE;

    private final DistributionStat timeToFirstByte = new DistributionStat();
    private final DistributionStat timeToLastByte = new DistributionStat();

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
        lastHeartbeat = DateTime.now();
    }

    public DateTime getCreateTime()
    {
        return createTime;
    }

    public synchronized DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    public synchronized DateTime getLastHeartbeat()
    {
        return lastHeartbeat;
    }

    public synchronized DateTime getEndTime()
    {
        return endTime;
    }

    public int getSplits()
    {
        return splits.get();
    }

    public int getStartedSplits()
    {
        return startedSplits.get();
    }

    public int getCompletedSplits()
    {
        return completedSplits.get();
    }

    public int getQueuedSplits()
    {
        return Math.max(0, getSplits() - Math.max(getStartedSplits(), getCompletedSplits()));
    }

    public int getRunningSplits()
    {
        return Math.max(0, getStartedSplits() - getCompletedSplits());
    }

    public synchronized Duration getSplitCpuTime()
    {
        return splitCpuTime;
    }

    public synchronized Duration getSplitWallTime()
    {
        return splitWallTime;
    }

    public synchronized Duration getSplitUserTime()
    {
        return splitUserTime;
    }

    public synchronized Duration getSinkBufferWaitTime()
    {
        return sinkBufferWaitTime;
    }

    public List<ExchangeClientStatus> getExchangeStatus()
    {
        return exchangeStatus.get();
    }

    public synchronized Duration getExchangeWaitTime()
    {
        return exchangeWaitTime;
    }

    public synchronized DataSize getInputDataSize()
    {
        return inputDataSize;
    }

    public long getInputPositionCount()
    {
        return inputPositionCount.get();
    }

    public synchronized DataSize getCompletedDataSize()
    {
        return completedDataSize;
    }

    public long getCompletedPositionCount()
    {
        return completedPositionCount.get();
    }

    public synchronized DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    public long getOutputPositionCount()
    {
        return outputPositionCount.get();
    }

    public DistributionStatSnapshot getTimeToFirstByte()
    {
        if (timeToLastByte.getAllTime().getCount() == 0) {
            return null;
        }
        return timeToFirstByte.snapshot();
    }

    public DistributionStatSnapshot getTimeToLastByte()
    {
        if (timeToLastByte.getAllTime().getCount() == 0) {
            return null;
        }
        return timeToLastByte.snapshot();
    }

    public ExecutionStatsSnapshot snapshot(boolean full)
    {
        return new ExecutionStatsSnapshot(getCreateTime(),
                getExecutionStartTime(),
                getLastHeartbeat(),
                getEndTime(),
                getSplits(),
                getQueuedSplits(),
                getStartedSplits(),
                getRunningSplits(),
                getCompletedSplits(),
                getSplitWallTime(),
                getSplitCpuTime(),
                getSplitUserTime(),
                getSinkBufferWaitTime(),
                getExchangeStatus(),
                getExchangeWaitTime(),
                getInputDataSize(),
                getCompletedDataSize(),
                getInputPositionCount(),
                getCompletedPositionCount(),
                getOutputDataSize(),
                getOutputPositionCount(),
                full ? getTimeToFirstByte() : null,
                full ? getTimeToLastByte() : null);
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

    public synchronized void addSinkBufferWaitTime(Duration duration)
    {
        sinkBufferWaitTime = new Duration(sinkBufferWaitTime.toMillis() + duration.toMillis(), TimeUnit.MILLISECONDS);
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

    public synchronized void recordHeartbeat()
    {
        this.lastHeartbeat = DateTime.now();
    }

    public synchronized void recordEnd()
    {
        if (endTime == null) {
            endTime = DateTime.now();
        }
    }

    public void addTimeToFirstByte(Duration duration)
    {
        timeToFirstByte.add((long) duration.toMillis());
    }

    public void addTimeToLastByte(Duration duration)
    {
        timeToLastByte.add((long) duration.toMillis());
    }

    public void add(ExecutionStats stats)
    {
        splits.addAndGet(stats.getSplits());
        startedSplits.addAndGet(stats.getStartedSplits());
        completedSplits.addAndGet(stats.getCompletedSplits());
        addSplitWallTime(stats.getSplitWallTime());
        addSplitCpuTime(stats.getSplitCpuTime());
        addSplitUserTime(stats.getSplitUserTime());
        addSinkBufferWaitTime(stats.getSinkBufferWaitTime());
        addExchangeWaitTime(stats.getExchangeWaitTime());
        addInputDataSize(stats.getInputDataSize());
        inputPositionCount.addAndGet(stats.getInputPositionCount());
        addCompletedDataSize(stats.getCompletedDataSize());
        completedPositionCount.addAndGet(stats.getCompletedPositionCount());
        addOutputDataSize(stats.getOutputDataSize());
        outputPositionCount.addAndGet(stats.getOutputPositionCount());
    }

    public void add(ExecutionStatsSnapshot stats)
    {
        splits.addAndGet(stats.getSplits());
        startedSplits.addAndGet(stats.getStartedSplits());
        completedSplits.addAndGet(stats.getCompletedSplits());
        if (stats.getSplitWallTime() != null) {
            addSplitWallTime(stats.getSplitWallTime());
        }
        if (stats.getSplitCpuTime() != null) {
            addSplitCpuTime(stats.getSplitCpuTime());
        }
        if (stats.getSplitUserTime() != null) {
            addSplitUserTime(stats.getSplitUserTime());
        }
        if (stats.getSinkBufferWaitTime() != null) {
            addSinkBufferWaitTime(stats.getSinkBufferWaitTime());
        }
        if (stats.getExchangeWaitTime() != null) {
            addExchangeWaitTime(stats.getExchangeWaitTime());
        }
        if (stats.getInputDataSize() != null) {
            addInputDataSize(stats.getInputDataSize());
        }
        inputPositionCount.addAndGet(stats.getInputPositionCount());
        if (stats.getCompletedDataSize() != null) {
            addCompletedDataSize(stats.getCompletedDataSize());
        }
        completedPositionCount.addAndGet(stats.getCompletedPositionCount());
        if (stats.getOutputDataSize() != null) {
            addOutputDataSize(stats.getOutputDataSize());
        }
        outputPositionCount.addAndGet(stats.getOutputPositionCount());
    }

    public static class ExecutionStatsSnapshot
    {
        private final DateTime createTime;
        private final DateTime executionStartTime;
        private final DateTime lastHeartbeat;
        private final DateTime endTime;
        private final int splits;
        private final int queuedSplits;
        private final int startedSplits;
        private final int runningSplits;
        private final int completedSplits;
        private final Duration splitWallTime;
        private final Duration splitCpuTime;
        private final Duration splitUserTime;
        private final Duration sinkBufferWaitTime;
        private final DistributionStatSnapshot timeToFirstByte;
        private final DistributionStatSnapshot timeToLastByte;
        private final List<ExchangeClientStatus> exchangeStatus;
        private final Duration exchangeWaitTime;
        private final DataSize inputDataSize;
        private final long inputPositionCount;
        private final DataSize completedDataSize;
        private final long completedPositionCount;
        private final DataSize outputDataSize;
        private final long outputPositionCount;

        public ExecutionStatsSnapshot()
        {
            this.createTime = null;
            this.executionStartTime = null;
            this.lastHeartbeat = null;
            this.endTime = null;
            this.splits = 0;
            this.queuedSplits = 0;
            this.startedSplits = 0;
            this.runningSplits = 0;
            this.completedSplits = 0;
            this.splitWallTime = null;
            this.splitCpuTime = null;
            this.splitUserTime = null;
            this.sinkBufferWaitTime = null;
            this.timeToFirstByte = null;
            this.timeToLastByte = null;
            this.exchangeStatus = null;
            this.exchangeWaitTime = null;
            this.inputDataSize = null;
            this.inputPositionCount = 0;
            this.completedDataSize = null;
            this.completedPositionCount = 0;
            this.outputDataSize = null;
            this.outputPositionCount = 0;
        }

        @JsonCreator
        public ExecutionStatsSnapshot(
                @JsonProperty("createTime") DateTime createTime,
                @JsonProperty("executionStartTime") DateTime executionStartTime,
                @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
                @JsonProperty("endTime") DateTime endTime,
                @JsonProperty("splits") int splits,
                @JsonProperty("queuedSplits") int queuedSplits,
                @JsonProperty("startedSplits") int startedSplits,
                @JsonProperty("runningSplits") int runningSplits,
                @JsonProperty("completedSplits") int completedSplits,
                @JsonProperty("splitWallTime") Duration splitWallTime,
                @JsonProperty("splitCpuTime") Duration splitCpuTime,
                @JsonProperty("splitUserTime") Duration splitUserTime,
                @JsonProperty("sinkBufferWaitTime") Duration sinkBufferWaitTime,
                @JsonProperty("exchangeStatus") List<ExchangeClientStatus> exchangeStatus,
                @JsonProperty("exchangeWaitTime") Duration exchangeWaitTime,
                @JsonProperty("inputDataSize") DataSize inputDataSize,
                @JsonProperty("completedDataSize") DataSize completedDataSize,
                @JsonProperty("inputPositionCount") long inputPositionCount,
                @JsonProperty("completedPositionCount") long completedPositionCount,
                @JsonProperty("outputDataSize") DataSize outputDataSize,
                @JsonProperty("outputPositionCount") long outputPositionCount,
                @JsonProperty("timeToFirstByte") DistributionStatSnapshot timeToFirstByte,
                @JsonProperty("timeToLastByte") DistributionStatSnapshot timeToLastByte)
        {
            this.createTime = createTime;
            this.executionStartTime = executionStartTime;
            this.lastHeartbeat = lastHeartbeat;
            this.endTime = endTime;
            this.splits = splits;
            this.queuedSplits = queuedSplits;
            this.startedSplits = startedSplits;
            this.runningSplits = runningSplits;
            this.completedSplits = completedSplits;
            this.splitWallTime = splitWallTime;
            this.splitCpuTime = splitCpuTime;
            this.splitUserTime = splitUserTime;
            this.sinkBufferWaitTime = sinkBufferWaitTime;
            this.timeToFirstByte = timeToFirstByte;
            this.timeToLastByte = timeToLastByte;
            if (exchangeStatus != null) {
                this.exchangeStatus = ImmutableList.copyOf(exchangeStatus);
            }
            else {
                this.exchangeStatus = ImmutableList.of();
            }
            this.exchangeWaitTime = exchangeWaitTime;
            this.inputDataSize = inputDataSize;
            this.inputPositionCount = inputPositionCount;
            this.completedDataSize = completedDataSize;
            this.completedPositionCount = completedPositionCount;
            this.outputDataSize = outputDataSize;
            this.outputPositionCount = outputPositionCount;
        }

        @JsonProperty
        public DateTime getCreateTime()
        {
            return createTime;
        }

        @JsonProperty
        public DateTime getExecutionStartTime()
        {
            return executionStartTime;
        }

        @JsonProperty
        public DateTime getLastHeartbeat()
        {
            return lastHeartbeat;
        }

        @JsonProperty
        public DateTime getEndTime()
        {
            return endTime;
        }

        @JsonProperty
        public int getSplits()
        {
            return splits;
        }

        @JsonProperty
        public int getQueuedSplits()
        {
            return queuedSplits;
        }

        @JsonProperty
        public int getStartedSplits()
        {
            return startedSplits;
        }

        @JsonProperty
        public int getRunningSplits()
        {
            return runningSplits;
        }

        @JsonProperty
        public int getCompletedSplits()
        {
            return completedSplits;
        }

        @JsonProperty
        public Duration getSplitWallTime()
        {
            return splitWallTime;
        }

        @JsonProperty
        public Duration getSplitCpuTime()
        {
            return splitCpuTime;
        }

        @JsonProperty
        public Duration getSplitUserTime()
        {
            return splitUserTime;
        }

        @JsonProperty
        public Duration getSinkBufferWaitTime()
        {
            return sinkBufferWaitTime;
        }

        @JsonProperty
        public DistributionStatSnapshot getTimeToFirstByte()
        {
            return timeToFirstByte;
        }

        @JsonProperty
        public DistributionStatSnapshot getTimeToLastByte()
        {
            return timeToLastByte;
        }

        @JsonProperty
        public List<ExchangeClientStatus> getExchangeStatus()
        {
            return exchangeStatus;
        }

        @JsonProperty
        public Duration getExchangeWaitTime()
        {
            return exchangeWaitTime;
        }

        @JsonProperty
        public DataSize getInputDataSize()
        {
            return inputDataSize;
        }

        @JsonProperty
        public long getInputPositionCount()
        {
            return inputPositionCount;
        }

        @JsonProperty
        public DataSize getCompletedDataSize()
        {
            return completedDataSize;
        }

        @JsonProperty
        public long getCompletedPositionCount()
        {
            return completedPositionCount;
        }

        @JsonProperty
        public DataSize getOutputDataSize()
        {
            return outputDataSize;
        }

        @JsonProperty
        public long getOutputPositionCount()
        {
            return outputPositionCount;
        }
    }
}

/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.operator.OperatorStats.SmallCounterStat.SmallCounterStatSnapshot;
import com.facebook.presto.util.CpuTimer;
import com.facebook.presto.util.CpuTimer.CpuDuration;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.airlift.stats.DecayCounter;
import io.airlift.stats.DecayCounter.DecayCounterSnapshot;
import io.airlift.stats.ExponentialDecay;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@ThreadSafe
public class OperatorStats
{
    private final TaskOutput taskOutput;

    private final AtomicLong startTime = new AtomicLong();
    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();

    private final AtomicReference<Duration> timeToFirstByte = new AtomicReference<>();
    private final AtomicReference<Duration> timeToLastByte = new AtomicReference<>();

    private final SmallCounterStat completedDataSize = new SmallCounterStat();
    private final SmallCounterStat completedPositions = new SmallCounterStat();

    private final AtomicReference<CpuTimer> cpuTimer = new AtomicReference<>();

    private final AtomicBoolean finished = new AtomicBoolean();

    public OperatorStats()
    {
        this.taskOutput = null;
    }

    public OperatorStats(TaskOutput taskOutput)
    {
        Preconditions.checkNotNull(taskOutput, "taskOutput is null");
        this.taskOutput = taskOutput;
    }

    public boolean isDone()
    {
        return finished.get() || (taskOutput != null && taskOutput.getState().isDone());
    }

    public void addDeclaredSize(long bytes)
    {
        if (taskOutput == null) {
            return;
        }

        taskOutput.getStats().addInputDataSize(new DataSize(bytes, Unit.BYTE));
    }

    public void addCompletedDataSize(long bytes)
    {
        // set time to first byte, if not already set
        if (timeToFirstByte.compareAndSet(null, Duration.nanosSince(startTime.get()))) {
            if (taskOutput != null) {
                taskOutput.getStats().addTimeToFirstByte(timeToFirstByte.get());
            }
        }

        completedDataSize.update(bytes);

        if (taskOutput == null || bytes == 0) {
            return;
        }

        DataSize dataSize = new DataSize(bytes, Unit.BYTE);
        taskOutput.getStats().addCompletedDataSize(dataSize);
        taskOutput.getStats().addInputDataSize(dataSize);

        updateTaskOutputTimings();
    }

    public void addCompletedPositions(long positions)
    {
        completedPositions.update(positions);

        if (taskOutput == null) {
            return;
        }

        taskOutput.getStats().addCompletedPositions(positions);
        taskOutput.getStats().addInputPositions(positions);

        updateTaskOutputTimings();
    }

    public void addExchangeWaitTime(Duration duration)
    {
        if (taskOutput == null) {
            return;
        }
        taskOutput.getStats().addExchangeWaitTime(duration);
    }

    public synchronized void setExchangeStatus(List<ExchangeClientStatus> exchangeStatus)
    {
        if (taskOutput == null) {
            return;
        }
        taskOutput.getStats().setExchangeStatus(exchangeStatus);
    }

    public void start()
    {
        if (!startTime.compareAndSet(0, System.nanoTime())) {
            // already started
            return;
        }
        executionStartTime.set(DateTime.now());
        cpuTimer.set(new CpuTimer());

        if (taskOutput == null) {
            return;
        }

        taskOutput.addActiveSplit(this);
        taskOutput.getStats().recordExecutionStart();
        taskOutput.getStats().splitStarted();
    }

    public void finish()
    {
        if (finished.get()) {
            return;
        }
        finished.set(true);
        timeToLastByte.set(Duration.nanosSince(startTime.get()));

        if (taskOutput == null) {
            return;
        }
        taskOutput.removeActiveSplit(this);
        taskOutput.getStats().addTimeToLastByte(Duration.nanosSince(startTime.get()));
        taskOutput.getStats().splitCompleted();
        updateTaskOutputTimings();
    }

    public SplitExecutionStats snapshot()
    {
        Duration wall = null;
        Duration cpu = null;
        Duration user = null;
        CpuTimer cpuTimer = this.cpuTimer.get();
        if (cpuTimer != null) {
            CpuDuration cpuDuration = cpuTimer.elapsedTime();
            wall = cpuDuration.getWall();
            cpu = cpuDuration.getCpu();
            user = cpuDuration.getUser();
        }

        return new SplitExecutionStats(executionStartTime.get(),
                timeToFirstByte.get(),
                timeToLastByte.get(),
                completedDataSize.snapshot(),
                completedPositions.snapshot(),
                wall,
                cpu,
                user);
    }

    private void updateTaskOutputTimings()
    {
        if (taskOutput == null) {
            return;
        }

        CpuTimer cpuTimer = this.cpuTimer.get();
        if (cpuTimer == null) {
            return;
        }

        CpuDuration splitTime = cpuTimer.startNewInterval();
        taskOutput.getStats().addSplitWallTime(splitTime.getWall());
        taskOutput.getStats().addSplitCpuTime(splitTime.getCpu());
        taskOutput.getStats().addSplitUserTime(splitTime.getUser());
    }

    public static class SplitExecutionStats
    {
        private final DateTime executionStartTime;

        private final Duration timeToFirstByte;
        private final Duration timeToLastByte;

        private final SmallCounterStatSnapshot completedDataSize;
        private final SmallCounterStatSnapshot completedPositions;

        private final Duration wall;
        private final Duration cpu;
        private final Duration user;

        @JsonCreator
        public SplitExecutionStats(
                @JsonProperty("executionStartTime") DateTime executionStartTime,
                @JsonProperty("timeToFirstByte") Duration timeToFirstByte,
                @JsonProperty("timeToLastByte") Duration timeToLastByte,
                @JsonProperty("completedDataSize") SmallCounterStatSnapshot completedDataSize,
                @JsonProperty("completedPositions") SmallCounterStatSnapshot completedPositions,
                @JsonProperty("wall") Duration wall,
                @JsonProperty("cpu") Duration cpu,
                @JsonProperty("user") Duration user)
        {
            this.executionStartTime = executionStartTime;
            this.timeToFirstByte = timeToFirstByte;
            this.timeToLastByte = timeToLastByte;
            this.completedDataSize = completedDataSize;
            this.completedPositions = completedPositions;
            this.wall = wall;
            this.cpu = cpu;
            this.user = user;
        }

        @JsonProperty
        public DateTime getExecutionStartTime()
        {
            return executionStartTime;
        }

        @JsonProperty
        public Duration getTimeToFirstByte()
        {
            return timeToFirstByte;
        }

        @JsonProperty
        public Duration getTimeToLastByte()
        {
            return timeToLastByte;
        }

        @JsonProperty
        public SmallCounterStatSnapshot getCompletedDataSize()
        {
            return completedDataSize;
        }

        @JsonProperty
        public SmallCounterStatSnapshot getCompletedPositions()
        {
            return completedPositions;
        }

        @JsonProperty
        public Duration getWall()
        {
            return wall;
        }

        @JsonProperty
        public Duration getCpu()
        {
            return cpu;
        }

        @JsonProperty
        public Duration getUser()
        {
            return user;
        }
    }

    public static class SmallCounterStat
    {
        private final AtomicLong count = new AtomicLong(0);
        private final DecayCounter tenSeconds = new DecayCounter(ExponentialDecay.seconds(10));
        private final DecayCounter thirtySeconds = new DecayCounter(ExponentialDecay.seconds(30));
        private final DecayCounter oneMinute = new DecayCounter(ExponentialDecay.oneMinute());

        public void update(long count)
        {
            tenSeconds.add(count);
            thirtySeconds.add(count);
            oneMinute.add(count);
            this.count.addAndGet(count);
        }

        public long getTotalCount()
        {
            return count.get();
        }

        public DecayCounter getTenSeconds()
        {
            return tenSeconds;
        }

        public DecayCounter getThirtySeconds()
        {
            return thirtySeconds;
        }

        public DecayCounter getOneMinute()
        {
            return oneMinute;
        }

        public SmallCounterStatSnapshot snapshot()
        {
            return new SmallCounterStatSnapshot(getTotalCount(), getTenSeconds().snapshot(), getThirtySeconds().snapshot(), getOneMinute().snapshot());
        }

        public static class SmallCounterStatSnapshot
        {
            private final long totalCount;
            private final DecayCounterSnapshot tenSeconds;
            private final DecayCounterSnapshot thirtySeconds;
            private final DecayCounterSnapshot oneMinute;

            @JsonCreator
            public SmallCounterStatSnapshot(@JsonProperty("totalCount") long totalCount,
                    @JsonProperty("tenSeconds") DecayCounterSnapshot tenSeconds,
                    @JsonProperty("thirtySeconds") DecayCounterSnapshot thirtySeconds,
                    @JsonProperty("oneMinute") DecayCounterSnapshot oneMinute)
            {
                this.totalCount = totalCount;
                this.tenSeconds = tenSeconds;
                this.thirtySeconds = thirtySeconds;
                this.oneMinute = oneMinute;
            }

            @JsonProperty
            public long getTotalCount()
            {
                return totalCount;
            }

            @JsonProperty
            public DecayCounterSnapshot getTenSeconds()
            {
                return tenSeconds;
            }

            @JsonProperty
            public DecayCounterSnapshot getThirtySeconds()
            {
                return thirtySeconds;
            }

            @JsonProperty
            public DecayCounterSnapshot getOneMinute()
            {
                return oneMinute;
            }
        }
    }
}

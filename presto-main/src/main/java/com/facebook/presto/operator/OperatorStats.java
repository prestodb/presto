/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskOutput;
import com.facebook.presto.util.CpuTimer;
import com.facebook.presto.util.CpuTimer.CpuDuration;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * This class is not thread safe, but the done state of the task is properly
 * propagated from the TaskOutput, which is thread safe, to this class.
 */
@NotThreadSafe
public class OperatorStats
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final TaskOutput taskOutput;

    private long declaredSize;
    private long declaredPositions;

    private long completedDataSize;
    private long completedPositions;

    private CpuTimer cpuTimer = new CpuTimer();
    private long exchangeWaitTime;

    private boolean finished;

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
        return finished || (taskOutput != null && taskOutput.getState().isDone());
    }

    public void addDeclaredSize(long bytes)
    {
        if (taskOutput == null) {
            return;
        }

        taskOutput.getStats().addInputDataSize(new DataSize(bytes, Unit.BYTE));
        declaredSize += bytes;
    }

    public void addCompletedDataSize(long bytes)
    {
        if (taskOutput == null) {
            return;
        }

        taskOutput.getStats().addCompletedDataSize(new DataSize(bytes, Unit.BYTE));
        completedDataSize += bytes;

        if (completedDataSize > declaredSize) {
            taskOutput.getStats().addInputDataSize(new DataSize(completedDataSize - declaredSize, Unit.BYTE));
            declaredSize = completedDataSize;
        }

        updateTimings();
    }

    public void addCompletedPositions(long positions)
    {
        if (taskOutput == null) {
            return;
        }

        taskOutput.getStats().addCompletedPositions(positions);
        completedPositions += positions;

        if (completedPositions > declaredPositions) {
            taskOutput.getStats().addInputPositions(completedPositions - declaredPositions);
            declaredPositions = completedPositions;
        }

        updateTimings();
    }

    public void addExchangeWaitTime(Duration duration)
    {
        if (taskOutput == null) {
            return;
        }
        taskOutput.getStats().addExchangeWaitTime(duration);
    }

    public void start()
    {
        if (taskOutput == null) {
            return;
        }

        taskOutput.getStats().splitStarted();
        cpuTimer = new CpuTimer();
    }

    public void finish()
    {
        if (finished) {
            return;
        }
        finished = true;

        if (taskOutput == null) {
            return;
        }

        updateTimings();
        taskOutput.getStats().splitCompleted();
    }

    private void updateTimings()
    {
        if (taskOutput == null) {
            return;
        }

        CpuDuration splitTime = cpuTimer.startNewInterval();
        taskOutput.getStats().addSplitWallTime(splitTime.getWall());
        taskOutput.getStats().addSplitCpuTime(splitTime.getCpu());
        taskOutput.getStats().addSplitUserTime(splitTime.getUser());
    }
}

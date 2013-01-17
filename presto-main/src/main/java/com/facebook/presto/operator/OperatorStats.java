/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskOutput;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

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

    long wallStartTime;
    long cpuStartTime;
    long userStartTime;

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
    }

    public void start()
    {
        if (taskOutput == null) {
            return;
        }

        taskOutput.getStats().splitStarted();
        wallStartTime = System.nanoTime();
        cpuStartTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        userStartTime = THREAD_MX_BEAN.getCurrentThreadUserTime();
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

        // update the timings
        taskOutput.getStats().addSplitWallTime(Duration.nanosSince(wallStartTime));
        taskOutput.getStats().addSplitCpuTime(new Duration(Math.max(0, THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStartTime), TimeUnit.NANOSECONDS));
        taskOutput.getStats().addSplitUserTime(new Duration(Math.max(0, THREAD_MX_BEAN.getCurrentThreadUserTime() - userStartTime), TimeUnit.NANOSECONDS));
        taskOutput.getStats().splitCompleted();
    }
}

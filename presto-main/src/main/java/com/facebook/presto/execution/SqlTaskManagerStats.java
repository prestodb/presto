package com.facebook.presto.execution;

import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class SqlTaskManagerStats
{
    private final CounterStat scheduledSplits = new CounterStat();
    private final CounterStat startedSplits = new CounterStat();
    private final CounterStat completedSplits = new CounterStat();

    private final CounterStat completedPositions = new CounterStat();
    private final CounterStat completedBytes = new CounterStat();

    private final CounterStat splitWallTime = new CounterStat();
    private final CounterStat splitCpuTime = new CounterStat();

    private final DistributionStat splitQueuedTime = new DistributionStat();

    private final DistributionStat timeToFirstByte = new DistributionStat();
    private final DistributionStat timeToLastByte = new DistributionStat();

    @Managed
    @Nested
    public CounterStat getScheduledSplits()
    {
        return scheduledSplits;
    }

    @Managed
    @Nested
    public CounterStat getStartedSplits()
    {
        return startedSplits;
    }

    @Managed
    @Nested
    public CounterStat getCompletedSplits()
    {
        return completedSplits;
    }

    @Managed
    @Nested
    public CounterStat getCompletedPositions()
    {
        return completedPositions;
    }

    @Managed
    @Nested
    public CounterStat getCompletedBytes()
    {
        return completedBytes;
    }

    @Managed
    @Nested
    public CounterStat getSplitWallTime()
    {
        return splitWallTime;
    }

    @Managed
    @Nested
    public CounterStat getSplitCpuTime()
    {
        return splitCpuTime;
    }

    @Managed
    @Nested
    public DistributionStat getTimeToFirstByte()
    {
        return timeToFirstByte;
    }

    @Managed
    @Nested
    public DistributionStat getSplitQueuedTime()
    {
        return splitQueuedTime;
    }

    @Managed
    @Nested
    public DistributionStat getTimeToLastByte()
    {
        return timeToLastByte;
    }

    @Managed
    public long getRunningSplits()
    {
        return Math.max(0, startedSplits.getTotalCount() - completedSplits.getTotalCount());
    }

    public void addSplits(int count)
    {
        scheduledSplits.update(count);
    }

    public void splitStarted()
    {
        startedSplits.update(1);
    }

    public void splitCompleted()
    {
        completedSplits.update(1);
    }

    public void addSplitCpuTime(Duration duration)
    {
        splitCpuTime.update((long) duration.toMillis());
    }

    public void addSplitWallTime(Duration duration)
    {
        splitWallTime.update((long) duration.toMillis());
    }

    public void addCompletedPositions(long positions)
    {
        completedPositions.update(positions);
    }

    public void addCompletedDataSize(DataSize addedDataSize)
    {
        completedBytes.update(addedDataSize.toBytes());
    }

    public void addSplitQueuedTime(Duration duration)
    {
        splitQueuedTime.add((long) duration.toMillis());
    }

    public void addTimeToFirstByte(Duration duration)
    {
        timeToFirstByte.add((long) duration.toMillis());
    }

    public void addTimeToLastByte(Duration duration)
    {
        timeToLastByte.add((long) duration.toMillis());
    }
}

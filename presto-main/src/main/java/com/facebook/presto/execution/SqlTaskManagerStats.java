/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SqlTaskManagerStats
{
    private final CounterStat scheduledSplits = new CounterStat();
    private final CounterStat startedSplits = new CounterStat();
    private final CounterStat completedSplits = new CounterStat();

    private final CounterStat completedPositions = new CounterStat();
    private final CounterStat completedBytes = new CounterStat();

    private final CounterStat splitWallTime = new CounterStat();
    private final CounterStat splitCpuTime = new CounterStat();

    private final TimeStat splitQueuedTime = new TimeStat(MILLISECONDS);

    private final TimeStat timeToFirstByte = new TimeStat(MILLISECONDS);
    private final TimeStat timeToLastByte = new TimeStat(MILLISECONDS);

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
    public TimeStat getTimeToFirstByte()
    {
        return timeToFirstByte;
    }

    @Managed
    @Nested
    public TimeStat getSplitQueuedTime()
    {
        return splitQueuedTime;
    }

    @Managed
    @Nested
    public TimeStat getTimeToLastByte()
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
        splitCpuTime.update(duration.toMillis());
    }

    public void addSplitWallTime(Duration duration)
    {
        splitWallTime.update(duration.toMillis());
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
        splitQueuedTime.add(duration);
    }

    public void addTimeToFirstByte(Duration duration)
    {
        timeToFirstByte.add(duration);
    }

    public void addTimeToLastByte(Duration duration)
    {
        timeToLastByte.add(duration);
    }
}

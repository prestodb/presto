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
package com.facebook.presto.server;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class TaskStatusResponseStats
{
    private final TimeStat taskStatusResponseTime = new TimeStat();

    private final CounterStat taskStatusMemoryIncreased = new CounterStat();
    private final CounterStat taskStatusSystemMemoryIncreased = new CounterStat();
    private final CounterStat queuedPartitionedDriversIncreased = new CounterStat();
    private final CounterStat runningPartitionedDriversIncreased = new CounterStat();
    private final CounterStat fullGcCountIncreased = new CounterStat();
    private final CounterStat physicalWrittenDataSizeIncreased = new CounterStat();
    private final CounterStat outputBufferOverutilized = new CounterStat();
    private final CounterStat completedDriverGroupsIncreased = new CounterStat();
    private final CounterStat taskStatusTimedOut = new CounterStat();

    public void incrementTaskStatusMemoryIncreased()
    {
        taskStatusMemoryIncreased.update(1);
    }

    public void incrementTaskStatusSystemMemoryIncreased()
    {
        taskStatusSystemMemoryIncreased.update(1);
    }

    public void incrementQueuedPartitionedDriversIncreased()
    {
        queuedPartitionedDriversIncreased.update(1);
    }

    public void incrementRunningPartitionedDriversIncreased()
    {
        runningPartitionedDriversIncreased.update(1);
    }

    public void incrementFullGcCountIncreased()
    {
        fullGcCountIncreased.update(1);
    }

    public void incrementPhysicalWrittenDataSizeIncreased()
    {
        physicalWrittenDataSizeIncreased.update(1);
    }

    public void incrementOutputBufferOverutilized()
    {
        outputBufferOverutilized.update(1);
    }

    public void incrementCompletedDriverGroupsIncreased()
    {
        completedDriverGroupsIncreased.update(1);
    }

    public void incrementTaskStatusTimedOut()
    {
        taskStatusTimedOut.update(1);
    }

    @Managed
    @Nested
    public TimeStat getTaskStatusResponseTime()
    {
        return taskStatusResponseTime;
    }

    @Managed
    @Nested
    public CounterStat getTaskStatusMemoryIncreased()
    {
        return taskStatusMemoryIncreased;
    }

    @Managed
    @Nested
    public CounterStat getTaskStatusSystemMemoryIncreased()
    {
        return taskStatusSystemMemoryIncreased;
    }

    @Managed
    @Nested
    public CounterStat getQueuedPartitionedDriversIncreased()
    {
        return queuedPartitionedDriversIncreased;
    }

    @Managed
    @Nested
    public CounterStat getRunningPartitionedDriversIncreased()
    {
        return runningPartitionedDriversIncreased;
    }

    @Managed
    @Nested
    public CounterStat getFullGcCountIncreased()
    {
        return fullGcCountIncreased;
    }

    @Managed
    @Nested
    public CounterStat getPhysicalWrittenDataSizeIncreased()
    {
        return physicalWrittenDataSizeIncreased;
    }

    @Managed
    @Nested
    public CounterStat getOutputBufferOverutilized()
    {
        return outputBufferOverutilized;
    }

    @Managed
    @Nested
    public CounterStat getCompletedDriverGroupsIncreased()
    {
        return completedDriverGroupsIncreased;
    }

    @Managed
    @Nested
    public CounterStat getTaskStatusTimedOut()
    {
        return taskStatusTimedOut;
    }
}

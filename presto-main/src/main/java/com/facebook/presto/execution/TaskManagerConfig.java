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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class TaskManagerConfig
{
    private boolean verboseStats;
    private boolean taskCpuTimerEnabled = true;
    private DataSize maxTaskMemoryUsage = new DataSize(256, Unit.MEGABYTE);
    private DataSize bigQueryMaxTaskMemoryUsage;
    private DataSize maxPartialAggregationMemoryUsage = new DataSize(16, Unit.MEGABYTE);
    private DataSize operatorPreAllocatedMemory = new DataSize(16, Unit.MEGABYTE);
    private DataSize maxTaskIndexMemoryUsage = new DataSize(64, Unit.MEGABYTE);
    private int maxShardProcessorThreads = Runtime.getRuntime().availableProcessors() * 4;
    private Integer minDrivers;

    private DataSize sinkMaxBufferSize = new DataSize(32, Unit.MEGABYTE);

    private Duration clientTimeout = new Duration(5, TimeUnit.MINUTES);
    private Duration infoMaxAge = new Duration(15, TimeUnit.MINUTES);
    private int writerCount = 1;
    private int httpNotificationThreads = 25;

    public boolean isVerboseStats()
    {
        return verboseStats;
    }

    @Config("task.verbose-stats")
    public TaskManagerConfig setVerboseStats(boolean verboseStats)
    {
        this.verboseStats = verboseStats;
        return this;
    }

    public boolean isTaskCpuTimerEnabled()
    {
        return taskCpuTimerEnabled;
    }

    @Config("task.cpu-timer-enabled")
    public TaskManagerConfig setTaskCpuTimerEnabled(boolean taskCpuTimerEnabled)
    {
        this.taskCpuTimerEnabled = taskCpuTimerEnabled;
        return this;
    }

    @NotNull
    public DataSize getMaxPartialAggregationMemoryUsage()
    {
        return maxPartialAggregationMemoryUsage;
    }

    @Config("task.max-partial-aggregation-memory")
    public TaskManagerConfig setMaxPartialAggregationMemoryUsage(DataSize maxPartialAggregationMemoryUsage)
    {
        this.maxPartialAggregationMemoryUsage = maxPartialAggregationMemoryUsage;
        return this;
    }

    public DataSize getBigQueryMaxTaskMemoryUsage()
    {
        if (bigQueryMaxTaskMemoryUsage == null) {
            return new DataSize(2 * maxTaskMemoryUsage.toBytes(), Unit.BYTE);
        }
        return bigQueryMaxTaskMemoryUsage;
    }

    @Config("experimental.big-query-max-task-memory")
    public TaskManagerConfig setBigQueryMaxTaskMemoryUsage(DataSize bigQueryMaxTaskMemoryUsage)
    {
        this.bigQueryMaxTaskMemoryUsage = bigQueryMaxTaskMemoryUsage;
        return this;
    }

    @NotNull
    public DataSize getMaxTaskMemoryUsage()
    {
        return maxTaskMemoryUsage;
    }

    @Config("task.max-memory")
    public TaskManagerConfig setMaxTaskMemoryUsage(DataSize maxTaskMemoryUsage)
    {
        this.maxTaskMemoryUsage = maxTaskMemoryUsage;
        return this;
    }

    @NotNull
    public DataSize getOperatorPreAllocatedMemory()
    {
        return operatorPreAllocatedMemory;
    }

    @Config("task.operator-pre-allocated-memory")
    public TaskManagerConfig setOperatorPreAllocatedMemory(DataSize operatorPreAllocatedMemory)
    {
        this.operatorPreAllocatedMemory = operatorPreAllocatedMemory;
        return this;
    }

    @NotNull
    public DataSize getMaxTaskIndexMemoryUsage()
    {
        return maxTaskIndexMemoryUsage;
    }

    @Config("task.max-index-memory")
    public TaskManagerConfig setMaxTaskIndexMemoryUsage(DataSize maxTaskIndexMemoryUsage)
    {
        this.maxTaskIndexMemoryUsage = maxTaskIndexMemoryUsage;
        return this;
    }

    @Min(1)
    public int getMaxShardProcessorThreads()
    {
        return maxShardProcessorThreads;
    }

    @Config("task.shard.max-threads")
    public TaskManagerConfig setMaxShardProcessorThreads(int maxShardProcessorThreads)
    {
        this.maxShardProcessorThreads = maxShardProcessorThreads;
        return this;
    }

    @Min(1)
    public int getMinDrivers()
    {
        if (minDrivers == null) {
            return 2 * maxShardProcessorThreads;
        }
        return minDrivers;
    }

    @Config("task.min-drivers")
    public TaskManagerConfig setMinDrivers(int minDrivers)
    {
        this.minDrivers = minDrivers;
        return this;
    }

    @NotNull
    public DataSize getSinkMaxBufferSize()
    {
        return sinkMaxBufferSize;
    }

    @Config("sink.max-buffer-size")
    public TaskManagerConfig setSinkMaxBufferSize(DataSize sinkMaxBufferSize)
    {
        this.sinkMaxBufferSize = sinkMaxBufferSize;
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    @Config("task.client.timeout")
    public TaskManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @NotNull
    public Duration getInfoMaxAge()
    {
        return infoMaxAge;
    }

    @Config("task.info.max-age")
    public TaskManagerConfig setInfoMaxAge(Duration infoMaxAge)
    {
        this.infoMaxAge = infoMaxAge;
        return this;
    }

    @Min(1)
    public int getWriterCount()
    {
        return writerCount;
    }

    @Config("task.writer-count")
    @ConfigDescription("Number of writers per task")
    public TaskManagerConfig setWriterCount(int writerCount)
    {
        this.writerCount = writerCount;
        return this;
    }

    @Min(1)
    public int getHttpNotificationThreads()
    {
        return httpNotificationThreads;
    }

    @Config("task.http-notification-threads")
    public TaskManagerConfig setHttpNotificationThreads(int httpNotificationThreads)
    {
        this.httpNotificationThreads = httpNotificationThreads;
        return this;
    }
}

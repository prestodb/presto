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
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

@DefunctConfig({"experimental.big-query-max-task-memory", "task.max-memory", "task.http-notification-threads"})
public class TaskManagerConfig
{
    private boolean verboseStats;
    private boolean taskCpuTimerEnabled = true;
    private DataSize maxPartialAggregationMemoryUsage = new DataSize(16, Unit.MEGABYTE);
    private DataSize operatorPreAllocatedMemory = new DataSize(16, Unit.MEGABYTE);
    private DataSize maxIndexMemoryUsage = new DataSize(64, Unit.MEGABYTE);
    private boolean shareIndexLoading;
    private int maxWorkerThreads = Runtime.getRuntime().availableProcessors() * 4;
    private Integer minDrivers;

    private DataSize sinkMaxBufferSize = new DataSize(32, Unit.MEGABYTE);

    private Duration clientTimeout = new Duration(2, TimeUnit.MINUTES);
    private Duration infoMaxAge = new Duration(15, TimeUnit.MINUTES);
    private Duration infoRefreshMaxWait = new Duration(200, TimeUnit.MILLISECONDS);
    private int writerCount = 1;
    private int taskDefaultConcurrency = 1;
    private int httpResponseThreads = 100;
    private int httpTimeoutThreads = 3;

    @MinDuration("1ms")
    @MaxDuration("10s")
    @NotNull
    public Duration getInfoRefreshMaxWait()
    {
        return infoRefreshMaxWait;
    }

    @Config("task.info-refresh-max-wait")
    public TaskManagerConfig setInfoRefreshMaxWait(Duration infoRefreshMaxWait)
    {
        this.infoRefreshMaxWait = infoRefreshMaxWait;
        return this;
    }

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
    public DataSize getMaxIndexMemoryUsage()
    {
        return maxIndexMemoryUsage;
    }

    @Config("task.max-index-memory")
    public TaskManagerConfig setMaxIndexMemoryUsage(DataSize maxIndexMemoryUsage)
    {
        this.maxIndexMemoryUsage = maxIndexMemoryUsage;
        return this;
    }

    @NotNull
    public boolean isShareIndexLoading()
    {
        return shareIndexLoading;
    }

    @Config("task.share-index-loading")
    public TaskManagerConfig setShareIndexLoading(boolean shareIndexLoading)
    {
        this.shareIndexLoading = shareIndexLoading;
        return this;
    }

    @Min(1)
    public int getMaxWorkerThreads()
    {
        return maxWorkerThreads;
    }

    @LegacyConfig("task.shard.max-threads")
    @Config("task.max-worker-threads")
    public TaskManagerConfig setMaxWorkerThreads(int maxWorkerThreads)
    {
        this.maxWorkerThreads = maxWorkerThreads;
        return this;
    }

    @Min(1)
    public int getMinDrivers()
    {
        if (minDrivers == null) {
            return 2 * maxWorkerThreads;
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
    public int getTaskDefaultConcurrency()
    {
        return taskDefaultConcurrency;
    }

    @Config("task.default-concurrency")
    @ConfigDescription("Default local concurrency for parallel operators")
    public TaskManagerConfig setTaskDefaultConcurrency(int taskDefaultConcurrency)
    {
        this.taskDefaultConcurrency = taskDefaultConcurrency;
        return this;
    }

    @Min(1)
    public int getHttpResponseThreads()
    {
        return httpResponseThreads;
    }

    @Config("task.http-response-threads")
    public TaskManagerConfig setHttpResponseThreads(int httpResponseThreads)
    {
        this.httpResponseThreads = httpResponseThreads;
        return this;
    }

    @Min(1)
    public int getHttpTimeoutThreads()
    {
        return httpTimeoutThreads;
    }

    @Config("task.http-timeout-threads")
    public TaskManagerConfig setHttpTimeoutThreads(int httpTimeoutThreads)
    {
        this.httpTimeoutThreads = httpTimeoutThreads;
        return this;
    }
}

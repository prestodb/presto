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
package io.prestosql.execution;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import io.prestosql.util.PowerOfTwo;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

@DefunctConfig({
        "experimental.big-query-max-task-memory",
        "task.max-memory",
        "task.http-notification-threads",
        "task.info-refresh-max-wait",
        "task.operator-pre-allocated-memory",
        "sink.new-implementation",
        "task.legacy-scheduling-behavior",
        "task.level-absolute-priority"})
public class TaskManagerConfig
{
    private boolean perOperatorCpuTimerEnabled = true;
    private boolean taskCpuTimerEnabled = true;
    private boolean statisticsCpuTimerEnabled = true;
    private DataSize maxPartialAggregationMemoryUsage = new DataSize(16, Unit.MEGABYTE);
    private DataSize maxLocalExchangeBufferSize = new DataSize(32, Unit.MEGABYTE);
    private DataSize maxIndexMemoryUsage = new DataSize(64, Unit.MEGABYTE);
    private boolean shareIndexLoading;
    private int maxWorkerThreads = Runtime.getRuntime().availableProcessors() * 2;
    private Integer minDrivers;
    private Integer initialSplitsPerNode;
    private int minDriversPerTask = 3;
    private int maxDriversPerTask = Integer.MAX_VALUE;
    private Duration splitConcurrencyAdjustmentInterval = new Duration(100, TimeUnit.MILLISECONDS);

    private DataSize sinkMaxBufferSize = new DataSize(32, Unit.MEGABYTE);
    private DataSize maxPagePartitioningBufferSize = new DataSize(32, Unit.MEGABYTE);

    private Duration clientTimeout = new Duration(2, TimeUnit.MINUTES);
    private Duration infoMaxAge = new Duration(15, TimeUnit.MINUTES);

    private Duration statusRefreshMaxWait = new Duration(1, TimeUnit.SECONDS);
    private Duration infoUpdateInterval = new Duration(3, TimeUnit.SECONDS);

    private int writerCount = 1;
    private int taskConcurrency = 16;
    private int httpResponseThreads = 100;
    private int httpTimeoutThreads = 3;

    private int taskNotificationThreads = 5;
    private int taskYieldThreads = 3;

    private BigDecimal levelTimeMultiplier = new BigDecimal(2.0);

    @MinDuration("1ms")
    @MaxDuration("10s")
    @NotNull
    public Duration getStatusRefreshMaxWait()
    {
        return statusRefreshMaxWait;
    }

    @Config("task.status-refresh-max-wait")
    public TaskManagerConfig setStatusRefreshMaxWait(Duration statusRefreshMaxWait)
    {
        this.statusRefreshMaxWait = statusRefreshMaxWait;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("10s")
    @NotNull
    public Duration getInfoUpdateInterval()
    {
        return infoUpdateInterval;
    }

    @Config("task.info-update-interval")
    @ConfigDescription("Interval between updating task data")
    public TaskManagerConfig setInfoUpdateInterval(Duration infoUpdateInterval)
    {
        this.infoUpdateInterval = infoUpdateInterval;
        return this;
    }

    public boolean isPerOperatorCpuTimerEnabled()
    {
        return perOperatorCpuTimerEnabled;
    }

    @LegacyConfig("task.verbose-stats")
    @Config("task.per-operator-cpu-timer-enabled")
    public TaskManagerConfig setPerOperatorCpuTimerEnabled(boolean perOperatorCpuTimerEnabled)
    {
        this.perOperatorCpuTimerEnabled = perOperatorCpuTimerEnabled;
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

    public boolean isStatisticsCpuTimerEnabled()
    {
        return statisticsCpuTimerEnabled;
    }

    @Config("task.statistics-cpu-timer-enabled")
    public TaskManagerConfig setStatisticsCpuTimerEnabled(boolean statisticsCpuTimerEnabled)
    {
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
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
    public DataSize getMaxLocalExchangeBufferSize()
    {
        return maxLocalExchangeBufferSize;
    }

    @Config("task.max-local-exchange-buffer-size")
    public TaskManagerConfig setMaxLocalExchangeBufferSize(DataSize size)
    {
        this.maxLocalExchangeBufferSize = size;
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

    public BigDecimal getLevelTimeMultiplier()
    {
        return levelTimeMultiplier;
    }

    @Config("task.level-time-multiplier")
    @ConfigDescription("Factor that determines the target scheduled time for a level relative to the next")
    @Min(0)
    public TaskManagerConfig setLevelTimeMultiplier(BigDecimal levelTimeMultiplier)
    {
        this.levelTimeMultiplier = levelTimeMultiplier;
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
    public int getInitialSplitsPerNode()
    {
        if (initialSplitsPerNode == null) {
            return maxWorkerThreads;
        }
        return initialSplitsPerNode;
    }

    @Config("task.initial-splits-per-node")
    public TaskManagerConfig setInitialSplitsPerNode(int initialSplitsPerNode)
    {
        this.initialSplitsPerNode = initialSplitsPerNode;
        return this;
    }

    @MinDuration("1ms")
    public Duration getSplitConcurrencyAdjustmentInterval()
    {
        return splitConcurrencyAdjustmentInterval;
    }

    @Config("task.split-concurrency-adjustment-interval")
    public TaskManagerConfig setSplitConcurrencyAdjustmentInterval(Duration splitConcurrencyAdjustmentInterval)
    {
        this.splitConcurrencyAdjustmentInterval = splitConcurrencyAdjustmentInterval;
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

    @Min(1)
    public int getMaxDriversPerTask()
    {
        return maxDriversPerTask;
    }

    @Config("task.max-drivers-per-task")
    @ConfigDescription("Maximum number of drivers a task can run")
    public TaskManagerConfig setMaxDriversPerTask(int maxDriversPerTask)
    {
        this.maxDriversPerTask = maxDriversPerTask;
        return this;
    }

    @Min(1)
    public int getMinDriversPerTask()
    {
        return minDriversPerTask;
    }

    @Config("task.min-drivers-per-task")
    @ConfigDescription("Minimum number of drivers guaranteed to run per task given there is sufficient work to do")
    public TaskManagerConfig setMinDriversPerTask(int minDriversPerTask)
    {
        this.minDriversPerTask = minDriversPerTask;
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

    @NotNull
    public DataSize getMaxPagePartitioningBufferSize()
    {
        return maxPagePartitioningBufferSize;
    }

    @Config("driver.max-page-partitioning-buffer-size")
    public TaskManagerConfig setMaxPagePartitioningBufferSize(DataSize size)
    {
        this.maxPagePartitioningBufferSize = size;
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
    @PowerOfTwo
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
    @PowerOfTwo
    public int getTaskConcurrency()
    {
        return taskConcurrency;
    }

    @Config("task.concurrency")
    @ConfigDescription("Default number of local parallel jobs per worker")
    public TaskManagerConfig setTaskConcurrency(int taskConcurrency)
    {
        this.taskConcurrency = taskConcurrency;
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

    @Min(1)
    public int getTaskNotificationThreads()
    {
        return taskNotificationThreads;
    }

    @Config("task.task-notification-threads")
    @ConfigDescription("Number of threads used for internal task event notifications")
    public TaskManagerConfig setTaskNotificationThreads(int taskNotificationThreads)
    {
        this.taskNotificationThreads = taskNotificationThreads;
        return this;
    }

    @Min(1)
    public int getTaskYieldThreads()
    {
        return taskYieldThreads;
    }

    @Config("task.task-yield-threads")
    @ConfigDescription("Number of threads used for setting yield signals")
    public TaskManagerConfig setTaskYieldThreads(int taskYieldThreads)
    {
        this.taskYieldThreads = taskYieldThreads;
        return this;
    }
}

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

import com.facebook.presto.connector.system.GlobalSystemConnector;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

@DefunctConfig({
        "query.max-pending-splits-per-node",
        "query.queue-config-file",
        "experimental.big-query-initial-hash-partitions",
        "experimental.max-concurrent-big-queries",
        "experimental.max-queued-big-queries",
        "query.remote-task.max-consecutive-error-count"})
public class QueryManagerConfig
{
    private int scheduleSplitBatchSize = 1000;
    private int minScheduleSplitBatchSize = 100;
    private int maxConcurrentQueries = 1000;
    private int maxQueuedQueries = 5000;

    private int hashPartitionCount = 100;
    private String partitioningProviderCatalog = GlobalSystemConnector.NAME;
    private ExchangeMaterializationStrategy exchangeMaterializationStrategy = ExchangeMaterializationStrategy.NONE;
    private Duration minQueryExpireAge = new Duration(15, TimeUnit.MINUTES);
    private int maxQueryHistory = 100;
    private int maxQueryLength = 1_000_000;
    private int maxStageCount = 100;
    private int stageCountWarningThreshold = 50;
    private int maxTotalRunningTaskCount = Integer.MAX_VALUE;
    private int maxQueryRunningTaskCount = Integer.MAX_VALUE;

    private Duration clientTimeout = new Duration(5, TimeUnit.MINUTES);

    private int queryManagerExecutorPoolSize = 5;

    private Duration remoteTaskMaxErrorDuration = new Duration(5, TimeUnit.MINUTES);
    private int remoteTaskMaxCallbackThreads = 1000;

    private String queryExecutionPolicy = "all-at-once";
    private Duration queryMaxRunTime = new Duration(100, TimeUnit.DAYS);
    private Duration queryMaxExecutionTime = new Duration(100, TimeUnit.DAYS);
    private Duration queryMaxCpuTime = new Duration(1_000_000_000, TimeUnit.DAYS);

    private int initializationRequiredWorkers = 1;
    private Duration initializationTimeout = new Duration(5, TimeUnit.MINUTES);

    private int requiredWorkers = 1;
    private Duration requiredWorkersMaxWait = new Duration(5, TimeUnit.MINUTES);

    private int querySubmissionMaxThreads = Runtime.getRuntime().availableProcessors() * 2;

    @Min(1)
    public int getScheduleSplitBatchSize()
    {
        return scheduleSplitBatchSize;
    }

    @Config("query.schedule-split-batch-size")
    public QueryManagerConfig setScheduleSplitBatchSize(int scheduleSplitBatchSize)
    {
        this.scheduleSplitBatchSize = scheduleSplitBatchSize;
        return this;
    }

    @Min(1)
    public int getMinScheduleSplitBatchSize()
    {
        return minScheduleSplitBatchSize;
    }

    @Config("query.min-schedule-split-batch-size")
    public QueryManagerConfig setMinScheduleSplitBatchSize(int minScheduleSplitBatchSize)
    {
        this.minScheduleSplitBatchSize = minScheduleSplitBatchSize;
        return this;
    }

    @Deprecated
    @Min(1)
    public int getMaxConcurrentQueries()
    {
        return maxConcurrentQueries;
    }

    @Deprecated
    @Config("query.max-concurrent-queries")
    public QueryManagerConfig setMaxConcurrentQueries(int maxConcurrentQueries)
    {
        this.maxConcurrentQueries = maxConcurrentQueries;
        return this;
    }

    @Deprecated
    @Min(1)
    public int getMaxQueuedQueries()
    {
        return maxQueuedQueries;
    }

    @Deprecated
    @Config("query.max-queued-queries")
    public QueryManagerConfig setMaxQueuedQueries(int maxQueuedQueries)
    {
        this.maxQueuedQueries = maxQueuedQueries;
        return this;
    }

    @Min(1)
    public int getHashPartitionCount()
    {
        return hashPartitionCount;
    }

    @LegacyConfig("query.initial-hash-partitions")
    @Config("query.hash-partition-count")
    public QueryManagerConfig setHashPartitionCount(int hashPartitionCount)
    {
        this.hashPartitionCount = hashPartitionCount;
        return this;
    }

    @NotNull
    public String getPartitioningProviderCatalog()
    {
        return partitioningProviderCatalog;
    }

    @Config("query.partitioning-provider-catalog")
    @ConfigDescription("Name of the catalog providing custom partitioning")
    public QueryManagerConfig setPartitioningProviderCatalog(String partitioningProviderCatalog)
    {
        this.partitioningProviderCatalog = partitioningProviderCatalog;
        return this;
    }

    @NotNull
    public ExchangeMaterializationStrategy getExchangeMaterializationStrategy()
    {
        return exchangeMaterializationStrategy;
    }

    @Config("query.exchange-materialization-strategy")
    @ConfigDescription("The exchange materialization strategy to use")
    public QueryManagerConfig setExchangeMaterializationStrategy(ExchangeMaterializationStrategy exchangeMaterializationStrategy)
    {
        this.exchangeMaterializationStrategy = exchangeMaterializationStrategy;
        return this;
    }

    @NotNull
    public Duration getMinQueryExpireAge()
    {
        return minQueryExpireAge;
    }

    @LegacyConfig("query.max-age")
    @Config("query.min-expire-age")
    public QueryManagerConfig setMinQueryExpireAge(Duration minQueryExpireAge)
    {
        this.minQueryExpireAge = minQueryExpireAge;
        return this;
    }

    @Min(0)
    public int getMaxQueryHistory()
    {
        return maxQueryHistory;
    }

    @Config("query.max-history")
    public QueryManagerConfig setMaxQueryHistory(int maxQueryHistory)
    {
        this.maxQueryHistory = maxQueryHistory;
        return this;
    }

    @Min(0)
    @Max(1_000_000_000)
    public int getMaxQueryLength()
    {
        return maxQueryLength;
    }

    @Config("query.max-length")
    public QueryManagerConfig setMaxQueryLength(int maxQueryLength)
    {
        this.maxQueryLength = maxQueryLength;
        return this;
    }

    @Min(1)
    public int getMaxStageCount()
    {
        return maxStageCount;
    }

    @Config("query.max-stage-count")
    public QueryManagerConfig setMaxStageCount(int maxStageCount)
    {
        this.maxStageCount = maxStageCount;
        return this;
    }

    @Min(1)
    public int getStageCountWarningThreshold()
    {
        return stageCountWarningThreshold;
    }

    @Config("query.stage-count-warning-threshold")
    @ConfigDescription("Emit a warning when stage count exceeds this threshold")
    public QueryManagerConfig setStageCountWarningThreshold(int stageCountWarningThreshold)
    {
        this.stageCountWarningThreshold = stageCountWarningThreshold;
        return this;
    }

    @Min(1)
    public int getMaxTotalRunningTaskCount()
    {
        return maxTotalRunningTaskCount;
    }

    @Config("experimental.max-total-running-task-count")
    @ConfigDescription("Maximal allowed running task from all queries")
    public QueryManagerConfig setMaxTotalRunningTaskCount(int maxTotalRunningTaskCount)
    {
        this.maxTotalRunningTaskCount = maxTotalRunningTaskCount;
        return this;
    }

    @Min(1)
    public int getMaxQueryRunningTaskCount()
    {
        return maxQueryRunningTaskCount;
    }

    @Config("experimental.max-query-running-task-count")
    @ConfigDescription("Maximal allowed running task for single query only if experimental.max-total-running-task-count is violated")
    public QueryManagerConfig setMaxQueryRunningTaskCount(int maxQueryRunningTaskCount)
    {
        this.maxQueryRunningTaskCount = maxQueryRunningTaskCount;
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    @Config("query.client.timeout")
    public QueryManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @Min(1)
    public int getQueryManagerExecutorPoolSize()
    {
        return queryManagerExecutorPoolSize;
    }

    @Config("query.manager-executor-pool-size")
    public QueryManagerConfig setQueryManagerExecutorPoolSize(int queryManagerExecutorPoolSize)
    {
        this.queryManagerExecutorPoolSize = queryManagerExecutorPoolSize;
        return this;
    }

    @Deprecated
    public Duration getRemoteTaskMinErrorDuration()
    {
        return remoteTaskMaxErrorDuration;
    }

    @Deprecated
    @Config("query.remote-task.min-error-duration")
    public QueryManagerConfig setRemoteTaskMinErrorDuration(Duration remoteTaskMinErrorDuration)
    {
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getRemoteTaskMaxErrorDuration()
    {
        return remoteTaskMaxErrorDuration;
    }

    @Config("query.remote-task.max-error-duration")
    public QueryManagerConfig setRemoteTaskMaxErrorDuration(Duration remoteTaskMaxErrorDuration)
    {
        this.remoteTaskMaxErrorDuration = remoteTaskMaxErrorDuration;
        return this;
    }

    @NotNull
    public Duration getQueryMaxRunTime()
    {
        return queryMaxRunTime;
    }

    @Config("query.max-run-time")
    public QueryManagerConfig setQueryMaxRunTime(Duration queryMaxRunTime)
    {
        this.queryMaxRunTime = queryMaxRunTime;
        return this;
    }

    @NotNull
    public Duration getQueryMaxExecutionTime()
    {
        return queryMaxExecutionTime;
    }

    @Config("query.max-execution-time")
    public QueryManagerConfig setQueryMaxExecutionTime(Duration queryMaxExecutionTime)
    {
        this.queryMaxExecutionTime = queryMaxExecutionTime;
        return this;
    }

    @NotNull
    @MinDuration("1ns")
    public Duration getQueryMaxCpuTime()
    {
        return queryMaxCpuTime;
    }

    @Config("query.max-cpu-time")
    public QueryManagerConfig setQueryMaxCpuTime(Duration queryMaxCpuTime)
    {
        this.queryMaxCpuTime = queryMaxCpuTime;
        return this;
    }

    @Min(1)
    public int getRemoteTaskMaxCallbackThreads()
    {
        return remoteTaskMaxCallbackThreads;
    }

    @Config("query.remote-task.max-callback-threads")
    public QueryManagerConfig setRemoteTaskMaxCallbackThreads(int remoteTaskMaxCallbackThreads)
    {
        this.remoteTaskMaxCallbackThreads = remoteTaskMaxCallbackThreads;
        return this;
    }

    @NotNull
    public String getQueryExecutionPolicy()
    {
        return queryExecutionPolicy;
    }

    @Config("query.execution-policy")
    public QueryManagerConfig setQueryExecutionPolicy(String queryExecutionPolicy)
    {
        this.queryExecutionPolicy = queryExecutionPolicy;
        return this;
    }

    @Min(1)
    public int getInitializationRequiredWorkers()
    {
        return initializationRequiredWorkers;
    }

    @Config("query-manager.initialization-required-workers")
    @ConfigDescription("Minimum number of workers that must be available before the cluster will accept queries")
    public QueryManagerConfig setInitializationRequiredWorkers(int initializationRequiredWorkers)
    {
        this.initializationRequiredWorkers = initializationRequiredWorkers;
        return this;
    }

    @NotNull
    public Duration getInitializationTimeout()
    {
        return initializationTimeout;
    }

    @Config("query-manager.initialization-timeout")
    @ConfigDescription("After this time, the cluster will accept queries even if the minimum required workers are not available")
    public QueryManagerConfig setInitializationTimeout(Duration initializationTimeout)
    {
        this.initializationTimeout = initializationTimeout;
        return this;
    }

    @Min(1)
    public int getRequiredWorkers()
    {
        return requiredWorkers;
    }

    @Config("query-manager.required-workers")
    @ConfigDescription("Minimum number of active workers that must be available before a query will start")
    public QueryManagerConfig setRequiredWorkers(int requiredWorkers)
    {
        this.requiredWorkers = requiredWorkers;
        return this;
    }

    @NotNull
    public Duration getRequiredWorkersMaxWait()
    {
        return requiredWorkersMaxWait;
    }

    @Config("query-manager.required-workers-max-wait")
    @ConfigDescription("Maximum time to wait for minimum number of workers before the query is failed")
    public QueryManagerConfig setRequiredWorkersMaxWait(Duration requiredWorkersMaxWait)
    {
        this.requiredWorkersMaxWait = requiredWorkersMaxWait;
        return this;
    }

    @Min(1)
    public int getQuerySubmissionMaxThreads()
    {
        return querySubmissionMaxThreads;
    }

    @Config("query-manager.query-submission-max-threads")
    public QueryManagerConfig setQuerySubmissionMaxThreads(int querySubmissionMaxThreads)
    {
        this.querySubmissionMaxThreads = querySubmissionMaxThreads;
        return this;
    }

    public enum ExchangeMaterializationStrategy
    {
        NONE,
        ALL,
    }
}

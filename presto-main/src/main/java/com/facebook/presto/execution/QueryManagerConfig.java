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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.DefunctConfig;
import com.facebook.airlift.configuration.LegacyConfig;
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.spi.api.Experimental;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.PETABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;

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
    private boolean useStreamingExchangeForMarkDistinct;
    private Duration minQueryExpireAge = new Duration(15, TimeUnit.MINUTES);
    private int maxQueryHistory = 100;
    private int maxQueryLength = 1_000_000;
    private int maxStageCount = 100;
    private int stageCountWarningThreshold = 50;
    private int maxTotalRunningTaskCountToKillQuery = Integer.MAX_VALUE;
    private int maxQueryRunningTaskCount = Integer.MAX_VALUE;
    private int maxTotalRunningTaskCountToNotExecuteNewQuery = Integer.MAX_VALUE;
    private double concurrencyThresholdToEnableResourceGroupRefresh = 1.0;
    private Duration resourceGroupRunTimeInfoRefreshInterval = new Duration(100, TimeUnit.MILLISECONDS);

    private Duration clientTimeout = new Duration(5, TimeUnit.MINUTES);

    private int queryManagerExecutorPoolSize = 5;

    private Duration remoteTaskMaxErrorDuration = new Duration(5, TimeUnit.MINUTES);
    private int remoteTaskMaxCallbackThreads = 1000;

    private String queryExecutionPolicy = "all-at-once";
    private Duration queryMaxRunTime = new Duration(100, TimeUnit.DAYS);
    private Duration queryMaxExecutionTime = new Duration(100, TimeUnit.DAYS);
    private Duration queryMaxCpuTime = new Duration(1_000_000_000, TimeUnit.DAYS);

    private DataSize queryMaxScanRawInputBytes = DataSize.succinctDataSize(1000, PETABYTE);
    private DataSize queryMaxOutputSize = DataSize.succinctDataSize(1000, PETABYTE);

    private int requiredWorkers = 1;
    private Duration requiredWorkersMaxWait = new Duration(5, TimeUnit.MINUTES);
    private int requiredCoordinators = 1;
    private Duration requiredCoordinatorsMaxWait = new Duration(5, TimeUnit.MINUTES);
    private int requiredResourceManagers = 1;

    private int querySubmissionMaxThreads = Runtime.getRuntime().availableProcessors() * 2;

    private int perQueryRetryLimit;
    private Duration perQueryRetryMaxExecutionTime = new Duration(5, MINUTES);
    private int globalQueryRetryFailureLimit = 150;
    private Duration globalQueryRetryFailureWindow = new Duration(5, MINUTES);

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

    @Config("query.use-streaming-exchange-for-mark-distinct")
    @ConfigDescription("Use streaming instead of materialization with mark distinct when materialized exchange is enabled")
    public QueryManagerConfig setUseStreamingExchangeForMarkDistinct(boolean useStreamingExchangeForMarkDistinct)
    {
        this.useStreamingExchangeForMarkDistinct = useStreamingExchangeForMarkDistinct;
        return this;
    }

    @NotNull
    public boolean getUseStreamingExchangeForMarkDistinct()
    {
        return useStreamingExchangeForMarkDistinct;
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
    @MinDuration("30s")
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
    public int getMaxTotalRunningTaskCountToKillQuery()
    {
        return maxTotalRunningTaskCountToKillQuery;
    }

    @Config("max-total-running-task-count-to-kill-query")
    @ConfigDescription("Query may be killed when running task count from all queries exceeds this threshold")
    public QueryManagerConfig setMaxTotalRunningTaskCountToKillQuery(int maxTotalRunningTaskCountToKillQuery)
    {
        this.maxTotalRunningTaskCountToKillQuery = maxTotalRunningTaskCountToKillQuery;
        return this;
    }

    @Min(1)
    public int getMaxQueryRunningTaskCount()
    {
        return maxQueryRunningTaskCount;
    }

    @Config("experimental.max-total-running-task-count-to-not-execute-new-query")
    @ConfigDescription("Keep new queries in the queue if total task count exceeds this threshold")
    public QueryManagerConfig setMaxTotalRunningTaskCountToNotExecuteNewQuery(int maxTotalRunningTaskCountToNotExecuteNewQuery)
    {
        this.maxTotalRunningTaskCountToNotExecuteNewQuery = maxTotalRunningTaskCountToNotExecuteNewQuery;
        return this;
    }

    public int getMaxTotalRunningTaskCountToNotExecuteNewQuery()
    {
        return maxTotalRunningTaskCountToNotExecuteNewQuery;
    }

    @Config("max-query-running-task-count")
    @ConfigDescription("Maximal allowed running task for single query only if max-total-running-task-count-to-kill-query is violated")
    public QueryManagerConfig setMaxQueryRunningTaskCount(int maxQueryRunningTaskCount)
    {
        this.maxQueryRunningTaskCount = maxQueryRunningTaskCount;
        return this;
    }

    public Double getConcurrencyThresholdToEnableResourceGroupRefresh()
    {
        return concurrencyThresholdToEnableResourceGroupRefresh;
    }

    @Config("concurrency-threshold-to-enable-resource-group-refresh")
    @ConfigDescription("Resource group concurrency threshold precentage, once crossed new queries won't run till updated resource group info comes from resource manager")
    public QueryManagerConfig setConcurrencyThresholdToEnableResourceGroupRefresh(double concurrencyThresholdToEnableResourceGroupRefresh)
    {
        this.concurrencyThresholdToEnableResourceGroupRefresh = concurrencyThresholdToEnableResourceGroupRefresh;
        return this;
    }

    public Duration getResourceGroupRunTimeInfoRefreshInterval()
    {
        return resourceGroupRunTimeInfoRefreshInterval;
    }

    @Config("resource-group-runtimeinfo-refresh-interval")
    @ConfigDescription("How frequently to poll the resource manager for resource group updates")
    public QueryManagerConfig setResourceGroupRunTimeInfoRefreshInterval(Duration resourceGroupRunTimeInfoRefreshInterval)
    {
        this.resourceGroupRunTimeInfoRefreshInterval = resourceGroupRunTimeInfoRefreshInterval;
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

    public DataSize getQueryMaxScanRawInputBytes()
    {
        return this.queryMaxScanRawInputBytes;
    }

    @Config("query.max-scan-raw-input-bytes")
    public QueryManagerConfig setQueryMaxScanRawInputBytes(DataSize queryMaxRawInputBytes)
    {
        this.queryMaxScanRawInputBytes = queryMaxRawInputBytes;
        return this;
    }

    public DataSize getQueryMaxOutputSize()
    {
        return queryMaxOutputSize;
    }

    @Config("query.max-output-size")
    @MinDataSize("1B")
    public QueryManagerConfig setQueryMaxOutputSize(DataSize queryMaxOutputSize)
    {
        this.queryMaxOutputSize = queryMaxOutputSize;
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
    public int getRequiredCoordinators()
    {
        return requiredCoordinators;
    }

    @Experimental
    @Config("query-manager.experimental.required-coordinators")
    @ConfigDescription("Minimum number of active coordinators that must be available before a query will start")
    public QueryManagerConfig setRequiredCoordinators(int requiredCoordinators)
    {
        this.requiredCoordinators = requiredCoordinators;
        return this;
    }

    @Min(1)
    public int getRequiredResourceManagers()
    {
        return requiredResourceManagers;
    }

    @Experimental
    @Config("query-manager.experimental.required-resource-managers")
    @ConfigDescription("Minimum number of active resource managers before coordinator becomes available to take traffic")
    public QueryManagerConfig setRequiredResourceManagers(int requiredResourceManagers)
    {
        this.requiredResourceManagers = requiredResourceManagers;
        return this;
    }

    @NotNull
    public Duration getRequiredCoordinatorsMaxWait()
    {
        return requiredCoordinatorsMaxWait;
    }

    @Experimental
    @Config("query-manager.experimental.required-coordinators-max-wait")
    @ConfigDescription("Maximum time to wait for minimum number of coordinators before the query is failed")
    public QueryManagerConfig setRequiredCoordinatorsMaxWait(Duration requiredCoordinatorsMaxWait)
    {
        this.requiredCoordinatorsMaxWait = requiredCoordinatorsMaxWait;
        return this;
    }

    @Min(1)
    public int getQuerySubmissionMaxThreads()
    {
        return querySubmissionMaxThreads;
    }

    @Experimental
    @Config("query-manager.experimental.query-submission-max-threads")
    public QueryManagerConfig setQuerySubmissionMaxThreads(int querySubmissionMaxThreads)
    {
        this.querySubmissionMaxThreads = querySubmissionMaxThreads;
        return this;
    }

    public int getPerQueryRetryLimit()
    {
        return perQueryRetryLimit;
    }

    @Config("per-query-retry-limit")
    @ConfigDescription("Per-query retry limit due to communication failures")
    public QueryManagerConfig setPerQueryRetryLimit(int perQueryRetryLimit)
    {
        this.perQueryRetryLimit = perQueryRetryLimit;
        return this;
    }

    public Duration getPerQueryRetryMaxExecutionTime()
    {
        return perQueryRetryMaxExecutionTime;
    }

    @Config("per-query-retry-max-execution-time")
    @ConfigDescription("max per-query execution time limit allowed for retry")
    public QueryManagerConfig setPerQueryRetryMaxExecutionTime(Duration perQueryRetryMaxExecutionTime)
    {
        this.perQueryRetryMaxExecutionTime = perQueryRetryMaxExecutionTime;
        return this;
    }

    public int getGlobalQueryRetryFailureLimit()
    {
        return globalQueryRetryFailureLimit;
    }

    @Config("global-query-retry-failure-limit")
    @ConfigDescription("A circuit breaker to stop query retry if the number of communication failures have gone over a limit")
    public QueryManagerConfig setGlobalQueryRetryFailureLimit(int globalQueryRetryFailureLimit)
    {
        this.globalQueryRetryFailureLimit = globalQueryRetryFailureLimit;
        return this;
    }

    public Duration getGlobalQueryRetryFailureWindow()
    {
        return globalQueryRetryFailureWindow;
    }

    @Config("global-query-retry-failure-window")
    @ConfigDescription("A circuit breaker profiling window to stop query retry if the number of communication failures have gone over a limit")
    public QueryManagerConfig setGlobalQueryRetryFailureWindow(Duration globalQueryRetryFailureWindow)
    {
        this.globalQueryRetryFailureWindow = globalQueryRetryFailureWindow;
        return this;
    }

    public enum ExchangeMaterializationStrategy
    {
        NONE,
        ALL,
    }
}

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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.Unit.PETABYTE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestQueryManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(QueryManagerConfig.class)
                .setMinQueryExpireAge(new Duration(15, TimeUnit.MINUTES))
                .setMaxQueryHistory(100)
                .setMaxQueryLength(1_000_000)
                .setMaxStageCount(100)
                .setStageCountWarningThreshold(50)
                .setMaxTotalRunningTaskCountToKillQuery(Integer.MAX_VALUE)
                .setMaxQueryRunningTaskCount(Integer.MAX_VALUE)
                .setMaxTotalRunningTaskCountToNotExecuteNewQuery(Integer.MAX_VALUE)
                .setConcurrencyThresholdToEnableResourceGroupRefresh(1)
                .setResourceGroupRunTimeInfoRefreshInterval(new Duration(100, MILLISECONDS))
                .setClientTimeout(new Duration(5, TimeUnit.MINUTES))
                .setScheduleSplitBatchSize(1000)
                .setMinScheduleSplitBatchSize(100)
                .setMaxConcurrentQueries(1000)
                .setMaxQueuedQueries(5000)
                .setHashPartitionCount(100)
                .setPartitioningProviderCatalog("system")
                .setExchangeMaterializationStrategy(ExchangeMaterializationStrategy.NONE)
                .setQueryManagerExecutorPoolSize(5)
                .setRemoteTaskMinErrorDuration(new Duration(5, TimeUnit.MINUTES))
                .setRemoteTaskMaxErrorDuration(new Duration(5, TimeUnit.MINUTES))
                .setRemoteTaskMaxCallbackThreads(1000)
                .setQueryExecutionPolicy("all-at-once")
                .setQueryMaxRunTime(new Duration(100, TimeUnit.DAYS))
                .setQueryMaxExecutionTime(new Duration(100, TimeUnit.DAYS))
                .setQueryMaxCpuTime(new Duration(1_000_000_000, TimeUnit.DAYS))
                .setQueryMaxScanRawInputBytes(new DataSize(1000, PETABYTE))
                .setQueryMaxOutputSize(new DataSize(1000, PETABYTE))
                .setRequiredWorkers(1)
                .setRequiredWorkersMaxWait(new Duration(5, TimeUnit.MINUTES))
                .setRequiredCoordinators(1)
                .setRequiredCoordinatorsMaxWait(new Duration(5, TimeUnit.MINUTES))
                .setRequiredResourceManagers(1)
                .setQuerySubmissionMaxThreads(Runtime.getRuntime().availableProcessors() * 2)
                .setUseStreamingExchangeForMarkDistinct(false)
                .setPerQueryRetryLimit(0)
                .setPerQueryRetryMaxExecutionTime(new Duration(5, MINUTES))
                .setGlobalQueryRetryFailureLimit(150)
                .setGlobalQueryRetryFailureWindow(new Duration(5, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.client.timeout", "10s")
                .put("query.min-expire-age", "30s")
                .put("query.max-history", "10")
                .put("query.max-length", "10000")
                .put("query.max-stage-count", "12345")
                .put("query.stage-count-warning-threshold", "12300")
                .put("max-total-running-task-count-to-kill-query", "60000")
                .put("max-query-running-task-count", "10000")
                .put("experimental.max-total-running-task-count-to-not-execute-new-query", "50000")
                .put("concurrency-threshold-to-enable-resource-group-refresh", "2")
                .put("resource-group-runtimeinfo-refresh-interval", "10ms")
                .put("query.schedule-split-batch-size", "99")
                .put("query.min-schedule-split-batch-size", "9")
                .put("query.max-concurrent-queries", "10")
                .put("query.max-queued-queries", "15")
                .put("query.hash-partition-count", "16")
                .put("query.partitioning-provider-catalog", "hive")
                .put("query.exchange-materialization-strategy", "ALL")
                .put("query.manager-executor-pool-size", "11")
                .put("query.remote-task.min-error-duration", "30s")
                .put("query.remote-task.max-error-duration", "60s")
                .put("query.remote-task.max-callback-threads", "10")
                .put("query.execution-policy", "phased")
                .put("query.max-run-time", "2h")
                .put("query.max-execution-time", "3h")
                .put("query.max-cpu-time", "2d")
                .put("query.max-scan-raw-input-bytes", "1MB")
                .put("query.max-output-size", "100MB")
                .put("query.use-streaming-exchange-for-mark-distinct", "true")
                .put("query-manager.required-workers", "333")
                .put("query-manager.required-workers-max-wait", "33m")
                .put("query-manager.experimental.required-coordinators", "999")
                .put("query-manager.experimental.required-coordinators-max-wait", "99m")
                .put("query-manager.experimental.required-resource-managers", "9")
                .put("query-manager.experimental.query-submission-max-threads", "5")
                .put("per-query-retry-limit", "10")
                .put("per-query-retry-max-execution-time", "1h")
                .put("global-query-retry-failure-limit", "200")
                .put("global-query-retry-failure-window", "1h")
                .build();

        QueryManagerConfig expected = new QueryManagerConfig()
                .setMinQueryExpireAge(new Duration(30, TimeUnit.SECONDS))
                .setMaxQueryHistory(10)
                .setMaxQueryLength(10000)
                .setMaxStageCount(12345)
                .setStageCountWarningThreshold(12300)
                .setMaxTotalRunningTaskCountToKillQuery(60000)
                .setMaxQueryRunningTaskCount(10000)
                .setMaxTotalRunningTaskCountToNotExecuteNewQuery(50000)
                .setConcurrencyThresholdToEnableResourceGroupRefresh(2)
                .setResourceGroupRunTimeInfoRefreshInterval(new Duration(10, MILLISECONDS))
                .setClientTimeout(new Duration(10, TimeUnit.SECONDS))
                .setScheduleSplitBatchSize(99)
                .setMinScheduleSplitBatchSize(9)
                .setMaxConcurrentQueries(10)
                .setMaxQueuedQueries(15)
                .setHashPartitionCount(16)
                .setPartitioningProviderCatalog("hive")
                .setExchangeMaterializationStrategy(ExchangeMaterializationStrategy.ALL)
                .setQueryManagerExecutorPoolSize(11)
                .setRemoteTaskMinErrorDuration(new Duration(60, SECONDS))
                .setRemoteTaskMaxErrorDuration(new Duration(60, SECONDS))
                .setRemoteTaskMaxCallbackThreads(10)
                .setQueryExecutionPolicy("phased")
                .setQueryMaxRunTime(new Duration(2, TimeUnit.HOURS))
                .setQueryMaxExecutionTime(new Duration(3, TimeUnit.HOURS))
                .setQueryMaxCpuTime(new Duration(2, TimeUnit.DAYS))
                .setQueryMaxScanRawInputBytes(new DataSize(1, MEGABYTE))
                .setQueryMaxOutputSize(new DataSize(100, MEGABYTE))
                .setRequiredWorkers(333)
                .setRequiredWorkersMaxWait(new Duration(33, TimeUnit.MINUTES))
                .setRequiredCoordinators(999)
                .setRequiredCoordinatorsMaxWait(new Duration(99, TimeUnit.MINUTES))
                .setRequiredResourceManagers(9)
                .setQuerySubmissionMaxThreads(5)
                .setUseStreamingExchangeForMarkDistinct(true)
                .setPerQueryRetryLimit(10)
                .setPerQueryRetryMaxExecutionTime(new Duration(1, HOURS))
                .setGlobalQueryRetryFailureLimit(200)
                .setGlobalQueryRetryFailureWindow(new Duration(1, HOURS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}

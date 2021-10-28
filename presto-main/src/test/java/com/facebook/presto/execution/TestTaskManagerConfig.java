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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.execution.TaskManagerConfig.TaskPriorityTracking.QUERY_FAIR;
import static com.facebook.presto.execution.TaskManagerConfig.TaskPriorityTracking.TASK_FAIR;
import static io.airlift.units.DataSize.Unit;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestTaskManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(TaskManagerConfig.class)
                .setInitialSplitsPerNode(Runtime.getRuntime().availableProcessors() * 2)
                .setSplitConcurrencyAdjustmentInterval(new Duration(100, TimeUnit.MILLISECONDS))
                .setStatusRefreshMaxWait(new Duration(1, SECONDS))
                .setInfoUpdateInterval(new Duration(3, SECONDS))
                .setInfoRefreshMaxWait(new Duration(0, SECONDS))
                .setPerOperatorCpuTimerEnabled(true)
                .setTaskCpuTimerEnabled(true)
                .setPerOperatorAllocationTrackingEnabled(false)
                .setTaskAllocationTrackingEnabled(false)
                .setMaxWorkerThreads(Runtime.getRuntime().availableProcessors() * 2)
                .setMinDrivers(Runtime.getRuntime().availableProcessors() * 2 * 2)
                .setMinDriversPerTask(3)
                .setMaxDriversPerTask(Integer.MAX_VALUE)
                .setMaxTasksPerStage(Integer.MAX_VALUE)
                .setInfoMaxAge(new Duration(15, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(2, TimeUnit.MINUTES))
                .setMaxIndexMemoryUsage(new DataSize(64, Unit.MEGABYTE))
                .setShareIndexLoading(false)
                .setMaxPartialAggregationMemoryUsage(new DataSize(16, Unit.MEGABYTE))
                .setMaxLocalExchangeBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setSinkMaxBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setMaxPagePartitioningBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setWriterCount(1)
                .setPartitionedWriterCount(null)
                .setTaskConcurrency(16)
                .setHttpResponseThreads(100)
                .setHttpTimeoutConcurrency(3)
                .setHttpTimeoutThreads(3)
                .setTaskNotificationThreads(5)
                .setTaskYieldThreads(3)
                .setLevelTimeMultiplier(new BigDecimal("2"))
                .setStatisticsCpuTimerEnabled(true)
                .setLegacyLifespanCompletionCondition(false)
                .setTaskPriorityTracking(TASK_FAIR)
                .setInterruptRunawaySplitsTimeout(new Duration(600, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("task.initial-splits-per-node", "1")
                .put("task.split-concurrency-adjustment-interval", "1s")
                .put("task.status-refresh-max-wait", "2s")
                .put("task.info-update-interval", "2s")
                .put("experimental.task.info-update-refresh-max-wait", "3s")
                .put("task.per-operator-cpu-timer-enabled", "false")
                .put("task.cpu-timer-enabled", "false")
                .put("task.per-operator-allocation-tracking-enabled", "true")
                .put("task.allocation-tracking-enabled", "true")
                .put("task.max-index-memory", "512MB")
                .put("task.share-index-loading", "true")
                .put("task.max-partial-aggregation-memory", "32MB")
                .put("task.max-local-exchange-buffer-size", "33MB")
                .put("task.max-worker-threads", "3")
                .put("task.min-drivers", "2")
                .put("task.min-drivers-per-task", "5")
                .put("task.max-drivers-per-task", "13")
                .put("stage.max-tasks-per-stage", "999")
                .put("task.info.max-age", "22m")
                .put("task.client.timeout", "10s")
                .put("sink.max-buffer-size", "42MB")
                .put("driver.max-page-partitioning-buffer-size", "40MB")
                .put("task.writer-count", "4")
                .put("task.partitioned-writer-count", "8")
                .put("task.concurrency", "8")
                .put("task.http-response-threads", "4")
                .put("task.http-timeout-concurrency", "2")
                .put("task.http-timeout-threads", "10")
                .put("task.task-notification-threads", "13")
                .put("task.task-yield-threads", "8")
                .put("task.level-time-multiplier", "2.1")
                .put("task.statistics-cpu-timer-enabled", "false")
                .put("task.legacy-lifespan-completion-condition", "true")
                .put("task.task-priority-tracking", "QUERY_FAIR")
                .put("task.interrupt-runaway-splits-timeout", "599s")
                .build();

        TaskManagerConfig expected = new TaskManagerConfig()
                .setInitialSplitsPerNode(1)
                .setSplitConcurrencyAdjustmentInterval(new Duration(1, SECONDS))
                .setStatusRefreshMaxWait(new Duration(2, SECONDS))
                .setInfoUpdateInterval(new Duration(2, SECONDS))
                .setInfoRefreshMaxWait(new Duration(3, SECONDS))
                .setPerOperatorCpuTimerEnabled(false)
                .setTaskCpuTimerEnabled(false)
                .setPerOperatorAllocationTrackingEnabled(true)
                .setTaskAllocationTrackingEnabled(true)
                .setMaxIndexMemoryUsage(new DataSize(512, Unit.MEGABYTE))
                .setShareIndexLoading(true)
                .setMaxPartialAggregationMemoryUsage(new DataSize(32, Unit.MEGABYTE))
                .setMaxLocalExchangeBufferSize(new DataSize(33, Unit.MEGABYTE))
                .setMaxWorkerThreads(3)
                .setMinDrivers(2)
                .setMinDriversPerTask(5)
                .setMaxDriversPerTask(13)
                .setMaxTasksPerStage(999)
                .setInfoMaxAge(new Duration(22, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(10, SECONDS))
                .setSinkMaxBufferSize(new DataSize(42, Unit.MEGABYTE))
                .setMaxPagePartitioningBufferSize(new DataSize(40, Unit.MEGABYTE))
                .setWriterCount(4)
                .setPartitionedWriterCount(8)
                .setTaskConcurrency(8)
                .setHttpResponseThreads(4)
                .setHttpTimeoutConcurrency(2)
                .setHttpTimeoutThreads(10)
                .setTaskNotificationThreads(13)
                .setTaskYieldThreads(8)
                .setLevelTimeMultiplier(new BigDecimal("2.1"))
                .setStatisticsCpuTimerEnabled(false)
                .setLegacyLifespanCompletionCondition(true)
                .setTaskPriorityTracking(QUERY_FAIR)
                .setInterruptRunawaySplitsTimeout(new Duration(599, SECONDS));

        assertFullMapping(properties, expected);
    }
}

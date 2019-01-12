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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit;

public class TestTaskManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(TaskManagerConfig.class)
                .setInitialSplitsPerNode(Runtime.getRuntime().availableProcessors() * 2)
                .setSplitConcurrencyAdjustmentInterval(new Duration(100, TimeUnit.MILLISECONDS))
                .setStatusRefreshMaxWait(new Duration(1, TimeUnit.SECONDS))
                .setInfoUpdateInterval(new Duration(3, TimeUnit.SECONDS))
                .setPerOperatorCpuTimerEnabled(true)
                .setTaskCpuTimerEnabled(true)
                .setMaxWorkerThreads(Runtime.getRuntime().availableProcessors() * 2)
                .setMinDrivers(Runtime.getRuntime().availableProcessors() * 2 * 2)
                .setMinDriversPerTask(3)
                .setMaxDriversPerTask(Integer.MAX_VALUE)
                .setInfoMaxAge(new Duration(15, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(2, TimeUnit.MINUTES))
                .setMaxIndexMemoryUsage(new DataSize(64, Unit.MEGABYTE))
                .setShareIndexLoading(false)
                .setMaxPartialAggregationMemoryUsage(new DataSize(16, Unit.MEGABYTE))
                .setMaxLocalExchangeBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setSinkMaxBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setMaxPagePartitioningBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setWriterCount(1)
                .setTaskConcurrency(16)
                .setHttpResponseThreads(100)
                .setHttpTimeoutThreads(3)
                .setTaskNotificationThreads(5)
                .setTaskYieldThreads(3)
                .setLevelTimeMultiplier(new BigDecimal("2"))
                .setStatisticsCpuTimerEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("task.initial-splits-per-node", "1")
                .put("task.split-concurrency-adjustment-interval", "1s")
                .put("task.status-refresh-max-wait", "2s")
                .put("task.info-update-interval", "2s")
                .put("task.per-operator-cpu-timer-enabled", "false")
                .put("task.cpu-timer-enabled", "false")
                .put("task.max-index-memory", "512MB")
                .put("task.share-index-loading", "true")
                .put("task.max-partial-aggregation-memory", "32MB")
                .put("task.max-local-exchange-buffer-size", "33MB")
                .put("task.max-worker-threads", "3")
                .put("task.min-drivers", "2")
                .put("task.min-drivers-per-task", "5")
                .put("task.max-drivers-per-task", "13")
                .put("task.info.max-age", "22m")
                .put("task.client.timeout", "10s")
                .put("sink.max-buffer-size", "42MB")
                .put("driver.max-page-partitioning-buffer-size", "40MB")
                .put("task.writer-count", "4")
                .put("task.concurrency", "8")
                .put("task.http-response-threads", "4")
                .put("task.http-timeout-threads", "10")
                .put("task.task-notification-threads", "13")
                .put("task.task-yield-threads", "8")
                .put("task.level-time-multiplier", "2.1")
                .put("task.statistics-cpu-timer-enabled", "false")
                .build();

        TaskManagerConfig expected = new TaskManagerConfig()
                .setInitialSplitsPerNode(1)
                .setSplitConcurrencyAdjustmentInterval(new Duration(1, TimeUnit.SECONDS))
                .setStatusRefreshMaxWait(new Duration(2, TimeUnit.SECONDS))
                .setInfoUpdateInterval(new Duration(2, TimeUnit.SECONDS))
                .setPerOperatorCpuTimerEnabled(false)
                .setTaskCpuTimerEnabled(false)
                .setMaxIndexMemoryUsage(new DataSize(512, Unit.MEGABYTE))
                .setShareIndexLoading(true)
                .setMaxPartialAggregationMemoryUsage(new DataSize(32, Unit.MEGABYTE))
                .setMaxLocalExchangeBufferSize(new DataSize(33, Unit.MEGABYTE))
                .setMaxWorkerThreads(3)
                .setMinDrivers(2)
                .setMinDriversPerTask(5)
                .setMaxDriversPerTask(13)
                .setInfoMaxAge(new Duration(22, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(10, TimeUnit.SECONDS))
                .setSinkMaxBufferSize(new DataSize(42, Unit.MEGABYTE))
                .setMaxPagePartitioningBufferSize(new DataSize(40, Unit.MEGABYTE))
                .setWriterCount(4)
                .setTaskConcurrency(8)
                .setHttpResponseThreads(4)
                .setHttpTimeoutThreads(10)
                .setTaskNotificationThreads(13)
                .setTaskYieldThreads(8)
                .setLevelTimeMultiplier(new BigDecimal("2.1"))
                .setStatisticsCpuTimerEnabled(false);

        assertFullMapping(properties, expected);
    }
}

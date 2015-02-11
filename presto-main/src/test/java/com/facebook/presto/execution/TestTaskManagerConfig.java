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
                .setVerboseStats(false)
                .setTaskCpuTimerEnabled(true)
                .setMaxShardProcessorThreads(Runtime.getRuntime().availableProcessors() * 4)
                .setMinDrivers(Runtime.getRuntime().availableProcessors() * 4 * 2)
                .setInfoMaxAge(new Duration(15, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(5, TimeUnit.MINUTES))
                .setMaxTaskMemoryUsage(new DataSize(256, Unit.MEGABYTE))
                .setBigQueryMaxTaskMemoryUsage(null)
                .setMaxTaskIndexMemoryUsage(new DataSize(64, Unit.MEGABYTE))
                .setOperatorPreAllocatedMemory(new DataSize(16, Unit.MEGABYTE))
                .setMaxPartialAggregationMemoryUsage(new DataSize(16, Unit.MEGABYTE))
                .setSinkMaxBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setWriterCount(1)
                .setHttpNotificationThreads(25));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("task.verbose-stats", "true")
                .put("task.cpu-timer-enabled", "false")
                .put("task.max-memory", "2GB")
                .put("experimental.big-query-max-task-memory", "4GB")
                .put("task.max-index-memory", "512MB")
                .put("task.operator-pre-allocated-memory", "2MB")
                .put("task.max-partial-aggregation-memory", "32MB")
                .put("task.shard.max-threads", "3")
                .put("task.min-drivers", "2")
                .put("task.info.max-age", "22m")
                .put("task.client.timeout", "10s")
                .put("sink.max-buffer-size", "42MB")
                .put("task.writer-count", "3")
                .put("task.http-notification-threads", "4")
                .build();

        TaskManagerConfig expected = new TaskManagerConfig()
                .setVerboseStats(true)
                .setTaskCpuTimerEnabled(false)
                .setMaxTaskMemoryUsage(new DataSize(2, Unit.GIGABYTE))
                .setBigQueryMaxTaskMemoryUsage(new DataSize(4, Unit.GIGABYTE))
                .setMaxTaskIndexMemoryUsage(new DataSize(512, Unit.MEGABYTE))
                .setOperatorPreAllocatedMemory(new DataSize(2, Unit.MEGABYTE))
                .setMaxPartialAggregationMemoryUsage(new DataSize(32, Unit.MEGABYTE))
                .setMaxShardProcessorThreads(3)
                .setMinDrivers(2)
                .setInfoMaxAge(new Duration(22, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(10, TimeUnit.SECONDS))
                .setSinkMaxBufferSize(new DataSize(42, Unit.MEGABYTE))
                .setWriterCount(3)
                .setHttpNotificationThreads(4);

        assertFullMapping(properties, expected);
    }
}

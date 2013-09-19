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
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestQueryManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(QueryManagerConfig.class)
                .setCoordinator(true)
                .setMaxShardProcessorThreads(Runtime.getRuntime().availableProcessors() * 4)
                .setMaxQueryAge(new Duration(15, TimeUnit.MINUTES))
                .setMaxQueryHistory(100)
                .setInfoMaxAge(new Duration(15, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(5, TimeUnit.MINUTES))
                .setMaxTaskMemoryUsage(new DataSize(256, Unit.MEGABYTE))
                .setOperatorPreAllocatedMemory(new DataSize(16, Unit.MEGABYTE))
                .setMaxPendingSplitsPerNode(100)
                .setExchangeMaxBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setExchangeConcurrentRequestMultiplier(3)
                .setQueryManagerExecutorPoolSize(5)
                .setSinkMaxBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setRemoteTaskMaxConsecutiveErrorCount(10)
                .setRemoteTaskMinErrorDuration(new Duration(2, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("coordinator", "false")
                .put("task.max-memory", "2GB")
                .put("task.operator-pre-allocated-memory", "2MB")
                .put("query.shard.max-threads", "3")
                .put("query.info.max-age", "22m")
                .put("query.client.timeout", "10s")
                .put("query.max-age", "30s")
                .put("query.max-history", "10")
                .put("query.max-pending-splits-per-node", "33")
                .put("query.manager-executor-pool-size", "11")
                .put("sink.max-buffer-size", "42MB")
                .put("exchange.max-buffer-size", "1GB")
                .put("exchange.concurrent-request-multiplier", "13")
                .put("query.remote-task.max-consecutive-error-count", "300")
                .put("query.remote-task.min-error-duration", "30s")
                .build();

        QueryManagerConfig expected = new QueryManagerConfig()
                .setCoordinator(false)
                .setMaxTaskMemoryUsage(new DataSize(2, Unit.GIGABYTE))
                .setOperatorPreAllocatedMemory(new DataSize(2, Unit.MEGABYTE))
                .setMaxShardProcessorThreads(3)
                .setMaxQueryAge(new Duration(30, TimeUnit.SECONDS))
                .setMaxQueryHistory(10)
                .setInfoMaxAge(new Duration(22, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(10, TimeUnit.SECONDS))
                .setMaxPendingSplitsPerNode(33)
                .setExchangeMaxBufferSize(new DataSize(1, Unit.GIGABYTE))
                .setExchangeConcurrentRequestMultiplier(13)
                .setQueryManagerExecutorPoolSize(11)
                .setSinkMaxBufferSize(new DataSize(42, Unit.MEGABYTE))
                .setRemoteTaskMaxConsecutiveErrorCount(300)
                .setRemoteTaskMinErrorDuration(new Duration(30, TimeUnit.SECONDS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testDeprecatedProperties()
    {
        Map<String, String> currentProperties = new ImmutableMap.Builder<String, String>()
                .put("task.max-memory", "2GB")
                .build();

        Map<String, String> oldProperties = new ImmutableMap.Builder<String, String>()
                .put("query.operator.max-memory", "2GB")
                .build();

        ConfigAssertions.assertDeprecatedEquivalence(QueryManagerConfig.class, currentProperties, oldProperties);
    }
}

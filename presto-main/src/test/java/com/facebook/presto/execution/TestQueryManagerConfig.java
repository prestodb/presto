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
                .setMaxQueryAge(new Duration(15, TimeUnit.MINUTES))
                .setMaxQueryHistory(100)
                .setClientTimeout(new Duration(5, TimeUnit.MINUTES))
                .setScheduleSplitBatchSize(1000)
                .setMaxConcurrentQueries(1000)
                .setMaxQueuedQueries(5000)
                .setQueueMaxMemory(new DataSize(32, DataSize.Unit.GIGABYTE))
                .setQueueMaxCpuTime(new Duration(30, TimeUnit.MINUTES))
                .setQueueMaxQueryCpuTime(new Duration(2, TimeUnit.HOURS))
                .setQueueRuntimeCap(new Duration(1, TimeUnit.HOURS))
                .setQueueQueuedTimeCap(new Duration(1, TimeUnit.HOURS))
                .setQueueIsPublic(true)
                .setQueueConfigFile(null)
                .setInitialHashPartitions(8)
                .setQueryManagerExecutorPoolSize(5)
                .setRemoteTaskMinErrorDuration(new Duration(2, TimeUnit.MINUTES))
                .setRemoteTaskMaxCallbackThreads(1000)
                .setQueryExecutionPolicy("all-at-once")
                .setQueryMaxRunTime(new Duration(100, TimeUnit.DAYS))
                .setQueryMaxCpuTime(new Duration(1_000_000_000, TimeUnit.DAYS))
        );
    }
//,
//  ,
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.client.timeout", "10s")
                .put("query.max-age", "30s")
                .put("query.max-history", "10")
                .put("query.schedule-split-batch-size", "99")
                .put("query.max-concurrent-queries", "10")
                .put("query.max-queued-queries", "15")
                .put("query.queue-max-memory", "11GB")
                .put("query.queue-max-cpu-time", "20m")
                .put("query.queue-max-query-cpu-time", "16m")
                .put("query.queue-run-time-cap", "31m")
                .put("query.queue-queued-time-cap", "31m")
                .put("query.queue-is-public", "false")
                .put("query.queue-config-file", "/etc/presto/queues.json")
                .put("query.initial-hash-partitions", "16")
                .put("query.manager-executor-pool-size", "11")
                .put("query.remote-task.min-error-duration", "30s")
                .put("query.remote-task.max-callback-threads", "10")
                .put("query.execution-policy", "phased")
                .put("query.max-run-time", "2h")
                .put("query.max-cpu-time", "2d")
                .build();

        QueryManagerConfig expected = new QueryManagerConfig()
                .setMaxQueryAge(new Duration(30, TimeUnit.SECONDS))
                .setMaxQueryHistory(10)
                .setClientTimeout(new Duration(10, TimeUnit.SECONDS))
                .setScheduleSplitBatchSize(99)
                .setMaxConcurrentQueries(10)
                .setMaxQueuedQueries(15)
                .setQueueMaxMemory(new DataSize(11, DataSize.Unit.GIGABYTE))
                .setQueueMaxCpuTime(new Duration(20, TimeUnit.MINUTES))
                .setQueueMaxQueryCpuTime(new Duration(16, TimeUnit.MINUTES))
                .setQueueRuntimeCap(new Duration(31, TimeUnit.MINUTES))
                .setQueueQueuedTimeCap(new Duration(31, TimeUnit.MINUTES))
                .setQueueIsPublic(false)
                .setQueueConfigFile("/etc/presto/queues.json")
                .setInitialHashPartitions(16)
                .setQueryManagerExecutorPoolSize(11)
                .setRemoteTaskMinErrorDuration(new Duration(30, TimeUnit.SECONDS))
                .setRemoteTaskMaxCallbackThreads(10)
                .setQueryExecutionPolicy("phased")
                .setQueryMaxRunTime(new Duration(2, TimeUnit.HOURS))
                .setQueryMaxCpuTime(new Duration(2, TimeUnit.DAYS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}

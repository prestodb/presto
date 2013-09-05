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
                .setMaxPendingSplitsPerNode(100)
                .setInitialHashPartitions(8)
                .setQueryManagerExecutorPoolSize(5)
                .setRemoteTaskMaxConsecutiveErrorCount(10)
                .setRemoteTaskMinErrorDuration(new Duration(2, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.client.timeout", "10s")
                .put("query.max-age", "30s")
                .put("query.max-history", "10")
                .put("query.max-pending-splits-per-node", "33")
                .put("query.initial-hash-partitions", "16")
                .put("query.manager-executor-pool-size", "11")
                .put("query.remote-task.max-consecutive-error-count", "300")
                .put("query.remote-task.min-error-duration", "30s")
                .build();

        QueryManagerConfig expected = new QueryManagerConfig()
                .setMaxQueryAge(new Duration(30, TimeUnit.SECONDS))
                .setMaxQueryHistory(10)
                .setClientTimeout(new Duration(10, TimeUnit.SECONDS))
                .setMaxPendingSplitsPerNode(33)
                .setInitialHashPartitions(16)
                .setQueryManagerExecutorPoolSize(11)
                .setRemoteTaskMaxConsecutiveErrorCount(300)
                .setRemoteTaskMinErrorDuration(new Duration(30, TimeUnit.SECONDS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}

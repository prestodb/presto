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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestResourceManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(ResourceManagerConfig.class)
                .setCompletedQueryExpirationTimeout(new Duration(10, MINUTES))
                .setQueryExpirationTimeout(new Duration(10, SECONDS))
                .setMaxCompletedQueries(100)
                .setHeartbeatConcurrency(3)
                .setHeartbeatThreads(3)
                .setNodeStatusTimeout(new Duration(30, SECONDS))
                .setMemoryPoolInfoRefreshDuration(new Duration(1, SECONDS))
                .setResourceManagerExecutorThreads(1000)
                .setNodeHeartbeatInterval(new Duration(1, SECONDS))
                .setQueryHeartbeatInterval(new Duration(1, SECONDS))
                .setProxyAsyncTimeout(new Duration(60, SECONDS))
                .setMemoryPoolFetchInterval(new Duration(1, SECONDS))
                .setResourceGroupServiceCacheExpireInterval(new Duration(10, SECONDS))
                .setResourceGroupServiceCacheRefreshInterval(new Duration(1, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("resource-manager.query-expiration-timeout", "2h")
                .put("resource-manager.completed-query-expiration-timeout", "1h")
                .put("resource-manager.max-completed-queries", "9")
                .put("resource-manager.heartbeat-threads", "5")
                .put("resource-manager.heartbeat-concurrency", "6")
                .put("resource-manager.node-status-timeout", "1h")
                .put("resource-manager.memory-pool-info-refresh-duration", "2h")
                .put("resource-manager.executor-threads", "1234")
                .put("resource-manager.node-heartbeat-interval", "25m")
                .put("resource-manager.query-heartbeat-interval", "75m")
                .put("resource-manager.proxy-async-timeout", "345m")
                .put("resource-manager.memory-pool-fetch-interval", "6m")
                .put("resource-manager.resource-group-service-cache-expire-interval", "1m")
                .put("resource-manager.resource-group-service-cache-refresh-interval", "10m")
                .build();

        ResourceManagerConfig expected = new ResourceManagerConfig()
                .setCompletedQueryExpirationTimeout(new Duration(1, HOURS))
                .setQueryExpirationTimeout(new Duration(2, HOURS))
                .setMaxCompletedQueries(9)
                .setHeartbeatThreads(5)
                .setHeartbeatConcurrency(6)
                .setNodeStatusTimeout(new Duration(1, HOURS))
                .setMemoryPoolInfoRefreshDuration(new Duration(2, HOURS))
                .setResourceManagerExecutorThreads(1234)
                .setNodeHeartbeatInterval(new Duration(25, MINUTES))
                .setQueryHeartbeatInterval(new Duration(75, MINUTES))
                .setProxyAsyncTimeout(new Duration(345, MINUTES))
                .setMemoryPoolFetchInterval(new Duration(6, MINUTES))
                .setResourceGroupServiceCacheExpireInterval(new Duration(1, MINUTES))
                .setResourceGroupServiceCacheRefreshInterval(new Duration(10, MINUTES));

        assertFullMapping(properties, expected);
    }
}

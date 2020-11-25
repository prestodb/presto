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
                .setNodeHeartbeatInterval(new Duration(1, SECONDS))
                .setQueryHeartbeatInterval(new Duration(1, SECONDS))
                .setNodeStatusTimeout(new Duration(30, SECONDS)));
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
                .put("resource-manager.query-heartbeat-interval", "3m")
                .put("resource-manager.node-heartbeat-interval", "4m")
                .put("resource-manager.node-status-timeout", "1h")
                .build();

        ResourceManagerConfig expected = new ResourceManagerConfig()
                .setCompletedQueryExpirationTimeout(new Duration(1, HOURS))
                .setQueryExpirationTimeout(new Duration(2, HOURS))
                .setMaxCompletedQueries(9)
                .setHeartbeatThreads(5)
                .setHeartbeatConcurrency(6)
                .setQueryHeartbeatInterval(new Duration(3, MINUTES))
                .setNodeHeartbeatInterval(new Duration(4, MINUTES))
                .setNodeStatusTimeout(new Duration(1, HOURS));

        assertFullMapping(properties, expected);
    }
}

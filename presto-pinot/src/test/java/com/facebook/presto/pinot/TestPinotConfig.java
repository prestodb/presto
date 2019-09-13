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
package com.facebook.presto.pinot;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestPinotConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(PinotConfig.class)
                .setZookeeperUrl(null)
                .setPinotCluster(null)
                .setControllerUrl(null)
                .setIdleTimeout(new Duration(5, TimeUnit.MINUTES))
                .setLimitAll(null)
                .setMaxBacklogPerServer(null)
                .setMaxConnectionsPerServer(null)
                .setMinConnectionsPerServer(null)
                .setCorePoolSize("50")
                .setThreadPoolSize("64")
                .setEstimatedSizeInBytesForNonNumericColumn(20)
                .setConnectionTimeout(new Duration(1, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("zookeeper-uri", "localhost:2181")
                .put("pinot-cluster", "pinot")
                .put("controller-url", "localhost:12345")
                .put("idle-timeout", "1h")
                .put("limit-all", "2147483646")
                .put("max-backlog-per-server", "15")
                .put("max-connections-per-server", "10")
                .put("min-connections-per-server", "1")
                .put("core-pool-size", "100")
                .put("thread-pool-size", "101")
                .put("estimated-size-in-bytes-for-non-numeric-column", "30")
                .put("connection-timeout", "8m").build();

        PinotConfig expected = new PinotConfig()
                .setZookeeperUrl("localhost:2181")
                .setPinotCluster("pinot")
                .setControllerUrl("localhost:12345")
                .setIdleTimeout(new Duration(1, TimeUnit.HOURS))
                .setLimitAll("2147483646").setMaxBacklogPerServer("15")
                .setMaxConnectionsPerServer("10")
                .setMinConnectionsPerServer("1")
                .setCorePoolSize("100")
                .setThreadPoolSize("101")
                .setEstimatedSizeInBytesForNonNumericColumn(30)
                .setConnectionTimeout(new Duration(8, TimeUnit.MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}

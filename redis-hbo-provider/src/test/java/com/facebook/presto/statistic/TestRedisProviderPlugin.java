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
package com.facebook.presto.statistic;

import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.github.fppt.jedismock.RedisServer;
import com.google.common.collect.ImmutableMap;
import io.lettuce.core.RedisURI;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;

public class TestRedisProviderPlugin
{
    private Map<String, String> createConfigMap(String serverUri)
    {
        Map<String, String> configMap = ImmutableMap.<String, String>builder()
                .put("coordinator", "true")
                .put("hbo.redis-provider.enabled", "true")
                .put("hbo.redis-provider.total-fetch-timeoutms", "5000")
                .put("hbo.redis-provider.total-set-timeoutms", "5000")
                .put("hbo.redis-provider.default-ttl-seconds", "4320000")
                .put("hbo.redis-provider.cluster-mode-enabled", "false")
                .put("hbo.redis-provider.server_uri", serverUri)
                .build();
        return configMap;
    }

    @Test
    public void testStartup()
            throws Exception
    {
        RedisServer server = RedisServer.newRedisServer(); // bind to a random port
        server.start();
        RedisProviderPlugin plugin = new RedisProviderPlugin(
                createConfigMap(RedisURI.create(server.getHost(), server.getBindPort()).toString()));
        HistoryBasedPlanStatisticsProvider hboProvider = plugin.getHistoryBasedPlanStatisticsProviders().iterator().next();
        assertInstanceOf(hboProvider, RedisPlanStatisticsProvider.class);
        server.stop();
        plugin.close();
    }
}

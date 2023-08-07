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
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;

public class TestRedisProviderPlugin
{
    private final String redisHboPropertyFilePath = "src/test/java/resources/test-redis-hbo-provider.properties";

    @Test
    public void testStartup()
            throws Exception
    {
        RedisServer server = RedisServer.newRedisServer(57872);
        server.start();
        RedisProviderPlugin plugin = new RedisProviderPlugin(redisHboPropertyFilePath);
        HistoryBasedPlanStatisticsProvider hboProvider = plugin.getHistoryBasedPlanStatisticsProviders().iterator().next();
        assertInstanceOf(hboProvider, RedisPlanStatisticsProvider.class);
        server.stop();
        plugin.close();
    }
}

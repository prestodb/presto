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

import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

public class RedisClusterAsyncCommandsFactory
{
    private RedisClusterAsyncCommandsFactory() {}

    public static RedisClusterClient getRedisClusterClient(RedisProviderConfig redisProviderConfig)
    {
        return RedisClusterClient.create(redisProviderConfig.getServerUri());
    }

    public static RedisClient getRedisClient(RedisProviderConfig redisProviderConfig)
    {
        return RedisClient.create(redisProviderConfig.getServerUri());
    }

    public static RedisClusterAsyncCommands<String, HistoricalPlanStatistics> getRedisClusterAsyncCommands(RedisProviderConfig redisProviderConfig,
            HistoricalStatisticsSerde historicalStatisticsSerde, AbstractRedisClient redisClient)
    {
        if (redisProviderConfig.getClusterModeEnabled()) {
            assert (redisClient instanceof RedisClusterClient);
            return ((RedisClusterClient) redisClient).connect(historicalStatisticsSerde).async();
        }
        assert (redisClient instanceof RedisClient);
        return ((RedisClient) redisClient).connect(historicalStatisticsSerde).async();
    }
}

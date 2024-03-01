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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import javax.inject.Inject;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisPlanStatisticsProvider
        implements HistoryBasedPlanStatisticsProvider
{
    private static final Logger log = Logger.get(RedisPlanStatisticsProvider.class);

    private final RedisProviderApiStats redisProviderApiStats;
    private RedisClusterAsyncCommands<String, HistoricalPlanStatistics> commands;

    private final long totalFetchTimeoutMillis;

    private final long totalSetTimeMillis;

    private final long defaultTTLSeconds;

    @Inject
    public RedisPlanStatisticsProvider(
            RedisClusterAsyncCommands<String, HistoricalPlanStatistics> commands,
            RedisProviderApiStats redisProviderApiStats,
            RedisProviderConfig config)
    {
        this.commands = commands;
        this.totalSetTimeMillis = config.getTotalSetTimeoutMs();
        this.totalFetchTimeoutMillis = config.getTotalFetchTimeoutMs();
        this.defaultTTLSeconds = config.getDefaultTtlSeconds();
        this.redisProviderApiStats = redisProviderApiStats;
    }

    public RedisPlanStatisticsProvider(RedisProviderApiStats redisProviderApiStats, RedisClusterAsyncCommands<String, HistoricalPlanStatistics> commands, long totalFetchTimeoutMillis, long totalSetTimeoutMillis, long defaultTTLSeconds)
    {
        this.commands = commands;
        this.totalSetTimeMillis = totalSetTimeoutMillis;
        this.totalFetchTimeoutMillis = totalFetchTimeoutMillis;
        this.defaultTTLSeconds = defaultTTLSeconds;
        this.redisProviderApiStats = redisProviderApiStats;
    }

    @Override
    public String getName()
    {
        return "redis";
    }

    @Override
    public Map<PlanNodeWithHash, HistoricalPlanStatistics> getStats(List<PlanNodeWithHash> planNodesWithHash, long timeoutMillis)
    {
        Map<PlanNodeWithHash, HistoricalPlanStatistics> result = redisProviderApiStats.execute(() -> {
            Map<PlanNodeWithHash, RedisFuture<HistoricalPlanStatistics>> redisFutureMap = new HashMap<>();
            for (PlanNodeWithHash planNodeWithHash : planNodesWithHash) {
                if (!planNodeWithHash.getHash().isPresent()) {
                    continue;
                }
                RedisFuture<HistoricalPlanStatistics> future = commands.get(planNodeWithHash.getHash().get());
                redisFutureMap.put(planNodeWithHash, future);
            }
            LettuceFutures.awaitAll(Duration.ofMillis(totalFetchTimeoutMillis), redisFutureMap.values().toArray(new RedisFuture[redisFutureMap.values().size()]));
            Map<PlanNodeWithHash, HistoricalPlanStatistics> output = new HashMap<>();
            for (Map.Entry<PlanNodeWithHash, RedisFuture<HistoricalPlanStatistics>> redisFutureEntry : redisFutureMap.entrySet()) {
                if (redisFutureEntry.getValue().isDone()) {
                    try {
                        output.put(redisFutureEntry.getKey(), redisFutureEntry.getValue().get());
                    }
                    catch (Exception e) {
                        log.error(String.format("Error reading done Redis future fore key %s", redisFutureEntry.getKey().toString()));
                        throw e;
                    }
                }
                else {
                    // cancel
                    log.debug(String.format("Redis Timeout: Couldn't receive stats for key %s from redis", redisFutureEntry.getKey().toString()));
                    redisFutureEntry.getValue().cancel(true);
                }
            }
            return output;
        }, RedisProviderApiStats.Operation.FetchStats);

        return (result == null) ? ImmutableMap.of() : result;
    }

    @Override
    public void putStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesAndStatistics)
    {
        redisProviderApiStats.execute(() -> {
            List<RedisFuture> redisFuturesList = new ArrayList<>();
            hashesAndStatistics.forEach((k, v) -> {
                if (k.getHash().isPresent()) {
                    redisFuturesList.add(commands.setex(k.getHash().get(), defaultTTLSeconds, v));
                }
            });
            LettuceFutures.awaitAll(Duration.ofMillis(totalSetTimeMillis), redisFuturesList.toArray(new RedisFuture[redisFuturesList.size()]));

            for (RedisFuture redisFuture : redisFuturesList) {
                if (!redisFuture.isDone()) {
                    log.error("Redis Timeout: Couldn't put stats within timeout");
                    redisFuture.cancel(true);
                }
            }
            return null;
        }, RedisProviderApiStats.Operation.PutStats);
    }

    // Only Visible for integration testing
    @VisibleForTesting
    public void resetConnection(RedisClusterAsyncCommands<String, HistoricalPlanStatistics> cmds)
    {
        commands = cmds;
    }
}

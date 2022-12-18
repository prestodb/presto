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
package com.facebook.presto.cost;

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.CachingPlanCanonicalInfoProvider;
import com.facebook.presto.sql.planner.PlanNodeCanonicalInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class HistoryBasedStatisticsCacheManager
{
    // Cache historical statistics of plan node.
    private final Map<QueryId, LoadingCache<PlanNodeWithHash, HistoricalPlanStatistics>> statisticsCache = new ConcurrentHashMap<>();

    // Cache hashes of plan node.
    private final Map<QueryId, Map<CachingPlanCanonicalInfoProvider.CacheKey, PlanNodeCanonicalInfo>> canonicalInfoCache = new ConcurrentHashMap<>();

    public HistoryBasedStatisticsCacheManager() {}

    public LoadingCache<PlanNodeWithHash, HistoricalPlanStatistics> getStatisticsCache(QueryId queryId, Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider)
    {
        return statisticsCache.computeIfAbsent(queryId, ignored -> CacheBuilder.newBuilder()
                .build(new CacheLoader<PlanNodeWithHash, HistoricalPlanStatistics>()
                {
                    @Override
                    public HistoricalPlanStatistics load(PlanNodeWithHash key)
                    {
                        return loadAll(Collections.singleton(key)).values().stream().findAny().orElseGet(HistoricalPlanStatistics::empty);
                    }

                    @Override
                    public Map<PlanNodeWithHash, HistoricalPlanStatistics> loadAll(Iterable<? extends PlanNodeWithHash> keys)
                    {
                        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = new HashMap<>(historyBasedPlanStatisticsProvider.get().getStats(ImmutableList.copyOf(keys)));
                        // loadAll excepts all keys to be written
                        for (PlanNodeWithHash key : keys) {
                            statistics.putIfAbsent(key, HistoricalPlanStatistics.empty());
                        }
                        return ImmutableMap.copyOf(statistics);
                    }
                }));
    }

    public Map<CachingPlanCanonicalInfoProvider.CacheKey, PlanNodeCanonicalInfo> getCanonicalInfoCache(QueryId queryId)
    {
        return canonicalInfoCache.computeIfAbsent(queryId, ignored -> new ConcurrentHashMap());
    }

    public void invalidate(QueryId queryId)
    {
        statisticsCache.remove(queryId);
        canonicalInfoCache.remove(queryId);
    }

    @VisibleForTesting
    public Map<QueryId, Map<CachingPlanCanonicalInfoProvider.CacheKey, PlanNodeCanonicalInfo>> getCanonicalInfoCache()
    {
        return canonicalInfoCache;
    }
}

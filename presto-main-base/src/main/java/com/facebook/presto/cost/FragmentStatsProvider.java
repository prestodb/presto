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
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import io.airlift.units.DataSize;

import java.util.Objects;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * Provides stats for completed plan fragments.
 */
public class FragmentStatsProvider
{
    private static final DataSize CACHE_SIZE = new DataSize(10, DataSize.Unit.MEGABYTE);

    private final Cache<QueryFragmentIdentifier, PlanNodeStatsEstimate> fragmentStatsMap = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE.toBytes())
            .expireAfterWrite(1, DAYS)
            .build();

    @Inject
    public FragmentStatsProvider() {}

    public void putStats(QueryId queryId, PlanFragmentId fragmentId, PlanNodeStatsEstimate planNodeStatsEstimate)
    {
        fragmentStatsMap.put(new QueryFragmentIdentifier(queryId, fragmentId), planNodeStatsEstimate);
    }

    public void invalidateStats(QueryId queryId, int maxFragmentId)
    {
        IntStream.rangeClosed(0, maxFragmentId)
                .forEach(fragmentId -> fragmentStatsMap.invalidate(new QueryFragmentIdentifier(queryId, new PlanFragmentId(fragmentId))));
    }

    public PlanNodeStatsEstimate getStats(QueryId queryId, PlanFragmentId fragmentId)
    {
        PlanNodeStatsEstimate estimate = fragmentStatsMap.getIfPresent(new QueryFragmentIdentifier(queryId, fragmentId));
        return estimate == null ? PlanNodeStatsEstimate.unknown() : estimate;
    }

    public static class QueryFragmentIdentifier
    {
        private final QueryId queryId;
        private final PlanFragmentId planFragmentId;

        public QueryFragmentIdentifier(QueryId queryId, PlanFragmentId planFragmentId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.planFragmentId = requireNonNull(planFragmentId, "planFragmentId is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QueryFragmentIdentifier that = (QueryFragmentIdentifier) o;
            return queryId.equals(that.queryId) && planFragmentId.equals(that.planFragmentId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queryId, planFragmentId);
        }
    }
}

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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * Coordinator-side registry of {@link JoinDynamicFilter} instances across queries.
 * Callers must call {@link #removeFiltersForQuery} when queries complete.
 */
@ThreadSafe
public class DynamicFilterService
{
    private final ConcurrentMap<QueryId, ConcurrentMap<String, JoinDynamicFilter>> filters = new ConcurrentHashMap<>();
    private final ConcurrentMap<QueryId, ConcurrentMap<PlanNodeId, Set<String>>> scanToFilterIds = new ConcurrentHashMap<>();

    private final DynamicFilterStats stats;

    public DynamicFilterService()
    {
        this(new DynamicFilterStats());
    }

    public DynamicFilterService(DynamicFilterStats stats)
    {
        this.stats = requireNonNull(stats, "stats is null");
    }

    public void registerFilter(QueryId queryId, String filterId, JoinDynamicFilter filter)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(filterId, "filterId is null");
        requireNonNull(filter, "filter is null");

        filters.computeIfAbsent(queryId, k -> new ConcurrentHashMap<>())
                .putIfAbsent(filterId, filter);
        stats.getFilterRegistrations().update(1);
    }

    public Optional<JoinDynamicFilter> getFilter(QueryId queryId, String filterId)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(filterId, "filterId is null");

        ConcurrentMap<String, JoinDynamicFilter> queryFilters = filters.get(queryId);
        if (queryFilters == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(queryFilters.get(filterId));
    }

    public void removeFiltersForQuery(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        filters.remove(queryId);
        scanToFilterIds.remove(queryId);
    }

    public void registerScanFilterMapping(QueryId queryId, PlanNodeId scanNodeId, Set<String> filterIds)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(scanNodeId, "scanNodeId is null");
        requireNonNull(filterIds, "filterIds is null");

        scanToFilterIds
                .computeIfAbsent(queryId, k -> new ConcurrentHashMap<>())
                .put(scanNodeId, ImmutableSet.copyOf(filterIds));
    }

    public Set<String> getFilterIdsForScan(QueryId queryId, PlanNodeId scanNodeId)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(scanNodeId, "scanNodeId is null");

        ConcurrentMap<PlanNodeId, Set<String>> queryScanMappings = scanToFilterIds.get(queryId);
        if (queryScanMappings == null) {
            return ImmutableSet.of();
        }
        Set<String> filterIds = queryScanMappings.get(scanNodeId);
        return filterIds != null ? filterIds : ImmutableSet.of();
    }

    public boolean hasFilter(QueryId queryId, String filterId)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(filterId, "filterId is null");

        ConcurrentMap<String, JoinDynamicFilter> queryFilters = filters.get(queryId);
        return queryFilters != null && queryFilters.containsKey(filterId);
    }

    public Map<String, JoinDynamicFilter> getAllFiltersForQuery(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");

        ConcurrentMap<String, JoinDynamicFilter> queryFilters = filters.get(queryId);
        if (queryFilters == null) {
            return ImmutableMap.of();
        }
        return ImmutableMap.copyOf(queryFilters);
    }

    public DynamicFilterStats getStats()
    {
        return stats;
    }
}

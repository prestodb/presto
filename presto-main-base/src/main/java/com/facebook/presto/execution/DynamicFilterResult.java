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
package com.facebook.presto.execution;

import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Wire DTO returned by the coordinator's long-poll endpoint ({@code GET /v1/dynamicFilters}).
 *
 * <p>Each response carries:
 * <ul>
 *   <li>{@code filters} — the resolved {@link com.facebook.presto.common.predicate.TupleDomain}
 *       per filter ID, keyed by filter ID string.</li>
 *   <li>{@code version} — monotonically increasing version used by the fetcher to request
 *       only filters newer than the last seen version.</li>
 *   <li>{@code operatorCompleted} — true when the build-side operator has finished and no
 *       further filter updates will arrive.</li>
 *   <li>{@code completedFilterIds} — the subset of filter IDs that are fully resolved.</li>
 * </ul>
 *
 * <p>Used by {@code DynamicFilterFetcher} (PR4 / #28044) and {@code DynamicFilterPusher}
 * to carry resolved filters from workers to the coordinator and back to the probe side.
 */
public class DynamicFilterResult
{
    private final Map<String, TupleDomain<String>> filters;
    private final long version;
    private final boolean operatorCompleted;
    private final Set<String> completedFilterIds;

    public DynamicFilterResult(Map<String, TupleDomain<String>> filters, long version, boolean operatorCompleted, Set<String> completedFilterIds)
    {
        this.filters = ImmutableMap.copyOf(requireNonNull(filters, "filters is null"));
        this.version = version;
        this.operatorCompleted = operatorCompleted;
        this.completedFilterIds = ImmutableSet.copyOf(requireNonNull(completedFilterIds, "completedFilterIds is null"));
    }

    public Map<String, TupleDomain<String>> getFilters()
    {
        return filters;
    }

    public long getVersion()
    {
        return version;
    }

    public boolean isOperatorCompleted()
    {
        return operatorCompleted;
    }

    public Set<String> getCompletedFilterIds()
    {
        return completedFilterIds;
    }
}

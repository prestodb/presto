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
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DynamicFilterResult
{
    private final Map<String, TupleDomain<String>> filters;
    private final long version;
    private final boolean operatorCompleted;
    private final Set<String> completedFilterIds;

    public DynamicFilterResult(Map<String, TupleDomain<String>> filters, long version, boolean operatorCompleted, Set<String> completedFilterIds)
    {
        this.filters = requireNonNull(filters, "filters is null");
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

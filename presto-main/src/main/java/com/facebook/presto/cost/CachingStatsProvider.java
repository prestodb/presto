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

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public final class CachingStatsProvider
        implements StatsProvider
{
    private final StatsCalculator statsCalculator;
    private final Optional<Memo> memo;
    private final Lookup lookup;
    private final Session session;
    private final TypeProvider types;

    private final Map<PlanNode, PlanNodeStatsEstimate> cache = new IdentityHashMap<>();

    public CachingStatsProvider(StatsCalculator statsCalculator, Session session, TypeProvider types)
    {
        this(statsCalculator, Optional.empty(), noLookup(), session, types);
    }

    public CachingStatsProvider(StatsCalculator statsCalculator, Optional<Memo> memo, Lookup lookup, Session session, TypeProvider types)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode node)
    {
        requireNonNull(node, "node is null");

        if (node instanceof GroupReference) {
            return getGroupStats((GroupReference) node);
        }

        PlanNodeStatsEstimate stats = cache.get(node);
        if (stats != null) {
            return stats;
        }

        stats = statsCalculator.calculateStats(node, this, lookup, session, types);
        verify(cache.put(node, stats) == null, "Stats already set");
        return stats;
    }

    private PlanNodeStatsEstimate getGroupStats(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingStatsProvider without memo cannot handle GroupReferences"));

        Optional<PlanNodeStatsEstimate> stats = memo.getStats(group);
        if (stats.isPresent()) {
            return stats.get();
        }

        PlanNodeStatsEstimate groupStats = statsCalculator.calculateStats(memo.getNode(group), this, lookup, session, types);
        verify(!memo.getStats(group).isPresent(), "Group stats already set");
        memo.storeStats(group, groupStats);
        return groupStats;
    }
}

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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Memo;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isEnableStatsCalculator;
import static com.facebook.presto.SystemSessionProperties.isIgnoreStatsCalculatorFailures;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public final class CachingStatsProvider
        implements StatsProvider
{
    private static final Logger log = Logger.get(CachingStatsProvider.class);

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
        if (!isEnableStatsCalculator(session)) {
            return PlanNodeStatsEstimate.unknown();
        }

        requireNonNull(node, "node is null");

        try {
            if (node instanceof GroupReference) {
                PlanNodeStatsEstimate result = getGroupStats((GroupReference) node);
                session.getPlanNodeStatsMap().put(node.getId(), result);
                return result;
            }

            PlanNodeStatsEstimate stats = cache.get(node);
            if (stats != null) {
                session.getPlanNodeStatsMap().put(node.getId(), stats);
                return stats;
            }

            stats = statsCalculator.calculateStats(node, this, lookup, session, types);
            verify(cache.put(node, stats) == null, "Stats already set");
            session.getPlanNodeStatsMap().put(node.getId(), stats);
            return stats;
        }
        catch (RuntimeException e) {
            if (isIgnoreStatsCalculatorFailures(session)) {
                log.error(e, "Error occurred when computing stats for query %s", session.getQueryId());
                return PlanNodeStatsEstimate.unknown();
            }
            throw e;
        }
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

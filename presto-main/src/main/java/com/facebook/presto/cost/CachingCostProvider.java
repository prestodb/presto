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
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.plan.PlanNode;
import io.airlift.log.Logger;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isEnableStatsCalculator;
import static com.facebook.presto.SystemSessionProperties.isIgnoreStatsCalculatorFailures;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class CachingCostProvider
        implements CostProvider
{
    private static final Logger log = Logger.get(CachingCostProvider.class);

    private final CostCalculator costCalculator;
    private final StatsProvider statsProvider;
    private final Optional<Memo> memo;
    private final Session session;
    private final TypeProvider types;

    private final Map<PlanNode, PlanNodeCostEstimate> cache = new IdentityHashMap<>();

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Session session, TypeProvider types)
    {
        this(costCalculator, statsProvider, Optional.empty(), session, types);
    }

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Optional<Memo> memo, Session session, TypeProvider types)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public PlanNodeCostEstimate getCumulativeCost(PlanNode node)
    {
        if (!isEnableStatsCalculator(session)) {
            return PlanNodeCostEstimate.unknown();
        }

        requireNonNull(node, "node is null");

        try {
            if (node instanceof GroupReference) {
                return getGroupCost((GroupReference) node);
            }

            PlanNodeCostEstimate cumulativeCost = cache.get(node);
            if (cumulativeCost != null) {
                return cumulativeCost;
            }

            cumulativeCost = calculateCumulativeCost(node);
            verify(cache.put(node, cumulativeCost) == null, "Cost already set");
            return cumulativeCost;
        }
        catch (RuntimeException e) {
            if (isIgnoreStatsCalculatorFailures(session)) {
                log.error(e, "Error occurred when computing cost for query %s", session.getQueryId());
                return PlanNodeCostEstimate.unknown();
            }
            throw e;
        }
    }

    private PlanNodeCostEstimate getGroupCost(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingCostProvider without memo cannot handle GroupReferences"));

        Optional<PlanNodeCostEstimate> cost = memo.getCumulativeCost(group);
        if (cost.isPresent()) {
            return cost.get();
        }

        PlanNodeCostEstimate cumulativeCost = calculateCumulativeCost(memo.getNode(group));
        verify(!memo.getCumulativeCost(group).isPresent(), "Group cost already set");
        memo.storeCumulativeCost(group, cumulativeCost);
        return cumulativeCost;
    }

    private PlanNodeCostEstimate calculateCumulativeCost(PlanNode node)
    {
        PlanNodeCostEstimate localCosts = costCalculator.calculateCost(node, statsProvider, session, types);

        PlanNodeCostEstimate sourcesCost = node.getSources().stream()
                .map(this::getCumulativeCost)
                .reduce(PlanNodeCostEstimate.zero(), PlanNodeCostEstimate::add);

        PlanNodeCostEstimate cumulativeCost = localCosts.add(sourcesCost);
        return cumulativeCost;
    }
}

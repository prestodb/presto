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

import static com.facebook.presto.cost.PlanNodeCostEstimate.ZERO_COST;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class CachingCostProvider
        implements CostProvider
{
    private final CostCalculator costCalculator;
    private final StatsProvider statsProvider;
    private final Optional<Memo> memo;
    private final Lookup lookup;
    private final Session session;
    private final TypeProvider types;

    private final Map<PlanNode, PlanNodeCostEstimate> cache = new IdentityHashMap<>();

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Session session, TypeProvider types)
    {
        this(costCalculator, statsProvider, Optional.empty(), noLookup(), session, types);
    }

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Optional<Memo> memo, Lookup lookup, Session session, TypeProvider types)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public PlanNodeCostEstimate getCumulativeCost(PlanNode node)
    {
        requireNonNull(node, "node is null");

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
        PlanNodeCostEstimate localCosts = costCalculator.calculateCost(node, statsProvider, lookup, session, types);

        PlanNodeCostEstimate sourcesCost = node.getSources().stream()
                .map(this::getCumulativeCost)
                .reduce(ZERO_COST, PlanNodeCostEstimate::add);

        PlanNodeCostEstimate cumulativeCost = localCosts.add(sourcesCost);
        return cumulativeCost;
    }
}

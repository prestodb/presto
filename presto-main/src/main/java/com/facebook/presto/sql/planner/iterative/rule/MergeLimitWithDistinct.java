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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.util.Predicates.predicate;
import static com.facebook.presto.util.Types.tryCast;

public class MergeLimitWithDistinct
        implements Rule
{
    private static final Predicate<AggregationNode> NOT_DISTINCT = predicate(MergeLimitWithDistinct::isDistinct).negate();

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        return tryCast(node, LimitNode.class)
                .flatMap(parent -> lookup.resolve(parent.getSource(), AggregationNode.class)
                        .filter(NOT_DISTINCT)
                        .map(child -> new DistinctLimitNode(
                                parent.getId(),
                                lookup.resolve(child.getSource()),
                                parent.getCount(),
                                false,
                                child.getHashSymbol())));
    }

    /**
     * Whether this node corresponds to a DISTINCT operation in SQL
     */
    private static boolean isDistinct(AggregationNode node)
    {
        return !node.getAggregations().isEmpty() ||
                node.getOutputSymbols().size() != node.getGroupingKeys().size() ||
                !node.getOutputSymbols().containsAll(node.getGroupingKeys());
    }
}

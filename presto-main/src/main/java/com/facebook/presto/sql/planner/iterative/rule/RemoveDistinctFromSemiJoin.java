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

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;

/**
 * Remove distinct node from following path:
 * <pre>
 *     SemiJoinNode
 *        - Source
 *        - FilteringSource
 *          - ProjectNode
 *              - AggregationNode
 * </pre>
 */
public class RemoveDistinctFromSemiJoin
    implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (node instanceof SemiJoinNode) {
           PlanNode filteringSource = lookup.resolve(((SemiJoinNode) node).getFilteringSource());
           Optional<PlanNode> result = rewriteUpstream(filteringSource, lookup);
           if (result.isPresent()) {
               return Optional.of(node.replaceChildren(ImmutableList.of(lookup.resolve(((SemiJoinNode) node).getSource()), result.get())));
           }
        }
        return Optional.empty();
    }

    private Optional<PlanNode> rewriteUpstream(PlanNode node, Lookup lookup)
    {
        if (node instanceof AggregationNode) {
            AggregationNode aggregationNode = (AggregationNode) node;
            if (isDistinctNode(aggregationNode)) {
                return Optional.of(lookup.resolve(aggregationNode.getSource()));
            }
        }
        else if (node instanceof ProjectNode) {
            Optional<PlanNode> result = rewriteUpstream(lookup.resolve(((ProjectNode) node).getSource()), lookup);
            if (result.isPresent()) {
                return Optional.of(node.replaceChildren(ImmutableList.of(result.get())));
            }
        }
        return Optional.empty();
    }

    private boolean isDistinctNode(AggregationNode aggregationNode)
    {
        List<Symbol> outputSymbols = aggregationNode.getOutputSymbols();
        List<Symbol> groupingKeys =  aggregationNode.getGroupingKeys();
        return  aggregationNode.getStep() == SINGLE
                && outputSymbols.size() == 1
                && groupingKeys.size() == 1
                && outputSymbols.get(0).equals(groupingKeys.get(0));
    }
}

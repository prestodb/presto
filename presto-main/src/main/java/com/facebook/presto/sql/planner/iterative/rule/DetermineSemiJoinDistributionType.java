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
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.PlanNodeWithCost;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;

public class DetermineSemiJoinDistributionType
        implements Rule
{
    private final CostComparator costComparator;
    private boolean isDeleteQuery;

    public DetermineSemiJoinDistributionType(CostComparator costComparator)
    {
        this.costComparator = costComparator;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Context context)
    {
        if (node instanceof DeleteNode) {
            isDeleteQuery = true;
            return Optional.empty();
        }
        if (!(node instanceof SemiJoinNode)) {
            return Optional.empty();
        }

        SemiJoinNode semiJoinNode = (SemiJoinNode) node;

        if (semiJoinNode.getDistributionType().isPresent()) {
            return Optional.empty();
        }

        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());

        if (joinDistributionType == AUTOMATIC) {
            return chooseCostBasedJoinTypeFor(semiJoinNode, context.getSession(), context.getLookup(), context.getSymbolAllocator(), joinDistributionType);
        }
        else {
            return chooseSyntacticJoinTypeFor(semiJoinNode, joinDistributionType);
        }
    }

    private Optional<PlanNode> chooseCostBasedJoinTypeFor(SemiJoinNode node, Session session, Lookup lookup, SymbolAllocator symbolAllocator, JoinDistributionType joinDistributionType)
    {
        Ordering<PlanNodeWithCost> planNodeOrdering = new Ordering<PlanNodeWithCost>()
        {
            @Override
            public int compare(PlanNodeWithCost node1, PlanNodeWithCost node2)
            {
                return costComparator.compare(session, node1.getCost(), node2.getCost());
            }
        };

        List<PlanNodeWithCost> possibleJoinNodes = new ArrayList<>();

        if (canRepartition(joinDistributionType)) {
            possibleJoinNodes.add(getSemiJoinNodeWithCost(node.withDistributionType(PARTITIONED), lookup, symbolAllocator, session));
        }
        if (joinDistributionType.canReplicate()) {
            possibleJoinNodes.add(getSemiJoinNodeWithCost(node.withDistributionType(REPLICATED), lookup, symbolAllocator, session));
        }

        if (possibleJoinNodes.stream().anyMatch(result -> result.getCost().hasUnknownComponents()) || possibleJoinNodes.isEmpty()) {
            return chooseSyntacticJoinTypeFor(node, joinDistributionType);
        }

        return planNodeOrdering.min(possibleJoinNodes).getPlanNode();
    }

    private PlanNodeWithCost getSemiJoinNodeWithCost(SemiJoinNode node, Lookup lookup, SymbolAllocator symbolAllocator, Session session)
    {
        return new PlanNodeWithCost(lookup.getCumulativeCost(node, session, symbolAllocator.getTypes()), Optional.of(node));
    }

    private Optional<PlanNode> chooseSyntacticJoinTypeFor(SemiJoinNode node, JoinDistributionType joinDistributionType)
    {
        if (canRepartition(joinDistributionType)) {
            return Optional.of(node.withDistributionType(PARTITIONED));
        }
        return Optional.of(node.withDistributionType(REPLICATED));
    }

    private boolean canRepartition(JoinDistributionType joinDistributionType)
    {
        // For delete queries, the TableScan node that corresponds to the table being deleted must be collocated
        // with the Delete node, so you can't do a distributed semi-join
        return joinDistributionType.canRepartition() && !isDeleteQuery;
    }
}

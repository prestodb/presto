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
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;

public class DetermineJoinDistributionType
        implements Rule
{
    private final CostComparator costComparator;

    public DetermineJoinDistributionType(CostComparator costComparator)
    {
        this.costComparator = costComparator;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Context context)
    {
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode joinNode = (JoinNode) node;

        if (joinNode.getDistributionType().isPresent()) {
            return Optional.empty();
        }

        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());

        if (joinDistributionType == AUTOMATIC) {
            return getCostBasedJoin(joinNode, context.getLookup(), context.getSymbolAllocator(), context.getSession(), joinDistributionType);
        }
        else {
            return getSyntacticOrderJoin(joinNode, context.getLookup(), joinDistributionType);
        }
    }

    private Optional<PlanNode> getCostBasedJoin(JoinNode joinNode, Lookup lookup, SymbolAllocator symbolAllocator, Session session, JoinDistributionType joinDistributionType)
    {
        Ordering<PlanNodeWithCost> planNodeOrdering = new Ordering<PlanNodeWithCost>() {
            @Override
            public int compare(PlanNodeWithCost node1, PlanNodeWithCost node2)
            {
                return costComparator.compare(session, node1.getCost(), node2.getCost());
            }
        };

        List<PlanNodeWithCost> possibleJoinNodes = new ArrayList<>();

        JoinNode.Type type = joinNode.getType();
        if (canRepartition(joinNode, joinDistributionType, lookup, type)) {
            JoinNode possibleJoinNode = joinNode.withDistributionType(PARTITIONED);
            possibleJoinNodes.add(getJoinNodeWithCost(possibleJoinNode, lookup, symbolAllocator, session));
            possibleJoinNodes.add(
                    getJoinNodeWithCost(possibleJoinNode.flipChildren().withDistributionType(PARTITIONED),
                            lookup, symbolAllocator, session));
        }

        if (type != FULL && joinDistributionType.canReplicate()) {
            // RIGHT OUTER JOIN only works with hash partitioned data.
            if (type != RIGHT) {
                possibleJoinNodes.add(getJoinNodeWithCost(joinNode.withDistributionType(REPLICATED),
                        lookup, symbolAllocator, session));
            }

            // Don't flip LEFT OUTER JOIN, as RIGHT OUTER JOIN only works with hash partitioned data.
            if (type != LEFT) {
                possibleJoinNodes.add(getJoinNodeWithCost(joinNode.flipChildren().withDistributionType(REPLICATED),
                        lookup, symbolAllocator, session));
            }
        }

        if (possibleJoinNodes.stream().anyMatch(result -> result.getCost().hasUnknownComponents()) || possibleJoinNodes.isEmpty()) {
            return getSyntacticOrderJoin(joinNode, lookup, joinDistributionType);
        }

        return planNodeOrdering.min(possibleJoinNodes).getPlanNode();
    }

    private static boolean canRepartition(JoinNode joinNode, JoinDistributionType joinDistributionType, Lookup lookup, JoinNode.Type type)
    {
        // The implementation of full outer join only works if the data is hash partitioned. See LookupJoinOperators#buildSideOuterJoinUnvisitedPositions
        return type == RIGHT || type == FULL || (joinDistributionType.canRepartition() && !mustBroadcastJoin(joinNode, lookup));
    }

    private static boolean mustBroadcastJoin(JoinNode node, Lookup lookup)
    {
        return isAtMostScalar(node.getRight(), lookup) || node.isCrossJoin();
    }

    private Optional<PlanNode> getSyntacticOrderJoin(JoinNode joinNode, Lookup lookup, JoinDistributionType joinDistributionType)
    {
        if (canRepartition(joinNode, joinDistributionType, lookup, joinNode.getType())) {
            return Optional.of(joinNode.withDistributionType(PARTITIONED));
        }
        return Optional.of(joinNode.withDistributionType(REPLICATED));
    }

    private PlanNodeWithCost getJoinNodeWithCost(JoinNode possibleJoinNode, Lookup lookup, SymbolAllocator symbolAllocator, Session session)
    {
        return new PlanNodeWithCost(lookup.getCumulativeCost(possibleJoinNode, session, symbolAllocator.getTypes()), Optional.of(possibleJoinNode));
    }
}

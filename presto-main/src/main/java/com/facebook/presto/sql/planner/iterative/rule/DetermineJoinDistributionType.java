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

import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

public class DetermineJoinDistributionType
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> !joinNode.getDistributionType().isPresent());

    private final CostComparator costComparator;

    public DetermineJoinDistributionType(CostComparator costComparator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());
        if (joinDistributionType == AUTOMATIC) {
            return Result.ofPlanNode(getCostBasedJoin(joinNode, context));
        }
        return Result.ofPlanNode(getSyntacticOrderJoin(joinNode, context, joinDistributionType));
    }

    public static boolean canReplicate(JoinNode joinNode, Context context)
    {
        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());
        if (!joinDistributionType.canReplicate()) {
            return false;
        }

        Optional<DataSize> joinMaxBroadcastTableSize = getJoinMaxBroadcastTableSize(context.getSession());
        if (!joinMaxBroadcastTableSize.isPresent()) {
            return true;
        }

        PlanNode buildSide = joinNode.getRight();
        PlanNodeStatsEstimate buildSideStatsEstimate = context.getStatsProvider().getStats(buildSide);
        double buildSideSizeInBytes = buildSideStatsEstimate.getOutputSizeInBytes(buildSide.getOutputSymbols(), context.getSymbolAllocator().getTypes());
        return buildSideSizeInBytes <= joinMaxBroadcastTableSize.get().toBytes();
    }

    private PlanNode getCostBasedJoin(JoinNode joinNode, Context context)
    {
        CostProvider costProvider = context.getCostProvider();
        List<PlanNodeWithCost> possibleJoinNodes = new ArrayList<>();

        if (!mustPartition(joinNode) && canReplicate(joinNode, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(costProvider, joinNode.withDistributionType(REPLICATED)));
        }
        if (!mustReplicate(joinNode, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(costProvider, joinNode.withDistributionType(PARTITIONED)));
        }

        JoinNode flipped = joinNode.flipChildren();
        if (!mustPartition(flipped) && canReplicate(flipped, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(costProvider, flipped.withDistributionType(REPLICATED)));
        }
        if (!mustReplicate(flipped, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(costProvider, flipped.withDistributionType(PARTITIONED)));
        }

        if (possibleJoinNodes.stream().anyMatch(result -> result.getCost().hasUnknownComponents()) || possibleJoinNodes.isEmpty()) {
            return getSyntacticOrderJoin(joinNode, context, AUTOMATIC);
        }

        // Using Ordering to facilitate rule determinism
        Ordering<PlanNodeWithCost> planNodeOrderings = costComparator.forSession(context.getSession()).onResultOf(PlanNodeWithCost::getCost);
        return planNodeOrderings.min(possibleJoinNodes).getPlanNode();
    }

    private PlanNode getSyntacticOrderJoin(JoinNode joinNode, Context context, JoinDistributionType joinDistributionType)
    {
        if (mustPartition(joinNode)) {
            return joinNode.withDistributionType(PARTITIONED);
        }
        if (mustReplicate(joinNode, context)) {
            return joinNode.withDistributionType(REPLICATED);
        }
        if (joinDistributionType.canPartition()) {
            return joinNode.withDistributionType(PARTITIONED);
        }
        return joinNode.withDistributionType(REPLICATED);
    }

    private static boolean mustPartition(JoinNode joinNode)
    {
        JoinNode.Type type = joinNode.getType();
        // With REPLICATED, the unmatched rows from right-side would be duplicated.
        return type == RIGHT || type == FULL;
    }

    private static boolean mustReplicate(JoinNode joinNode, Context context)
    {
        JoinNode.Type type = joinNode.getType();
        if (joinNode.getCriteria().isEmpty() && (type == INNER || type == LEFT)) {
            // There is nothing to partition on
            return true;
        }
        return isAtMostScalar(joinNode.getRight(), context.getLookup());
    }

    private static PlanNodeWithCost getJoinNodeWithCost(CostProvider costProvider, JoinNode possibleJoinNode)
    {
        return new PlanNodeWithCost(costProvider.getCumulativeCost(possibleJoinNode), possibleJoinNode);
    }

    private static class PlanNodeWithCost
    {
        private final PlanNode planNode;
        private final PlanNodeCostEstimate cost;

        public PlanNodeWithCost(PlanNodeCostEstimate cost, PlanNode planNode)
        {
            this.cost = requireNonNull(cost, "cost is null");
            this.planNode = requireNonNull(planNode, "planNode is null");
        }

        public PlanNode getPlanNode()
        {
            return planNode;
        }

        public PlanNodeCostEstimate getCost()
        {
            return cost;
        }
    }
}

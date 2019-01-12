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

package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.PlanNodeCostEstimate;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsProvider;
import io.prestosql.cost.TaskCountEstimator;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.getJoinDistributionType;
import static io.prestosql.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateJoinExchangeCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateJoinInputCost;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

public class DetermineJoinDistributionType
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> !joinNode.getDistributionType().isPresent());

    private final CostComparator costComparator;
    private final TaskCountEstimator taskCountEstimator;

    public DetermineJoinDistributionType(CostComparator costComparator, TaskCountEstimator taskCountEstimator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "exchangeCostCalculator is null");
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
        List<PlanNodeWithCost> possibleJoinNodes = new ArrayList<>();

        addJoinsWithDifferentDistributions(joinNode, possibleJoinNodes, context);
        addJoinsWithDifferentDistributions(joinNode.flipChildren(), possibleJoinNodes, context);

        if (possibleJoinNodes.stream().anyMatch(result -> result.getCost().hasUnknownComponents()) || possibleJoinNodes.isEmpty()) {
            return getSyntacticOrderJoin(joinNode, context, AUTOMATIC);
        }

        // Using Ordering to facilitate rule determinism
        Ordering<PlanNodeWithCost> planNodeOrderings = costComparator.forSession(context.getSession()).onResultOf(PlanNodeWithCost::getCost);
        return planNodeOrderings.min(possibleJoinNodes).getPlanNode();
    }

    private void addJoinsWithDifferentDistributions(JoinNode joinNode, List<PlanNodeWithCost> possibleJoinNodes, Context context)
    {
        if (!mustPartition(joinNode) && canReplicate(joinNode, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(context, joinNode.withDistributionType(REPLICATED)));
        }
        if (!mustReplicate(joinNode, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(context, joinNode.withDistributionType(PARTITIONED)));
        }
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

    private PlanNodeWithCost getJoinNodeWithCost(Context context, JoinNode possibleJoinNode)
    {
        TypeProvider types = context.getSymbolAllocator().getTypes();
        StatsProvider stats = context.getStatsProvider();
        boolean replicated = possibleJoinNode.getDistributionType().get().equals(REPLICATED);
        /*
         *   HACK!
         *
         *   Currently cost model always has to compute the total cost of an operation.
         *   For JOIN the total cost consist of 4 parts:
         *     - Cost of exchanges that have to be introduced to execute a JOIN
         *     - Cost of building a hash table
         *     - Cost of probing a hash table
         *     - Cost of building an output for matched rows
         *
         *   When output size for a JOIN cannot be estimated the cost model returns
         *   UNKNOWN cost for the join.
         *
         *   However assuming the cost of JOIN output is always the same, we can still make
         *   cost based decisions based on the input cost for different types of JOINs.
         *
         *   Although the side flipping can be made purely based on stats (smaller side
         *   always goes to the right), determining JOIN type is not that simple. As when
         *   choosing REPLICATED over REPARTITIONED join the cost of exchanging and building
         *   the hash table scales with the number of nodes where the build side is replicated.
         */
        int estimatedSourceDistributedTaskCount = taskCountEstimator.estimateSourceDistributedTaskCount();
        PlanNodeCostEstimate exchangesCost = calculateJoinExchangeCost(
                possibleJoinNode.getLeft(),
                possibleJoinNode.getRight(),
                stats,
                types,
                replicated,
                estimatedSourceDistributedTaskCount);
        PlanNodeCostEstimate inputCost = calculateJoinInputCost(
                possibleJoinNode.getLeft(),
                possibleJoinNode.getRight(),
                stats,
                types,
                replicated,
                estimatedSourceDistributedTaskCount);
        return new PlanNodeWithCost(exchangesCost.add(inputCost), possibleJoinNode);
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

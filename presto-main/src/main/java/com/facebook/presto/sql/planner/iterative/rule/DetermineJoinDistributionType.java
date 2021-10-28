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
import com.facebook.presto.cost.LocalCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
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
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
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

    public static boolean isBelowMaxBroadcastSize(JoinNode joinNode, Context context)
    {
        DataSize joinMaxBroadcastTableSize = getJoinMaxBroadcastTableSize(context.getSession());

        PlanNode buildSide = joinNode.getRight();
        PlanNodeStatsEstimate buildSideStatsEstimate = context.getStatsProvider().getStats(buildSide);
        double buildSideSizeInBytes = buildSideStatsEstimate.getOutputSizeInBytes(buildSide.getOutputVariables());
        return buildSideSizeInBytes <= joinMaxBroadcastTableSize.toBytes();
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
        if (!mustPartition(joinNode) && isBelowMaxBroadcastSize(joinNode, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(context, joinNode.withDistributionType(REPLICATED)));
        }
        // don't consider partitioned inequality joins because they execute on a single node.
        if (!mustReplicate(joinNode, context) && !joinNode.getCriteria().isEmpty()) {
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

    private boolean mustPartition(JoinNode joinNode)
    {
        return joinNode.getType().mustPartition();
    }

    private static boolean mustReplicate(JoinNode joinNode, Context context)
    {
        if (joinNode.getType().mustReplicate(joinNode.getCriteria())) {
            return true;
        }
        return isAtMostScalar(joinNode.getRight(), context.getLookup());
    }

    private PlanNodeWithCost getJoinNodeWithCost(Context context, JoinNode possibleJoinNode)
    {
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
         *
         *   TODO Decision about the distribution should be based on LocalCostEstimate only when PlanCostEstimate cannot be calculated. Otherwise cost comparator cannot take query.max-memory into account.
         */
        int estimatedSourceDistributedTaskCount = taskCountEstimator.estimateSourceDistributedTaskCount();
        LocalCostEstimate cost = calculateJoinCostWithoutOutput(
                possibleJoinNode.getLeft(),
                possibleJoinNode.getRight(),
                stats,
                replicated,
                estimatedSourceDistributedTaskCount);
        return new PlanNodeWithCost(cost.toPlanCost(), possibleJoinNode);
    }
}

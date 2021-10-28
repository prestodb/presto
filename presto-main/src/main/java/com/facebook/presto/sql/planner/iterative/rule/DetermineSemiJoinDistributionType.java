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
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;
import static java.util.Objects.requireNonNull;

/**
 * This rule must run after the distribution type has already been set for delete queries,
 * since semi joins in delete queries must be replicated.
 * Once we have better pattern matching, we can fold that optimizer into this one.
 */
public class DetermineSemiJoinDistributionType
        implements Rule<SemiJoinNode>
{
    private final TaskCountEstimator taskCountEstimator;
    private final CostComparator costComparator;

    private static final Pattern<SemiJoinNode> PATTERN = semiJoin().matching(semiJoin -> !semiJoin.getDistributionType().isPresent());

    public DetermineSemiJoinDistributionType(CostComparator costComparator, TaskCountEstimator taskCountEstimator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());
        switch (joinDistributionType) {
            case AUTOMATIC:
                return Result.ofPlanNode(getCostBasedDistributionType(semiJoinNode, context));
            case PARTITIONED:
                return Result.ofPlanNode(semiJoinNode.withDistributionType(PARTITIONED));
            case BROADCAST:
                return Result.ofPlanNode(semiJoinNode.withDistributionType(REPLICATED));
            default:
                throw new IllegalArgumentException("Unknown join_distribution_type: " + joinDistributionType);
        }
    }

    private PlanNode getCostBasedDistributionType(SemiJoinNode node, Context context)
    {
        if (!canReplicate(node, context)) {
            return node.withDistributionType(PARTITIONED);
        }

        List<PlanNodeWithCost> possibleJoinNodes = new ArrayList<>();
        possibleJoinNodes.add(getSemiJoinNodeWithCost(node.withDistributionType(REPLICATED), context));
        possibleJoinNodes.add(getSemiJoinNodeWithCost(node.withDistributionType(PARTITIONED), context));

        if (possibleJoinNodes.stream().anyMatch(result -> result.getCost().hasUnknownComponents())) {
            return node.withDistributionType(PARTITIONED);
        }

        // Using Ordering to facilitate rule determinism
        Ordering<PlanNodeWithCost> planNodeOrderings = costComparator.forSession(context.getSession()).onResultOf(PlanNodeWithCost::getCost);
        return planNodeOrderings.min(possibleJoinNodes).getPlanNode();
    }

    private boolean canReplicate(SemiJoinNode node, Context context)
    {
        DataSize joinMaxBroadcastTableSize = getJoinMaxBroadcastTableSize(context.getSession());

        PlanNode buildSide = node.getFilteringSource();
        PlanNodeStatsEstimate buildSideStatsEstimate = context.getStatsProvider().getStats(buildSide);
        double buildSideSizeInBytes = buildSideStatsEstimate.getOutputSizeInBytes(buildSide.getOutputVariables());
        return buildSideSizeInBytes <= joinMaxBroadcastTableSize.toBytes();
    }

    private PlanNodeWithCost getSemiJoinNodeWithCost(SemiJoinNode possibleJoinNode, Context context)
    {
        StatsProvider stats = context.getStatsProvider();
        boolean replicated = possibleJoinNode.getDistributionType().get().equals(REPLICATED);
        /*
         *   HACK!
         *
         *   Currently cost model always has to compute the total cost of an operation.
         *   For SEMI-JOIN the total cost consist of 4 parts:
         *     - Cost of exchanges that have to be introduced to execute a JOIN
         *     - Cost of building a hash table
         *     - Cost of probing a hash table
         *     - Cost of building an output for matched rows
         *
         *   When output size for a SEMI-JOIN cannot be estimated the cost model returns
         *   UNKNOWN cost for the join.
         *
         *   However assuming the cost of SEMI-JOIN output is always the same, we can still make
         *   cost based decisions based on the input cost for different types of SEMI-JOINs.
         *
         *   TODO Decision about the distribution should be based on LocalCostEstimate only when PlanCostEstimate cannot be calculated. Otherwise cost comparator cannot take query.max-memory into account.
         */

        int estimatedSourceDistributedTaskCount = taskCountEstimator.estimateSourceDistributedTaskCount();
        LocalCostEstimate cost = calculateJoinCostWithoutOutput(
                possibleJoinNode.getSource(),
                possibleJoinNode.getFilteringSource(),
                stats,
                replicated,
                estimatedSourceDistributedTaskCount);
        return new PlanNodeWithCost(cost.toPlanCost(), possibleJoinNode);
    }
}

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
import com.facebook.presto.cost.LocalCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.statistics.HistoryBasedSourceInfo;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.confidenceBasedBroadcastEnabled;
import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static com.facebook.presto.SystemSessionProperties.isSizeBasedJoinDistributionTypeEnabled;
import static com.facebook.presto.SystemSessionProperties.isUseBroadcastJoinWhenBuildSizeSmallProbeSizeUnknownEnabled;
import static com.facebook.presto.SystemSessionProperties.treatLowConfidenceZeroEstimationAsUnknownEnabled;
import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.LOW;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.planner.iterative.ConfidenceBasedBroadcastUtil.confidenceBasedBroadcast;
import static com.facebook.presto.sql.planner.iterative.ConfidenceBasedBroadcastUtil.treatLowConfidenceZeroEstimationsAsUnknown;
import static com.facebook.presto.sql.planner.iterative.rule.JoinSwappingUtils.isBelowBroadcastLimit;
import static com.facebook.presto.sql.planner.iterative.rule.JoinSwappingUtils.isSmallerThanThreshold;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Double.NaN;
import static java.util.Objects.requireNonNull;

public class DetermineJoinDistributionType
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> !joinNode.getDistributionType().isPresent());

    private final CostComparator costComparator;
    private final TaskCountEstimator taskCountEstimator;

    // records whether distribution decision was cost-based
    private String statsSource;

    public DetermineJoinDistributionType(CostComparator costComparator, TaskCountEstimator taskCountEstimator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public boolean isCostBased(Session session)
    {
        return getJoinDistributionType(session) == AUTOMATIC;
    }

    @Override
    public String getStatsSource()
    {
        return statsSource;
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
            PlanNode resultNode = getCostBasedJoin(joinNode, context);
            statsSource = context.getStatsProvider().getStats(joinNode).getSourceInfo().getSourceInfoName();
            return Result.ofPlanNode(resultNode);
        }
        return Result.ofPlanNode(getSyntacticOrderJoin(joinNode, context, joinDistributionType));
    }

    public static boolean isBelowMaxBroadcastSize(JoinNode joinNode, Context context)
    {
        DataSize joinMaxBroadcastTableSize = getJoinMaxBroadcastTableSize(context.getSession());

        PlanNode buildSide = joinNode.getRight();
        PlanNodeStatsEstimate buildSideStatsEstimate = context.getStatsProvider().getStats(buildSide);

        if (treatLowConfidenceZeroEstimationAsUnknownEnabled(context.getSession()) && isLowConfidenceZero(buildSide, context)) {
            return false;
        }

        double buildSideSizeInBytes = buildSideStatsEstimate.getOutputSizeInBytes(buildSide);
        return buildSideSizeInBytes <= joinMaxBroadcastTableSize.toBytes()
                || (isSizeBasedJoinDistributionTypeEnabled(context.getSession())
                && getSourceTablesSizeInBytes(buildSide, context) <= joinMaxBroadcastTableSize.toBytes());
    }

    private PlanNode getCostBasedJoin(JoinNode joinNode, Context context)
    {
        List<PlanNodeWithCost> possibleJoinNodes = new ArrayList<>();

        addJoinsWithDifferentDistributions(joinNode, possibleJoinNodes, context);
        addJoinsWithDifferentDistributions(joinNode.flipChildren(), possibleJoinNodes, context);

        if (isBelowMaxBroadcastSize(joinNode, context) && isBelowMaxBroadcastSize(joinNode.flipChildren(), context) && !mustPartition(joinNode) && confidenceBasedBroadcastEnabled(context.getSession())) {
            Optional<JoinNode> result = confidenceBasedBroadcast(joinNode, context);
            if (result.isPresent()) {
                return result.get();
            }
        }

        boolean buildSideLowConfidenceZero = isLowConfidenceZero(joinNode.getRight(), context);
        boolean probeSideLowConfidenceZero = isLowConfidenceZero(joinNode.getLeft(), context);
        if ((buildSideLowConfidenceZero || probeSideLowConfidenceZero) && treatLowConfidenceZeroEstimationAsUnknownEnabled(context.getSession())) {
            Optional<JoinNode> result = treatLowConfidenceZeroEstimationsAsUnknown(probeSideLowConfidenceZero, buildSideLowConfidenceZero, joinNode, context);
            if (result.isPresent()) {
                return result.get();
            }
        }

        if (possibleJoinNodes.stream().anyMatch(result -> result.getCost().hasUnknownComponents()) || possibleJoinNodes.isEmpty()) {
            // TODO: currently this session parameter is added so as to roll out the plan change gradually, after proved to be a better choice, make it default and get rid of the session parameter here.
            if (isUseBroadcastJoinWhenBuildSizeSmallProbeSizeUnknownEnabled(context.getSession()) && possibleJoinNodes.stream().anyMatch(result -> ((JoinNode) result.getPlanNode()).getDistributionType().get().equals(REPLICATED))) {
                JoinNode broadcastJoin = (JoinNode) getOnlyElement(possibleJoinNodes.stream().filter(result -> ((JoinNode) result.getPlanNode()).getDistributionType().get().equals(REPLICATED)).map(x -> x.getPlanNode()).collect(toImmutableList()));
                if (context.getStatsProvider().getStats(broadcastJoin.getBuild()).getSourceInfo() instanceof HistoryBasedSourceInfo) {
                    return broadcastJoin;
                }
            }
            if (isSizeBasedJoinDistributionTypeEnabled(context.getSession())) {
                return getSizeBasedJoin(joinNode, context);
            }
            return getSyntacticOrderJoin(joinNode, context, AUTOMATIC);
        }

        // Using Ordering to facilitate rule determinism
        Ordering<PlanNodeWithCost> planNodeOrderings = costComparator.forSession(context.getSession()).onResultOf(PlanNodeWithCost::getCost);
        return planNodeOrderings.min(possibleJoinNodes).getPlanNode();
    }

    private JoinNode getSizeBasedJoin(JoinNode joinNode, Context context)
    {
        boolean isRightSideSmall = isBelowBroadcastLimit(joinNode.getRight(), context);
        if (isRightSideSmall && !mustPartition(joinNode)) {
            // choose right join side with small source tables as replicated build side
            return joinNode.withDistributionType(REPLICATED);
        }

        boolean isLeftSideSmall = isBelowBroadcastLimit(joinNode.getLeft(), context);
        JoinNode flippedJoin = joinNode.flipChildren();
        if (isLeftSideSmall && !mustPartition(flippedJoin)) {
            // choose join left side with small source tables as replicated build side
            return flippedJoin.withDistributionType(REPLICATED);
        }

        if (isRightSideSmall) {
            // right side is small enough, but must be partitioned
            return joinNode.withDistributionType(PARTITIONED);
        }

        if (isLeftSideSmall) {
            // left side is small enough, but must be partitioned
            return flippedJoin.withDistributionType(PARTITIONED);
        }

        // Flip join sides if one side is smaller than the other by more than SIZE_DIFFERENCE_THRESHOLD times.
        // We use 8x factor because getFirstKnownOutputSizeInBytes may not have accounted for the reduction in the size of
        // the output from a filter or aggregation due to lack of estimates.
        // We use getFirstKnownOutputSizeInBytes instead of getSourceTablesSizeInBytes to account for the reduction in
        // output size from the operators between the join and the table scan as much as possible when comparing the sizes of the join sides.

        // All the REPLICATED cases were handled in the code above, so now we only consider PARTITIONED cases here
        if (isSmallerThanThreshold(joinNode.getRight(), joinNode.getLeft(), context) && !mustReplicate(joinNode, context)) {
            return joinNode.withDistributionType(PARTITIONED);
        }

        if (isSmallerThanThreshold(joinNode.getLeft(), joinNode.getRight(), context) && !mustReplicate(flippedJoin, context)) {
            return flippedJoin.withDistributionType(PARTITIONED);
        }

        // neither side is small enough, choose syntactic join order
        return getSyntacticOrderJoin(joinNode, context, AUTOMATIC);
    }

    public static double getSourceTablesSizeInBytes(PlanNode node, Context context)
    {
        return getSourceTablesSizeInBytes(node, context.getLookup(), context.getStatsProvider());
    }

    @VisibleForTesting
    static double getSourceTablesSizeInBytes(PlanNode node, Lookup lookup, StatsProvider statsProvider)
    {
        boolean hasExpandingNodes = PlanNodeSearcher.searchFrom(node, lookup)
                .whereIsInstanceOfAny(JoinSwappingUtils.EXPANDING_NODE_CLASSES)
                .matches();
        if (hasExpandingNodes) {
            return NaN;
        }

        List<PlanNode> sourceNodes = PlanNodeSearcher.searchFrom(node, lookup)
                .whereIsInstanceOfAny(ImmutableList.of(TableScanNode.class, ValuesNode.class, RemoteSourceNode.class, CteConsumerNode.class))
                .findAll();

        return sourceNodes.stream()
                .mapToDouble(sourceNode -> statsProvider.getStats(sourceNode).getOutputSizeInBytes(sourceNode))
                .sum();
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

    private JoinNode getSyntacticOrderJoin(JoinNode joinNode, Context context, JoinDistributionType joinDistributionType)
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

    public static boolean mustPartition(JoinNode joinNode)
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

    private static boolean isLowConfidenceZero(PlanNode planNode, Context context)
    {
        PlanNodeStatsEstimate statsEstimate = context.getStatsProvider().getStats(planNode);
        return statsEstimate.confidenceLevel() == LOW && statsEstimate.getOutputRowCount() == 0;
    }
}

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
import com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.UnionNode;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.cost.PlanCostEstimate.cpuCost;
import static com.facebook.presto.cost.PlanCostEstimate.networkCost;
import static java.util.Objects.requireNonNull;

/**
 * A wrapper around CostCalculator that estimates ExchangeNodes cost.
 * <p>
 * Certain rules (e.g. {@link ReorderJoins} and {@link DetermineJoinDistributionType}) are run before exchanges
 * are added to a plan. This cost calculator adds the implied costs for the exchanges that will be added later.
 * It is needed to account for the differences in exchange costs for different types of joins.
 */
@ThreadSafe
public class CostCalculatorWithEstimatedExchanges
        implements CostCalculator
{
    private final CostCalculator costCalculator;
    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public CostCalculatorWithEstimatedExchanges(CostCalculator costCalculator, TaskCountEstimator taskCountEstimator)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public PlanCostEstimate calculateCost(PlanNode node, StatsProvider stats, Session session, TypeProvider types)
    {
        ExchangeCostEstimator exchangeCostEstimator = new ExchangeCostEstimator(stats, types, taskCountEstimator);
        PlanCostEstimate estimatedExchangeCost = node.accept(exchangeCostEstimator, null);
        return costCalculator.calculateCost(node, stats, session, types).add(estimatedExchangeCost);
    }

    private static class ExchangeCostEstimator
            extends PlanVisitor<PlanCostEstimate, Void>
    {
        private final StatsProvider stats;
        private final TypeProvider types;
        private final TaskCountEstimator taskCountEstimator;

        ExchangeCostEstimator(StatsProvider stats, TypeProvider types, TaskCountEstimator taskCountEstimator)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.types = requireNonNull(types, "types is null");
            this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        }

        @Override
        protected PlanCostEstimate visitPlan(PlanNode node, Void context)
        {
            // TODO implement logic for other node types and return PlanCostEstimate.unknown() here (or throw)
            return PlanCostEstimate.zero();
        }

        @Override
        public PlanCostEstimate visitGroupReference(GroupReference node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource();
            double inputSizeInBytes = getStats(source).getOutputSizeInBytes(source.getOutputSymbols(), types);

            PlanCostEstimate remoteRepartitionCost = calculateRemoteRepartitionCost(inputSizeInBytes);
            PlanCostEstimate localRepartitionCost = calculateLocalRepartitionCost(inputSizeInBytes);

            // TODO consider cost of aggregation itself, not only exchanges, based on aggregation's properties
            return remoteRepartitionCost.add(localRepartitionCost);
        }

        @Override
        public PlanCostEstimate visitJoin(JoinNode node, Void context)
        {
            return calculateJoinExchangeCost(
                    node.getLeft(),
                    node.getRight(),
                    stats,
                    types,
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)),
                    taskCountEstimator.estimateSourceDistributedTaskCount());
        }

        @Override
        public PlanCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return calculateJoinExchangeCost(
                    node.getSource(),
                    node.getFilteringSource(),
                    stats,
                    types,
                    Objects.equals(node.getDistributionType(), Optional.of(SemiJoinNode.DistributionType.REPLICATED)),
                    taskCountEstimator.estimateSourceDistributedTaskCount());
        }

        @Override
        public PlanCostEstimate visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            return calculateJoinExchangeCost(
                    node.getLeft(),
                    node.getRight(),
                    stats,
                    types,
                    node.getDistributionType() == SpatialJoinNode.DistributionType.REPLICATED,
                    taskCountEstimator.estimateSourceDistributedTaskCount());
        }

        @Override
        public PlanCostEstimate visitUnion(UnionNode node, Void context)
        {
            // this assumes that all union inputs will be gathered over the network
            // that is not aways true
            // but this estimate is better that returning UNKNOWN, as it sets
            // cumulative cost to unknown
            double inputSizeInBytes = getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types);
            return calculateRemoteGatherCost(inputSizeInBytes);
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }
    }

    public static PlanCostEstimate calculateRemoteGatherCost(double inputSizeInBytes)
    {
        return networkCost(inputSizeInBytes);
    }

    public static PlanCostEstimate calculateRemoteRepartitionCost(double inputSizeInBytes)
    {
        return new PlanCostEstimate(inputSizeInBytes, 0, inputSizeInBytes);
    }

    public static PlanCostEstimate calculateLocalRepartitionCost(double inputSizeInBytes)
    {
        return cpuCost(inputSizeInBytes);
    }

    public static PlanCostEstimate calculateRemoteReplicateCost(double inputSizeInBytes, int destinationTaskCount)
    {
        return networkCost(inputSizeInBytes * destinationTaskCount);
    }

    public static PlanCostEstimate calculateJoinCostWithoutOutput(
            PlanNode probe,
            PlanNode build,
            StatsProvider stats,
            TypeProvider types,
            boolean replicated,
            int estimatedSourceDistributedTaskCount)
    {
        PlanCostEstimate exchangesCost = calculateJoinExchangeCost(
                probe,
                build,
                stats,
                types,
                replicated,
                estimatedSourceDistributedTaskCount);
        PlanCostEstimate inputCost = calculateJoinInputCost(
                probe,
                build,
                stats,
                types,
                replicated,
                estimatedSourceDistributedTaskCount);
        return exchangesCost.add(inputCost);
    }

    private static PlanCostEstimate calculateJoinExchangeCost(
            PlanNode probe,
            PlanNode build,
            StatsProvider stats,
            TypeProvider types,
            boolean replicated,
            int estimatedSourceDistributedTaskCount)
    {
        double probeSizeInBytes = stats.getStats(probe).getOutputSizeInBytes(probe.getOutputSymbols(), types);
        double buildSizeInBytes = stats.getStats(build).getOutputSizeInBytes(build.getOutputSymbols(), types);
        if (replicated) {
            // assuming the probe side of a replicated join is always source distributed
            PlanCostEstimate replicateCost = calculateRemoteReplicateCost(buildSizeInBytes, estimatedSourceDistributedTaskCount);
            // cost of the copies repartitioning is added in CostCalculatorUsingExchanges#calculateJoinCost
            PlanCostEstimate localRepartitionCost = calculateLocalRepartitionCost(buildSizeInBytes);
            return replicateCost.add(localRepartitionCost);
        }
        else {
            PlanCostEstimate probeCost = calculateRemoteRepartitionCost(probeSizeInBytes);
            PlanCostEstimate buildRemoteRepartitionCost = calculateRemoteRepartitionCost(buildSizeInBytes);
            PlanCostEstimate buildLocalRepartitionCost = calculateLocalRepartitionCost(buildSizeInBytes);
            return probeCost
                    .add(buildRemoteRepartitionCost)
                    .add(buildLocalRepartitionCost);
        }
    }

    public static PlanCostEstimate calculateJoinInputCost(
            PlanNode probe,
            PlanNode build,
            StatsProvider stats,
            TypeProvider types,
            boolean replicated,
            int estimatedSourceDistributedTaskCount)
    {
        int buildSizeMultiplier = replicated ? estimatedSourceDistributedTaskCount : 1;

        PlanNodeStatsEstimate probeStats = stats.getStats(probe);
        PlanNodeStatsEstimate buildStats = stats.getStats(build);

        double buildSideSize = buildStats.getOutputSizeInBytes(build.getOutputSymbols(), types);
        double probeSideSize = probeStats.getOutputSizeInBytes(probe.getOutputSymbols(), types);

        double cpuCost = probeSideSize + buildSideSize * buildSizeMultiplier;

        /*
         * HACK!
         *
         * Stats model doesn't multiply the number of rows by the number of tasks for replicated
         * exchange to avoid misestimation of the JOIN output.
         *
         * Thus the cost estimation for the operations that come after a replicated exchange is
         * underestimated. And the cost of operations over the replicated copies must be explicitly
         * added here.
         */
        if (replicated) {
            // add the cost of a local repartitioning of build side copies
            // cost of the repartitioning of a single data copy has been already added in calculateExchangeCost
            cpuCost += buildSideSize * (buildSizeMultiplier - 1);
        }

        double memoryCost = buildSideSize * buildSizeMultiplier;

        return new PlanCostEstimate(cpuCost, memoryCost, 0);
    }
}

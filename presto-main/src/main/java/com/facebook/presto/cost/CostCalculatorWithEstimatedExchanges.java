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
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntSupplier;

import static com.facebook.presto.cost.CostCalculatorUsingExchanges.currentNumberOfWorkerNodes;
import static com.facebook.presto.cost.PlanNodeCostEstimate.ZERO_COST;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.util.Objects.requireNonNull;

/**
 * This is a wrapper class around CostCalculator that estimates ExchangeNodes cost.
 */
@ThreadSafe
public class CostCalculatorWithEstimatedExchanges
        implements CostCalculator
{
    private final CostCalculator costCalculator;
    private final IntSupplier numberOfNodes;

    @Inject
    public CostCalculatorWithEstimatedExchanges(CostCalculator costCalculator, NodeSchedulerConfig nodeSchedulerConfig, InternalNodeManager nodeManager)
    {
        this(costCalculator, currentNumberOfWorkerNodes(nodeSchedulerConfig.isIncludeCoordinator(), nodeManager));
    }

    public CostCalculatorWithEstimatedExchanges(CostCalculator costCalculator, IntSupplier numberOfNodes)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.numberOfNodes = requireNonNull(numberOfNodes, "numberOfNodes is null");
    }

    @Override
    public PlanNodeCostEstimate calculateCost(PlanNode node, StatsProvider stats, Lookup lookup, Session session, TypeProvider types)
    {
        ExchangeCostEstimator exchangeCostEstimator = new ExchangeCostEstimator(numberOfNodes.getAsInt(), stats, lookup, types);
        PlanNodeCostEstimate estimatedExchangeCost = node.accept(exchangeCostEstimator, null);
        return costCalculator.calculateCost(node, stats, lookup, session, types).add(estimatedExchangeCost);
    }

    private static class ExchangeCostEstimator
            extends PlanVisitor<PlanNodeCostEstimate, Void>
    {
        private final int numberOfNodes;
        private final StatsProvider stats;
        private final Lookup lookup;
        private final TypeProvider types;

        ExchangeCostEstimator(int numberOfNodes, StatsProvider stats, Lookup lookup, TypeProvider types)
        {
            this.numberOfNodes = numberOfNodes;
            this.stats = requireNonNull(stats, "stats is null");
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected PlanNodeCostEstimate visitPlan(PlanNode node, Void context)
        {
            // TODO implement logic for other node types and return UNKNOWN_COST here (or throw)
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitGroupReference(GroupReference node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNodeCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            List<Symbol> sourceSymbols = node.getSource().getOutputSymbols();

            PlanNodeCostEstimate remoteRepartitionCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                    numberOfNodes,
                    sourceStats,
                    sourceSymbols,
                    REPARTITION,
                    REMOTE,
                    types);
            PlanNodeCostEstimate localRepartitionCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                    numberOfNodes,
                    sourceStats,
                    sourceSymbols,
                    REPARTITION,
                    LOCAL,
                    types);

            // TODO consider cost of aggregation itself, not only exchanges, based on aggregation's properties
            return remoteRepartitionCost.add(localRepartitionCost);
        }

        @Override
        public PlanNodeCostEstimate visitJoin(JoinNode node, Void context)
        {
            return calculateJoinCost(
                    node.getLeft(),
                    node.getRight(),
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)));
        }

        @Override
        public PlanNodeCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return calculateJoinCost(
                    node.getSource(),
                    node.getFilteringSource(),
                    Objects.equals(node.getDistributionType(), Optional.of(SemiJoinNode.DistributionType.REPLICATED)));
        }

        private PlanNodeCostEstimate calculateJoinCost(PlanNode probe, PlanNode build, boolean replicated)
        {
            if (replicated) {
                PlanNodeCostEstimate replicateCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                        numberOfNodes,
                        getStats(build),
                        build.getOutputSymbols(),
                        REPLICATE,
                        REMOTE,
                        types);
                // cost of the copies repartitioning is added in CostCalculatorUsingExchanges#calculateJoinCost
                PlanNodeCostEstimate localRepartitionCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                        numberOfNodes,
                        getStats(build),
                        build.getOutputSymbols(),
                        REPARTITION,
                        LOCAL,
                        types);
                return replicateCost.add(localRepartitionCost);
            }
            else {
                PlanNodeCostEstimate probeCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                        numberOfNodes,
                        getStats(probe),
                        probe.getOutputSymbols(),
                        REPARTITION,
                        REMOTE,
                        types);
                PlanNodeCostEstimate buildRemoteRepartitionCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                        numberOfNodes,
                        getStats(build),
                        build.getOutputSymbols(),
                        REPARTITION,
                        REMOTE,
                        types);
                PlanNodeCostEstimate buildLocalRepartitionCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                        numberOfNodes,
                        getStats(build),
                        build.getOutputSymbols(),
                        REPARTITION,
                        LOCAL,
                        types);
                return probeCost
                        .add(buildRemoteRepartitionCost)
                        .add(buildLocalRepartitionCost);
            }
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }
    }
}

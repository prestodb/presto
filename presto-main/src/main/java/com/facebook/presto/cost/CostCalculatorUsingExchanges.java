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
import com.facebook.presto.spi.Node;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntSupplier;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.ZERO_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of CostCalculator. It assumes that ExchangeNodes are already in the plan.
 */
@ThreadSafe
public class CostCalculatorUsingExchanges
        implements CostCalculator
{
    private final IntSupplier numberOfNodes;

    @Inject
    public CostCalculatorUsingExchanges(NodeSchedulerConfig nodeSchedulerConfig, InternalNodeManager nodeManager)
    {
        this(currentNumberOfWorkerNodes(nodeSchedulerConfig.isIncludeCoordinator(), nodeManager));
    }

    static IntSupplier currentNumberOfWorkerNodes(boolean includeCoordinator, InternalNodeManager nodeManager)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        return () -> {
            Set<Node> activeNodes = nodeManager.getAllNodes().getActiveNodes();
            if (includeCoordinator) {
                return activeNodes.size();
            }
            return toIntExact(activeNodes.stream()
                    .filter(node -> !node.isCoordinator())
                    .count());
        };
    }

    public CostCalculatorUsingExchanges(IntSupplier numberOfNodes)
    {
        this.numberOfNodes = requireNonNull(numberOfNodes, "numberOfNodes is null");
    }

    @Override
    public PlanNodeCostEstimate calculateCost(PlanNode node, StatsProvider stats, Lookup lookup, Session session, TypeProvider types)
    {
        CostEstimator costEstimator = new CostEstimator(numberOfNodes.getAsInt(), stats, types);
        return node.accept(costEstimator, null);
    }

    private static class CostEstimator
            extends PlanVisitor<PlanNodeCostEstimate, Void>
    {
        private final int numberOfNodes;
        private final StatsProvider stats;
        private final TypeProvider types;

        CostEstimator(int numberOfNodes, StatsProvider stats, TypeProvider types)
        {
            this.numberOfNodes = numberOfNodes;
            this.stats = requireNonNull(stats, "stats is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected PlanNodeCostEstimate visitPlan(PlanNode node, Void context)
        {
            return UNKNOWN_COST;
        }

        @Override
        public PlanNodeCostEstimate visitGroupReference(GroupReference node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNodeCostEstimate visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return cpuCost(getStats(node).getOutputSizeInBytes(ImmutableList.of(node.getIdColumn()), types));
        }

        @Override
        public PlanNodeCostEstimate visitOutput(OutputNode node, Void context)
        {
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitTableScan(TableScanNode node, Void context)
        {
            // TODO: add network cost, based on input size in bytes? Or let connector provide this cost?
            return cpuCost(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
        }

        @Override
        public PlanNodeCostEstimate visitFilter(FilterNode node, Void context)
        {
            return cpuCost(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
        }

        @Override
        public PlanNodeCostEstimate visitProject(ProjectNode node, Void context)
        {
            return cpuCost(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
        }

        @Override
        public PlanNodeCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            PlanNodeStatsEstimate aggregationStats = getStats(node);
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            double cpuCost = sourceStats.getOutputSizeInBytes(node.getSource().getOutputSymbols(), types);
            double memoryCost = aggregationStats.getOutputSizeInBytes(node.getOutputSymbols(), types);
            return new PlanNodeCostEstimate(cpuCost, memoryCost, 0);
        }

        @Override
        public PlanNodeCostEstimate visitJoin(JoinNode node, Void context)
        {
            return calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)));
        }

        private PlanNodeCostEstimate calculateJoinCost(PlanNode join, PlanNode probe, PlanNode build, boolean replicated)
        {
            int numberOfNodesMultiplier = replicated ? numberOfNodes : 1;

            PlanNodeStatsEstimate probeStats = getStats(probe);
            PlanNodeStatsEstimate buildStats = getStats(build);
            PlanNodeStatsEstimate outputStats = getStats(join);

            double buildSideSize = buildStats.getOutputSizeInBytes(build.getOutputSymbols(), types);
            double probeSideSize = probeStats.getOutputSizeInBytes(probe.getOutputSymbols(), types);
            double joinOutputSize = outputStats.getOutputSizeInBytes(join.getOutputSymbols(), types);

            double cpuCost = probeSideSize +
                    buildSideSize * numberOfNodesMultiplier +
                    joinOutputSize;

            if (replicated) {
                // add the cost of a local repartitioning of build side copies
                // cost of the repartitioning of a single data copy has been already added in calculateExchangeCost
                cpuCost += buildSideSize * (numberOfNodesMultiplier - 1);
            }

            double memoryCost = buildSideSize * numberOfNodesMultiplier;

            return new PlanNodeCostEstimate(cpuCost, memoryCost, 0);
        }

        @Override
        public PlanNodeCostEstimate visitExchange(ExchangeNode node, Void context)
        {
            return calculateExchangeCost(numberOfNodes, getStats(node), node.getOutputSymbols(), node.getType(), node.getScope(), types);
        }

        @Override
        public PlanNodeCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return calculateJoinCost(
                    node,
                    node.getSource(),
                    node.getFilteringSource(),
                    node.getDistributionType().orElse(SemiJoinNode.DistributionType.PARTITIONED).equals(SemiJoinNode.DistributionType.REPLICATED));
        }

        @Override
        public PlanNodeCostEstimate visitValues(ValuesNode node, Void context)
        {
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitLimit(LimitNode node, Void context)
        {
            // This is just a wild guess. First of all, LimitNode is rather rare except as a top node of a query plan,
            // so proper cost estimation is not that important. Second, since LimitNode can lead to incomplete evaluation
            // of the source, true cost estimation should be implemented as a "constraint" enforced on a sub-tree and
            // evaluated in context of actual source node type (and their sources).
            return cpuCost(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }
    }

    public static PlanNodeCostEstimate calculateExchangeCost(
            int numberOfNodes,
            PlanNodeStatsEstimate exchangeStats,
            List<Symbol> symbols,
            ExchangeNode.Type type,
            ExchangeNode.Scope scope,
            TypeProvider types)
    {
        double exchangeSize = exchangeStats.getOutputSizeInBytes(symbols, types);

        double network;
        double cpu = 0;

        switch (type) {
            case GATHER:
                network = exchangeSize;
                break;
            case REPARTITION:
                network = exchangeSize;
                cpu = exchangeSize;
                break;
            case REPLICATE:
                network = exchangeSize * numberOfNodes;
                break;
            default:
                throw new UnsupportedOperationException(format("Unsupported type [%s] of the exchange", type));
        }

        if (scope == LOCAL) {
            network = 0;
        }

        return new PlanNodeCostEstimate(cpu, 0, network);
    }
}

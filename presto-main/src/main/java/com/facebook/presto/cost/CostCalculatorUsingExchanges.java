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
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
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

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.ZERO_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of CostCalculator. It assumes that ExchangeNodes are already in the plan.
 */
@ThreadSafe
public class CostCalculatorUsingExchanges
        implements CostCalculator
{
    private final int numberOfNodes;

    @Inject
    public CostCalculatorUsingExchanges(InternalNodeManager nodeManager)
    {
        this(nodeManager.getAllNodes().getActiveNodes().size());
    }

    public CostCalculatorUsingExchanges(int numberOfNodes)
    {
        this.numberOfNodes = numberOfNodes;
    }

    @Override
    public PlanNodeCostEstimate calculateCost(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        CostEstimator costEstimator = new CostEstimator(
                session,
                types,
                lookup,
                numberOfNodes);

        return planNode.accept(costEstimator, null);
    }

    private class CostEstimator
            extends PlanVisitor<PlanNodeCostEstimate, Void>
    {
        private final Session session;
        private final Map<Symbol, Type> types;
        private final Lookup lookup;
        private final int numberOfNodes;

        public CostEstimator(Session session, Map<Symbol, Type> types, Lookup lookup, int numberOfNodes)
        {
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.lookup = lookup;
            this.numberOfNodes = numberOfNodes;
        }

        @Override
        protected PlanNodeCostEstimate visitPlan(PlanNode node, Void context)
        {
            return UNKNOWN_COST;
        }

        @Override
        public PlanNodeCostEstimate visitOutput(OutputNode node, Void context)
        {
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitFilter(FilterNode node, Void context)
        {
            return cpuCost(getStats(node.getSource()).getOutputSizeInBytes());
        }

        @Override
        public PlanNodeCostEstimate visitProject(ProjectNode node, Void context)
        {
            return cpuCost(getStats(node).getOutputSizeInBytes());
        }

        @Override
        public PlanNodeCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            PlanNodeStatsEstimate aggregationStats = getStats(node);
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            return PlanNodeCostEstimate.builder()
                    .setCpuCost(sourceStats.getOutputSizeInBytes())
                    .setMemoryCost(aggregationStats.getOutputSizeInBytes())
                    .build();
        }

        @Override
        public PlanNodeCostEstimate visitJoin(JoinNode node, Void context)
        {
            return calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    node.getDistributionType().orElse(JoinNode.DistributionType.PARTITIONED).equals(JoinNode.DistributionType.REPLICATED));
        }

        private PlanNodeCostEstimate calculateJoinCost(PlanNode join, PlanNode probe, PlanNode build, boolean replicated)
        {
            int numberOfNodesMultiplier = replicated ? numberOfNodes : 1;

            PlanNodeStatsEstimate probeStats = getStats(probe);
            PlanNodeStatsEstimate buildStats = getStats(build);
            PlanNodeStatsEstimate outputStats = getStats(join);

            double cpuCost = probeStats.getOutputSizeInBytes() +
                    buildStats.getOutputSizeInBytes() * numberOfNodesMultiplier +
                    outputStats.getOutputSizeInBytes();

            double memoryCost = buildStats.getOutputSizeInBytes() * numberOfNodesMultiplier;

            return PlanNodeCostEstimate.builder()
                    .setCpuCost(cpuCost)
                    .setMemoryCost(memoryCost)
                    .build();
        }

        @Override
        public PlanNodeCostEstimate visitExchange(ExchangeNode node, Void context)
        {
            return calculateExchangeCost(numberOfNodes, getStats(node), node.getType(), node.getScope());
        }

        @Override
        public PlanNodeCostEstimate visitTableScan(TableScanNode node, Void context)
        {
            return cpuCost(getStats(node).getOutputSizeInBytes()); // TODO: add network cost, based on input size in bytes?
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
        public PlanNodeCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return calculateJoinCost(
                    node,
                    node.getSource(),
                    node.getFilteringSource(),
                    node.getDistributionType().orElse(SemiJoinNode.DistributionType.PARTITIONED).equals(SemiJoinNode.DistributionType.REPLICATED));
        }

        @Override
        public PlanNodeCostEstimate visitLimit(LimitNode node, Void context)
        {
            return cpuCost(getStats(node).getOutputSizeInBytes());
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return lookup.getStats(node, session, types);
        }
    }

    public static PlanNodeCostEstimate calculateExchangeCost(int numberOfNodes, PlanNodeStatsEstimate exchangeStats, ExchangeNode.Type type, ExchangeNode.Scope scope)
    {
        double network = 0;
        double cpu = 0;

        switch (type) {
            case GATHER:
                network = exchangeStats.getOutputSizeInBytes();
                break;
            case REPARTITION:
                network = exchangeStats.getOutputSizeInBytes();
                cpu = exchangeStats.getOutputSizeInBytes();
                break;
            case REPLICATE:
                network = exchangeStats.getOutputSizeInBytes() * numberOfNodes;
                break;
            default:
                throw new UnsupportedOperationException(format("Unsupported type [%s] of the exchange", type));
        }

        if (scope.equals(ExchangeNode.Scope.LOCAL)) {
            network = 0;
        }

        return PlanNodeCostEstimate.builder()
                .setNetworkCost(network)
                .setCpuCost(cpu)
                .build();
    }
}

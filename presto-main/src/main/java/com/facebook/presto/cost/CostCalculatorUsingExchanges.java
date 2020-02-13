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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateJoinInputCost;
import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateLocalRepartitionCost;
import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteGatherCost;
import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteRepartitionCost;
import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteReplicateCost;
import static com.facebook.presto.cost.LocalCostEstimate.addPartialComponents;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of CostCalculator. It assumes that ExchangeNodes are already in the plan.
 */
@ThreadSafe
public class CostCalculatorUsingExchanges
        implements CostCalculator
{
    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public CostCalculatorUsingExchanges(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public PlanCostEstimate calculateCost(PlanNode node, StatsProvider stats, CostProvider sourcesCosts, Session session)
    {
        CostEstimator costEstimator = new CostEstimator(stats, sourcesCosts, taskCountEstimator);
        return node.accept(costEstimator, null);
    }

    private static class CostEstimator
            extends InternalPlanVisitor<PlanCostEstimate, Void>
    {
        private final StatsProvider stats;
        private final CostProvider sourcesCosts;
        private final TaskCountEstimator taskCountEstimator;

        CostEstimator(StatsProvider stats, CostProvider sourcesCosts, TaskCountEstimator taskCountEstimator)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.sourcesCosts = requireNonNull(sourcesCosts, "sourcesCosts is null");
            this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        }

        @Override
        public PlanCostEstimate visitPlan(PlanNode node, Void context)
        {
            // TODO implement cost estimates for all plan nodes
            return PlanCostEstimate.unknown();
        }

        @Override
        public PlanCostEstimate visitGroupReference(GroupReference node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanCostEstimate visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(ImmutableList.of(node.getIdVariable())));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitRowNumber(RowNumberNode node, Void context)
        {
            List<VariableReferenceExpression> variables = node.getOutputVariables();
            // when maxRowCountPerPartition is set, the RowNumberOperator
            // copies values for all the columns into a page builder
            if (!node.getMaxRowCountPerPartition().isPresent()) {
                variables = ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(node.getPartitionBy())
                        .add(node.getRowNumberVariable())
                        .build();
            }
            PlanNodeStatsEstimate stats = getStats(node);
            double cpuCost = stats.getOutputSizeInBytes(variables);
            double memoryCost = node.getPartitionBy().isEmpty() ? 0 : stats.getOutputSizeInBytes(node.getSource().getOutputVariables());
            LocalCostEstimate localCost = LocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitOutput(OutputNode node, Void context)
        {
            return costForStreaming(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitTableScan(TableScanNode node, Void context)
        {
            // TODO: add network cost, based on input size in bytes? Or let connector provide this cost?
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputVariables()));
            return costForSource(node, localCost);
        }

        @Override
        public PlanCostEstimate visitFilter(FilterNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputVariables()));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitProject(ProjectNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputVariables()));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            if (node.getStep() != FINAL && node.getStep() != SINGLE) {
                return PlanCostEstimate.unknown();
            }
            PlanNodeStatsEstimate aggregationStats = getStats(node);
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            double cpuCost = sourceStats.getOutputSizeInBytes(node.getSource().getOutputVariables());
            double memoryCost = aggregationStats.getOutputSizeInBytes(node.getOutputVariables());
            LocalCostEstimate localCost = LocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForAccumulation(node, localCost);
        }

        @Override
        public PlanCostEstimate visitJoin(JoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)));
            return costForLookupJoin(node, localCost);
        }

        private LocalCostEstimate calculateJoinCost(PlanNode join, PlanNode probe, PlanNode build, boolean replicated)
        {
            LocalCostEstimate joinInputCost = calculateJoinInputCost(
                    probe,
                    build,
                    stats,
                    replicated,
                    taskCountEstimator.estimateSourceDistributedTaskCount());
            LocalCostEstimate joinOutputCost = calculateJoinOutputCost(join);
            return addPartialComponents(joinInputCost, joinOutputCost);
        }

        private LocalCostEstimate calculateJoinOutputCost(PlanNode join)
        {
            PlanNodeStatsEstimate outputStats = getStats(join);
            double joinOutputSize = outputStats.getOutputSizeInBytes(join.getOutputVariables());
            return LocalCostEstimate.ofCpu(joinOutputSize);
        }

        @Override
        public PlanCostEstimate visitExchange(ExchangeNode node, Void context)
        {
            return costForStreaming(node, calculateExchangeCost(node));
        }

        private LocalCostEstimate calculateExchangeCost(ExchangeNode node)
        {
            double inputSizeInBytes = getStats(node).getOutputSizeInBytes(node.getOutputVariables());
            switch (node.getScope()) {
                case LOCAL:
                    switch (node.getType()) {
                        case GATHER:
                            return LocalCostEstimate.zero();
                        case REPARTITION:
                            return calculateLocalRepartitionCost(inputSizeInBytes);
                        case REPLICATE:
                            return LocalCostEstimate.zero();
                        default:
                            throw new IllegalArgumentException("Unexpected type: " + node.getType());
                    }
                case REMOTE_STREAMING:
                case REMOTE_MATERIALIZED:
                    switch (node.getType()) {
                        case GATHER:
                            return calculateRemoteGatherCost(inputSizeInBytes);
                        case REPARTITION:
                            return calculateRemoteRepartitionCost(inputSizeInBytes);
                        case REPLICATE:
                            // assuming that destination is always source distributed
                            // it is true as now replicated exchange is used for joins only
                            // for replicated join probe side is usually source distributed
                            return calculateRemoteReplicateCost(inputSizeInBytes, taskCountEstimator.estimateSourceDistributedTaskCount());
                        default:
                            throw new IllegalArgumentException("Unexpected type: " + node.getType());
                    }
                default:
                    throw new IllegalArgumentException("Unexpected scope: " + node.getScope());
            }
        }

        @Override
        public PlanCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getSource(),
                    node.getFilteringSource(),
                    node.getDistributionType().orElse(SemiJoinNode.DistributionType.PARTITIONED).equals(SemiJoinNode.DistributionType.REPLICATED));
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    node.getDistributionType() == SpatialJoinNode.DistributionType.REPLICATED);
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitValues(ValuesNode node, Void context)
        {
            return costForSource(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return costForAccumulation(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitLimit(LimitNode node, Void context)
        {
            // This is just a wild guess. First of all, LimitNode is rather rare except as a top node of a query plan,
            // so proper cost estimation is not that important. Second, since LimitNode can lead to incomplete evaluation
            // of the source, true cost estimation should be implemented as a "constraint" enforced on a sub-tree and
            // evaluated in context of actual source node type (and their sources).
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputVariables()));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitUnion(UnionNode node, Void context)
        {
            // Cost will be accounted either in CostCalculatorUsingExchanges#CostEstimator#visitExchange
            // or in CostCalculatorWithEstimatedExchanges#CostEstimator#visitUnion
            // This stub is needed just to avoid the cumulative cost being set to unknown
            return costForStreaming(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitSort(SortNode node, Void context)
        {
            double cpuCost = getStats(node).getOutputSizeInBytes(node.getOutputVariables());
            double memoryCost = getStats(node).getOutputSizeInBytes(node.getOutputVariables());
            LocalCostEstimate localCost = LocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForAccumulation(node, localCost);
        }

        @Override
        public PlanCostEstimate visitSample(SampleNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputVariables()));
            return costForStreaming(node, localCost);
        }

        private PlanCostEstimate costForSource(PlanNode node, LocalCostEstimate localCost)
        {
            verify(node.getSources().isEmpty(), "Unexpected sources for %s: %s", node, node.getSources());
            return new PlanCostEstimate(localCost.getCpuCost(), localCost.getMaxMemory(), localCost.getMaxMemory(), localCost.getNetworkCost());
        }

        private PlanCostEstimate costForAccumulation(PlanNode node, LocalCostEstimate localCost)
        {
            PlanCostEstimate sourcesCost = getSourcesEstimations(node)
                    .reduce(PlanCostEstimate.zero(), CostCalculatorUsingExchanges::addParallelSiblingsCost);
            return new PlanCostEstimate(
                    sourcesCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            sourcesCost.getMaxMemory(), // Accumulating operator allocates insignificant amount of memory (usually none) before first input page is received
                            sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    localCost.getMaxMemory(), // Source freed its memory allocations when finished its output
                    sourcesCost.getNetworkCost() + localCost.getNetworkCost());
        }

        private PlanCostEstimate costForStreaming(PlanNode node, LocalCostEstimate localCost)
        {
            PlanCostEstimate sourcesCost = getSourcesEstimations(node)
                    .reduce(PlanCostEstimate.zero(), CostCalculatorUsingExchanges::addParallelSiblingsCost);
            return new PlanCostEstimate(
                    sourcesCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            sourcesCost.getMaxMemory(), // Streaming operator allocates insignificant amount of memory (usually none) before first input page is received
                            sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory(),
                    sourcesCost.getNetworkCost() + localCost.getNetworkCost());
        }

        private PlanCostEstimate costForLookupJoin(PlanNode node, LocalCostEstimate localCost)
        {
            verify(node.getSources().size() == 2, "Unexpected number of sources for %s: %s", node, node.getSources());
            List<PlanCostEstimate> sourcesCosts = getSourcesEstimations(node).collect(toImmutableList());
            verify(sourcesCosts.size() == 2);
            PlanCostEstimate probeCost = sourcesCosts.get(0);
            PlanCostEstimate buildCost = sourcesCosts.get(1);

            return new PlanCostEstimate(
                    probeCost.getCpuCost() + buildCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            probeCost.getMaxMemory() + buildCost.getMaxMemory(), // Probe and build execute independently, so their max memory allocations can be realized at the same time
                            probeCost.getMaxMemory() + buildCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    probeCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory(), // Build side finished and freed its memory allocations
                    probeCost.getNetworkCost() + buildCost.getNetworkCost() + localCost.getNetworkCost());
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }

        private Stream<PlanCostEstimate> getSourcesEstimations(PlanNode node)
        {
            return node.getSources().stream()
                    .map(sourcesCosts::getCost);
        }
    }

    private static PlanCostEstimate addParallelSiblingsCost(PlanCostEstimate a, PlanCostEstimate b)
    {
        return new PlanCostEstimate(
                a.getCpuCost() + b.getCpuCost(),
                a.getMaxMemory() + b.getMaxMemory(),
                a.getMaxMemoryWhenOutputting() + b.getMaxMemoryWhenOutputting(),
                a.getNetworkCost() + b.getNetworkCost());
    }
}

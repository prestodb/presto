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
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Double.NaN;
import static java.util.Objects.requireNonNull;

public class CachingCostProvider
        implements CostProvider
{
    private final CostCalculator costCalculator;
    private final StatsProvider statsProvider;
    private final Optional<Memo> memo;
    private final Session session;
    private final TypeProvider types;

    private final Map<PlanNode, PlanCostEstimate> cache = new IdentityHashMap<>();

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Session session, TypeProvider types)
    {
        this(costCalculator, statsProvider, Optional.empty(), session, types);
    }

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Optional<Memo> memo, Session session, TypeProvider types)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public PlanNodeCostEstimate getCumulativeCost(PlanNode node)
    {
        return getPlanCost(node).toPlanNodeCostEstimate();
    }

    private PlanCostEstimate getPlanCost(PlanNode node)
    {
        requireNonNull(node, "node is null");

        if (node instanceof GroupReference) {
            return getGroupCost((GroupReference) node);
        }

        PlanCostEstimate cumulativeCost = cache.get(node);
        if (cumulativeCost == null) {
            cumulativeCost = calculateCumulativeCost(node);
            verify(cache.put(node, cumulativeCost) == null, "Cost already set");
        }
        return cumulativeCost;
    }

    private PlanCostEstimate getGroupCost(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingCostProvider without memo cannot handle GroupReferences"));

        Optional<PlanCostEstimate> cost = memo.getCumulativeCost(group);
        if (cost.isPresent()) {
            return cost.get();
        }

        PlanCostEstimate cumulativeCost = calculateCumulativeCost(memo.getNode(group));
        verify(!memo.getCumulativeCost(group).isPresent(), "Group cost already set");
        memo.storeCumulativeCost(group, cumulativeCost);
        return cumulativeCost;
    }

    private PlanCostEstimate calculateCumulativeCost(PlanNode node)
    {
        PlanNodeCostEstimate localCost = costCalculator.calculateCost(node, statsProvider, session, types);

        List<PlanCostEstimate> sourcesCosts = node.getSources().stream()
                .map(this::getPlanCost)
                .collect(toImmutableList());

        MemoryEstimate memoryEstimate = node.accept(new PeakMemoryEstimator(), new EstimationContext(localCost.getMemoryCost(), sourcesCosts));
        verify(memoryEstimate != null);

        PlanCostEstimate sourcesCost = sourcesCosts.stream()
                .reduce(PlanCostEstimate.zero(), CachingCostProvider::addSiblingsCost);

        return new PlanCostEstimate(
                localCost.getCpuCost() + sourcesCost.getCpuCost(),
                Math.max(sourcesCost.getMaxMemory(), Math.max(memoryEstimate.maxMemoryBeforeOutputting, memoryEstimate.maxMemoryWhenOutputting)),
                memoryEstimate.maxMemoryWhenOutputting,
                localCost.getNetworkCost() + sourcesCost.getNetworkCost());
    }

    private static PlanCostEstimate addSiblingsCost(PlanCostEstimate a, PlanCostEstimate b)
    {
        return new PlanCostEstimate(
                a.getCpuCost() + b.getCpuCost(),
                a.getMaxMemory() + b.getMaxMemory(),
                a.getMaxMemoryWhenOutputting() + b.getMaxMemoryWhenOutputting(),
                a.getNetworkCost() + b.getNetworkCost());
    }

    private static class EstimationContext
    {
        private final double localMaxMemory;
        private final List<PlanCostEstimate> sourcesCosts;

        public EstimationContext(double localMaxMemory, List<PlanCostEstimate> sourcesCosts)
        {
            this.localMaxMemory = localMaxMemory;
            this.sourcesCosts = requireNonNull(sourcesCosts, "sourcesCosts is null");
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("localMaxMemory", localMaxMemory)
                    .add("sourcesCosts", sourcesCosts)
                    .toString();
        }
    }

    private static class MemoryEstimate
    {
        private final double maxMemoryBeforeOutputting;
        private final double maxMemoryWhenOutputting;

        public MemoryEstimate(double maxMemoryBeforeOutputting, double maxMemoryWhenOutputting)
        {
            this.maxMemoryBeforeOutputting = maxMemoryBeforeOutputting;
            this.maxMemoryWhenOutputting = maxMemoryWhenOutputting;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("maxMemoryBeforeOutputting", maxMemoryBeforeOutputting)
                    .add("maxMemoryWhenOutputting", maxMemoryWhenOutputting)
                    .toString();
        }
    }

    private static class PeakMemoryEstimator
            extends PlanVisitor<MemoryEstimate, EstimationContext>
    {
        @Override
        protected MemoryEstimate visitPlan(PlanNode node, EstimationContext context)
        {
            throw new UnsupportedOperationException("Unsupported plan node type: " + node.getClass().getName());
        }

        @Override
        public MemoryEstimate visitRemoteSource(RemoteSourceNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitAggregation(AggregationNode node, EstimationContext context)
        {
            double sourceMaxMemoryWhenOutputting = getOnlySourceMaxMemoryWhenOutputting(node, context);
            if (node.getStep() == AggregationNode.Step.SINGLE || node.getStep() == AggregationNode.Step.FINAL) {
                return new MemoryEstimate(
                        context.localMaxMemory + sourceMaxMemoryWhenOutputting,
                        context.localMaxMemory);
            }
            else {
                return new MemoryEstimate(
                        context.localMaxMemory + sourceMaxMemoryWhenOutputting,
                        context.localMaxMemory + sourceMaxMemoryWhenOutputting);
            }
        }

        @Override
        public MemoryEstimate visitFilter(FilterNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitProject(ProjectNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitTopN(TopNNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitOutput(OutputNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitLimit(LimitNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitDistinctLimit(DistinctLimitNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitSample(SampleNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitTableScan(TableScanNode node, EstimationContext context)
        {
            return memoryEstimateForSource(node, context);
        }

        @Override
        public MemoryEstimate visitExplainAnalyze(ExplainAnalyzeNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitValues(ValuesNode node, EstimationContext context)
        {
            return memoryEstimateForSource(node, context);
        }

        @Override
        public MemoryEstimate visitIndexSource(IndexSourceNode node, EstimationContext context)
        {
            return memoryEstimateForSource(node, context);
        }

        @Override
        public MemoryEstimate visitJoin(JoinNode node, EstimationContext context)
        {
            return memoryEstimateForLookupJoin(node, context);
        }

        @Override
        public MemoryEstimate visitSemiJoin(SemiJoinNode node, EstimationContext context)
        {
            return memoryEstimateForLookupJoin(node, context);
        }

        @Override
        public MemoryEstimate visitSpatialJoin(SpatialJoinNode node, EstimationContext context)
        {
            return memoryEstimateForLookupJoin(node, context);
        }

        @Override
        public MemoryEstimate visitIndexJoin(IndexJoinNode node, EstimationContext context)
        {
            PlanCostEstimate sourcesCost = context.sourcesCosts.stream()
                    .reduce(PlanCostEstimate.zero(), CachingCostProvider::addSiblingsCost);

            return new MemoryEstimate(
                    sourcesCost.getMaxMemoryWhenOutputting(),
                    context.localMaxMemory + sourcesCost.getMaxMemoryWhenOutputting());
        }

        @Override
        public MemoryEstimate visitSort(SortNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitWindow(WindowNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitTableWriter(TableWriterNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitDelete(DeleteNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitMetadataDelete(MetadataDeleteNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitTableFinish(TableFinishNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitUnion(UnionNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitIntersect(IntersectNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitExcept(ExceptNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitUnnest(UnnestNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitMarkDistinct(MarkDistinctNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitGroupId(GroupIdNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitRowNumber(RowNumberNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitTopNRowNumber(TopNRowNumberNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitExchange(ExchangeNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitEnforceSingleRow(EnforceSingleRowNode node, EstimationContext context)
        {
            double sourceMaxMemoryWhenOutputting = getOnlySourceMaxMemoryWhenOutputting(node, context);
            return new MemoryEstimate(context.localMaxMemory + sourceMaxMemoryWhenOutputting, 0);
        }

        @Override
        public MemoryEstimate visitApply(ApplyNode node, EstimationContext context)
        {
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitAssignUniqueId(AssignUniqueId node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitGroupReference(GroupReference node, EstimationContext context)
        {
            throw new IllegalStateException("Unexpected GroupReference");
        }

        @Override
        public MemoryEstimate visitLateralJoin(LateralJoinNode node, EstimationContext context)
        {
            return new MemoryEstimate(NaN, NaN);
        }

        private static MemoryEstimate memoryEstimateForSource(PlanNode node, EstimationContext context)
        {
            verify(context.sourcesCosts.isEmpty(), "Unexpected number of sources for %s: %s", node, context.sourcesCosts.size());
            return new MemoryEstimate(0, context.localMaxMemory);
        }

        private static MemoryEstimate memoryEstimateForStreaming(EstimationContext context)
        {
            PlanCostEstimate sourcesCost = sumSourcesCost(context);
            return new MemoryEstimate(
                    sourcesCost.getMaxMemory(),
                    context.localMaxMemory + sourcesCost.getMaxMemoryWhenOutputting());
        }

        private static MemoryEstimate memoryEstimateForLookupJoin(PlanNode node, EstimationContext context)
        {
            verify(context.sourcesCosts.size() == 2, "Unexpected number of sources for %s: %s", node, context.sourcesCosts.size());
            double localMaxMemory = context.localMaxMemory;
            double probeMaxMemoryWhenOutputting = context.sourcesCosts.get(0).getMaxMemoryWhenOutputting();
            double buildMaxMemoryWhenOutputting = context.sourcesCosts.get(1).getMaxMemoryWhenOutputting();

            return new MemoryEstimate(
                    localMaxMemory + probeMaxMemoryWhenOutputting + buildMaxMemoryWhenOutputting,
                    localMaxMemory + probeMaxMemoryWhenOutputting);
        }

        private static PlanCostEstimate sumSourcesCost(EstimationContext context)
        {
            return context.sourcesCosts.stream()
                    .reduce(PlanCostEstimate.zero(), CachingCostProvider::addSiblingsCost);
        }

        private static double getOnlySourceMaxMemoryWhenOutputting(PlanNode forNode, EstimationContext context)
        {
            verify(context.sourcesCosts.size() == 1, "Unexpected number of sources for %s: %s", forNode, context.sourcesCosts.size());
            return getOnlyElement(context.sourcesCosts).getMaxMemoryWhenOutputting();
        }
    }
}

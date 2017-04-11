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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.split.SampledSplitSource;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class DistributedExecutionPlanner
{
    private static final Logger log = Logger.get(DistributedExecutionPlanner.class);

    private final SplitManager splitManager;

    @Inject
    public DistributedExecutionPlanner(SplitManager splitManager)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
    }

    public StageExecutionPlan plan(SubPlan root, Session session)
    {
        Visitor visitor = new Visitor(session);
        try {
            return plan(root, visitor);
        }
        catch (Throwable t) {
            visitor.getSplitSources().forEach(DistributedExecutionPlanner::closeSplitSource);
            throw t;
        }
    }

    private static void closeSplitSource(SplitSource source)
    {
        try {
            source.close();
        }
        catch (Throwable t) {
            log.warn(t, "Error closing split source");
        }
    }

    private StageExecutionPlan plan(SubPlan root, Visitor visitor)
    {
        PlanFragment currentFragment = root.getFragment();

        // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
        Map<PlanNodeId, SplitSource> splitSources = currentFragment.getRoot().accept(visitor, null);

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            dependencies.add(plan(childPlan, visitor));
        }

        return new StageExecutionPlan(
                currentFragment,
                splitSources,
                dependencies.build());
    }

    private final class Visitor
            extends PlanVisitor<Void, Map<PlanNodeId, SplitSource>>
    {
        private final Session session;
        private final List<SplitSource> splitSources = new ArrayList<>();

        private Visitor(Session session)
        {
            this.session = session;
        }

        public List<SplitSource> getSplitSources()
        {
            return splitSources;
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableScan(TableScanNode node, Void context)
        {
            // get dataSource for table
            SplitSource splitSource = splitManager.getSplits(session, node.getLayout().get());

            splitSources.add(splitSource);

            return ImmutableMap.of(node.getId(), splitSource);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitJoin(JoinNode node, Void context)
        {
            Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
            Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(leftSplits)
                    .putAll(rightSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSemiJoin(SemiJoinNode node, Void context)
        {
            Map<PlanNodeId, SplitSource> sourceSplits = node.getSource().accept(this, context);
            Map<PlanNodeId, SplitSource> filteringSourceSplits = node.getFilteringSource().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(sourceSplits)
                    .putAll(filteringSourceSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitIndexJoin(IndexJoinNode node, Void context)
        {
            return node.getProbeSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRemoteSource(RemoteSourceNode node, Void context)
        {
            // remote source node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitValues(ValuesNode node, Void context)
        {
            // values node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitFilter(FilterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSample(SampleNode node, Void context)
        {
            switch (node.getSampleType()) {
                case BERNOULLI:
                    return node.getSource().accept(this, context);
                case SYSTEM:
                    Map<PlanNodeId, SplitSource> nodeSplits = node.getSource().accept(this, context);
                    // TODO: when this happens we should switch to either BERNOULLI or page sampling
                    if (nodeSplits.size() == 1) {
                        PlanNodeId planNodeId = getOnlyElement(nodeSplits.keySet());
                        SplitSource sampledSplitSource = new SampledSplitSource(nodeSplits.get(planNodeId), node.getSampleRatio());
                        return ImmutableMap.of(planNodeId, sampledSplitSource);
                    }
                    // table sampling on a sub query without splits is meaningless
                    return nodeSplits;

                default:
                    throw new UnsupportedOperationException("Sampling is not supported for type " + node.getSampleType());
            }
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAggregation(AggregationNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitGroupId(GroupIdNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRowNumber(RowNumberNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTopNRowNumber(TopNRowNumberNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUnnest(UnnestNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitOutput(OutputNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableWriter(TableWriterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableFinish(TableFinishNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitDelete(DeleteNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMetadataDelete(MetadataDeleteNode node, Void context)
        {
            // MetadataDelete node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUnion(UnionNode node, Void context)
        {
            return processSources(node.getSources(), context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitExchange(ExchangeNode node, Void context)
        {
            return processSources(node.getSources(), context);
        }

        private Map<PlanNodeId, SplitSource> processSources(List<PlanNode> sources, Void context)
        {
            ImmutableMap.Builder<PlanNodeId, SplitSource> result = ImmutableMap.builder();
            for (PlanNode child : sources) {
                result.putAll(child.accept(this, context));
            }

            return result.build();
        }

        @Override
        protected Map<PlanNodeId, SplitSource> visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}

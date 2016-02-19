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
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
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

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DistributedExecutionPlanner
{
    private final SplitManager splitManager;

    @Inject
    public DistributedExecutionPlanner(SplitManager splitManager)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
    }

    public StageExecutionPlan plan(SubPlan root, Session session)
    {
        PlanFragment currentFragment = root.getFragment();

        // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
        Visitor visitor = new Visitor(session);
        Optional<SplitSource> splits = currentFragment.getRoot().accept(visitor, null);

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            dependencies.add(plan(childPlan, session));
        }

        return new StageExecutionPlan(
                currentFragment,
                splits,
                dependencies.build());
    }

    private final class Visitor
            extends PlanVisitor<Void, Optional<SplitSource>>
    {
        private final Session session;

        private Visitor(Session session)
        {
            this.session = session;
        }

        @Override
        public Optional<SplitSource> visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitTableScan(TableScanNode node, Void context)
        {
            // get dataSource for table
            SplitSource splitSource = splitManager.getSplits(session, node.getLayout().get());

            return Optional.of(splitSource);
        }

        @Override
        public Optional<SplitSource> visitJoin(JoinNode node, Void context)
        {
            Optional<SplitSource> leftSplits = node.getLeft().accept(this, context);
            Optional<SplitSource> rightSplits = node.getRight().accept(this, context);
            if (leftSplits.isPresent() && rightSplits.isPresent()) {
                throw new IllegalArgumentException("Both left and right join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            return leftSplits.isPresent() ? leftSplits : rightSplits;
        }

        @Override
        public Optional<SplitSource> visitSemiJoin(SemiJoinNode node, Void context)
        {
            Optional<SplitSource> sourceSplits = node.getSource().accept(this, context);
            Optional<SplitSource> filteringSourceSplits = node.getFilteringSource().accept(this, context);
            if (sourceSplits.isPresent() && filteringSourceSplits.isPresent()) {
                throw new IllegalArgumentException("Both source and filteringSource semi join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            return sourceSplits.isPresent() ? sourceSplits : filteringSourceSplits;
        }

        @Override
        public Optional<SplitSource> visitIndexJoin(IndexJoinNode node, Void context)
        {
            return node.getProbeSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitRemoteSource(RemoteSourceNode node, Void context)
        {
            // remote source node does not have splits
            return Optional.empty();
        }

        @Override
        public Optional<SplitSource> visitValues(ValuesNode node, Void context)
        {
            // values node does not have splits
            return Optional.empty();
        }

        @Override
        public Optional<SplitSource> visitFilter(FilterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitSample(SampleNode node, Void context)
        {
            switch (node.getSampleType()) {
                case BERNOULLI:
                case POISSONIZED:
                    return node.getSource().accept(this, context);

                case SYSTEM:
                    Optional<SplitSource> nodeSplits = node.getSource().accept(this, context);
                    if (nodeSplits.isPresent()) {
                        SplitSource sampledSplitSource = new SampledSplitSource(nodeSplits.get(), node.getSampleRatio());
                        return Optional.of(sampledSplitSource);
                    }
                    // table sampling on a sub query without splits is meaningless
                    return nodeSplits;

                default:
                    throw new UnsupportedOperationException("Sampling is not supported for type " + node.getSampleType());
            }
        }

        @Override
        public Optional<SplitSource> visitAggregation(AggregationNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitGroupId(GroupIdNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitRowNumber(RowNumberNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitTopNRowNumber(TopNRowNumberNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitUnnest(UnnestNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitOutput(OutputNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitTableWriter(TableWriterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitTableFinish(TableFinishNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitDelete(DeleteNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Optional<SplitSource> visitMetadataDelete(MetadataDeleteNode node, Void context)
        {
            // MetadataDelete node does not have splits
            return Optional.empty();
        }

        @Override
        public Optional<SplitSource> visitUnion(UnionNode node, Void context)
        {
            Optional<SplitSource> result = Optional.empty();
            for (PlanNode child : node.getSources()) {
                Optional<SplitSource> source = child.accept(this, context);

                if (result.isPresent() && source.isPresent()) {
                    throw new IllegalArgumentException("Multiple children are source-distributed");
                }

                if (source.isPresent()) {
                    result = source;
                }
            }

            return result;
        }

        @Override
        protected Optional<SplitSource> visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}

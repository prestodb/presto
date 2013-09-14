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

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DistributedExecutionPlanner
{
    private final SplitManager splitManager;
    private final Session session;
    private final ShardManager shardManager;

    @Inject
    public DistributedExecutionPlanner(SplitManager splitManager,
            Session session,
            ShardManager shardManager)
    {
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.session = checkNotNull(session, "session is null");
        this.shardManager = checkNotNull(shardManager, "databaseShardManager is null");
    }

    public StageExecutionPlan plan(SubPlan root)
    {
        return plan(root, Predicates.<Partition>alwaysTrue());
    }

    public StageExecutionPlan plan(SubPlan root, Predicate<Partition> tableWriterPartitionPredicate)
    {
        PlanFragment currentFragment = root.getFragment();

        // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
        Visitor visitor = new Visitor();
        NodeSplits nodeSplits = currentFragment.getRoot().accept(visitor, tableWriterPartitionPredicate);

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            dependencies.add(plan(childPlan, tableWriterPartitionPredicate));
        }

        return new StageExecutionPlan(currentFragment,
                nodeSplits.dataSource,
                dependencies.build(),
                visitor.getOutputReceivers());
    }

    private final class Visitor
            extends PlanVisitor<Predicate<Partition>, NodeSplits>
    {
        private final Map<PlanNodeId, OutputReceiver> outputReceivers = new HashMap<>();

        public Map<PlanNodeId, OutputReceiver> getOutputReceivers()
        {
            return ImmutableMap.copyOf(outputReceivers);
        }

        @Override
        public NodeSplits visitTableScan(TableScanNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            // get dataSource for table
            DataSource dataSource = splitManager.getSplits(session,
                    node.getTable(),
                    node.getPartitionPredicate(),
                    node.getUpstreamPredicateHint(),
                    tableWriterPartitionPredicate,
                    node.getAssignments());

            return new NodeSplits(node.getId(), dataSource);
        }

        @Override
        public NodeSplits visitJoin(JoinNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            NodeSplits leftSplits = node.getLeft().accept(this, tableWriterPartitionPredicate);
            NodeSplits rightSplits = node.getRight().accept(this, tableWriterPartitionPredicate);
            if (leftSplits.dataSource.isPresent() && rightSplits.dataSource.isPresent()) {
                throw new IllegalArgumentException("Both left and right join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            return leftSplits.dataSource.isPresent() ? leftSplits : rightSplits;
        }

        @Override
        public NodeSplits visitSemiJoin(SemiJoinNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            NodeSplits sourceSplits = node.getSource().accept(this, tableWriterPartitionPredicate);
            NodeSplits filteringSourceSplits = node.getFilteringSource().accept(this, tableWriterPartitionPredicate);
            if (sourceSplits.dataSource.isPresent() && filteringSourceSplits.dataSource.isPresent()) {
                throw new IllegalArgumentException("Both source and filteringSource semi join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            return sourceSplits.dataSource.isPresent() ? sourceSplits : filteringSourceSplits;
        }

        @Override
        public NodeSplits visitExchange(ExchangeNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            // exchange node does not have splits
            return new NodeSplits(node.getId());
        }

        @Override
        public NodeSplits visitFilter(FilterNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitAggregation(AggregationNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitWindow(WindowNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitProject(ProjectNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitTopN(TopNNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitOutput(OutputNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitLimit(LimitNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitSort(SortNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitSink(SinkNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            return node.getSource().accept(this, tableWriterPartitionPredicate);
        }

        @Override
        public NodeSplits visitTableWriter(final TableWriterNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            TableWriter tableWriter = new TableWriter(node, shardManager);

            // get source splits
            NodeSplits nodeSplits = node.getSource().accept(this, tableWriter.getPartitionPredicate());
            checkState(nodeSplits.dataSource.isPresent(), "No splits present for import");
            DataSource dataSource = nodeSplits.dataSource.get();

            // record output
            outputReceivers.put(node.getId(), tableWriter.getOutputReceiver());

            // wrap splits with table writer info
            Iterable<Split> newSplits = tableWriter.wrapSplits(nodeSplits.planNodeId, dataSource.getSplits());
            return new NodeSplits(node.getId(), new DataSource(dataSource.getDataSourceName(), newSplits));
        }

        @Override
        protected NodeSplits visitPlan(PlanNode node, Predicate<Partition> tableWriterPartitionPredicate)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }

    private class NodeSplits
    {
        private final PlanNodeId planNodeId;
        private final Optional<DataSource> dataSource;

        private NodeSplits(PlanNodeId planNodeId)
        {
            this.planNodeId = planNodeId;
            this.dataSource = Optional.absent();
        }

        private NodeSplits(PlanNodeId planNodeId, DataSource dataSource)
        {
            this.planNodeId = planNodeId;
            this.dataSource = Optional.of(dataSource);
        }
    }
}

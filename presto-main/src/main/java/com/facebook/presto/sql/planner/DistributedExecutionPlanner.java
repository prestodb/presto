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
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MaterializedViewWriterNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DistributedExecutionPlanner
{
    private final SplitManager splitManager;
    private final ShardManager shardManager;

    @Inject
    public DistributedExecutionPlanner(SplitManager splitManager, ShardManager shardManager)
    {
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.shardManager = checkNotNull(shardManager, "databaseShardManager is null");
    }

    public StageExecutionPlan plan(SubPlan root)
    {
        return plan(root, Predicates.<Partition>alwaysTrue());
    }

    public StageExecutionPlan plan(SubPlan root, Predicate<Partition> materializedViewPartitionPredicate)
    {
        PlanFragment currentFragment = root.getFragment();

        // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
        Visitor visitor = new Visitor();
        NodeSplits nodeSplits = currentFragment.getRoot().accept(visitor, materializedViewPartitionPredicate);

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            dependencies.add(plan(childPlan, materializedViewPartitionPredicate));
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
        public NodeSplits visitTableScan(TableScanNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            List<Partition> partitions = FluentIterable.from(getPartitions(node))
                    .filter(materializedViewPartitionPredicate)
                    .toList();

            // get dataSource for table
            DataSource dataSource = splitManager.getPartitionSplits(node.getTable(), partitions);

            return new NodeSplits(node.getId(), dataSource);
        }

        private List<Partition> getPartitions(TableScanNode node)
        {
            if (node.getGeneratedPartitions().isPresent()) {
                return node.getGeneratedPartitions().get().getPartitions();
            }

            PartitionResult allPartitions = splitManager.getPartitions(node.getTable(), Optional.<TupleDomain>absent());
            return allPartitions.getPartitions();
        }

        @Override
        public NodeSplits visitJoin(JoinNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            NodeSplits leftSplits = node.getLeft().accept(this, materializedViewPartitionPredicate);
            NodeSplits rightSplits = node.getRight().accept(this, materializedViewPartitionPredicate);
            if (leftSplits.dataSource.isPresent() && rightSplits.dataSource.isPresent()) {
                throw new IllegalArgumentException("Both left and right join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            return leftSplits.dataSource.isPresent() ? leftSplits : rightSplits;
        }

        @Override
        public NodeSplits visitSemiJoin(SemiJoinNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            NodeSplits sourceSplits = node.getSource().accept(this, materializedViewPartitionPredicate);
            NodeSplits filteringSourceSplits = node.getFilteringSource().accept(this, materializedViewPartitionPredicate);
            if (sourceSplits.dataSource.isPresent() && filteringSourceSplits.dataSource.isPresent()) {
                throw new IllegalArgumentException("Both source and filteringSource semi join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            return sourceSplits.dataSource.isPresent() ? sourceSplits : filteringSourceSplits;
        }

        @Override
        public NodeSplits visitExchange(ExchangeNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            // exchange node does not have splits
            return new NodeSplits(node.getId());
        }

        @Override
        public NodeSplits visitFilter(FilterNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitSample(SampleNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            switch(node.getSampleType()) {
                case BERNOULLI:
                    return node.getSource().accept(this, materializedViewPartitionPredicate);

                case SYSTEM: {
                    NodeSplits nodeSplits = node.getSource().accept(this, materializedViewPartitionPredicate);
                    if (nodeSplits.dataSource.isPresent()) {
                        DataSource dataSource = nodeSplits.dataSource.get();
                        final double ratio = node.getSampleRatio();
                        Iterable<Split> sampleIterable = Iterables.filter(dataSource.getSplits(), new Predicate<Split>()
                        {
                            public boolean apply(@Nullable Split input)
                            {
                                return ThreadLocalRandom.current().nextDouble() < ratio;
                            }
                        });
                        DataSource sampledDataSource = new DataSource(dataSource.getDataSourceName(), sampleIterable);

                        return new NodeSplits(node.getId(), sampledDataSource);
                    }
                    else {
                        // table sampling on a sub query without splits is meaningless
                        return nodeSplits;
                    }
                }
                default:
                    throw new UnsupportedOperationException("Sampling is not supported for type " + node.getSampleType());
            }
        }

        @Override
        public NodeSplits visitAggregation(AggregationNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitWindow(WindowNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitProject(ProjectNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitTopN(TopNNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitOutput(OutputNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitLimit(LimitNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitSort(SortNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitSink(SinkNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitTableWriter(TableWriterNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitTableCommit(TableCommitNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            return node.getSource().accept(this, materializedViewPartitionPredicate);
        }

        @Override
        public NodeSplits visitMaterializedViewWriter(MaterializedViewWriterNode node, Predicate<Partition> materializedViewPartitionPredicate)
        {
            MaterializedViewWriter materializedViewWriter = new MaterializedViewWriter(node, shardManager);

            // get source splits
            NodeSplits nodeSplits = node.getSource().accept(this, materializedViewWriter.getPartitionPredicate());
            checkState(nodeSplits.dataSource.isPresent(), "No splits present for import");
            DataSource dataSource = nodeSplits.dataSource.get();

            // record output
            outputReceivers.put(node.getId(), materializedViewWriter.getOutputReceiver());

            // wrap splits with table writer info
            Iterable<Split> newSplits = materializedViewWriter.wrapSplits(nodeSplits.planNodeId, dataSource.getSplits());
            return new NodeSplits(node.getId(), new DataSource(dataSource.getDataSourceName(), newSplits));
        }

        @Override
        protected NodeSplits visitPlan(PlanNode node, Predicate<Partition> materializedViewPartitionPredicate)
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

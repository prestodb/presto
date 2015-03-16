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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

class PropertyDerivations
{
    private static final Visitor INSTANCE = new Visitor();

    private PropertyDerivations() {}

    public static ActualProperties deriveProperties(PlanNode node, ActualProperties inputProperties)
    {
        return deriveProperties(node, ImmutableList.of(inputProperties));
    }

    public static ActualProperties deriveProperties(PlanNode node, List<ActualProperties> inputProperties)
    {
        return node.accept(INSTANCE, inputProperties);
    }

    private static class Visitor
            extends PlanVisitor<List<ActualProperties>, ActualProperties>
    {
        @Override
        protected ActualProperties visitPlan(PlanNode node, List<ActualProperties> inputProperties)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitOutput(OutputNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitMarkDistinct(MarkDistinctNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);
            checkState(properties.isPartitionedOnKeys(node.getDistinctSymbols()), "Expected input to be partition on DISTINCT columns");
            return properties;
        }

        @Override
        public ActualProperties visitWindow(WindowNode node, List<ActualProperties> inputProperties)
        {
            Set<Symbol> grouping = ImmutableSet.<Symbol>builder()
                    .addAll(node.getOrderBy())
                    .addAll(node.getPartitionBy())
                    .build();

            ActualProperties properties = Iterables.getOnlyElement(inputProperties);
            return new ActualProperties(properties.getPartitioning(), properties.getPlacement(), GroupingProperties.grouped(grouping));
        }

        @Override
        public ActualProperties visitAggregation(AggregationNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return new ActualProperties(
                    properties.getPartitioning(),
                    properties.getPlacement(),
                    GroupingProperties.grouped(ImmutableSet.copyOf(node.getGroupBy())));
        }

        @Override
        public ActualProperties visitRowNumber(RowNumberNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return new ActualProperties(
                    properties.getPartitioning(),
                    properties.getPlacement(),
                    GroupingProperties.grouped(ImmutableSet.copyOf(node.getPartitionBy())));
        }

        @Override
        public ActualProperties visitTopNRowNumber(TopNRowNumberNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            Set<Symbol> grouping = ImmutableSet.<Symbol>builder()
                    .addAll(node.getPartitionBy())
                    .addAll(node.getOrderBy())
                    .build();

            return new ActualProperties(
                    properties.getPartitioning(),
                    properties.getPlacement(),
                    GroupingProperties.grouped(grouping));
        }

        @Override
        public ActualProperties visitTopN(TopNNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return new ActualProperties(
                    properties.getPartitioning(),
                    properties.getPlacement(),
                    GroupingProperties.grouped(ImmutableSet.copyOf(node.getOrderBy())));
        }

        @Override
        public ActualProperties visitSort(SortNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return new ActualProperties(
                    properties.getPartitioning(),
                    properties.getPlacement(),
                    GroupingProperties.grouped(ImmutableSet.copyOf(node.getOrderBy())));
        }

        @Override
        public ActualProperties visitLimit(LimitNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitDistinctLimit(DistinctLimitNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return new ActualProperties(
                    properties.getPartitioning(),
                    properties.getPlacement(),
                    GroupingProperties.grouped(ImmutableSet.copyOf(node.getDistinctSymbols())));
        }

        @Override
        public ActualProperties visitTableCommit(TableCommitNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return new ActualProperties(
                    properties.getPartitioning(),
                    properties.getPlacement(),
                    GroupingProperties.ungrouped());
        }

        @Override
        public ActualProperties visitJoin(JoinNode node, List<ActualProperties> inputProperties)
        {
            return inputProperties.get(0);
        }

        @Override
        public ActualProperties visitSemiJoin(SemiJoinNode node, List<ActualProperties> inputProperties)
        {
            return inputProperties.get(0);
        }

        @Override
        public ActualProperties visitIndexJoin(IndexJoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = inputProperties.get(0);

            // TODO: does index join preserve grouping?
            return new ActualProperties(properties.getPartitioning(), properties.getPlacement(), GroupingProperties.ungrouped());
        }

        @Override
        public ActualProperties visitExchange(ExchangeNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = inputProperties.get(0);

            switch (node.getType()) {
                case GATHER:
                    return new ActualProperties(PartitioningProperties.unpartitioned(), PlacementProperties.anywhere(), GroupingProperties.ungrouped());
                case REPARTITION:
                    return new ActualProperties(PartitioningProperties.partitioned(node.getPartitionKeys(), node.getHashSymbol()), PlacementProperties.anywhere(), GroupingProperties.ungrouped());
                case REPLICATE:
                    return new ActualProperties(properties.getPartitioning(), PlacementProperties.anywhere(), GroupingProperties.ungrouped());
            }

            throw new UnsupportedOperationException("not yet implemented");
        }
    }
}

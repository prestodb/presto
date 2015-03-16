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
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitWindow(WindowNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitAggregation(AggregationNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            if (properties.isUnpartitioned()) {
                return ActualProperties.builder()
                        .unpartitioned()
                        .coordinatorOnly(properties)
                        .build();
            }

            return ActualProperties.partitioned(properties);
        }

        @Override
        public ActualProperties visitRowNumber(RowNumberNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitTopNRowNumber(TopNRowNumberNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitTopN(TopNNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitSort(SortNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitLimit(LimitNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitDistinctLimit(DistinctLimitNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitTableCommit(TableCommitNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return ActualProperties.builder()
                    .unpartitioned()
                    .coordinatorOnly(properties)
                    .build();
        }

        @Override
        public ActualProperties visitJoin(JoinNode node, List<ActualProperties> inputProperties)
        {
            // TODO: include all equivalent columns in partitioning properties
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
            return inputProperties.get(0);
        }

        @Override
        public ActualProperties visitExchange(ExchangeNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = inputProperties.get(0);

            switch (node.getType()) {
                case GATHER:
                    return ActualProperties.unpartitioned();
                case REPARTITION:
                    return ActualProperties.hashPartitioned(node.getPartitionKeys());
                case REPLICATE:
                    return ActualProperties.partitioned(properties);
            }

            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public ActualProperties visitFilter(FilterNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitProject(ProjectNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            if (properties.isHashPartitioned()) {
                Optional<List<Symbol>> translated = translate(properties.getHashPartitioningColumns().get(), computeIdentityTranslations(node.getAssignments()));

                if (translated.isPresent()) {
                    return ActualProperties.builder()
                            .coordinatorOnly(properties)
                            .hashPartitioned(translated.get())
                            .build();
                }
            }

            if (properties.hasKnownPartitioningScheme()) {
                Optional<List<Symbol>> translated = translate(properties.getPartitioningColumns().get(), computeIdentityTranslations(node.getAssignments()));

                if (translated.isPresent()) {
                    return ActualProperties.builder()
                            .coordinatorOnly(properties)
                            .partitioned(ImmutableSet.copyOf(translated.get()))
                            .build();
                }
            }

            return ActualProperties.builder()
                    .coordinatorOnly(properties)
                    .partitioned()
                    .build();
        }

        @Override
        public ActualProperties visitTableWriter(TableWriterNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ActualProperties.Builder derived = ActualProperties.builder()
                    .coordinatorOnly(properties);

            if (properties.isUnpartitioned()) {
                derived.unpartitioned();
            }
            else {
                derived.partitioned();
            }

            return derived.build();
        }

        @Override
        public ActualProperties visitSample(SampleNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitUnnest(UnnestNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, Expression> assignments)
        {
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
                if (assignment.getValue() instanceof QualifiedNameReference) {
                    inputToOutput.put(Symbol.fromQualifiedName(((QualifiedNameReference) assignment.getValue()).getName()), assignment.getKey());
                }
            }
            return inputToOutput;
        }

        /**
         * @return Optional.empty() if not all columns could be translated
         */
        private static Optional<List<Symbol>> translate(Collection<Symbol> columns, Map<Symbol, Symbol> mappings)
        {
            ImmutableList.Builder<Symbol> builder = ImmutableList.builder();

            for (Symbol column : columns) {
                Symbol translated = mappings.get(column);
                if (translated == null) {
                    return Optional.empty();
                }
                builder.add(translated);
            }

            return Optional.of(builder.build());
        }
    }
}

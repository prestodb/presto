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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SetOperationNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.optimizations.WindowNodeUtil;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeJoinNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.StarJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer.IndexKeyTracer;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Ensures that all dependencies (i.e., symbols in expressions) for a plan node are provided by its source nodes
 */
public final class ValidateDependenciesChecker
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        validate(plan, types);
    }

    public static void validate(PlanNode plan, TypeProvider types)
    {
        plan.accept(new Visitor(types), ImmutableSet.of());
    }

    private static class Visitor
            extends InternalPlanVisitor<Void, Set<VariableReferenceExpression>>
    {
        private final TypeProvider types;

        public Visitor(TypeProvider types)
        {
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public Void visitPlan(PlanNode node, Set<VariableReferenceExpression> boundVariables)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getGroupingKeys(), "Invalid node. Grouping key variables (%s) not in source plan output (%s)", node.getGroupingKeys(), node.getSource().getOutputVariables());

            for (Aggregation aggregation : node.getAggregations().values()) {
                Set<VariableReferenceExpression> dependencies = extractAggregationUniqueVariables(aggregation, types);
                checkDependencies(inputs, dependencies, "Invalid node. Aggregation dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputVariables());
                aggregation.getMask().ifPresent(mask -> {
                    checkDependencies(inputs, ImmutableSet.of(mask), "Invalid node. Aggregation mask symbol (%s) not in source plan output (%s)", mask, node.getSource().getOutputVariables());
                });
            }

            return null;
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getInputVariables(), "Invalid node. Grouping symbols (%s) not in source plan output (%s)", node.getInputVariables(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getDistinctVariables(), "Invalid node. Mark distinct symbols (%s) not in source plan output (%s)", node.getDistinctVariables(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);

            checkDependencies(inputs, node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputVariables());
            if (node.getOrderingScheme().isPresent()) {
                checkDependencies(
                        inputs,
                        node.getOrderingScheme().get().getOrderByVariables(),
                        "Invalid node. Order by symbols (%s) not in source plan output (%s)",
                        node.getOrderingScheme().get().getOrderByVariables(), node.getSource().getOutputVariables());
            }

            ImmutableList.Builder<VariableReferenceExpression> bounds = ImmutableList.builder();
            for (WindowNode.Frame frame : node.getFrames()) {
                if (frame.getStartValue().isPresent()) {
                    bounds.add(frame.getStartValue().get());
                }
                if (frame.getEndValue().isPresent()) {
                    bounds.add(frame.getEndValue().get());
                }
            }
            checkDependencies(inputs, bounds.build(), "Invalid node. Frame bounds (%s) not in source plan output (%s)", bounds.build(), node.getSource().getOutputVariables());

            ImmutableList.Builder<VariableReferenceExpression> symbolsForFrameBoundsComparison = ImmutableList.builder();
            for (WindowNode.Frame frame : node.getFrames()) {
                if (frame.getSortKeyCoercedForFrameStartComparison().isPresent()) {
                    symbolsForFrameBoundsComparison.add(frame.getSortKeyCoercedForFrameStartComparison().get());
                }
                if (frame.getSortKeyCoercedForFrameEndComparison().isPresent()) {
                    symbolsForFrameBoundsComparison.add(frame.getSortKeyCoercedForFrameEndComparison().get());
                }
            }
            checkDependencies(inputs, symbolsForFrameBoundsComparison.build(), "Invalid node. Symbols for frame bound comparison (%s) not in source plan output (%s)", symbolsForFrameBoundsComparison.build(), node.getSource().getOutputVariables());

            for (WindowNode.Function function : node.getWindowFunctions().values()) {
                Set<VariableReferenceExpression> dependencies = WindowNodeUtil.extractWindowFunctionUniqueVariables(function, types);
                checkDependencies(inputs, dependencies, "Invalid node. Window function dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputVariables());
            }

            return null;
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputVariables());
            checkDependencies(
                    inputs,
                    node.getOrderingScheme().getOrderByVariables(),
                    "Invalid node. Order by symbols (%s) not in source plan output (%s)",
                    node.getOrderingScheme().getOrderByVariables(), node.getSource().getOutputVariables());

            return null;
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getPartitionBy(), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getOutputVariables(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputVariables(), node.getSource().getOutputVariables());

            // Only verify names here as filter expression would contain type cast, which will be translated to an non-existent variable in
            // SqlToRowExpressionTranslator
            // TODO https://github.com/prestodb/presto/issues/12892
            Set<String> dependencies;
            if (isExpression(node.getPredicate())) {
                dependencies = VariablesExtractor.extractUnique(castToExpression(node.getPredicate()), types).stream().map(VariableReferenceExpression::getName).collect(toImmutableSet());
            }
            else {
                dependencies = VariablesExtractor.extractUnique(node.getPredicate()).stream().map(VariableReferenceExpression::getName).collect(toImmutableSet());
            }
            checkArgument(
                    inputs.stream().map(VariableReferenceExpression::getName).collect(toImmutableSet()).containsAll(dependencies),
                    "Symbol from filter (%s) not in sources (%s)",
                    dependencies,
                    inputs);

            return null;
        }

        @Override
        public Void visitSample(SampleNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            for (RowExpression expression : node.getAssignments().getExpressions()) {
                Set<VariableReferenceExpression> dependencies;
                if (isExpression(expression)) {
                    dependencies = VariablesExtractor.extractUnique(castToExpression(expression), types);
                }
                else {
                    dependencies = VariablesExtractor.extractUnique(expression);
                }
                checkDependencies(inputs, dependencies, "Invalid node. Expression dependencies (%s) not in source plan output (%s)", dependencies, inputs);
            }

            return null;
        }

        @Override
        public Void visitTopN(TopNNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getOutputVariables(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputVariables(), node.getSource().getOutputVariables());
            checkDependencies(
                    inputs,
                    node.getOrderingScheme().getOrderByVariables(),
                    "Invalid node. Order by dependencies (%s) not in source plan output (%s)",
                    node.getOrderingScheme().getOrderByVariables(),
                    node.getSource().getOutputVariables());

            return null;
        }

        @Override
        public Void visitSort(SortNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            Set<VariableReferenceExpression> inputs = createInputs(source, boundVariables);
            checkDependencies(inputs, node.getOutputVariables(), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputVariables(), node.getSource().getOutputVariables());
            checkDependencies(
                    inputs,
                    node.getOrderingScheme().getOrderByVariables(),
                    "Invalid node. Order by dependencies (%s) not in source plan output (%s)",
                    node.getOrderingScheme().getOrderByVariables(), node.getSource().getOutputVariables());

            return null;
        }

        @Override
        public Void visitOutput(OutputNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getOutputVariables(), "Invalid node. Output column dependencies (%s) not in source plan output (%s)", node.getOutputVariables(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitOffset(OffsetNode node, Set<VariableReferenceExpression> boundSymbols)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundSymbols); // visit child

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkDependencies(source.getOutputVariables(), node.getOutputVariables(), "Invalid node. Output column dependencies (%s) not in source plan output (%s)", node.getOutputVariables(), source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getLeft().accept(this, boundVariables);
            node.getRight().accept(this, boundVariables);

            Set<VariableReferenceExpression> leftInputs = createInputs(node.getLeft(), boundVariables);
            Set<VariableReferenceExpression> rightInputs = createInputs(node.getRight(), boundVariables);
            Set<VariableReferenceExpression> allInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(leftInputs)
                    .addAll(rightInputs)
                    .build();

            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                checkArgument(leftInputs.contains(clause.getLeft()), "Symbol from join clause (%s) not in left source (%s)", clause.getLeft(), node.getLeft().getOutputVariables());
                checkArgument(rightInputs.contains(clause.getRight()), "Symbol from join clause (%s) not in right source (%s)", clause.getRight(), node.getRight().getOutputVariables());
            }

            node.getFilter().ifPresent(predicate -> {
                // Only verify names here as filter expression would contain type cast, which will be translated to an non-existent variable in
                // SqlToRowExpressionTranslator
                // TODO https://github.com/prestodb/presto/issues/12892
                Set<String> predicateVariables;
                if (isExpression(predicate)) {
                    predicateVariables = VariablesExtractor.extractUnique(castToExpression(predicate), types).stream().map(VariableReferenceExpression::getName).collect(toImmutableSet());
                }
                else {
                    predicateVariables = VariablesExtractor.extractUnique(predicate).stream().map(VariableReferenceExpression::getName).collect(toImmutableSet());
                }
                checkArgument(
                        allInputs.stream().map(VariableReferenceExpression::getName).collect(toImmutableSet()).containsAll(predicateVariables),
                        "Symbol from filter (%s) not in sources (%s)",
                        predicateVariables,
                        allInputs);
            });

            checkLeftOutputVariablesBeforeRight(node.getLeft().getOutputVariables(), node.getOutputVariables());
            return null;
        }

        @Override
        public Void visitStarJoin(StarJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getLeft().accept(this, boundVariables); // visit child
            node.getRight().forEach(x -> x.accept(this, boundVariables));

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables);
            node.getFilteringSource().accept(this, boundVariables);

            checkArgument(node.getSource().getOutputVariables().contains(node.getSourceJoinVariable()), "Symbol from semi join clause (%s) not in source (%s)", node.getSourceJoinVariable(), node.getSource().getOutputVariables());
            checkArgument(node.getFilteringSource().getOutputVariables().contains(node.getFilteringSourceJoinVariable()), "Symbol from semi join clause (%s) not in filtering source (%s)", node.getSourceJoinVariable(), node.getFilteringSource().getOutputVariables());

            Set<VariableReferenceExpression> outputs = createInputs(node, boundVariables);
            checkArgument(outputs.containsAll(node.getSource().getOutputVariables()), "Semi join output symbols (%s) must contain all of the source symbols (%s)", node.getOutputVariables(), node.getSource().getOutputVariables());
            checkArgument(outputs.contains(node.getSemiJoinOutput()),
                    "Semi join output symbols (%s) must contain join result (%s)",
                    node.getOutputVariables(),
                    node.getSemiJoinOutput());

            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getLeft().accept(this, boundVariables);
            node.getRight().accept(this, boundVariables);

            Set<VariableReferenceExpression> leftInputs = createInputs(node.getLeft(), boundVariables);
            Set<VariableReferenceExpression> rightInputs = createInputs(node.getRight(), boundVariables);
            Set<VariableReferenceExpression> allInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(leftInputs)
                    .addAll(rightInputs)
                    .build();

            Set<VariableReferenceExpression> predicateVariables;
            if (isExpression(node.getFilter())) {
                predicateVariables = VariablesExtractor.extractUnique(castToExpression(node.getFilter()), types);
            }
            else {
                predicateVariables = VariablesExtractor.extractUnique(node.getFilter());
            }

            checkArgument(
                    allInputs.containsAll(predicateVariables),
                    "Symbol from filter (%s) not in sources (%s)",
                    predicateVariables,
                    allInputs);

            checkLeftOutputVariablesBeforeRight(node.getLeft().getOutputVariables(), node.getOutputVariables());
            return null;
        }

        @Override
        public Void visitMergeJoin(MergeJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getLeft().accept(this, boundVariables);
            node.getRight().accept(this, boundVariables);

            Set<VariableReferenceExpression> leftInputs = createInputs(node.getLeft(), boundVariables);
            Set<VariableReferenceExpression> rightInputs = createInputs(node.getRight(), boundVariables);
            Set<VariableReferenceExpression> allInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(leftInputs)
                    .addAll(rightInputs)
                    .build();

            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                checkArgument(leftInputs.contains(clause.getLeft()), "Symbol from join clause (%s) not in left source (%s)", clause.getLeft(), node.getLeft().getOutputVariables());
                checkArgument(rightInputs.contains(clause.getRight()), "Symbol from join clause (%s) not in right source (%s)", clause.getRight(), node.getRight().getOutputVariables());
            }

            return null;
        }

        private void checkLeftOutputVariablesBeforeRight(List<VariableReferenceExpression> leftVariables, List<VariableReferenceExpression> outputVariables)
        {
            int leftMaxPosition = -1;
            Optional<Integer> rightMinPosition = Optional.empty();
            Set<VariableReferenceExpression> leftVariablesSet = new HashSet<>(leftVariables);
            for (int i = 0; i < outputVariables.size(); i++) {
                VariableReferenceExpression variable = outputVariables.get(i);
                if (leftVariablesSet.contains(variable)) {
                    leftMaxPosition = i;
                }
                else if (!rightMinPosition.isPresent()) {
                    rightMinPosition = Optional.of(i);
                }
            }
            checkState(!rightMinPosition.isPresent() || rightMinPosition.get() > leftMaxPosition, "Not all left output variables are before right output variables");
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getProbeSource().accept(this, boundVariables);
            node.getIndexSource().accept(this, boundVariables);

            Set<VariableReferenceExpression> probeInputs = createInputs(node.getProbeSource(), boundVariables);
            Set<VariableReferenceExpression> indexSourceInputs = createInputs(node.getIndexSource(), boundVariables);
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                checkArgument(probeInputs.contains(clause.getProbe()), "Probe variable from index join clause (%s) not in probe source (%s)", clause.getProbe(), node.getProbeSource().getOutputVariables());
                checkArgument(indexSourceInputs.contains(clause.getIndex()), "Index variable from index join clause (%s) not in index source (%s)", clause.getIndex(), node.getIndexSource().getOutputVariables());
            }

            Set<VariableReferenceExpression> lookupVariables = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .collect(toImmutableSet());
            Map<VariableReferenceExpression, VariableReferenceExpression> trace = IndexKeyTracer.trace(node.getIndexSource(), lookupVariables);
            checkArgument(!trace.isEmpty() && lookupVariables.containsAll(trace.keySet()),
                    "Index lookup symbols are not traceable to index source: %s",
                    lookupVariables);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Set<VariableReferenceExpression> boundVariables)
        {
            checkDependencies(
                    node.getOutputVariables(),
                    node.getLookupVariables(),
                    "Lookup variables must be part of output symbols");
            checkDependencies(
                    node.getAssignments().keySet(),
                    node.getOutputVariables(),
                    "Assignments must contain mappings for output symbols");

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Set<VariableReferenceExpression> boundVariables)
        {
            //We don't have to do a check here as TableScanNode has no dependencies.
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Set<VariableReferenceExpression> boundVariables)
        {
            Set<VariableReferenceExpression> correlatedDependencies = VariablesExtractor.extractUnique(node, types);
            checkDependencies(
                    boundVariables,
                    correlatedDependencies,
                    "Invalid node. Expression correlated dependencies (%s) not satisfied by (%s)",
                    correlatedDependencies,
                    boundVariables);
            return null;
        }

        @Override
        public Void visitUnnest(UnnestNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables);

            Set<VariableReferenceExpression> required = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(node.getReplicateVariables())
                    .addAll(node.getUnnestVariables().keySet())
                    .build();

            checkDependencies(source.getOutputVariables(), required, "Invalid node. Dependencies (%s) not in source plan output (%s)", required, source.getOutputVariables());

            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Set<VariableReferenceExpression> boundVariables)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                checkDependencies(
                        subplan.getOutputVariables(),
                        node.getInputs().get(i),
                        "EXCHANGE subplan must provide all of the necessary symbols");
                subplan.accept(this, boundVariables); // visit child
            }

            checkDependencies(
                    node.getOutputVariables(),
                    node.getPartitioningScheme().getOutputLayout(),
                    "EXCHANGE must provide all of the necessary symbols for partition function");

            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitTableWriteMerge(TableWriterMergeNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitDelete(DeleteNode node, Set<VariableReferenceExpression> boundVariables)
        {
            PlanNode source = node.getSource();
            source.accept(this, boundVariables); // visit child

            checkArgument(source.getOutputVariables().contains(node.getRowId()), "Invalid node. Row ID symbol (%s) is not in source plan output (%s)", node.getRowId(), node.getSource().getOutputVariables());

            return null;
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return null;
        }

        @Override
        public Void visitStatisticsWriterNode(StatisticsWriterNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables); // visit child

            StatisticAggregationsDescriptor<VariableReferenceExpression> descriptor = node.getDescriptor();
            Set<VariableReferenceExpression> dependencies = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(descriptor.getGrouping().values())
                    .addAll(descriptor.getColumnStatistics().values())
                    .addAll(descriptor.getTableStatistics().values())
                    .build();
            List<VariableReferenceExpression> outputVariables = node.getSource().getOutputVariables();
            checkDependencies(dependencies, dependencies, "Invalid node. Dependencies (%s) not in source plan output (%s)", dependencies, outputVariables);
            return null;
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return visitSetOperation(node, boundVariables);
        }

        private Void visitSetOperation(SetOperationNode node, Set<VariableReferenceExpression> boundVariables)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                checkDependencies(subplan.getOutputVariables(), node.sourceOutputLayout(i), "%s subplan must provide all of the necessary symbols", node.getClass().getSimpleName());
                subplan.accept(this, boundVariables); // visit child
            }

            return null;
        }

        @Override
        public Void visitIntersect(IntersectNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return visitSetOperation(node, boundVariables);
        }

        @Override
        public Void visitExcept(ExceptNode node, Set<VariableReferenceExpression> boundVariables)
        {
            return visitSetOperation(node, boundVariables);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Set<VariableReferenceExpression> boundVariables)
        {
            node.getSource().accept(this, boundVariables); // visit child

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Set<VariableReferenceExpression> boundVariables)
        {
            Set<VariableReferenceExpression> subqueryCorrelation = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(boundVariables)
                    .addAll(node.getCorrelation())
                    .build();

            node.getInput().accept(this, boundVariables); // visit child
            node.getSubquery().accept(this, subqueryCorrelation); // visit child

            checkDependencies(node.getInput().getOutputVariables(), node.getCorrelation(), "APPLY input must provide all the necessary correlation variables for subquery");
            checkDependencies(VariablesExtractor.extractUnique(node.getSubquery(), types), node.getCorrelation(), "not all APPLY correlation symbols are used in subquery");

            ImmutableSet<VariableReferenceExpression> inputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(createInputs(node.getSubquery(), boundVariables))
                    .addAll(createInputs(node.getInput(), boundVariables))
                    .build();

            for (RowExpression expression : node.getSubqueryAssignments().getExpressions()) {
                Set<VariableReferenceExpression> dependencies;
                if (isExpression(expression)) {
                    dependencies = VariablesExtractor.extractUnique(castToExpression(expression), types);
                }
                else {
                    dependencies = VariablesExtractor.extractUnique(expression);
                }
                checkDependencies(inputs, dependencies, "Invalid node. Expression dependencies (%s) not in source plan output (%s)", dependencies, inputs);
            }

            return null;
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Set<VariableReferenceExpression> boundVariables)
        {
            Set<VariableReferenceExpression> subqueryCorrelation = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(boundVariables)
                    .addAll(node.getCorrelation())
                    .build();

            node.getInput().accept(this, boundVariables); // visit child
            node.getSubquery().accept(this, subqueryCorrelation); // visit child

            checkDependencies(
                    node.getInput().getOutputVariables(),
                    node.getCorrelation(),
                    "LATERAL input must provide all the necessary correlation symbols for subquery");
            checkDependencies(
                    VariablesExtractor.extractUnique(node.getSubquery(), types),
                    node.getCorrelation(),
                    "not all LATERAL correlation symbols are used in subquery");

            return null;
        }

        private static ImmutableSet<VariableReferenceExpression> createInputs(PlanNode source, Set<VariableReferenceExpression> boundVariables)
        {
            return ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(source.getOutputVariables())
                    .addAll(boundVariables)
                    .build();
        }

        private void checkDependencies(Collection<VariableReferenceExpression> inputs, Collection<VariableReferenceExpression> required, String message, Object... parameters)
        {
            checkArgument(ImmutableSet.copyOf(inputs).containsAll(required), message, parameters);
        }
    }
}

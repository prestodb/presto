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

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SetOperationNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregations;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils.fromListMultimap;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Removes all computation that does is not referenced transitively from the root of the plan
 * <p>
 * E.g.,
 * <p>
 * {@code Output[$0] -> Project[$0 := $1 + $2, $3 = $4 / $5] -> ...}
 * <p>
 * gets rewritten as
 * <p>
 * {@code Output[$0] -> Project[$0 := $1 + $2] -> ...}
 */
public class PruneUnreferencedOutputs
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(variableAllocator), plan, ImmutableSet.of());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Set<VariableReferenceExpression>>
    {
        private final PlanVariableAllocator variableAllocator;

        public Rewriter(PlanVariableAllocator variableAllocator)
        {
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            return context.defaultRewrite(node, ImmutableSet.copyOf(node.getSource().getOutputVariables()));
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedOutputVariables = Sets.newHashSet(context.get());
            node.getPartitioningScheme().getHashColumn().ifPresent(expectedOutputVariables::add);
            node.getPartitioningScheme().getPartitioning().getVariableReferences()
                    .forEach(expectedOutputVariables::add);
            node.getOrderingScheme().ifPresent(orderingScheme -> expectedOutputVariables.addAll(orderingScheme.getOrderByVariables()));

            List<List<VariableReferenceExpression>> inputsBySource = new ArrayList<>(node.getInputs().size());
            for (int i = 0; i < node.getInputs().size(); i++) {
                inputsBySource.add(new ArrayList<>());
            }

            List<VariableReferenceExpression> newOutputVariables = new ArrayList<>(node.getOutputVariables().size());
            for (int i = 0; i < node.getOutputVariables().size(); i++) {
                VariableReferenceExpression outputVariable = node.getOutputVariables().get(i);
                if (expectedOutputVariables.contains(outputVariable)) {
                    newOutputVariables.add(outputVariable);
                    for (int source = 0; source < node.getInputs().size(); source++) {
                        inputsBySource.get(source).add(node.getInputs().get(source).get(i));
                    }
                }
            }

            // newOutputVariables contains all partition, sort and hash variables so simply swap the output layout
            PartitioningScheme partitioningScheme = new PartitioningScheme(
                    node.getPartitioningScheme().getPartitioning(),
                    newOutputVariables,
                    node.getPartitioningScheme().getHashColumn(),
                    node.getPartitioningScheme().isReplicateNullsAndAny(),
                    node.getPartitioningScheme().getBucketToPartition());

            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(inputsBySource.get(i));

                rewrittenSources.add(context.rewrite(
                        node.getSources().get(i),
                        expectedInputs.build()));
            }

            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    partitioningScheme,
                    rewrittenSources.build(),
                    inputsBySource,
                    node.isEnsureSourceOrdering(),
                    node.getOrderingScheme());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedFilterInputs = new HashSet<>();
            if (node.getFilter().isPresent()) {
                if (isExpression(node.getFilter().get())) {
                    expectedFilterInputs = ImmutableSet.<VariableReferenceExpression>builder()
                            .addAll(VariablesExtractor.extractUnique(castToExpression(node.getFilter().get()), variableAllocator.getTypes()))
                            .addAll(context.get())
                            .build();
                }
                else {
                    expectedFilterInputs = ImmutableSet.<VariableReferenceExpression>builder()
                            .addAll(VariablesExtractor.extractUnique(node.getFilter().get()))
                            .addAll(context.get())
                            .build();
                }
            }

            ImmutableSet.Builder<VariableReferenceExpression> leftInputsBuilder = ImmutableSet.builder();
            leftInputsBuilder.addAll(context.get()).addAll(Iterables.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft));
            if (node.getLeftHashVariable().isPresent()) {
                leftInputsBuilder.add(node.getLeftHashVariable().get());
            }
            leftInputsBuilder.addAll(expectedFilterInputs);
            Set<VariableReferenceExpression> leftInputs = leftInputsBuilder.build();

            ImmutableSet.Builder<VariableReferenceExpression> rightInputsBuilder = ImmutableSet.builder();
            rightInputsBuilder.addAll(context.get()).addAll(Iterables.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight));
            if (node.getRightHashVariable().isPresent()) {
                rightInputsBuilder.add(node.getRightHashVariable().get());
            }
            rightInputsBuilder.addAll(expectedFilterInputs);
            Set<VariableReferenceExpression> rightInputs = rightInputsBuilder.build();

            PlanNode left = context.rewrite(node.getLeft(), leftInputs);
            PlanNode right = context.rewrite(node.getRight(), rightInputs);

            List<VariableReferenceExpression> outputVariables;
            if (node.isCrossJoin()) {
                // do not prune nested joins output since it is not supported
                // TODO: remove this "if" branch when output symbols selection is supported by nested loop join
                outputVariables = ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build();
            }
            else {
                outputVariables = node.getOutputVariables().stream()
                        .filter(variable -> context.get().contains(variable))
                        .distinct()
                        .collect(toImmutableList());
            }

            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    left,
                    right,
                    node.getCriteria(),
                    outputVariables,
                    node.getFilter(),
                    node.getLeftHashVariable(),
                    node.getRightHashVariable(),
                    node.getDistributionType(),
                    node.getDynamicFilters());
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> sourceInputsBuilder = ImmutableSet.builder();
            sourceInputsBuilder.addAll(context.get()).add(node.getSourceJoinVariable());
            if (node.getSourceHashVariable().isPresent()) {
                sourceInputsBuilder.add(node.getSourceHashVariable().get());
            }
            Set<VariableReferenceExpression> sourceInputs = sourceInputsBuilder.build();

            ImmutableSet.Builder<VariableReferenceExpression> filteringSourceInputBuilder = ImmutableSet.builder();
            filteringSourceInputBuilder.add(node.getFilteringSourceJoinVariable());
            if (node.getFilteringSourceHashVariable().isPresent()) {
                filteringSourceInputBuilder.add(node.getFilteringSourceHashVariable().get());
            }
            Set<VariableReferenceExpression> filteringSourceInputs = filteringSourceInputBuilder.build();

            PlanNode source = context.rewrite(node.getSource(), sourceInputs);
            PlanNode filteringSource = context.rewrite(node.getFilteringSource(), filteringSourceInputs);

            return new SemiJoinNode(node.getId(),
                    source,
                    filteringSource,
                    node.getSourceJoinVariable(),
                    node.getFilteringSourceJoinVariable(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashVariable(),
                    node.getFilteringSourceHashVariable(),
                    node.getDistributionType());
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> filterSymbols;
            if (isExpression(node.getFilter())) {
                filterSymbols = VariablesExtractor.extractUnique(castToExpression(node.getFilter()), variableAllocator.getTypes());
            }
            else {
                filterSymbols = VariablesExtractor.extractUnique(node.getFilter());
            }
            Set<VariableReferenceExpression> requiredInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(filterSymbols)
                    .addAll(context.get())
                    .build();

            ImmutableSet.Builder<VariableReferenceExpression> leftInputs = ImmutableSet.builder();
            node.getLeftPartitionVariable().map(leftInputs::add);

            ImmutableSet.Builder<VariableReferenceExpression> rightInputs = ImmutableSet.builder();
            node.getRightPartitionVariable().map(rightInputs::add);

            PlanNode left = context.rewrite(node.getLeft(), leftInputs.addAll(requiredInputs).build());
            PlanNode right = context.rewrite(node.getRight(), rightInputs.addAll(requiredInputs).build());

            List<VariableReferenceExpression> outputVariables = node.getOutputVariables().stream()
                    .filter(context.get()::contains)
                    .distinct()
                    .collect(toImmutableList());

            return new SpatialJoinNode(node.getId(), node.getType(), left, right, outputVariables, node.getFilter(), node.getLeftPartitionVariable(), node.getRightPartitionVariable(), node.getKdbTree());
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> probeInputsBuilder = ImmutableSet.builder();
            probeInputsBuilder.addAll(context.get())
                    .addAll(Iterables.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe));
            if (node.getProbeHashVariable().isPresent()) {
                probeInputsBuilder.add(node.getProbeHashVariable().get());
            }
            Set<VariableReferenceExpression> probeInputs = probeInputsBuilder.build();

            ImmutableSet.Builder<VariableReferenceExpression> indexInputBuilder = ImmutableSet.builder();
            indexInputBuilder.addAll(context.get())
                    .addAll(Iterables.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getIndex));
            if (node.getIndexHashVariable().isPresent()) {
                indexInputBuilder.add(node.getIndexHashVariable().get());
            }
            Set<VariableReferenceExpression> indexInputs = indexInputBuilder.build();

            PlanNode probeSource = context.rewrite(node.getProbeSource(), probeInputs);
            PlanNode indexSource = context.rewrite(node.getIndexSource(), indexInputs);

            return new IndexJoinNode(node.getId(), node.getType(), probeSource, indexSource, node.getCriteria(), node.getProbeHashVariable(), node.getIndexHashVariable());
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            List<VariableReferenceExpression> newOutputVariables = node.getOutputVariables().stream()
                    .filter(context.get()::contains)
                    .collect(toImmutableList());

            Set<VariableReferenceExpression> newLookupVariables = node.getLookupVariables().stream()
                    .filter(context.get()::contains)
                    .collect(toImmutableSet());

            Map<VariableReferenceExpression, ColumnHandle> newAssignments = newOutputVariables.stream()
                    .collect(toImmutableMap(identity(), node.getAssignments()::get));

            return new IndexSourceNode(node.getId(), node.getIndexHandle(), node.getTableHandle(), newLookupVariables, newOutputVariables, newAssignments, node.getCurrentConstraint());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(node.getGroupingKeys());
            if (node.getHashVariable().isPresent()) {
                expectedInputs.add(node.getHashVariable().get());
            }

            ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();

                if (context.get().contains(variable)) {
                    Aggregation aggregation = entry.getValue();
                    expectedInputs.addAll(extractAggregationUniqueVariables(aggregation, variableAllocator.getTypes()));
                    aggregation.getMask().ifPresent(expectedInputs::add);
                    aggregations.put(variable, aggregation);
                }
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new AggregationNode(node.getId(),
                    source,
                    aggregations.build(),
                    node.getGroupingSets(),
                    ImmutableList.of(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(node.getPartitionBy());

            node.getOrderingScheme().ifPresent(orderingScheme ->
                    orderingScheme.getOrderByVariables()
                            .forEach(expectedInputs::add));

            for (WindowNode.Frame frame : node.getFrames()) {
                if (frame.getStartValue().isPresent()) {
                    expectedInputs.add(frame.getStartValue().get());
                }
                if (frame.getEndValue().isPresent()) {
                    expectedInputs.add(frame.getEndValue().get());
                }
            }

            if (node.getHashVariable().isPresent()) {
                expectedInputs.add(node.getHashVariable().get());
            }

            ImmutableMap.Builder<VariableReferenceExpression, WindowNode.Function> functionsBuilder = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                WindowNode.Function function = entry.getValue();
                if (context.get().contains(variable)) {
                    expectedInputs.addAll(WindowNodeUtil.extractWindowFunctionUniqueVariables(function, variableAllocator.getTypes()));
                    functionsBuilder.put(variable, entry.getValue());
                }
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            Map<VariableReferenceExpression, WindowNode.Function> functions = functionsBuilder.build();

            if (functions.size() == 0) {
                return source;
            }

            return new WindowNode(
                    node.getId(),
                    source,
                    node.getSpecification(),
                    functions,
                    node.getHashVariable(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            List<VariableReferenceExpression> newOutputs = node.getOutputVariables().stream()
                    .filter(context.get()::contains)
                    .collect(toImmutableList());

            Map<VariableReferenceExpression, ColumnHandle> newAssignments = newOutputs.stream()
                    .collect(Collectors.toMap(identity(), node.getAssignments()::get));

            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    newOutputs,
                    newAssignments,
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs;
            if (isExpression(node.getPredicate())) {
                expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(VariablesExtractor.extractUnique(castToExpression(node.getPredicate()), variableAllocator.getTypes()))
                        .addAll(context.get())
                        .build();
            }
            else {
                expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(VariablesExtractor.extractUnique(node.getPredicate()))
                        .addAll(context.get())
                        .build();
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.builder();

            List<VariableReferenceExpression> newAggregationArguments = node.getAggregationArguments().stream()
                    .filter(context.get()::contains)
                    .collect(Collectors.toList());
            expectedInputs.addAll(newAggregationArguments);

            ImmutableList.Builder<List<VariableReferenceExpression>> newGroupingSets = ImmutableList.builder();
            Map<VariableReferenceExpression, VariableReferenceExpression> newGroupingMapping = new HashMap<>();

            for (List<VariableReferenceExpression> groupingSet : node.getGroupingSets()) {
                ImmutableList.Builder<VariableReferenceExpression> newGroupingSet = ImmutableList.builder();

                for (VariableReferenceExpression output : groupingSet) {
                    if (context.get().contains(output)) {
                        newGroupingSet.add(output);
                        newGroupingMapping.putIfAbsent(output, node.getGroupingColumns().get(output));
                        expectedInputs.add(node.getGroupingColumns().get(output));
                    }
                }
                newGroupingSets.add(newGroupingSet.build());
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new GroupIdNode(node.getId(), source, newGroupingSets.build(), newGroupingMapping, newAggregationArguments, node.getGroupIdVariable());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (!context.get().contains(node.getMarkerVariable())) {
                return context.rewrite(node.getSource(), context.get());
            }

            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(node.getDistinctVariables())
                    .addAll(context.get().stream()
                            .filter(variable -> !variable.equals(node.getMarkerVariable()))
                            .collect(toImmutableList()));

            if (node.getHashVariable().isPresent()) {
                expectedInputs.add(node.getHashVariable().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new MarkDistinctNode(node.getId(), source, node.getMarkerVariable(), node.getDistinctVariables(), node.getHashVariable());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            List<VariableReferenceExpression> replicateVariables = node.getReplicateVariables().stream()
                    .filter(context.get()::contains)
                    .collect(toImmutableList());

            Optional<VariableReferenceExpression> ordinalityVariable = node.getOrdinalityVariable();
            if (ordinalityVariable.isPresent() && !context.get().contains(ordinalityVariable.get())) {
                ordinalityVariable = Optional.empty();
            }
            Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables = node.getUnnestVariables();
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(replicateVariables)
                    .addAll(unnestVariables.keySet());

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new UnnestNode(node.getId(), source, replicateVariables, unnestVariables, ordinalityVariable);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.builder();

            Assignments.Builder builder = Assignments.builder();
            node.getAssignments().forEach((variable, expression) -> {
                if (context.get().contains(variable)) {
                    if (isExpression(expression)) {
                        expectedInputs.addAll(VariablesExtractor.extractUnique(castToExpression(expression), variableAllocator.getTypes()));
                    }
                    else {
                        expectedInputs.addAll(VariablesExtractor.extractUnique(expression));
                    }
                    builder.put(variable, expression);
                }
            });

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new ProjectNode(node.getId(), source, builder.build(), node.getLocality());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.copyOf(node.getOutputVariables());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new OutputNode(node.getId(), source, node.getColumnNames(), node.getOutputVariables());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new LimitNode(node.getId(), source, node.getCount(), node.getStep());
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs;
            if (node.getHashVariable().isPresent()) {
                expectedInputs = ImmutableSet.copyOf(concat(node.getDistinctVariables(), ImmutableList.of(node.getHashVariable().get())));
            }
            else {
                expectedInputs = ImmutableSet.copyOf(node.getDistinctVariables());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new DistinctLimitNode(node.getId(), source, node.getLimit(), node.isPartial(), node.getDistinctVariables(), node.getHashVariable());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(node.getOrderingScheme().getOrderByVariables());

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TopNNode(node.getId(), source, node.getCount(), node.getOrderingScheme(), node.getStep());
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> inputsBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = inputsBuilder
                    .addAll(context.get())
                    .addAll(node.getPartitionBy());

            if (node.getHashVariable().isPresent()) {
                inputsBuilder.add(node.getHashVariable().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new RowNumberNode(node.getId(), source, node.getPartitionBy(), node.getRowNumberVariable(), node.getMaxRowCountPerPartition(), node.getHashVariable());
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(node.getPartitionBy())
                    .addAll(node.getOrderingScheme().getOrderByVariables());

            if (node.getHashVariable().isPresent()) {
                expectedInputs.add(node.getHashVariable().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TopNRowNumberNode(node.getId(),
                    source,
                    node.getSpecification(),
                    node.getRowNumberVariable(),
                    node.getMaxRowCountPerPartition(),
                    node.isPartial(),
                    node.getHashVariable());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.copyOf(concat(context.get(), node.getOrderingScheme().getOrderByVariables()));

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new SortNode(node.getId(), source, node.getOrderingScheme(), node.isPartial());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(node.getColumns());
            if (node.getTablePartitioningScheme().isPresent()) {
                PartitioningScheme partitioningScheme = node.getTablePartitioningScheme().get();
                partitioningScheme.getPartitioning().getVariableReferences().forEach(expectedInputs::add);
                partitioningScheme.getHashColumn().ifPresent(expectedInputs::add);
            }
            if (node.getPreferredShufflePartitioningScheme().isPresent()) {
                PartitioningScheme partitioningScheme = node.getPreferredShufflePartitioningScheme().get();
                partitioningScheme.getPartitioning().getVariableReferences().forEach(expectedInputs::add);
                partitioningScheme.getHashColumn().ifPresent(expectedInputs::add);
            }
            if (node.getStatisticsAggregation().isPresent()) {
                StatisticAggregations aggregations = node.getStatisticsAggregation().get();
                expectedInputs.addAll(aggregations.getGroupingVariables());
                aggregations.getAggregations()
                        .values()
                        .forEach(aggregation -> expectedInputs.addAll(extractAggregationUniqueVariables(aggregation, variableAllocator.getTypes())));
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new TableWriterNode(
                    node.getId(),
                    source,
                    node.getTarget(),
                    node.getRowCountVariable(),
                    node.getFragmentVariable(),
                    node.getTableCommitContextVariable(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getTablePartitioningScheme(),
                    node.getPreferredShufflePartitioningScheme(),
                    node.getStatisticsAggregation());
        }

        @Override
        public PlanNode visitTableWriteMerge(TableWriterMergeNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputVariables()));
            return new TableWriterMergeNode(
                    node.getId(),
                    source,
                    node.getRowCountVariable(),
                    node.getFragmentVariable(),
                    node.getTableCommitContextVariable(),
                    node.getStatisticsAggregation());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputVariables()));
            return new StatisticsWriterNode(
                    node.getId(),
                    source,
                    node.getTableHandle(),
                    node.getRowCountVariable(),
                    node.isRowCountEnabled(),
                    node.getDescriptor());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputVariables()));
            return new TableFinishNode(
                    node.getId(),
                    source,
                    node.getTarget(),
                    node.getRowCountVariable(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.of(node.getRowId()));
            return new DeleteNode(node.getId(), source, node.getRowId(), node.getOutputVariables());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMapping = rewriteSetOperationVariableMapping(node, context);
            ImmutableList<PlanNode> rewrittenSubPlans = rewriteSetOperationSubPlans(node, context, rewrittenVariableMapping);
            return new UnionNode(node.getId(), rewrittenSubPlans, ImmutableList.copyOf(rewrittenVariableMapping.keySet()), fromListMultimap(rewrittenVariableMapping));
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMapping = rewriteSetOperationVariableMapping(node, context);
            ImmutableList<PlanNode> rewrittenSubPlans = rewriteSetOperationSubPlans(node, context, rewrittenVariableMapping);
            return new IntersectNode(node.getId(), rewrittenSubPlans, ImmutableList.copyOf(rewrittenVariableMapping.keySet()), fromListMultimap(rewrittenVariableMapping));
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMapping = rewriteSetOperationVariableMapping(node, context);
            ImmutableList<PlanNode> rewrittenSubPlans = rewriteSetOperationSubPlans(node, context, rewrittenVariableMapping);
            return new ExceptNode(node.getId(), rewrittenSubPlans, ImmutableList.copyOf(rewrittenVariableMapping.keySet()), fromListMultimap(rewrittenVariableMapping));
        }

        private ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewriteSetOperationVariableMapping(SetOperationNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            // Find out which output variables we need to keep
            ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMappingBuilder = ImmutableListMultimap.builder();
            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                if (context.get().contains(variable)) {
                    rewrittenVariableMappingBuilder.putAll(
                            variable,
                            node.getVariableMapping().get(variable));
                }
            }
            return rewrittenVariableMappingBuilder.build();
        }

        private ImmutableList<PlanNode> rewriteSetOperationSubPlans(SetOperationNode node, RewriteContext<Set<VariableReferenceExpression>> context, ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMapping)
        {
            // Find the corresponding input symbol to the remaining output symbols and prune the subplans
            ImmutableList.Builder<PlanNode> rewrittenSubPlans = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                ImmutableSet.Builder<VariableReferenceExpression> expectedInputSymbols = ImmutableSet.builder();
                for (Collection<VariableReferenceExpression> variables : rewrittenVariableMapping.asMap().values()) {
                    expectedInputSymbols.add(Iterables.get(variables, i));
                }
                rewrittenSubPlans.add(context.rewrite(node.getSources().get(i), expectedInputSymbols.build()));
            }
            return rewrittenSubPlans.build();
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableList.Builder<VariableReferenceExpression> rewrittenOutputVariablesBuilder = ImmutableList.builder();
            ImmutableList.Builder<ImmutableList.Builder<RowExpression>> rowBuildersBuilder = ImmutableList.builder();
            // Initialize builder for each row
            for (int i = 0; i < node.getRows().size(); i++) {
                rowBuildersBuilder.add(ImmutableList.builder());
            }
            ImmutableList<ImmutableList.Builder<RowExpression>> rowBuilders = rowBuildersBuilder.build();
            for (int i = 0; i < node.getOutputVariables().size(); i++) {
                VariableReferenceExpression outputVariable = node.getOutputVariables().get(i);
                // If output symbol is used
                if (context.get().contains(outputVariable)) {
                    rewrittenOutputVariablesBuilder.add(outputVariable);
                    // Add the value of the output symbol for each row
                    for (int j = 0; j < node.getRows().size(); j++) {
                        rowBuilders.get(j).add(node.getRows().get(j).get(i));
                    }
                }
            }
            List<List<RowExpression>> rewrittenRows = rowBuilders.stream()
                    .map(ImmutableList.Builder::build)
                    .collect(toImmutableList());
            return new ValuesNode(node.getId(), rewrittenOutputVariablesBuilder.build(), rewrittenRows);
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            // remove unused apply nodes
            if (intersection(node.getSubqueryAssignments().getVariables(), context.get()).isEmpty()) {
                return context.rewrite(node.getInput(), context.get());
            }

            // extract symbols required subquery plan
            ImmutableSet.Builder<VariableReferenceExpression> subqueryAssignmentsVariablesBuilder = ImmutableSet.builder();
            Assignments.Builder subqueryAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getSubqueryAssignments().getMap().entrySet()) {
                VariableReferenceExpression output = entry.getKey();
                RowExpression expression = entry.getValue();
                if (context.get().contains(output)) {
                    if (isExpression(expression)) {
                        subqueryAssignmentsVariablesBuilder.addAll(VariablesExtractor.extractUnique(castToExpression(expression), variableAllocator.getTypes()));
                    }
                    else {
                        subqueryAssignmentsVariablesBuilder.addAll(VariablesExtractor.extractUnique(expression));
                    }
                    subqueryAssignments.put(output, expression);
                }
            }

            Set<VariableReferenceExpression> subqueryAssignmentsVariables = subqueryAssignmentsVariablesBuilder.build();
            PlanNode subquery = context.rewrite(node.getSubquery(), subqueryAssignmentsVariables);

            // prune not used correlation symbols
            Set<VariableReferenceExpression> subquerySymbols = VariablesExtractor.extractUnique(subquery, variableAllocator.getTypes());
            List<VariableReferenceExpression> newCorrelation = node.getCorrelation().stream()
                    .filter(subquerySymbols::contains)
                    .collect(toImmutableList());

            Set<VariableReferenceExpression> inputContext = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(newCorrelation)
                    .addAll(subqueryAssignmentsVariables) // need to include those: e.g: "expr" from "expr IN (SELECT 1)"
                    .build();
            PlanNode input = context.rewrite(node.getInput(), inputContext);
            Assignments assignments = subqueryAssignments.build();
            verifySubquerySupported(assignments);
            return new ApplyNode(node.getId(), input, subquery, assignments, newCorrelation, node.getOriginSubqueryError());
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (!context.get().contains(node.getIdVariable())) {
                return context.rewrite(node.getSource(), context.get());
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitLateralJoin(LateralJoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            PlanNode subquery = context.rewrite(node.getSubquery(), context.get());

            // remove unused lateral nodes
            if (intersection(ImmutableSet.copyOf(subquery.getOutputVariables()), context.get()).isEmpty() && isScalar(subquery)) {
                return context.rewrite(node.getInput(), context.get());
            }

            // prune not used correlation symbols
            Set<VariableReferenceExpression> subqueryVariables = VariablesExtractor.extractUnique(subquery, variableAllocator.getTypes());
            List<VariableReferenceExpression> newCorrelation = node.getCorrelation().stream()
                    .filter(subqueryVariables::contains)
                    .collect(toImmutableList());

            Set<VariableReferenceExpression> inputContext = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(newCorrelation)
                    .build();
            PlanNode input = context.rewrite(node.getInput(), inputContext);

            // remove unused lateral nodes
            if (intersection(ImmutableSet.copyOf(input.getOutputVariables()), inputContext).isEmpty() && isScalar(input)) {
                return subquery;
            }

            return new LateralJoinNode(node.getId(), input, subquery, newCorrelation, node.getType(), node.getOriginSubqueryError());
        }
    }
}

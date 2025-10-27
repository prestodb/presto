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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SetOperationNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.StatisticAggregations;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
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
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");

        Rewriter rewriter = new Rewriter();
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, ImmutableSet.of());
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Set<VariableReferenceExpression>>
    {
        private boolean planChanged;

        public boolean isPlanChanged()
        {
            return planChanged;
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
            planChanged = node.getOutputVariables().size() != newOutputVariables.size();

            // newOutputVariables contains all partition, sort and hash variables so simply swap the output layout
            PartitioningScheme partitioningScheme = new PartitioningScheme(
                    node.getPartitioningScheme().getPartitioning(),
                    newOutputVariables,
                    node.getPartitioningScheme().getHashColumn(),
                    node.getPartitioningScheme().isReplicateNullsAndAny(),
                    node.getPartitioningScheme().isScaleWriters(),
                    node.getPartitioningScheme().getEncoding(),
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
                    node.getSourceLocation(),
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
                expectedFilterInputs = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(VariablesExtractor.extractUnique(node.getFilter().get()))
                        .addAll(context.get())
                        .build();
            }

            ImmutableSet.Builder<VariableReferenceExpression> leftInputsBuilder = ImmutableSet.builder();
            leftInputsBuilder.addAll(context.get()).addAll(Iterables.transform(node.getCriteria(), EquiJoinClause::getLeft));
            if (node.getLeftHashVariable().isPresent()) {
                leftInputsBuilder.add(node.getLeftHashVariable().get());
            }
            leftInputsBuilder.addAll(expectedFilterInputs);
            Set<VariableReferenceExpression> leftInputs = leftInputsBuilder.build();

            ImmutableSet.Builder<VariableReferenceExpression> rightInputsBuilder = ImmutableSet.builder();
            rightInputsBuilder.addAll(context.get()).addAll(Iterables.transform(node.getCriteria(), EquiJoinClause::getRight));
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

            planChanged = node.getOutputVariables().size() != outputVariables.size();

            return new JoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
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

            return new SemiJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    filteringSource,
                    node.getSourceJoinVariable(),
                    node.getFilteringSourceJoinVariable(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashVariable(),
                    node.getFilteringSourceHashVariable(),
                    node.getDistributionType(),
                    node.getDynamicFilters());
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> filterSymbols = VariablesExtractor.extractUnique(node.getFilter());
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

            planChanged = outputVariables.size() != node.getOutputVariables().size();

            return new SpatialJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getType(),
                    left,
                    right,
                    outputVariables,
                    node.getProbeGeometryVariable(),
                    node.getBuildGeometryVariable(),
                    node.getRadiusVariable(),
                    node.getFilter(),
                    node.getLeftPartitionVariable(),
                    node.getRightPartitionVariable(),
                    node.getKdbTree());
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedFilterInputs = new HashSet<>();
            if (node.getFilter().isPresent()) {
                expectedFilterInputs = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(VariablesExtractor.extractUnique(node.getFilter().get()))
                        .build();
            }

            ImmutableSet.Builder<VariableReferenceExpression> probeInputsBuilder = ImmutableSet.builder();
            probeInputsBuilder.addAll(context.get())
                    .addAll(Iterables.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe));
            if (node.getProbeHashVariable().isPresent()) {
                probeInputsBuilder.add(node.getProbeHashVariable().get());
            }
            probeInputsBuilder.addAll(expectedFilterInputs);
            Set<VariableReferenceExpression> probeInputs = probeInputsBuilder.build();

            ImmutableSet.Builder<VariableReferenceExpression> indexInputBuilder = ImmutableSet.builder();
            indexInputBuilder.addAll(context.get())
                    .addAll(Iterables.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getIndex));
            if (node.getIndexHashVariable().isPresent()) {
                indexInputBuilder.add(node.getIndexHashVariable().get());
            }
            indexInputBuilder.addAll(expectedFilterInputs);
            // Lookup variables must not be pruned.
            indexInputBuilder.addAll(node.getLookupVariables());
            Set<VariableReferenceExpression> indexInputs = indexInputBuilder.build();

            PlanNode probeSource = context.rewrite(node.getProbeSource(), probeInputs);
            PlanNode indexSource = context.rewrite(node.getIndexSource(), indexInputs);

            return new IndexJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getType(),
                    probeSource,
                    indexSource,
                    node.getCriteria(),
                    node.getFilter(),
                    node.getProbeHashVariable(),
                    node.getIndexHashVariable(),
                    node.getLookupVariables());
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

            planChanged = newLookupVariables.size() != node.getLookupVariables().size();

            return new IndexSourceNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), node.getIndexHandle(), node.getTableHandle(), newLookupVariables, newOutputVariables, newAssignments, node.getCurrentConstraint());
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
                    expectedInputs.addAll(extractAggregationUniqueVariables(aggregation));
                    aggregation.getMask().ifPresent(expectedInputs::add);
                    aggregations.put(variable, aggregation);
                }
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new AggregationNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    aggregations.build(),
                    node.getGroupingSets(),
                    ImmutableList.of(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable(),
                    node.getAggregationId());
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
                if (frame.getSortKeyCoercedForFrameStartComparison().isPresent()) {
                    expectedInputs.add(frame.getSortKeyCoercedForFrameStartComparison().get());
                }
                if (frame.getSortKeyCoercedForFrameEndComparison().isPresent()) {
                    expectedInputs.add(frame.getSortKeyCoercedForFrameEndComparison().get());
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
                    expectedInputs.addAll(WindowNodeUtil.extractWindowFunctionUniqueVariables(function));
                    functionsBuilder.put(variable, entry.getValue());
                }
            }

            Map<VariableReferenceExpression, WindowNode.Function> functions = functionsBuilder.build();
            if (functions.size() == 0) {
                // As the window plan node is getting skipped, use the inputs needed by the parent of the Window plan node
                return context.rewrite(node.getSource(), context.get());
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new WindowNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    node.getSpecification(),
                    functions,
                    node.getHashVariable(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix());
        }

        @Override
        public PlanNode visitTableFunctionProcessor(TableFunctionProcessorNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            return node.getSource().map(source -> new TableFunctionProcessorNode(
                    node.getId(),
                    node.getName(),
                    node.getProperOutputs(),
                    Optional.of(context.rewrite(source, ImmutableSet.copyOf(source.getOutputVariables()))),
                    node.isPruneWhenEmpty(),
                    node.getPassThroughSpecifications(),
                    node.getRequiredVariables(),
                    node.getMarkerVariables(),
                    node.getSpecification(),
                    node.getPrePartitioned(),
                    node.getPreSorted(),
                    node.getHashSymbol(),
                    node.getHandle()
            )).orElse(node);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            List<VariableReferenceExpression> newOutputs = node.getOutputVariables().stream()
                    .filter(context.get()::contains)
                    .collect(toImmutableList());

            Map<VariableReferenceExpression, ColumnHandle> newAssignments = newOutputs.stream()
                    .collect(Collectors.toMap(identity(), node.getAssignments()::get));

            planChanged = newOutputs.size() != node.getOutputVariables().size();

            return new TableScanNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getTable(),
                    newOutputs,
                    newAssignments,
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.getCteMaterializationInfo());
        }

        @Override
        public PlanNode visitMergeWriter(MergeWriterNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(node.getMergeProcessorProjectedVariables())
                    .build();

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new MergeWriterNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    node.getTarget(),
                    node.getMergeProcessorProjectedVariables(),
                    node.getOutputVariables());
        }

        @Override
        public PlanNode visitMergeProcessor(MergeProcessorNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .add(node.getTargetTableRowIdColumnVariable())
                    .add(node.getMergeRowVariable())
                    .build();

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new MergeProcessorNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    node.getTarget(),
                    node.getTargetTableRowIdColumnVariable(),
                    node.getMergeRowVariable(),
                    node.getTargetColumnVariables(),
                    node.getOutputVariables());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(VariablesExtractor.extractUnique(node.getPredicate()))
                    .addAll(context.get())
                    .build();

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new FilterNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getPredicate());
        }

        @Override
        public PlanNode visitCteConsumer(CteConsumerNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            // Some output can be pruned but current implementation of PhysicalCteProducer does not allow cteconsumer pruning
            return node;
        }

        @Override
        public PlanNode visitCteProducer(CteProducerNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.copyOf(node.getOutputVariables());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new CteProducerNode(node.getSourceLocation(), node.getId(), source, node.getCteId(), node.getRowCountVariable(), node.getOutputVariables());
        }

        @Override
        public PlanNode visitSequence(SequenceNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> cteProducersBuilder = ImmutableSet.builder();
            node.getCteProducers().forEach(leftSource -> cteProducersBuilder.addAll(leftSource.getOutputVariables()));
            Set<VariableReferenceExpression> leftInputs = cteProducersBuilder.build();
            List<PlanNode> cteProducers = node.getCteProducers().stream()
                    .map(leftSource -> context.rewrite(leftSource, leftInputs)).collect(toImmutableList());
            Set<VariableReferenceExpression> rightInputs = ImmutableSet.copyOf(node.getPrimarySource().getOutputVariables());
            PlanNode primarySource = context.rewrite(node.getPrimarySource(), rightInputs);
            return new SequenceNode(node.getSourceLocation(), node.getId(), cteProducers, primarySource, node.getCteDependencyGraph());
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
                planChanged = groupingSet.size() != newGroupingSet.build().size();
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new GroupIdNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, newGroupingSets.build(), newGroupingMapping, newAggregationArguments, node.getGroupIdVariable());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (!context.get().contains(node.getMarkerVariable())) {
                planChanged = true;
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

            return new MarkDistinctNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getMarkerVariable(), node.getDistinctVariables(), node.getHashVariable());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            List<VariableReferenceExpression> replicateVariables = node.getReplicateVariables().stream()
                    .filter(context.get()::contains)
                    .collect(toImmutableList());

            planChanged = replicateVariables.size() != node.getReplicateVariables().size();

            Optional<VariableReferenceExpression> ordinalityVariable = node.getOrdinalityVariable();
            if (ordinalityVariable.isPresent() && !context.get().contains(ordinalityVariable.get())) {
                planChanged = true;
                ordinalityVariable = Optional.empty();
            }
            Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables = node.getUnnestVariables();
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(replicateVariables)
                    .addAll(unnestVariables.keySet());

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new UnnestNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, replicateVariables, unnestVariables, ordinalityVariable);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.builder();

            Assignments.Builder builder = Assignments.builder();
            node.getAssignments().forEach((variable, expression) -> {
                if (context.get().contains(variable)) {
                    expectedInputs.addAll(VariablesExtractor.extractUnique(expression));
                    builder.put(variable, expression);
                }
                else {
                    planChanged = true;
                }
            });

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new ProjectNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, builder.build(), node.getLocality());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.copyOf(node.getOutputVariables());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new OutputNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getColumnNames(), node.getOutputVariables());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new LimitNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getCount(), node.getStep());
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
            return new DistinctLimitNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getLimit(), node.isPartial(), node.getDistinctVariables(), node.getHashVariable(), node.getTimeoutMillis());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(node.getOrderingScheme().getOrderByVariables());

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TopNNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getCount(), node.getOrderingScheme(), node.getStep());
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

            return new RowNumberNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getPartitionBy(), node.getRowNumberVariable(), node.getMaxRowCountPerPartition(), node.isPartial(), node.getHashVariable());
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

            return new TopNRowNumberNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
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

            return new SortNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getOrderingScheme(), node.isPartial(), node.getPartitionBy());
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
            if (node.getStatisticsAggregation().isPresent()) {
                StatisticAggregations aggregations = node.getStatisticsAggregation().get();
                expectedInputs.addAll(aggregations.getGroupingVariables());
                aggregations.getAggregations()
                        .values()
                        .forEach(aggregation -> expectedInputs.addAll(extractAggregationUniqueVariables(aggregation)));
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new TableWriterNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    node.getTarget(),
                    node.getRowCountVariable(),
                    node.getFragmentVariable(),
                    node.getTableCommitContextVariable(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getNotNullColumnVariables(),
                    node.getTablePartitioningScheme(),
                    node.getStatisticsAggregation(),
                    node.getTaskCountIfScaledWriter(),
                    node.getIsTemporaryTableWriter());
        }

        @Override
        public PlanNode visitTableWriteMerge(TableWriterMergeNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputVariables()));
            return new TableWriterMergeNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    node.getRowCountVariable(),
                    node.getFragmentVariable(),
                    node.getTableCommitContextVariable(),
                    node.getStatisticsAggregation());
        }

        @Override
        public PlanNode visitCallDistributedProcedure(CallDistributedProcedureNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputVariables()));
            return new CallDistributedProcedureNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    node.getTarget(),
                    node.getRowCountVariable(),
                    node.getFragmentVariable(),
                    node.getTableCommitContextVariable(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getNotNullColumnVariables(),
                    node.getPartitioningScheme());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputVariables()));
            return new StatisticsWriterNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
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
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    node.getTarget(),
                    node.getRowCountVariable(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor(),
                    node.getCteMaterializationInfo());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
            node.getRowId().ifPresent(r -> builder.add(r));
            if (node.getInputDistribution().isPresent()) {
                builder.addAll(node.getInputDistribution().get().getInputVariables());
            }
            PlanNode source = context.rewrite(node.getSource(), builder.build());
            return new DeleteNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, node.getRowId(), node.getOutputVariables(), node.getInputDistribution());
        }

        @Override
        public PlanNode visitUpdate(UpdateNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            return new UpdateNode(node.getSourceLocation(), node.getId(), node.getSource(), node.getRowId(), node.getColumnValueAndRowIdSymbols(), node.getOutputVariables());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMapping = rewriteSetOperationVariableMapping(node, context, true);
            ImmutableList<PlanNode> rewrittenSubPlans = rewriteSetOperationSubPlans(node, context, rewrittenVariableMapping);
            return new UnionNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), rewrittenSubPlans, ImmutableList.copyOf(rewrittenVariableMapping.keySet()), fromListMultimap(rewrittenVariableMapping));
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMapping = rewriteSetOperationVariableMapping(node, context, false);
            ImmutableList<PlanNode> rewrittenSubPlans = rewriteSetOperationSubPlans(node, context, rewrittenVariableMapping);
            return new IntersectNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), rewrittenSubPlans, ImmutableList.copyOf(rewrittenVariableMapping.keySet()), fromListMultimap(rewrittenVariableMapping));
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMapping = rewriteSetOperationVariableMapping(node, context, false);
            ImmutableList<PlanNode> rewrittenSubPlans = rewriteSetOperationSubPlans(node, context, rewrittenVariableMapping);
            return new ExceptNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), rewrittenSubPlans, ImmutableList.copyOf(rewrittenVariableMapping.keySet()), fromListMultimap(rewrittenVariableMapping));
        }

        private ListMultimap<VariableReferenceExpression, VariableReferenceExpression> rewriteSetOperationVariableMapping(SetOperationNode node, RewriteContext<Set<VariableReferenceExpression>> context, boolean pruneUnreferencedOutput)
        {
            // Find out which output variables we need to keep
            ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> rewrittenVariableMappingBuilder = ImmutableListMultimap.builder();
            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                if (context.get().contains(variable) || !pruneUnreferencedOutput) {
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
            List<VariableReferenceExpression> rewrittenOutputVariables = rewrittenOutputVariablesBuilder.build();
            planChanged = rewrittenOutputVariables.size() != node.getOutputVariables().size();
            return new ValuesNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), rewrittenOutputVariables, rewrittenRows, node.getValuesNodeLabel());
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            // remove unused apply nodes
            if (intersection(node.getSubqueryAssignments().getVariables(), context.get()).isEmpty()) {
                planChanged = true;
                return context.rewrite(node.getInput(), context.get());
            }

            // extract symbols required subquery plan
            ImmutableSet.Builder<VariableReferenceExpression> subqueryAssignmentsVariablesBuilder = ImmutableSet.builder();
            Assignments.Builder subqueryAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getSubqueryAssignments().getMap().entrySet()) {
                VariableReferenceExpression output = entry.getKey();
                RowExpression expression = entry.getValue();
                if (context.get().contains(output)) {
                    subqueryAssignmentsVariablesBuilder.addAll(VariablesExtractor.extractUnique(expression));
                    subqueryAssignments.put(output, expression);
                }
            }

            Set<VariableReferenceExpression> subqueryAssignmentsVariables = subqueryAssignmentsVariablesBuilder.build();
            PlanNode subquery = context.rewrite(node.getSubquery(), subqueryAssignmentsVariables);

            // prune not used correlation symbols
            Set<VariableReferenceExpression> subquerySymbols = VariablesExtractor.extractUnique(subquery);
            List<VariableReferenceExpression> newCorrelation = node.getCorrelation().stream()
                    .filter(subquerySymbols::contains)
                    .collect(toImmutableList());
            planChanged = newCorrelation.size() != node.getCorrelation().size();

            Set<VariableReferenceExpression> inputContext = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(newCorrelation)
                    .addAll(subqueryAssignmentsVariables) // need to include those: e.g: "expr" from "expr IN (SELECT 1)"
                    .build();
            PlanNode input = context.rewrite(node.getInput(), inputContext);
            Assignments assignments = subqueryAssignments.build();
            verifySubquerySupported(assignments);
            return new ApplyNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), input, subquery, assignments, newCorrelation, node.getOriginSubqueryError(), node.getMayParticipateInAntiJoin());
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (!context.get().contains(node.getIdVariable())) {
                planChanged = true;
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
                planChanged = true;
                return context.rewrite(node.getInput(), context.get());
            }

            // prune not used correlation symbols
            Set<VariableReferenceExpression> subqueryVariables = VariablesExtractor.extractUnique(subquery);
            List<VariableReferenceExpression> newCorrelation = node.getCorrelation().stream()
                    .filter(subqueryVariables::contains)
                    .collect(toImmutableList());
            planChanged = newCorrelation.size() != node.getCorrelation().size();

            Set<VariableReferenceExpression> inputContext = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(newCorrelation)
                    .build();
            PlanNode input = context.rewrite(node.getInput(), inputContext);

            // remove unused lateral nodes
            if (intersection(ImmutableSet.copyOf(input.getOutputVariables()), inputContext).isEmpty() && isScalar(input)) {
                planChanged = true;
                return subquery;
            }

            return new LateralJoinNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), input, subquery, newCorrelation, node.getType(), node.getOriginSubqueryError());
        }
    }
}

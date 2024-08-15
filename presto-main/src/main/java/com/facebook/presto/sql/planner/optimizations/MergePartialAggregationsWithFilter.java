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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isMergeAggregationsWithAndWithoutFilter;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.removeFilterAndMask;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Merge partial aggregations which have mask with the partial aggregation without mask when all other fields are the same
 *
 * <pre>
 *     - Aggregation (Final)
 *          sum_1 := sum(partial_sum_1)
 *          sum_2 := sum(partial_sum_2)
 *          group_by_key [gb]
 *          - Exchange
 *              - Aggregation (Partial)
 *                  partial_sum_1 := sum(a)
 *                  partial_sum_2 := sum(a) mask m
 *                  group_by_key [gb]
 * </pre>
 * into
 * <pre>
 *     - Aggregation (Final)
 *          sum_1 := sum(partial_sum_1)
 *          sum_2 := sum(partial_sum_2)
 *          group_by_key [gb]
 *          - Project
 *              partial_sum_2 := IF(m, partial_sum_1, null)
 *              - Exchange
 *                  - Aggregation (Partial)
 *                      partial_sum_1 := sum(a)
 *                      group_by_key [gb, m]
 * </pre>
 */
public class MergePartialAggregationsWithFilter
        implements PlanOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private boolean isEnabledForTesting;

    public MergePartialAggregationsWithFilter(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || isMergeAggregationsWithAndWithoutFilter(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isEnabled(session)) {
            Rewriter rewriter = new Rewriter(session, variableAllocator, idAllocator, functionAndTypeManager);
            PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, new Context());
            return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
        }

        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Context
    {
        private final Map<VariableReferenceExpression, VariableReferenceExpression> partialResultToMask;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> partialOutputMapping;

        public Context()
        {
            partialResultToMask = new HashMap<>();
            partialOutputMapping = new HashMap<>();
        }

        public boolean isEmpty()
        {
            return partialOutputMapping.isEmpty();
        }

        public void clear()
        {
            partialResultToMask.clear();
            partialOutputMapping.clear();
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getPartialOutputMapping()
        {
            return partialOutputMapping;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getPartialResultToMask()
        {
            return partialResultToMask;
        }
    }

    private static class Rewriter
            extends SimplePlanRewriter<Context>
    {
        private final Session session;
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;
        private boolean planChanged;

        public Rewriter(Session session, VariableAllocator variableAllocator, PlanNodeIdAllocator planNodeIdAllocator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        public static RowExpression ifThenElse(RowExpression... arguments)
        {
            return specialForm(SpecialFormExpression.Form.IF, arguments[1].getType(), arguments);
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Context> context)
        {
            List<PlanNode> children = node.getSources().stream()
                    .map(child -> context.rewrite(child, context.get()))
                    .collect(toImmutableList());
            if (!context.get().isEmpty()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unexpected plan node between partial and final aggregation");
            }
            return replaceChildren(node, children);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Context> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), context.get());
            // Before optimization, for aggregations with filter, input rows will be skipped if mask is false. However, after optimization, the partial
            // aggregation output is projected to be NULL if mask is false. We need to have the function to not calledOnNullInput to ensure correctness.
            // Applying optimizations on global aggregations will lead to exception at
            // https://github.com/prestodb/presto/blob/dfbf21744ccd900d1a650571ffc35915db9b9f59/presto-main/src/main/java/com/facebook/presto/operator/HashAggregationOperator.java#L627
            boolean canOptimize = !node.getGroupingKeys().isEmpty() && node.getAggregations().values().stream()
                    .map(x -> functionAndTypeManager.getFunctionMetadata(x.getFunctionHandle())).noneMatch(x -> x.isCalledOnNullInput());
            if (canOptimize) {
                checkState(node.getAggregations().values().stream().noneMatch(x -> x.getFilter().isPresent()), "All aggregation filters should already be rewritten to mask before this optimization");
                if (node.getStep().equals(PARTIAL)) {
                    planChanged = true;
                    return createPartialAggregationNode(node, rewrittenSource, context);
                }
                else if (node.getStep().equals(FINAL)) {
                    planChanged = true;
                    return createFinalAggregationNode(node, rewrittenSource, context);
                }
            }
            return node.replaceChildren(ImmutableList.of(rewrittenSource));
        }

        private AggregationNode createPartialAggregationNode(AggregationNode node, PlanNode rewrittenSource, RewriteContext<Context> context)
        {
            checkState(context.get().isEmpty(), "There should be no partial aggregation left unmerged for a partial aggregation node");
            Map<AggregationNode.Aggregation, VariableReferenceExpression> aggregationsWithoutMaskToOutput = node.getAggregations().entrySet().stream()
                    .filter(x -> !x.getValue().getMask().isPresent())
                    .collect(toImmutableMap(x -> x.getValue(), x -> x.getKey(), (a, b) -> a));
            Map<AggregationNode.Aggregation, VariableReferenceExpression> aggregationsToMergeOutput = node.getAggregations().entrySet().stream()
                    .filter(x -> x.getValue().getMask().isPresent() && aggregationsWithoutMaskToOutput.containsKey(removeFilterAndMask(x.getValue())))
                    .collect(toImmutableMap(x -> x.getValue(), x -> x.getKey()));

            context.get().getPartialResultToMask().putAll(aggregationsToMergeOutput.entrySet().stream()
                    .collect(toImmutableMap(x -> x.getValue(), x -> x.getKey().getMask().get())));
            context.get().getPartialOutputMapping().putAll(aggregationsToMergeOutput.entrySet().stream()
                    .collect(toImmutableMap(x -> x.getValue(), x -> aggregationsWithoutMaskToOutput.get(removeFilterAndMask(x.getKey())))));

            Set<VariableReferenceExpression> maskVariables = new HashSet<>(context.get().getPartialResultToMask().values());
            if (maskVariables.isEmpty()) {
                return (AggregationNode) node.replaceChildren(ImmutableList.of(rewrittenSource));
            }

            ImmutableList.Builder<VariableReferenceExpression> groupingVariables = ImmutableList.builder();
            AggregationNode.GroupingSetDescriptor groupingSetDescriptor = node.getGroupingSets();
            groupingVariables.addAll(groupingSetDescriptor.getGroupingKeys());
            groupingVariables.addAll(maskVariables);
            AggregationNode.GroupingSetDescriptor partialGroupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                    groupingVariables.build(), groupingSetDescriptor.getGroupingSetCount(), groupingSetDescriptor.getGlobalGroupingSets());

            Set<VariableReferenceExpression> partialResultToMerge = new HashSet<>(aggregationsToMergeOutput.values());
            Map<VariableReferenceExpression, AggregationNode.Aggregation> newAggregations = node.getAggregations().entrySet().stream()
                    .filter(x -> !partialResultToMerge.contains(x.getKey())).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            return new AggregationNode(
                    node.getSourceLocation(),
                    node.getId(),
                    rewrittenSource,
                    newAggregations,
                    partialGroupingSetDescriptor,
                    node.getPreGroupedVariables(),
                    PARTIAL,
                    node.getHashVariable(),
                    node.getGroupIdVariable(),
                    node.getAggregationId());
        }

        private AggregationNode createFinalAggregationNode(AggregationNode node, PlanNode rewrittenSource, RewriteContext<Context> context)
        {
            if (context.get().isEmpty()) {
                return (AggregationNode) node.replaceChildren(ImmutableList.of(rewrittenSource));
            }
            List<VariableReferenceExpression> intermediateVariables = node.getAggregations().values().stream()
                    .map(x -> (VariableReferenceExpression) x.getArguments().get(0)).collect(Collectors.toList());
            checkState(intermediateVariables.containsAll(context.get().partialResultToMask.keySet()));

            ImmutableList.Builder<RowExpression> projectionsFromPartialAgg = ImmutableList.builder();
            ImmutableList.Builder<VariableReferenceExpression> variablesForPartialAggResult = ImmutableList.builder();
            ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> newFinalAggregationMap = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation aggregation = entry.getValue();
                checkState(aggregation.getArguments().size() > 0 && aggregation.getArguments().get(0) instanceof VariableReferenceExpression);
                VariableReferenceExpression partialInput = (VariableReferenceExpression) aggregation.getArguments().get(0);
                if (!context.get().partialResultToMask.containsKey(partialInput)) {
                    newFinalAggregationMap.put(entry.getKey(), entry.getValue());
                    continue;
                }
                VariableReferenceExpression maskVariable = context.get().getPartialResultToMask().get(partialInput);
                VariableReferenceExpression toMergePartialInput = context.get().getPartialOutputMapping().get(partialInput);
                RowExpression conditionalResult = ifThenElse(maskVariable, toMergePartialInput, constantNull(toMergePartialInput.getType()));
                projectionsFromPartialAgg.add(conditionalResult);
                VariableReferenceExpression maskedPartialResult = variableAllocator.newVariable(toMergePartialInput);
                variablesForPartialAggResult.add(maskedPartialResult);

                CallExpression originalExpression = aggregation.getCall();
                CallExpression newExpression = new CallExpression(originalExpression.getSourceLocation(),
                        originalExpression.getDisplayName(),
                        originalExpression.getFunctionHandle(),
                        originalExpression.getType(),
                        ImmutableList.<RowExpression>builder()
                                .add(maskedPartialResult)
                                .addAll(originalExpression.getArguments().subList(1, originalExpression.getArguments().size()))
                                .build());

                AggregationNode.Aggregation newFinalAggregation = new AggregationNode.Aggregation(
                        newExpression,
                        aggregation.getFilter(),
                        aggregation.getOrderBy(),
                        aggregation.isDistinct(),
                        aggregation.getMask());
                newFinalAggregationMap.put(entry.getKey(), newFinalAggregation);
            }

            PlanNode projectNode = addProjections(rewrittenSource, planNodeIdAllocator, variableAllocator, projectionsFromPartialAgg.build(), variablesForPartialAggResult.build());
            context.get().clear();
            return new AggregationNode(
                    node.getSourceLocation(),
                    node.getId(),
                    projectNode,
                    newFinalAggregationMap.build(),
                    node.getGroupingSets(),
                    node.getPreGroupedVariables(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable(),
                    node.getAggregationId());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), context.get());
            if (!context.get().isEmpty()) {
                Assignments.Builder assignments = Assignments.builder();
                Map<VariableReferenceExpression, RowExpression> excludeMergedAssignments = node.getAssignments().getMap().entrySet().stream()
                        .filter(x -> !(x.getValue() instanceof VariableReferenceExpression && context.get().getPartialOutputMapping().containsKey(x.getValue())))
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                assignments.putAll(excludeMergedAssignments);
                assignments.putAll(identityAssignments(context.get().getPartialResultToMask().values()));
                return new ProjectNode(
                        node.getSourceLocation(),
                        node.getId(),
                        rewrittenSource,
                        assignments.build(),
                        node.getLocality());
            }
            return node.replaceChildren(ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Context> context)
        {
            ImmutableList.Builder rewriteChildren = ImmutableList.builder();
            for (PlanNode child : node.getSources()) {
                context.get().clear();
                rewriteChildren.add(context.rewrite(child, context.get()));
            }
            List<PlanNode> children = rewriteChildren.build();
            if (!context.get().isEmpty()) {
                PartitioningScheme partitioning = new PartitioningScheme(
                        node.getPartitioningScheme().getPartitioning(),
                        children.get(children.size() - 1).getOutputVariables(),
                        node.getPartitioningScheme().getHashColumn(),
                        node.getPartitioningScheme().isReplicateNullsAndAny(),
                        node.getPartitioningScheme().getBucketToPartition());

                return new ExchangeNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getType(),
                        node.getScope(),
                        partitioning,
                        children,
                        children.stream().map(x -> x.getOutputVariables()).collect(toImmutableList()),
                        node.isEnsureSourceOrdering(),
                        node.getOrderingScheme());
            }
            return node.replaceChildren(children);
        }
    }
}

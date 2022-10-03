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
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isOptimizeConditionalAggregationEnabled;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.function.Function.identity;

/**
 * Rewrite IF(predicate, AGG(x)) to AGG(x) with Mask.
 * The plan will change
 * When the aggregation does not have mask
 * <p>
 * From:
 * <pre>
 * - Project (val <- IF(condition, agg))
 *   - Aggregation (agg <- AGG(x))
 * </pre>
 * To:
 * <pre>
 * - Project (val <- IF(condition, agg))
 *   - Aggregation (agg <- AGG(x), mask <- Mask)
 *     - Project (Mask <- condition)
 * </pre>
 * <p>
 * Or
 * When the aggregation already has mask
 * <p>
 * From:
 * <pre>
 * - Project (val <- IF(condition, agg))
 *   - Aggregation (agg <- AGG(x), mask)
 * </pre>
 * To:
 * <pre>
 * - Project (val <- IF(condition, agg))
 *   - Aggregation (agg <- AGG(x), mask <- Mask)
 *     - Project (Mask <- AND(mask, condition))
 * </pre>
 * <p>
 */
public class RewriteIfOverAggregation
        implements PlanOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public RewriteIfOverAggregation(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public PlanNode optimize(PlanNode plan,
            Session session,
            TypeProvider types,
            PlanVariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        if (isOptimizeConditionalAggregationEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(
                    new Rewriter(variableAllocator, idAllocator, new RowExpressionDeterminismEvaluator(functionAndTypeManager)), plan, ImmutableMap.of());
        }
        return plan;
    }

    // Map<VariableReferenceExpression, RowExpression> stores the candidate IF expressions for rewrite.
    private static class Rewriter
            extends SimplePlanRewriter<Map<VariableReferenceExpression, RowExpression>>
    {
        private final PlanVariableAllocator planVariableAllocator;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;

        private Rewriter(PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, RowExpressionDeterminismEvaluator determinismEvaluator)
        {
            this.planVariableAllocator = variableAllocator;
            this.planNodeIdAllocator = idAllocator;
            this.determinismEvaluator = determinismEvaluator;
        }

        private static VariableReferenceExpression getTrueValueFromIf(RowExpression rowExpression)
        {
            checkState(rowExpression instanceof SpecialFormExpression
                    && ((SpecialFormExpression) rowExpression).getArguments().get(1) instanceof VariableReferenceExpression);
            return (VariableReferenceExpression) ((SpecialFormExpression) rowExpression).getArguments().get(1);
        }

        private static RowExpression inlineReferences(RowExpression expression, Assignments assignments)
        {
            return RowExpressionVariableInliner.inlineVariables(variable -> assignments.getMap().getOrDefault(variable, variable), expression);
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Map<VariableReferenceExpression, RowExpression>> context)
        {
            // This optimizer targets plan generated from IF(predicate, AGG(x)). The plan generated will be Project <- Aggregation, where project includes the IF expression.
            // Here we pass an empty map in context by default, so as to not pass the candidate IF expressions unless it's a project node.
            return context.defaultRewrite(node, ImmutableMap.of());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Map<VariableReferenceExpression, RowExpression>> context)
        {
            // The rewrite will change the aggregation output, hence we only do the rewrite if the aggregation output is only used in the IF expression, i.e. variables which
            // occurs only once in the assignments.
            Set<VariableReferenceExpression> candidateVariables = node.getAssignments().getExpressions().stream()
                    .flatMap(expression -> extractAll(expression).stream())
                    .collect(Collectors.groupingBy(identity(), Collectors.counting())).entrySet().stream()
                    .filter(entry -> entry.getValue() == 1)
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());

            ImmutableSet.Builder<RowExpression> candidateIfBuilder = ImmutableSet.builder();
            IfExpressionExtractor ifExpressionExtractor = new IfExpressionExtractor();
            // Collect candidate IF expressions in assignments
            node.getAssignments().getExpressions().forEach(expression -> expression.accept(ifExpressionExtractor, candidateIfBuilder));
            // The true value should be only used once in the assignments
            Map<VariableReferenceExpression, RowExpression> candidatesInAssignments = candidateIfBuilder.build().stream()
                    .filter(x -> candidateVariables.contains(getTrueValueFromIf(x)))
                    .collect(toImmutableMap(Rewriter::getTrueValueFromIf, identity()));

            // The true value used in the candidate passed from context should only be used once in the assignments, i.e. an identity assignment in this case.
            // Also inline the if expression so that it can be resolved when we push the condition to aggregation.
            Map<VariableReferenceExpression, RowExpression> candidatePassedFromContext = context.get().entrySet().stream()
                    .filter(x -> candidateVariables.contains(x.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, x -> inlineReferences(x.getValue(), node.getAssignments())));

            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> candidates = ImmutableMap.builder();
            candidates.putAll(candidatesInAssignments);
            candidates.putAll(candidatePassedFromContext);

            return context.defaultRewrite(node, candidates.build());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Map<VariableReferenceExpression, RowExpression>> context)
        {
            Map<VariableReferenceExpression, RowExpression> candidate = context.get().entrySet().stream()
                    .filter(x -> node.getAggregations().containsKey(x.getKey()))
                    .filter(x -> extractUnique(x.getValue()).stream().filter(variable -> !variable.equals(x.getKey())).allMatch(node.getSource().getOutputVariables()::contains)) // only if expression can be resolved by aggregation inputs
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
            if (candidate.isEmpty()) {
                return context.defaultRewrite(node, ImmutableMap.of());
            }

            Assignments.Builder sourceProjection = Assignments.builder();
            ImmutableMap.Builder<VariableReferenceExpression, Aggregation> newAggregations = ImmutableMap.builder();
            candidate.forEach((aggregationOutput, ifExpression) -> {
                checkState(ifExpression instanceof SpecialFormExpression && ((SpecialFormExpression) ifExpression).getForm().equals(IF));
                RowExpression condition = ((SpecialFormExpression) ifExpression).getArguments().get(0);
                Aggregation aggregation = node.getAggregations().get(aggregationOutput);
                RowExpression maskExpression = aggregation.getMask().isPresent() ? and(aggregation.getMask().get(), condition) : condition;
                VariableReferenceExpression maskVariable = planVariableAllocator.newVariable(maskExpression);
                Aggregation newAggregation = new Aggregation(
                        aggregation.getCall(),
                        aggregation.getFilter(),
                        aggregation.getOrderBy(),
                        aggregation.isDistinct(),
                        Optional.of(maskVariable));
                sourceProjection.put(maskVariable, maskExpression);
                newAggregations.put(aggregationOutput, newAggregation);
            });

            sourceProjection.putAll(node.getSource().getOutputVariables().stream().collect(toImmutableMap(identity(), identity())));
            newAggregations.putAll(
                    node.getAggregations().entrySet().stream().filter(x -> !candidate.containsKey(x.getKey())).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

            AggregationNode aggregationNode = new AggregationNode(
                    node.getSourceLocation(),
                    node.getId(),
                    new ProjectNode(planNodeIdAllocator.getNextId(), node.getSource(), sourceProjection.build()),
                    newAggregations.build(),
                    node.getGroupingSets(),
                    node.getPreGroupedVariables(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable());

            return context.defaultRewrite(aggregationNode, ImmutableMap.of());
        }

        private boolean isCandidateIfExpression(RowExpression rowExpression)
        {
            return determinismEvaluator.isDeterministic(rowExpression) &&
                    rowExpression instanceof SpecialFormExpression && ((SpecialFormExpression) rowExpression).getForm().equals(IF)
                    && ((SpecialFormExpression) rowExpression).getArguments().get(1) instanceof VariableReferenceExpression
                    && (
                    ((SpecialFormExpression) rowExpression).getArguments().size() == 2
                            || ((SpecialFormExpression) rowExpression).getArguments().get(2) instanceof ConstantExpression
                            && ((ConstantExpression) ((SpecialFormExpression) rowExpression).getArguments().get(2)).isNull());
        }

        // Extract candidate IF expression from row expression
        private class IfExpressionExtractor
                extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<RowExpression>>
        {
            @Override
            public Void visitSpecialForm(SpecialFormExpression specialForm, ImmutableSet.Builder<RowExpression> context)
            {
                if (isCandidateIfExpression(specialForm)) {
                    context.add(specialForm);
                }
                return super.visitSpecialForm(specialForm, context);
            }
        }
    }
}

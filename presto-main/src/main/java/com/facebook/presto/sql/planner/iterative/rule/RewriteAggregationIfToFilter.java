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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationIfToFilterRewriteStrategy;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.SystemSessionProperties.getAggregationIfToFilterRewriteStrategy;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSortedMap.toImmutableSortedMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * A optimizer rule which rewrites
 * AGG(IF(condition, expr))
 * to
 * AGG(IF(condition, expr)) FILTER (WHERE condition).
 * if AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY is FILTER_WITH_IF,
 * or
 * AGG(expr) FILTER (WHERE condition).
 * if AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY is UNWRAP_IF.
 * <p>
 * The rewritten plan is more efficient because:
 * 1. The filter can be pushed down to the scan node.
 * 2. The rows not matching the condition are not aggregated.
 * <p>
 * Note that unwrapping the IF expression in the aggregate might cause issues if the true branch return errors for rows not matching the filters. For example:
 * 'IF(CARDINALITY(array) > 0, array[1]))'
 * Session property AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY and canUnwrapIf() control whether to enable IF unwrapping.
 */
public class RewriteAggregationIfToFilter
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(CHILD)));

    private final FunctionAndTypeManager functionAndTypeManager;
    private final RowExpressionDeterminismEvaluator rowExpressionDeterminismEvaluator;

    public RewriteAggregationIfToFilter(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        rowExpressionDeterminismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getAggregationIfToFilterRewriteStrategy(session).ordinal() > AggregationIfToFilterRewriteStrategy.DISABLED.ordinal();
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        ProjectNode sourceProject = captures.get(CHILD);

        Set<Aggregation> aggregationsToRewrite = aggregationNode.getAggregations().values().stream()
                .filter(aggregation -> shouldRewriteAggregation(aggregation, sourceProject))
                .collect(toImmutableSet());
        if (aggregationsToRewrite.isEmpty()) {
            return Result.empty();
        }

        // Get the corresponding assignments in the input project.
        // The aggregationReferences only has the aggregations to rewrite, thus the sourceAssignments only has IF expressions with NULL false results.
        // Multiple aggregations may reference the same input. We use a map to dedup them based on the VariableReferenceExpression, so that we only do the rewrite once per input
        // IF expression.
        // The order of sourceAssignments determines the order of generating the new variables for the IF conditions and results. We use a sorted map to get a deterministic
        // order based on the name of the VariableReferenceExpressions.
        Map<VariableReferenceExpression, RowExpression> sourceAssignments = aggregationsToRewrite.stream()
                .map(aggregation -> (VariableReferenceExpression) aggregation.getArguments().get(0))
                .collect(toImmutableSortedMap(VariableReferenceExpression::compareTo, identity(), variable -> sourceProject.getAssignments().get(variable), (left, right) -> left));

        Assignments.Builder newAssignments = Assignments.builder();
        newAssignments.putAll(sourceProject.getAssignments());

        // Map from the aggregation reference to the IF condition reference which will be put in the mask.
        Map<VariableReferenceExpression, VariableReferenceExpression> aggregationReferenceToConditionReference = new HashMap<>();
        // Map from the aggregation reference to the IF result reference. This only contains the aggregates where the IF can be safely unwrapped.
        // E.g., SUM(IF(CARDINALITY(array) > 0, array[1])) will not be included in this map as array[1] can return errors if we unwrap the IF.
        Map<VariableReferenceExpression, VariableReferenceExpression> aggregationReferenceToIfResultReference = new HashMap<>();

        boolean unwrapIfEnabled = (getAggregationIfToFilterRewriteStrategy(context.getSession()) == AggregationIfToFilterRewriteStrategy.UNWRAP_IF);
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : sourceAssignments.entrySet()) {
            VariableReferenceExpression outputVariable = entry.getKey();
            SpecialFormExpression ifExpression = (SpecialFormExpression) entry.getValue();

            RowExpression condition = ifExpression.getArguments().get(0);
            VariableReferenceExpression conditionReference = context.getVariableAllocator().newVariable(condition);
            newAssignments.put(conditionReference, condition);
            aggregationReferenceToConditionReference.put(outputVariable, conditionReference);

            if (unwrapIfEnabled && canUnwrapIf(ifExpression)) {
                RowExpression trueResult = ifExpression.getArguments().get(1);
                VariableReferenceExpression ifResultReference = context.getVariableAllocator().newVariable(trueResult);
                newAssignments.put(ifResultReference, trueResult);
                aggregationReferenceToIfResultReference.put(outputVariable, ifResultReference);
            }
        }

        // Build new aggregations.
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        // Stores the masks used to build the filter predicates. Use set to dedup the predicates.
        ImmutableSortedSet.Builder<VariableReferenceExpression> masks = ImmutableSortedSet.naturalOrder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            VariableReferenceExpression output = entry.getKey();
            Aggregation aggregation = entry.getValue();
            if (!aggregationsToRewrite.contains(aggregation)) {
                aggregations.put(output, aggregation);
                continue;
            }
            VariableReferenceExpression aggregationReference = (VariableReferenceExpression) aggregation.getArguments().get(0);
            CallExpression callExpression = aggregation.getCall();
            VariableReferenceExpression ifResultReference = aggregationReferenceToIfResultReference.get(aggregationReference);
            if (ifResultReference != null) {
                callExpression = new CallExpression(
                        callExpression.getDisplayName(),
                        callExpression.getFunctionHandle(),
                        callExpression.getType(),
                        ImmutableList.of(ifResultReference));
            }
            VariableReferenceExpression mask = aggregationReferenceToConditionReference.get(aggregationReference);
            aggregations.put(output, new Aggregation(
                    callExpression,
                    Optional.empty(),
                    aggregation.getOrderBy(),
                    aggregation.isDistinct(),
                    Optional.of(aggregationReferenceToConditionReference.get(aggregationReference))));
            masks.add(mask);
        }

        RowExpression predicate = TRUE_CONSTANT;
        if (!aggregationNode.hasNonEmptyGroupingSet() && aggregationsToRewrite.size() == aggregationNode.getAggregations().size()) {
            // All aggregations are rewritten by this rule. We can add a filter with all the masks to make the query more efficient.
            predicate = or(masks.build());
        }
        return Result.ofPlanNode(
                new AggregationNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        sourceProject.getSource(),
                                        newAssignments.build()),
                                predicate),
                        aggregations.build(),
                        aggregationNode.getGroupingSets(),
                        aggregationNode.getPreGroupedVariables(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashVariable(),
                        aggregationNode.getGroupIdVariable()));
    }

    private boolean shouldRewriteAggregation(Aggregation aggregation, ProjectNode sourceProject)
    {
        if (functionAndTypeManager.getFunctionMetadata(aggregation.getFunctionHandle()).isCalledOnNullInput()) {
            // This rewrite will filter out the null values. It could change the behavior if the aggregation is also applied on NULLs.
            return false;
        }
        if (!(aggregation.getArguments().size() == 1 && aggregation.getArguments().get(0) instanceof VariableReferenceExpression)) {
            // Currently we only handle aggregation with a single VariableReferenceExpression. The detailed expressions are in a project node below this aggregation.
            return false;
        }
        if (aggregation.getFilter().isPresent() || aggregation.getMask().isPresent()) {
            // Do not rewrite the aggregation if it already has a filter or mask.
            return false;
        }
        RowExpression sourceExpression = sourceProject.getAssignments().get((VariableReferenceExpression) aggregation.getArguments().get(0));
        if (!(sourceExpression instanceof SpecialFormExpression) || !rowExpressionDeterminismEvaluator.isDeterministic(sourceExpression)) {
            return false;
        }
        SpecialFormExpression expression = (SpecialFormExpression) sourceExpression;
        // Only rewrite the aggregation if the else branch is not present or the else result is NULL.
        return expression.getForm() == IF && Expressions.isNull(expression.getArguments().get(2));
    }

    private boolean canUnwrapIf(SpecialFormExpression ifExpression)
    {
        // Some use cases use IF expression to avoid returning errors when evaluating the true branch. For example, IF(CARDINALITY(array) > 0, array[1])).
        // We shouldn't unwrap the IF for those cases.
        // But if the condition expression doesn't reference any variables referenced in the true branch, unwrapping the if should not cause exceptions for the true branch.
        Set<VariableReferenceExpression> ifConditionReferences = VariablesExtractor.extractUnique(ifExpression.getArguments().get(0));
        Set<VariableReferenceExpression> ifResultReferences = VariablesExtractor.extractUnique(ifExpression.getArguments().get(1));
        if (ifConditionReferences.stream().noneMatch(ifResultReferences::contains)) {
            return true;
        }

        AtomicBoolean result = new AtomicBoolean(true);
        ifExpression.getArguments().get(1).accept(new DefaultRowExpressionTraversalVisitor<AtomicBoolean>()
        {
            @Override
            public Void visitLambda(LambdaDefinitionExpression lambda, AtomicBoolean result)
            {
                // Unwrapping the IF expression in the aggregate might cause issues if the true branch return errors for rows not matching the filters.
                // To be safe, we don't unwrap the IF expressions when the true branch has lambdas.
                result.set(false);
                return null;
            }

            @Override
            public Void visitCall(CallExpression call, AtomicBoolean result)
            {
                Optional<OperatorType> operatorType = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getOperatorType();
                // Unwrapping the IF expression in the aggregate might cause issues if the true branch return errors for rows not matching the filters.
                // For example, array[1] could return out of bound error and a / b could return DIVISION_BY_ZERO error. So we doesn't unwrap the IF expression in these cases.
                if (operatorType.isPresent() && (operatorType.get() == OperatorType.DIVIDE || operatorType.get() == OperatorType.SUBSCRIPT)) {
                    result.set(false);
                    return null;
                }
                return super.visitCall(call, result);
            }
        }, result);
        return result.get();
    }
}

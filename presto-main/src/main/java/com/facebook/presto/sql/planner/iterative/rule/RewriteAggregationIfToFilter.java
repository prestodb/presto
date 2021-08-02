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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.Expressions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSortedMap.toImmutableSortedMap;
import static java.util.function.Function.identity;

/**
 * A optimizer rule which rewrites
 * AGG(IF(condition, expr))
 * to
 * AGG(expr) FILTER (WHERE condition).
 * <p>
 * The latter plan is more efficient because:
 * 1. The filter can be pushed down to the scan node.
 * 2. The rows not matching the condition are not aggregated.
 * 3. The IF() expression wrapper is removed.
 */
public class RewriteAggregationIfToFilter
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(CHILD)));

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
        // We don't remove the IF expression now in case the aggregation has other references to it. These will be cleaned up by the PruneUnreferencedOutputs rule later.
        newAssignments.putAll(sourceProject.getAssignments());

        // Map from the aggregation reference to the IF condition reference.
        Map<VariableReferenceExpression, VariableReferenceExpression> aggregationReferenceToConditionReference = new HashMap<>();
        // Map from the aggregation reference to the IF result reference.
        Map<VariableReferenceExpression, VariableReferenceExpression> aggregationReferenceToIfResultReference = new HashMap<>();

        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : sourceAssignments.entrySet()) {
            VariableReferenceExpression outputVariable = entry.getKey();
            SpecialFormExpression ifExpression = (SpecialFormExpression) entry.getValue();

            RowExpression condition = ifExpression.getArguments().get(0);
            VariableReferenceExpression conditionReference = context.getVariableAllocator().newVariable(condition);
            newAssignments.put(conditionReference, condition);
            aggregationReferenceToConditionReference.put(outputVariable, conditionReference);

            RowExpression trueResult = ifExpression.getArguments().get(1);
            VariableReferenceExpression ifResultReference = context.getVariableAllocator().newVariable(trueResult);
            newAssignments.put(ifResultReference, trueResult);
            aggregationReferenceToIfResultReference.put(outputVariable, ifResultReference);
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
            VariableReferenceExpression mask = aggregationReferenceToConditionReference.get(aggregationReference);
            aggregations.put(output, new Aggregation(
                    new CallExpression(
                            callExpression.getDisplayName(),
                            callExpression.getFunctionHandle(),
                            callExpression.getType(),
                            ImmutableList.of(aggregationReferenceToIfResultReference.get(aggregationReference))),
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
        if (!(aggregation.getArguments().size() == 1 && aggregation.getArguments().get(0) instanceof VariableReferenceExpression)) {
            // Currently we only handle aggregation with a single VariableReferenceExpression. The detailed expressions are in a project node below this aggregation.
            return false;
        }
        if (aggregation.getFilter().isPresent() || aggregation.getMask().isPresent()) {
            // Do not rewrite the aggregation if it already has a filter or mask.
            return false;
        }
        RowExpression sourceExpression = sourceProject.getAssignments().get((VariableReferenceExpression) aggregation.getArguments().get(0));
        if (!(sourceExpression instanceof SpecialFormExpression)) {
            return false;
        }
        SpecialFormExpression expression = (SpecialFormExpression) sourceExpression;
        // Only rewrite the aggregation if the else branch is not present.
        return expression.getForm() == IF && Expressions.isNull(expression.getArguments().get(2));
    }
}

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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPushdownThroughUnnest;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.planner.plan.Patterns.project;

/**
 * Pushes projections and filter conjuncts that don't depend on unnest output
 * variables below the UnnestNode. This avoids recomputing expressions and
 * filtering rows after they've been multiplied by the unnest.
 *
 * Handles these plan shapes:
 * <pre>
 * 1. Project -> Unnest
 * 2. Project -> Filter -> Unnest
 * </pre>
 *
 * Example (shape 1):
 * <pre>
 * - Project (x + 1 AS x_plus_1, y)       - Project (x_plus_1, y)
 *   - Unnest (a -> y)                       - Unnest (a -> y)
 *     - TableScan (x, a)         =>           - Project (x + 1 AS x_plus_1, a)
 *                                                 - TableScan (x, a)
 * </pre>
 *
 * Example (shape 2):
 * <pre>
 * - Project (x + 1 AS x_plus_1, y)       - Project (x_plus_1, y)
 *   - Filter (x > 10 AND y > 0)            - Filter (y > 0)
 *     - Unnest (a -> y)            =>         - Unnest (a -> y)
 *       - TableScan (x, a)                       - Filter (x > 10)
 *                                                     - Project (x + 1 AS x_plus_1, a)
 *                                                         - TableScan (x, a)
 * </pre>
 */
public class PushdownThroughUnnest
        implements Rule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();
    private final DeterminismEvaluator determinismEvaluator;

    public PushdownThroughUnnest(FunctionAndTypeManager functionAndTypeManager)
    {
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushdownThroughUnnest(session);
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        // Determine the plan shape: Project -> Unnest or Project -> Filter -> Unnest
        PlanNode child = context.getLookup().resolve(project.getSource());

        UnnestNode unnest;
        Optional<FilterNode> filterNode;

        if (child instanceof UnnestNode) {
            unnest = (UnnestNode) child;
            filterNode = Optional.empty();
        }
        else if (child instanceof FilterNode && context.getLookup().resolve(((FilterNode) child).getSource()) instanceof UnnestNode) {
            filterNode = Optional.of((FilterNode) child);
            unnest = (UnnestNode) context.getLookup().resolve(((FilterNode) child).getSource());
        }
        else {
            return Result.empty();
        }

        // Determine which variables are produced by the unnest operation itself
        Set<VariableReferenceExpression> unnestProducedVariables = getUnnestProducedVariables(unnest);

        // Partition project assignments into pushable and remaining
        Map<VariableReferenceExpression, RowExpression> pushableAssignments = new LinkedHashMap<>();
        Map<VariableReferenceExpression, RowExpression> remainingAssignments = new LinkedHashMap<>();

        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : project.getAssignments().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            RowExpression expression = entry.getValue();

            Set<VariableReferenceExpression> referencedVariables = VariablesExtractor.extractUnique(expression);

            if (referencedVariables.stream().noneMatch(unnestProducedVariables::contains)
                    && !isIdentityAssignment(variable, expression)
                    && determinismEvaluator.isDeterministic(expression)) {
                pushableAssignments.put(variable, expression);
            }
            else {
                remainingAssignments.put(variable, expression);
            }
        }

        // Partition filter conjuncts (if filter exists) into pushable and remaining
        List<RowExpression> pushableConjuncts = new ArrayList<>();
        List<RowExpression> remainingConjuncts = new ArrayList<>();

        if (filterNode.isPresent()) {
            List<RowExpression> conjuncts = extractConjuncts(filterNode.get().getPredicate());
            for (RowExpression conjunct : conjuncts) {
                Set<VariableReferenceExpression> referencedVariables = VariablesExtractor.extractUnique(conjunct);
                if (referencedVariables.stream().noneMatch(unnestProducedVariables::contains)
                        && determinismEvaluator.isDeterministic(conjunct)) {
                    pushableConjuncts.add(conjunct);
                }
                else {
                    remainingConjuncts.add(conjunct);
                }
            }
        }

        // Nothing to push down
        if (pushableAssignments.isEmpty() && pushableConjuncts.isEmpty()) {
            return Result.empty();
        }

        // Build the new source below the unnest
        PlanNode newUnnestSource = unnest.getSource();

        // Add pushed-down filter below the unnest
        if (filterNode.isPresent() && !pushableConjuncts.isEmpty()) {
            newUnnestSource = new FilterNode(
                    filterNode.get().getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newUnnestSource,
                    and(pushableConjuncts));
        }

        // Add pushed-down projections below the unnest
        if (!pushableAssignments.isEmpty()) {
            Assignments.Builder belowUnnestAssignments = Assignments.builder();

            // Pass through all variables needed by the unnest source
            for (VariableReferenceExpression variable : newUnnestSource.getOutputVariables()) {
                belowUnnestAssignments.put(variable, variable);
            }

            // Add the pushable expressions
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : pushableAssignments.entrySet()) {
                belowUnnestAssignments.put(entry.getKey(), entry.getValue());
            }

            newUnnestSource = new ProjectNode(
                    project.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newUnnestSource,
                    belowUnnestAssignments.build(),
                    project.getLocality());
        }

        // Build the new UnnestNode
        ImmutableList.Builder<VariableReferenceExpression> newReplicateVariables = ImmutableList.builder();
        newReplicateVariables.addAll(unnest.getReplicateVariables());
        newReplicateVariables.addAll(pushableAssignments.keySet());

        PlanNode result = new UnnestNode(
                unnest.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                newUnnestSource,
                newReplicateVariables.build(),
                unnest.getUnnestVariables(),
                unnest.getOrdinalityVariable());

        // Add remaining filter on top (if any conjuncts remain)
        if (!remainingConjuncts.isEmpty()) {
            result = new FilterNode(
                    filterNode.get().getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    result,
                    and(remainingConjuncts));
        }

        // Build the remaining project on top
        Assignments.Builder topAssignments = Assignments.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : remainingAssignments.entrySet()) {
            topAssignments.put(entry.getKey(), entry.getValue());
        }
        for (VariableReferenceExpression pushedVariable : pushableAssignments.keySet()) {
            topAssignments.put(pushedVariable, pushedVariable);
        }

        result = new ProjectNode(
                project.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                result,
                topAssignments.build(),
                project.getLocality());

        return Result.ofPlanNode(result);
    }

    private static Set<VariableReferenceExpression> getUnnestProducedVariables(UnnestNode unnest)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.<VariableReferenceExpression>builder();
        unnest.getUnnestVariables().values().stream()
                .flatMap(List::stream)
                .forEach(builder::add);
        unnest.getOrdinalityVariable().ifPresent(builder::add);
        return builder.build();
    }

    private static boolean isIdentityAssignment(VariableReferenceExpression variable, RowExpression expression)
    {
        return expression instanceof VariableReferenceExpression
                && variable.equals(expression);
    }
}

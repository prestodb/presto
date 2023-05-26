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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isRewriteLeftJoinNullFilterToSemiJoinEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.coalesceNullToFalse;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

/**
 * Rewrite Left Join with is null check on right join key to semi join, when other output from right input is not used.
 * <pre>
 *     - Filter
 *          r.k is null
 *          - Left Join
 *              l.k = r.k
 *              - Scan l
 *                  k
 *              - Scan r
 *                  k
 * </pre>
 * to
 * <pre>
 *     - Project
 *          l.k := l.k
 *          r.k := NULL
 *          - Filter
 *              not(coalesce(semiJoinOutput, false))
 *              - Semi Join
 *                  source: l.k
 *                  filter source: r.k
 *                  filter output: semiJoinOutput
 *                  - scan l
 *                      k
 *                  - scan r
 *                      k
 * </pre>
 */
public class LeftJoinNullFilterToSemiJoin
        implements Rule<FilterNode>
{
    private static final Capture<JoinNode> CHILD = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(join().matching(x -> x.getType().equals(JoinNode.Type.LEFT)
            && x.getCriteria().size() == 1 && !x.getFilter().isPresent() && x.getDynamicFilters().isEmpty()).capturedAs(CHILD)));
    private final FunctionAndTypeManager functionAndTypeManager;

    public LeftJoinNullFilterToSemiJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteLeftJoinNullFilterToSemiJoinEnabled(session);
    }

    private boolean isRightKeyNullExpression(RowExpression expression, VariableReferenceExpression rightKey)
    {
        return expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm().equals(SpecialFormExpression.Form.IS_NULL)
                && ((SpecialFormExpression) expression).getArguments().get(0).equals(rightKey);
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(CHILD);
        VariableReferenceExpression rightKey = joinNode.getCriteria().get(0).getRight();
        VariableReferenceExpression leftKey = joinNode.getCriteria().get(0).getLeft();
        // if output other than join key from right side are used, cannot rewrite
        if (joinNode.getOutputVariables().stream().anyMatch(x -> joinNode.getRight().getOutputVariables().contains(x) && !x.equals(rightKey))) {
            return Result.empty();
        }

        List<RowExpression> andConjuncts = extractConjuncts(filterNode.getPredicate());
        List<RowExpression> rightKeyNull = andConjuncts.stream().filter(x -> isRightKeyNullExpression(x, rightKey)).collect(toImmutableList());
        // if no null check just return
        if (rightKeyNull.isEmpty()) {
            return Result.empty();
        }
        List<RowExpression> remainingConjunct = andConjuncts.stream().filter(x -> !rightKeyNull.contains(x)).collect(toImmutableList());
        List<VariableReferenceExpression> variablesInRemainingConjuncts = remainingConjunct.stream().flatMap(x -> extractAll(x).stream()).collect(toImmutableList());
        // Variables from right side referred in other expressions, not only null check.
        if (variablesInRemainingConjuncts.stream().anyMatch(x -> joinNode.getRight().getOutputVariables().contains(x))) {
            return Result.empty();
        }

        VariableReferenceExpression semiJoinOutput = context.getVariableAllocator().newVariable("semiJoinOutput", BOOLEAN);
        SemiJoinNode semiJoinNode = new SemiJoinNode(filterNode.getSourceLocation(), context.getIdAllocator().getNextId(), joinNode.getLeft(), joinNode.getRight(),
                leftKey, rightKey, semiJoinOutput, Optional.empty(), Optional.empty(), Optional.empty(), ImmutableMap.of());
        RowExpression filterExpression = not(functionAndTypeManager, coalesceNullToFalse(semiJoinOutput));
        FilterNode semiFilterNode = new FilterNode(filterNode.getSourceLocation(), context.getIdAllocator().getNextId(), semiJoinNode, filterExpression);
        Map<VariableReferenceExpression, RowExpression> outputAssignments =
                filterNode.getOutputVariables().stream().collect(toImmutableMap(identity(), x -> x.equals(rightKey) ? constantNull(rightKey.getType()) : x));
        PlanNode planNode = new ProjectNode(context.getIdAllocator().getNextId(), semiFilterNode, Assignments.builder().putAll(outputAssignments).build());
        if (!remainingConjunct.isEmpty()) {
            planNode = new FilterNode(filterNode.getSourceLocation(), context.getIdAllocator().getNextId(), planNode, and(remainingConjunct));
        }
        return Result.ofPlanNode(planNode);
    }
}

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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isRewriteCrossJoinArrayContainsToInnerJoinEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Inner join with contains function inside join clause will be run as cross join with filter.
 * When the join condition has pattern of contains(array, element), we can rewrite it to a inner join. For example:
 * <pre>
 * - Filter contains(l_array_key, r_key)
 *      - Cross join
 *          - scan l
 *          - scan r
 * </pre>
 * into:
 * <pre>
 *     - Join
 *          field = r_key
 *          - Unnest
 *              field <- unnest distinct_array
 *              - project
 *                  distinct_array := array_distinct(l_array_key)
 *                  - scan l
 *                      l_array_key
 *              - scan r
 *                  r_key
 * </pre>
 */
public class CrossJoinWithArrayContainsToInnerJoin
        implements Rule<FilterNode>
{
    private static final String CONTAINS_FUNCTION_NAME = "presto.default.contains";
    private static final List<Type> SUPPORTED_JOIN_KEY_TYPE = ImmutableList.of(BIGINT, INTEGER, VARCHAR, DATE);
    private static final Capture<JoinNode> CHILD = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(join().matching(x -> x.getType().equals(JoinNode.Type.INNER) && x.getCriteria().isEmpty()).capturedAs(CHILD)));

    private final FunctionAndTypeManager functionAndTypeManager;

    public CrossJoinWithArrayContainsToInnerJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    public static RowExpression getCandidateArrayContainsExpression(RowExpression filterPredicate, List<VariableReferenceExpression> leftInput, List<VariableReferenceExpression> rightInput)
    {
        List<RowExpression> andConjuncts = extractConjuncts(filterPredicate);
        for (RowExpression conjunct : andConjuncts) {
            if (isValidArrayContainsFilter(conjunct, leftInput, rightInput)) {
                return conjunct;
            }
        }
        return null;
    }

    private static boolean isValidArrayContainsFilter(RowExpression filterExpression, List<VariableReferenceExpression> left, List<VariableReferenceExpression> right)
    {
        if (filterExpression instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) filterExpression;
            if (callExpression.getFunctionHandle().getName().equals(CONTAINS_FUNCTION_NAME)) {
                RowExpression array = callExpression.getArguments().get(0);
                RowExpression element = callExpression.getArguments().get(1);
                checkState(array.getType() instanceof ArrayType && ((ArrayType) array.getType()).getElementType().equals(element.getType()));
                return (SUPPORTED_JOIN_KEY_TYPE.contains(element.getType())) && ((left.contains(array) && right.contains(element)) || (right.contains(array) && left.contains(element)));
            }
        }
        return false;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteCrossJoinArrayContainsToInnerJoinEnabled(session);
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(CHILD);
        if (!(joinNode.getType().equals(JoinNode.Type.INNER) && joinNode.getCriteria().isEmpty())) {
            return Result.empty();
        }
        List<VariableReferenceExpression> leftInput = joinNode.getLeft().getOutputVariables();
        List<VariableReferenceExpression> rightInput = joinNode.getRight().getOutputVariables();
        RowExpression filterExpression = node.getPredicate();
        RowExpression arrayContainsExpression = getCandidateArrayContainsExpression(filterExpression, leftInput, rightInput);
        if (arrayContainsExpression == null) {
            return Result.empty();
        }
        List<RowExpression> andConjuncts = extractConjuncts(filterExpression);
        List<RowExpression> remainingConjuncts = andConjuncts.stream().filter(x -> !x.equals(arrayContainsExpression)).collect(toImmutableList());

        VariableReferenceExpression array = (VariableReferenceExpression) ((CallExpression) arrayContainsExpression).getArguments().get(0);
        VariableReferenceExpression element = (VariableReferenceExpression) ((CallExpression) arrayContainsExpression).getArguments().get(1);
        boolean arrayAtLeftInput = leftInput.contains(array);
        PlanNode inputWithArray = arrayAtLeftInput ? joinNode.getLeft() : joinNode.getRight();

        CallExpression arrayDistinct = call(functionAndTypeManager, "array_distinct", new ArrayType(element.getType()), array);
        VariableReferenceExpression arrayDistinctVariable = context.getVariableAllocator().newVariable(arrayDistinct);
        PlanNode project = PlannerUtils.addProjections(inputWithArray, context.getIdAllocator(), ImmutableMap.of(arrayDistinctVariable, arrayDistinct));
        VariableReferenceExpression unnestVariable = context.getVariableAllocator().newVariable("field", element.getType());
        UnnestNode unnest = new UnnestNode(inputWithArray.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                project,
                project.getOutputVariables(),
                ImmutableMap.of(arrayDistinctVariable, ImmutableList.of(unnestVariable)),
                Optional.empty());

        JoinNode.EquiJoinClause equiJoinClause = arrayAtLeftInput ? new JoinNode.EquiJoinClause(unnestVariable, element) : new JoinNode.EquiJoinClause(element, unnestVariable);

        JoinNode newJoinNode = new JoinNode(joinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                joinNode.getType(),
                arrayAtLeftInput ? unnest : joinNode.getLeft(),
                arrayAtLeftInput ? joinNode.getRight() : unnest,
                ImmutableList.of(equiJoinClause),
                joinNode.getOutputVariables(),
                joinNode.getFilter(),
                Optional.empty(),
                Optional.empty(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters());

        if (!remainingConjuncts.isEmpty()) {
            return Result.ofPlanNode(new FilterNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), newJoinNode, and(remainingConjuncts)));
        }

        return Result.ofPlanNode(newJoinNode);
    }
}

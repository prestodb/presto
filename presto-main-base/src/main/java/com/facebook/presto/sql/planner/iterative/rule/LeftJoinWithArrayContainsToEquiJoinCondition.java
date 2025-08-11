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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.LeftJoinArrayContainsToInnerJoinStrategy;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getLeftJoinArrayContainsToInnerJoinStrategy;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * When the join condition of a left join has pattern of contains(array, element) where array is from the right-side relation and element is from the left-side relation, we can rewrite it as an equi join condition. For example:
 * <pre>
 * - Left Join
 *      empty join clause
 *      filter: contains(r_array, l_key)
 *      - scan l
 *      - scan r
 * </pre>
 * into:
 * <pre>
 *     - Left Join
 *          l_key = field
 *          - scan l
 *          - Unnest
 *              field <- unnest distinct_array
 *              - project
 *                  distinct_array := remove_nulls(array_distinct(r_array))
 *                  - scan r
 *                      r_array
 * </pre>
 */
public class LeftJoinWithArrayContainsToEquiJoinCondition
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(x -> x.getType().equals(JoinType.LEFT) && x.getCriteria().isEmpty() && x.getFilter().isPresent());
    private final FunctionAndTypeManager functionAndTypeManager;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    private final FunctionResolution functionResolution;

    public LeftJoinWithArrayContainsToEquiJoinCondition(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        // TODO: implement cost based with HBO
        return getLeftJoinArrayContainsToInnerJoinStrategy(session).equals(LeftJoinArrayContainsToInnerJoinStrategy.ALWAYS_ENABLED);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        RowExpression filterPredicate = node.getFilter().get();
        List<VariableReferenceExpression> leftInput = node.getLeft().getOutputVariables();
        List<VariableReferenceExpression> rightInput = node.getRight().getOutputVariables();
        List<RowExpression> andConjuncts = extractConjuncts(filterPredicate);
        Optional<RowExpression> arrayContains = andConjuncts.stream().filter(rowExpression -> isSupportedJoinCondition(rowExpression, leftInput, rightInput)).findFirst();
        if (!arrayContains.isPresent()) {
            return Result.empty();
        }
        List<RowExpression> remainingConjuncts = andConjuncts.stream().filter(rowExpression -> !rowExpression.equals(arrayContains.get())).collect(toImmutableList());
        RowExpression array = ((CallExpression) arrayContains.get()).getArguments().get(0);
        RowExpression element = ((CallExpression) arrayContains.get()).getArguments().get(1);
        checkState(array.getType() instanceof ArrayType && ((ArrayType) array.getType()).getElementType().equals(element.getType()));

        PlanNode newLeft = node.getLeft();
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> leftAssignment = ImmutableMap.builder();
        VariableReferenceExpression elementVariable;
        if (!(element instanceof VariableReferenceExpression)) {
            elementVariable = context.getVariableAllocator().newVariable(element);
            leftAssignment.put(elementVariable, element);
            newLeft = PlannerUtils.addProjections(node.getLeft(), context.getIdAllocator(), leftAssignment.build());
        }
        else {
            elementVariable = (VariableReferenceExpression) element;
        }

        CallExpression arrayDistinct = call(functionAndTypeManager, "array_distinct", new ArrayType(element.getType()), array);
        CallExpression arrayFilterNull = call(functionAndTypeManager, "remove_nulls", arrayDistinct.getType(), ImmutableList.of(arrayDistinct));
        VariableReferenceExpression arrayFilterNullVariable = context.getVariableAllocator().newVariable(arrayFilterNull);
        PlanNode newRight = PlannerUtils.addProjections(node.getRight(), context.getIdAllocator(), ImmutableMap.of(arrayFilterNullVariable, arrayFilterNull));
        VariableReferenceExpression unnestVariable = context.getVariableAllocator().newVariable("unnest", element.getType());

        UnnestNode unnestNode = new UnnestNode(newRight.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                newRight,
                newRight.getOutputVariables(),
                ImmutableMap.of(arrayFilterNullVariable, ImmutableList.of(unnestVariable)),
                Optional.empty());

        EquiJoinClause equiJoinClause = new EquiJoinClause(elementVariable, unnestVariable);

        return Result.ofPlanNode(new JoinNode(node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getType(),
                newLeft,
                unnestNode,
                ImmutableList.of(equiJoinClause),
                node.getOutputVariables(),
                remainingConjuncts.isEmpty() ? Optional.empty() : Optional.of(and(remainingConjuncts)),
                Optional.empty(),
                Optional.empty(),
                node.getDistributionType(),
                node.getDynamicFilters()));
    }

    private boolean isSupportedJoinCondition(RowExpression rowExpression, List<VariableReferenceExpression> leftInput, List<VariableReferenceExpression> rightInput)
    {
        if (rowExpression instanceof CallExpression && functionResolution.isArrayContainsFunction(((CallExpression) rowExpression).getFunctionHandle())) {
            RowExpression arrayExpression = ((CallExpression) rowExpression).getArguments().get(0);
            RowExpression elementExpression = ((CallExpression) rowExpression).getArguments().get(1);
            return determinismEvaluator.isDeterministic(arrayExpression) && rightInput.containsAll(extractAll(arrayExpression))
                    && determinismEvaluator.isDeterministic(elementExpression) && leftInput.containsAll(extractAll(elementExpression));
        }
        return false;
    }
}

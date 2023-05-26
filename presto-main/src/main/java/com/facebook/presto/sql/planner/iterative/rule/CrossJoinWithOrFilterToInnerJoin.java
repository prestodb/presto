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
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isRewriteCrossJoinOrToInnerJoinEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractDisjuncts;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.coalesceNullToFalse;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.not;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Inner join with "or" inside join clause will be run as cross join with filter, which can degrade performance, especially when selectivity of join is low.
 * When the join condition has pattern of l_key1=r_key1 or l_key1=r_key2, we can rewrite it to a inner join. For example:
 * <pre>
 * - Filter l_key1=r_key1 or l_key1=r_key2
 *      - Cross join
 *          - scan l
 *          - scan r
 * </pre>
 * into:
 * <pre>
 *     - Project
 *          - Filter
 *              CASE field WHEN 1 l_key1 = r_key1 WHEN 2 NOT(coalesce(l_key1 = r_key1, false)) and l_key2 = r_key2 else NULL END
 *              - Inner Join
 *                  l_key = r_key and l_field = r_field
 *                  - Project
 *                      key1 := key1
 *                      key2 := key2
 *                      field := field
 *                      key := case field when 1 then key1 when 2 then key2 else null end
 *                      - Unnest
 *                          field <- unnest arr
 *                          - Project
 *                              key1 := key1
 *                              key2 := key2
 *                              arr := array[1, 2]
 *                              _ scan l
 *                                  key1, key2
 *                   - Project
 *                      key1 := key1
 *                      key2 := key2
 *                      field := field
 *                      key := case field when 1 then key1 when 2 then key2 else null end
 *                      - Unnest
 *                          field <- unnest arr
 *                          - Project
 *                              key1 := key1
 *                              key2 := key2
 *                              arr := array[1, 2]
 *                              _ scan r
 *                                  key1, key2
 * </pre>
 */
public class CrossJoinWithOrFilterToInnerJoin
        implements Rule<FilterNode>
{
    private static final List<Type> SUPPORTED_JOIN_KEY_TYPE = ImmutableList.of(BIGINT, INTEGER, VARCHAR, DATE);
    private static final Capture<JoinNode> CHILD = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(join().matching(x -> x.getType().equals(JoinNode.Type.INNER) && x.getCriteria().isEmpty()).capturedAs(CHILD)));

    private final FunctionAndTypeManager functionAndTypeManager;

    public CrossJoinWithOrFilterToInnerJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    // Valid only if it's an equal expression, and one argument from left of join and the other argument from right of join.
    private static boolean isValidExpression(RowExpression rowExpression, List<VariableReferenceExpression> leftInput, List<VariableReferenceExpression> rightInput)
    {
        if (!(rowExpression instanceof CallExpression) || !((CallExpression) rowExpression).getDisplayName().equals("EQUAL")) {
            return false;
        }
        CallExpression callExpression = (CallExpression) rowExpression;
        RowExpression argument0 = callExpression.getArguments().get(0);
        RowExpression argument1 = callExpression.getArguments().get(1);
        return SUPPORTED_JOIN_KEY_TYPE.containsAll(ImmutableList.of(argument0.getType(), argument1.getType()))
                && ((leftInput.contains(argument0) && rightInput.contains(argument1)) || (leftInput.contains(argument1) && rightInput.contains(argument0)));
    }

    public static RowExpression getCandidateOrExpression(RowExpression filterPredicate, List<VariableReferenceExpression> leftInput, List<VariableReferenceExpression> rightInput)
    {
        List<RowExpression> andConjuncts = extractConjuncts(filterPredicate);
        for (RowExpression conjunct : andConjuncts) {
            List<RowExpression> equalExpressionList = extractDisjuncts(conjunct);
            if (!equalExpressionList.isEmpty() && equalExpressionList.stream().allMatch(x -> isValidExpression(x, leftInput, rightInput))) {
                return conjunct;
            }
        }
        return null;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteCrossJoinOrToInnerJoinEnabled(session);
    }

    private RewrittenJoinInput rewriteJoinInput(List<VariableReferenceExpression> variablesInOrCondition, PlanNode joinInput, Type finalJoinKeyType, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> castVariableMap = new HashMap<>();
        Map<VariableReferenceExpression, RowExpression> castExpressionMap = new HashMap<>();
        if (!variablesInOrCondition.stream().allMatch(x -> x.getType().equals(finalJoinKeyType))) {
            // cast all to varchar type
            for (int i = 0; i < variablesInOrCondition.size(); ++i) {
                CallExpression castExpression = call("CAST", functionAndTypeManager.lookupCast(CastType.CAST, variablesInOrCondition.get(i).getType(), VARCHAR), VARCHAR, variablesInOrCondition.get(i));
                VariableReferenceExpression castVariable = variableAllocator.newVariable(castExpression);
                castVariableMap.put(variablesInOrCondition.get(i), castVariable);
                castExpressionMap.put(castVariable, castExpression);
            }
        }

        ImmutableList.Builder<RowExpression> constantsArgument = ImmutableList.builder();
        for (int i = 0; i < variablesInOrCondition.size(); ++i) {
            constantsArgument.add(constant((long) i + 1, INTEGER));
        }
        CallExpression arrayConstruct = call(functionAndTypeManager, "array_constructor", new ArrayType(INTEGER), constantsArgument.build());

        VariableReferenceExpression arrayVariable = variableAllocator.newVariable(arrayConstruct);
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> projectAssignment = ImmutableMap.builder();
        PlanNode project = PlannerUtils.addProjections(joinInput, idAllocator, projectAssignment.put(arrayVariable, arrayConstruct).putAll(castExpressionMap).build());

        VariableReferenceExpression unnestVariable = variableAllocator.newVariable("field", INTEGER);
        UnnestNode unnest = new UnnestNode(joinInput.getSourceLocation(),
                idAllocator.getNextId(),
                project,
                project.getOutputVariables().stream().filter(x -> !x.equals(arrayVariable)).collect(toImmutableList()),
                ImmutableMap.of(arrayVariable, ImmutableList.of(unnestVariable)),
                Optional.empty());

        ImmutableList.Builder<RowExpression> whenExpression = ImmutableList.builder();
        whenExpression.add(unnestVariable);
        for (int i = 0; i < variablesInOrCondition.size(); ++i) {
            whenExpression.add(new SpecialFormExpression(WHEN, finalJoinKeyType, constant((long) i + 1, INTEGER), castVariableMap.isEmpty() ? variablesInOrCondition.get(i) : castVariableMap.get(variablesInOrCondition.get(i))));
        }
        whenExpression.add(constantNull(finalJoinKeyType));
        SpecialFormExpression joinKeyExpression = new SpecialFormExpression(SWITCH, finalJoinKeyType, whenExpression.build());
        VariableReferenceExpression newJoinVariable = variableAllocator.newVariable(joinKeyExpression);
        PlanNode rewrittenInput = PlannerUtils.addProjections(unnest, idAllocator, variableAllocator, ImmutableList.of(joinKeyExpression), ImmutableList.of(newJoinVariable));
        return new RewrittenJoinInput(rewrittenInput, unnestVariable, newJoinVariable);
    }

    private VariableReferenceExpression getVariableInEqualComparison(RowExpression rowExpression, List<VariableReferenceExpression> candidate)
    {
        checkArgument(rowExpression instanceof CallExpression && ((CallExpression) rowExpression).getDisplayName().equals("EQUAL"));
        CallExpression callExpression = (CallExpression) rowExpression;
        RowExpression argument0 = callExpression.getArguments().get(0);
        RowExpression argument1 = callExpression.getArguments().get(1);
        if (candidate.contains(argument0)) {
            return (VariableReferenceExpression) argument0;
        }
        else if (candidate.contains(argument1)) {
            return (VariableReferenceExpression) argument1;
        }
        checkState(false, "argument does not exist in candidate list");
        return null;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(CHILD);
        if (!(joinNode.getType().equals(JoinNode.Type.INNER) && joinNode.getCriteria().isEmpty())) {
            return Result.empty();
        }
        RowExpression candidateOrExpressions = getCandidateOrExpression(filterNode.getPredicate(), joinNode.getLeft().getOutputVariables(), joinNode.getRight().getOutputVariables());
        if (candidateOrExpressions == null) {
            return Result.empty();
        }
        List<RowExpression> andConjuncts = extractConjuncts(filterNode.getPredicate());
        List<RowExpression> leftAndConjuncts = andConjuncts.stream().filter(x -> !x.equals(candidateOrExpressions)).collect(toImmutableList());
        List<RowExpression> equalExpressionList = extractDisjuncts(candidateOrExpressions);
        List<VariableReferenceExpression> variablesUsedInOrComparisionFromLeft = equalExpressionList.stream().map(x -> getVariableInEqualComparison(x, joinNode.getLeft().getOutputVariables())).collect(toImmutableList());
        List<VariableReferenceExpression> variablesUsedInOrComparisionFromRight = equalExpressionList.stream().map(x -> getVariableInEqualComparison(x, joinNode.getRight().getOutputVariables())).collect(toImmutableList());

        if (variablesUsedInOrComparisionFromLeft.isEmpty() || variablesUsedInOrComparisionFromRight.isEmpty()) {
            return Result.empty();
        }
        // Apply optimization only when the variables in or condition is of type int/bigint/varchar/date types.
        if (variablesUsedInOrComparisionFromLeft.stream().anyMatch(x -> !SUPPORTED_JOIN_KEY_TYPE.contains(x.getType()))
                || variablesUsedInOrComparisionFromRight.stream().anyMatch(x -> !SUPPORTED_JOIN_KEY_TYPE.contains(x.getType()))) {
            return Result.empty();
        }

        // Check if all candidate variables are of the same type
        Type joinKeyType = VARCHAR;
        List<Type> leftOrPredicateTypes = variablesUsedInOrComparisionFromLeft.stream().map(x -> x.getType()).distinct().collect(toImmutableList());
        List<Type> rightOrPredicateTypes = variablesUsedInOrComparisionFromRight.stream().map(x -> x.getType()).distinct().collect(toImmutableList());
        if (leftOrPredicateTypes.size() == 1 && rightOrPredicateTypes.size() == 1 && leftOrPredicateTypes.get(0).equals(rightOrPredicateTypes.get(0))) {
            joinKeyType = leftOrPredicateTypes.get(0);
        }

        RewrittenJoinInput leftJoinInput = rewriteJoinInput(variablesUsedInOrComparisionFromLeft, joinNode.getLeft(), joinKeyType, context.getVariableAllocator(), context.getIdAllocator());
        RewrittenJoinInput rightJoinInput = rewriteJoinInput(variablesUsedInOrComparisionFromRight, joinNode.getRight(), joinKeyType, context.getVariableAllocator(), context.getIdAllocator());

        ImmutableList.Builder<VariableReferenceExpression> joinOutput = ImmutableList.builder();
        joinOutput.addAll(joinNode.getOutputVariables()).add(leftJoinInput.getJoinKey()).add(leftJoinInput.getUnnestIndex());
        JoinNode newJoinNode = new JoinNode(joinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                joinNode.getType(),
                leftJoinInput.getNode(),
                rightJoinInput.getNode(),
                ImmutableList.of(new JoinNode.EquiJoinClause(leftJoinInput.getJoinKey(), rightJoinInput.getJoinKey()),
                        new JoinNode.EquiJoinClause(leftJoinInput.getUnnestIndex(), rightJoinInput.getUnnestIndex())),
                joinOutput.build(),
                joinNode.getFilter(),
                Optional.empty(),
                Optional.empty(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters());

        // Deduplicate the rows which matched multiple times
        ImmutableList.Builder<RowExpression> whenExpression = ImmutableList.builder();
        whenExpression.add(leftJoinInput.getUnnestIndex());
        for (int i = 0; i < equalExpressionList.size(); ++i) {
            ImmutableList.Builder<RowExpression> matchCondition = ImmutableList.builder();
            for (int j = 0; j < i; ++j) {
                matchCondition.add(not(functionAndTypeManager, coalesceNullToFalse(equalExpressionList.get(j))));
            }
            matchCondition.add(equalExpressionList.get(i));
            whenExpression.add(new SpecialFormExpression(WHEN, BOOLEAN, constant((long) i + 1, INTEGER), and(matchCondition.build())));
        }
        whenExpression.add(constantNull(BOOLEAN));
        SpecialFormExpression dedupFilter = new SpecialFormExpression(SWITCH, BOOLEAN, whenExpression.build());
        FilterNode newFilterNode = new FilterNode(joinNode.getSourceLocation(), context.getIdAllocator().getNextId(), newJoinNode, dedupFilter);
        if (!leftAndConjuncts.isEmpty()) {
            newFilterNode = new FilterNode(filterNode.getSourceLocation(), context.getIdAllocator().getNextId(), newFilterNode, and(leftAndConjuncts));
        }
        // So that the output of new node is exactly the same
        Assignments.Builder identity = Assignments.builder();
        identity.putAll(filterNode.getOutputVariables().stream().collect(toImmutableMap(x -> x, x -> x)));
        ProjectNode projectUnusedOutput = new ProjectNode(context.getIdAllocator().getNextId(), newFilterNode, identity.build());
        return Result.ofPlanNode(projectUnusedOutput);
    }

    private static class RewrittenJoinInput
    {
        private final PlanNode node;
        private final VariableReferenceExpression unnestIndex;
        private final VariableReferenceExpression joinKey;

        public RewrittenJoinInput(PlanNode node, VariableReferenceExpression unnestIndex, VariableReferenceExpression joinKey)
        {
            this.node = node;
            this.unnestIndex = unnestIndex;
            this.joinKey = joinKey;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public VariableReferenceExpression getJoinKey()
        {
            return joinKey;
        }

        public VariableReferenceExpression getUnnestIndex()
        {
            return unnestIndex;
        }
    }
}

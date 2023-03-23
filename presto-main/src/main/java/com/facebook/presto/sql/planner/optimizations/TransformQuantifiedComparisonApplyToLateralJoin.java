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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.QuantifiedComparisonExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.plan.AggregationNode.globalAggregation;
import static com.facebook.presto.spi.relation.QuantifiedComparisonExpression.Quantifier.ALL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.sql.relational.Expressions.buildSwitch;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static com.facebook.presto.sql.relational.Expressions.searchedCaseExpression;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TransformQuantifiedComparisonApplyToLateralJoin
        implements PlanOptimizer
{
    private final StandardFunctionResolution functionResolution;
    private final LogicalRowExpressions logicalRowExpressions;

    public TransformQuantifiedComparisonApplyToLateralJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                functionAndTypeManager);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return rewriteWith(new Rewriter(functionResolution, idAllocator, variableAllocator, logicalRowExpressions), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final StandardFunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final LogicalRowExpressions logicalRowExpressions;

        public Rewriter(StandardFunctionResolution functionResolution, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, LogicalRowExpressions logicalRowExpressions)
        {
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.logicalRowExpressions = requireNonNull(logicalRowExpressions, "logicalRowExpressions is null");
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context)
        {
            if (node.getSubqueryAssignments().size() != 1) {
                return context.defaultRewrite(node);
            }

            RowExpression expression = getOnlyElement(node.getSubqueryAssignments().getExpressions());
            if (!(expression instanceof QuantifiedComparisonExpression)) {
                return context.defaultRewrite(node);
            }

            QuantifiedComparisonExpression quantifiedComparison = (QuantifiedComparisonExpression) expression;

            return rewriteQuantifiedApplyNode(node, quantifiedComparison, context);
        }

        private PlanNode rewriteQuantifiedApplyNode(ApplyNode node, QuantifiedComparisonExpression quantifiedComparison, RewriteContext<PlanNode> context)
        {
            PlanNode subqueryPlan = context.rewrite(node.getSubquery());

            VariableReferenceExpression outputColumn = getOnlyElement(subqueryPlan.getOutputVariables());
            Type outputColumnType = outputColumn.getType();
            checkState(outputColumnType.isOrderable(), "Subquery result type must be orderable");

            VariableReferenceExpression minValue = variableAllocator.newVariable(outputColumn.getSourceLocation(), "min", outputColumnType);
            VariableReferenceExpression maxValue = variableAllocator.newVariable(outputColumn.getSourceLocation(), "max", outputColumnType);
            VariableReferenceExpression countAllValue = variableAllocator.newVariable(outputColumn.getSourceLocation(), "count_all", BigintType.BIGINT);
            VariableReferenceExpression countNonNullValue = variableAllocator.newVariable(outputColumn.getSourceLocation(), "count_non_null", BigintType.BIGINT);

            List<RowExpression> outputColumnReferences = ImmutableList.of(outputColumn);

            subqueryPlan = new AggregationNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    subqueryPlan,
                    ImmutableMap.of(
                            minValue, new Aggregation(
                                    new CallExpression(
                                            quantifiedComparison.getSourceLocation(),
                                            "min",
                                            functionResolution.minFunction(outputColumnType),
                                            outputColumnType,
                                            outputColumnReferences),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.empty()),
                            maxValue, new Aggregation(
                                    new CallExpression(
                                            quantifiedComparison.getSourceLocation(),
                                            "max",
                                            functionResolution.maxFunction(outputColumnType),
                                            outputColumnType,
                                            outputColumnReferences),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.empty()),
                            countAllValue, new Aggregation(
                                    new CallExpression(
                                            quantifiedComparison.getSourceLocation(),
                                            "count",
                                            functionResolution.countFunction(),
                                            BIGINT,
                                            emptyList()),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.empty()),
                            countNonNullValue, new Aggregation(
                                    new CallExpression(
                                            quantifiedComparison.getSourceLocation(),
                                            "count",
                                            functionResolution.countFunction(outputColumnType),
                                            BIGINT,
                                            outputColumnReferences),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.empty())),
                    globalAggregation(),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            PlanNode lateralJoinNode = new LateralJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    context.rewrite(node.getInput()),
                    subqueryPlan,
                    node.getCorrelation(),
                    LateralJoinNode.Type.INNER,
                    node.getOriginSubqueryError());

            RowExpression valueComparedToSubquery = rewriteUsingBounds(
                    quantifiedComparison,
                    minValue,
                    maxValue,
                    countAllValue,
                    countNonNullValue);

            VariableReferenceExpression quantifiedComparisonVariable = getOnlyElement(node.getSubqueryAssignments().getVariables());

            return projectExpressions(lateralJoinNode, Assignments.of(quantifiedComparisonVariable, valueComparedToSubquery));
        }

        public RowExpression rewriteUsingBounds(
                QuantifiedComparisonExpression quantifiedComparison,
                VariableReferenceExpression minValue,
                VariableReferenceExpression maxValue,
                VariableReferenceExpression countAllValue,
                VariableReferenceExpression countNonNullValue)
        {
            ConstantExpression emptySetResult = quantifiedComparison.getQuantifier().equals(ALL) ? TRUE_CONSTANT : FALSE_CONSTANT;
            Function<List<RowExpression>, RowExpression> quantifier = quantifiedComparison.getQuantifier().equals(ALL) ?
                    logicalRowExpressions::combineConjuncts : logicalRowExpressions::combineDisjuncts;
            RowExpression comparisonWithExtremeValue = getBoundComparisons(quantifiedComparison, minValue, maxValue);

            RowExpression whenClause = specialForm(
                    WHEN,
                    BOOLEAN,
                    comparisonExpression(functionResolution, NOT_EQUAL, countAllValue, countNonNullValue),
                    new ConstantExpression(null, BOOLEAN));

            return buildSwitch(
                    countAllValue,
                    ImmutableList.of(specialForm(WHEN, BOOLEAN, new ConstantExpression(0L, BIGINT), emptySetResult)),
                    Optional.of(quantifier.apply(ImmutableList.of(
                            comparisonWithExtremeValue,
                            searchedCaseExpression(
                                    ImmutableList.of(whenClause),
                                    Optional.of(emptySetResult))))),
                    BOOLEAN);
        }

        private RowExpression getBoundComparisons(QuantifiedComparisonExpression quantifiedComparison, VariableReferenceExpression minValue, VariableReferenceExpression maxValue)
        {
            if (quantifiedComparison.getOperator() == EQUAL && quantifiedComparison.getQuantifier() == ALL) {
                // A = ALL B <=> min B = max B && A = min B
                return logicalRowExpressions.combineConjuncts(
                        comparisonExpression(functionResolution, EQUAL, minValue, maxValue),
                        comparisonExpression(functionResolution, EQUAL, quantifiedComparison.getValue(), maxValue));
            }

            if (EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL).contains(quantifiedComparison.getOperator())) {
                // A < ALL B <=> A < min B
                // A > ALL B <=> A > max B
                // A < ANY B <=> A < max B
                // A > ANY B <=> A > min B
                VariableReferenceExpression boundValue = shouldCompareValueWithLowerBound(quantifiedComparison) ? minValue : maxValue;
                return comparisonExpression(functionResolution, quantifiedComparison.getOperator(), quantifiedComparison.getValue(), boundValue);
            }
            throw new IllegalArgumentException("Unsupported quantified comparison: " + quantifiedComparison);
        }

        private static boolean shouldCompareValueWithLowerBound(QuantifiedComparisonExpression quantifiedComparison)
        {
            switch (quantifiedComparison.getQuantifier()) {
                case ALL:
                    switch (quantifiedComparison.getOperator()) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return true;
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return false;
                    }
                    break;
                case ANY:
                case SOME:
                    switch (quantifiedComparison.getOperator()) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return false;
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return true;
                    }
                    break;
            }
            throw new IllegalArgumentException("Unexpected quantifier: " + quantifiedComparison.getQuantifier());
        }

        private ProjectNode projectExpressions(PlanNode input, Assignments subqueryAssignments)
        {
            Assignments assignments = Assignments.builder()
                    .putAll(identityAssignments(input.getOutputVariables()))
                    .putAll(subqueryAssignments)
                    .build();
            return new ProjectNode(
                    idAllocator.getNextId(),
                    input,
                    assignments);
        }
    }
}

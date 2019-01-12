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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.WhenClause;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.prestosql.sql.planner.plan.AggregationNode.globalAggregation;
import static io.prestosql.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.prestosql.sql.tree.QuantifiedComparisonExpression.Quantifier.ALL;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TransformQuantifiedComparisonApplyToLateralJoin
        implements PlanOptimizer
{
    private final Metadata metadata;

    public TransformQuantifiedComparisonApplyToLateralJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return rewriteWith(new Rewriter(idAllocator, types, symbolAllocator, metadata), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private static final QualifiedName MIN = QualifiedName.of("min");
        private static final QualifiedName MAX = QualifiedName.of("max");
        private static final QualifiedName COUNT = QualifiedName.of("count");

        private final PlanNodeIdAllocator idAllocator;
        private final TypeProvider types;
        private final SymbolAllocator symbolAllocator;
        private final Metadata metadata;

        public Rewriter(PlanNodeIdAllocator idAllocator, TypeProvider types, SymbolAllocator symbolAllocator, Metadata metadata)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.types = requireNonNull(types, "types is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context)
        {
            if (node.getSubqueryAssignments().size() != 1) {
                return context.defaultRewrite(node);
            }

            Expression expression = getOnlyElement(node.getSubqueryAssignments().getExpressions());
            if (!(expression instanceof QuantifiedComparisonExpression)) {
                return context.defaultRewrite(node);
            }

            QuantifiedComparisonExpression quantifiedComparison = (QuantifiedComparisonExpression) expression;

            return rewriteQuantifiedApplyNode(node, quantifiedComparison, context);
        }

        private PlanNode rewriteQuantifiedApplyNode(ApplyNode node, QuantifiedComparisonExpression quantifiedComparison, RewriteContext<PlanNode> context)
        {
            PlanNode subqueryPlan = context.rewrite(node.getSubquery());

            Symbol outputColumn = getOnlyElement(subqueryPlan.getOutputSymbols());
            Type outputColumnType = types.get(outputColumn);
            checkState(outputColumnType.isOrderable(), "Subquery result type must be orderable");

            Symbol minValue = symbolAllocator.newSymbol(MIN.toString(), outputColumnType);
            Symbol maxValue = symbolAllocator.newSymbol(MAX.toString(), outputColumnType);
            Symbol countAllValue = symbolAllocator.newSymbol("count_all", BigintType.BIGINT);
            Symbol countNonNullValue = symbolAllocator.newSymbol("count_non_null", BigintType.BIGINT);

            FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
            List<Expression> outputColumnReferences = ImmutableList.of(outputColumn.toSymbolReference());
            List<TypeSignature> outputColumnTypeSignature = ImmutableList.of(outputColumnType.getTypeSignature());

            subqueryPlan = new AggregationNode(
                    idAllocator.getNextId(),
                    subqueryPlan,
                    ImmutableMap.of(
                            minValue, new Aggregation(
                                    new FunctionCall(MIN, outputColumnReferences),
                                    functionRegistry.resolveFunction(MIN, fromTypeSignatures(outputColumnTypeSignature)),
                                    Optional.empty()),
                            maxValue, new Aggregation(
                                    new FunctionCall(MAX, outputColumnReferences),
                                    functionRegistry.resolveFunction(MAX, fromTypeSignatures(outputColumnTypeSignature)),
                                    Optional.empty()),
                            countAllValue, new Aggregation(
                                    new FunctionCall(COUNT, emptyList()),
                                    functionRegistry.resolveFunction(COUNT, emptyList()),
                                    Optional.empty()),
                            countNonNullValue, new Aggregation(
                                    new FunctionCall(COUNT, outputColumnReferences),
                                    functionRegistry.resolveFunction(COUNT, fromTypeSignatures(outputColumnTypeSignature)),
                                    Optional.empty())),
                    globalAggregation(),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            PlanNode lateralJoinNode = new LateralJoinNode(
                    node.getId(),
                    context.rewrite(node.getInput()),
                    subqueryPlan,
                    node.getCorrelation(),
                    LateralJoinNode.Type.INNER,
                    node.getOriginSubquery());

            Expression valueComparedToSubquery = rewriteUsingBounds(quantifiedComparison, minValue, maxValue, countAllValue, countNonNullValue);

            Symbol quantifiedComparisonSymbol = getOnlyElement(node.getSubqueryAssignments().getSymbols());

            return projectExpressions(lateralJoinNode, Assignments.of(quantifiedComparisonSymbol, valueComparedToSubquery));
        }

        public Expression rewriteUsingBounds(QuantifiedComparisonExpression quantifiedComparison, Symbol minValue, Symbol maxValue, Symbol countAllValue, Symbol countNonNullValue)
        {
            BooleanLiteral emptySetResult = quantifiedComparison.getQuantifier().equals(ALL) ? TRUE_LITERAL : FALSE_LITERAL;
            Function<List<Expression>, Expression> quantifier = quantifiedComparison.getQuantifier().equals(ALL) ?
                    ExpressionUtils::combineConjuncts : ExpressionUtils::combineDisjuncts;
            Expression comparisonWithExtremeValue = getBoundComparisons(quantifiedComparison, minValue, maxValue);

            return new SimpleCaseExpression(
                    countAllValue.toSymbolReference(),
                    ImmutableList.of(new WhenClause(
                            new GenericLiteral("bigint", "0"),
                            emptySetResult)),
                    Optional.of(quantifier.apply(ImmutableList.of(
                            comparisonWithExtremeValue,
                            new SearchedCaseExpression(
                                    ImmutableList.of(
                                            new WhenClause(
                                                    new ComparisonExpression(NOT_EQUAL, countAllValue.toSymbolReference(), countNonNullValue.toSymbolReference()),
                                                    new Cast(new NullLiteral(), BooleanType.BOOLEAN.toString()))),
                                    Optional.of(emptySetResult))))));
        }

        private Expression getBoundComparisons(QuantifiedComparisonExpression quantifiedComparison, Symbol minValue, Symbol maxValue)
        {
            if (quantifiedComparison.getOperator() == EQUAL && quantifiedComparison.getQuantifier() == ALL) {
                // A = ALL B <=> min B = max B && A = min B
                return combineConjuncts(
                        new ComparisonExpression(EQUAL, minValue.toSymbolReference(), maxValue.toSymbolReference()),
                        new ComparisonExpression(EQUAL, quantifiedComparison.getValue(), maxValue.toSymbolReference()));
            }

            if (EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL).contains(quantifiedComparison.getOperator())) {
                // A < ALL B <=> A < min B
                // A > ALL B <=> A > max B
                // A < ANY B <=> A < max B
                // A > ANY B <=> A > min B
                Symbol boundValue = shouldCompareValueWithLowerBound(quantifiedComparison) ? minValue : maxValue;
                return new ComparisonExpression(quantifiedComparison.getOperator(), quantifiedComparison.getValue(), boundValue.toSymbolReference());
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
                    .putIdentities(input.getOutputSymbols())
                    .putAll(subqueryAssignments)
                    .build();
            return new ProjectNode(
                    idAllocator.getNextId(),
                    input,
                    assignments);
        }
    }
}

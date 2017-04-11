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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.NOT_EQUAL;
import static com.facebook.presto.sql.tree.QuantifiedComparisonExpression.Quantifier.ALL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TransformQuantifiedComparisonApplyToScalarApply
        implements PlanOptimizer
{
    private final Metadata metadata;

    public TransformQuantifiedComparisonApplyToScalarApply(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
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
        private final Map<Symbol, Type> types;
        private final SymbolAllocator symbolAllocator;
        private final Metadata metadata;

        public Rewriter(PlanNodeIdAllocator idAllocator, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, Metadata metadata)
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
                            minValue, new FunctionCall(MIN, outputColumnReferences),
                            maxValue, new FunctionCall(MAX, outputColumnReferences),
                            countAllValue, new FunctionCall(COUNT, emptyList()),
                            countNonNullValue, new FunctionCall(COUNT, outputColumnReferences)
                    ),
                    ImmutableMap.of(
                            minValue, functionRegistry.resolveFunction(MIN, fromTypeSignatures(outputColumnTypeSignature)),
                            maxValue, functionRegistry.resolveFunction(MAX, fromTypeSignatures(outputColumnTypeSignature)),
                            countAllValue, functionRegistry.resolveFunction(COUNT, emptyList()),
                            countNonNullValue, functionRegistry.resolveFunction(COUNT, fromTypeSignatures(outputColumnTypeSignature))
                    ),
                    ImmutableMap.of(),
                    ImmutableList.of(ImmutableList.of()),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            PlanNode applyNode = new ApplyNode(
                    node.getId(),
                    context.rewrite(node.getInput()),
                    subqueryPlan,
                    Assignments.identity(minValue, maxValue),
                    node.getCorrelation());

            Expression valueComparedToSubquery = rewriteUsingBounds(quantifiedComparison, minValue, maxValue, countAllValue, countNonNullValue);

            Symbol quantifiedComparisonSymbol = getOnlyElement(node.getSubqueryAssignments().getSymbols());

            return projectExpressions(applyNode, Assignments.of(quantifiedComparisonSymbol, valueComparedToSubquery));
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
                            emptySetResult
                    )),
                    Optional.of(quantifier.apply(ImmutableList.of(
                            comparisonWithExtremeValue,
                            new SearchedCaseExpression(
                                    ImmutableList.of(
                                            new WhenClause(
                                                    new ComparisonExpression(NOT_EQUAL, countAllValue.toSymbolReference(), countNonNullValue.toSymbolReference()),
                                                    new Cast(new NullLiteral(), BooleanType.BOOLEAN.toString())
                                            )
                                    ),
                                    Optional.of(emptySetResult)
                            )
                    )))
            );
        }

        private Expression getBoundComparisons(QuantifiedComparisonExpression quantifiedComparison, Symbol minValue, Symbol maxValue)
        {
            if (quantifiedComparison.getComparisonType() == EQUAL && quantifiedComparison.getQuantifier() == ALL) {
                // A = ALL B <=> min B = max B && A = min B
                return combineConjuncts(
                        new ComparisonExpression(EQUAL, minValue.toSymbolReference(), maxValue.toSymbolReference()),
                        new ComparisonExpression(EQUAL, quantifiedComparison.getValue(), maxValue.toSymbolReference())
                );
            }

            if (EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL).contains(quantifiedComparison.getComparisonType())) {
                // A < ALL B <=> A < min B
                // A > ALL B <=> A > max B
                // A < ANY B <=> A < max B
                // A > ANY B <=> A > min B
                Symbol boundValue = shouldCompareValueWithLowerBound(quantifiedComparison) ? minValue : maxValue;
                return new ComparisonExpression(quantifiedComparison.getComparisonType(), quantifiedComparison.getValue(), boundValue.toSymbolReference());
            }
            throw new IllegalArgumentException("Unsupported quantified comparison: " + quantifiedComparison);
        }

        private static boolean shouldCompareValueWithLowerBound(QuantifiedComparisonExpression quantifiedComparison)
        {
            switch (quantifiedComparison.getQuantifier()) {
                case ALL:
                    switch (quantifiedComparison.getComparisonType()) {
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
                    switch (quantifiedComparison.getComparisonType()) {
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

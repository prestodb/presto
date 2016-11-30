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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.SemanticExceptions.throwNotSupportedException;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.QuantifiedComparisonExpression.Quantifier.ALL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
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

            Expression expression = getOnlyElement(node.getSubqueryAssignments().values());
            if (!(expression instanceof QuantifiedComparisonExpression)) {
                return context.defaultRewrite(node);
            }

            QuantifiedComparisonExpression quantifiedComparison = (QuantifiedComparisonExpression) expression;

            if (quantifiedComparison.getComparisonType() == EQUAL && quantifiedComparison.getQuantifier() == ALL) {
                // A = ALL B <=> min B = max B && A = min B
                return rewriteQuantifiedEqualsAllApplyNode(node, quantifiedComparison, context);
            }

            if (EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL).contains(quantifiedComparison.getComparisonType())) {
                // A < ALL B <=> A < min B
                // A > ALL B <=> A > max B
                // A < ANY B <=> A < max B
                // A > ANY B <=> A > min B
                return rewriteQuantifiedOrderableApplyNode(node, quantifiedComparison, context);
            }

            return context.defaultRewrite(node);
        }

        private PlanNode rewriteQuantifiedEqualsAllApplyNode(ApplyNode node, QuantifiedComparisonExpression quantifiedComparison, RewriteContext<PlanNode> context)
        {
            PlanNode subqueryPlan = context.rewrite(node.getSubquery());
            Symbol outputColumnSymbol = getOnlyElement(subqueryPlan.getOutputSymbols());
            Type outputColumnType = types.get(outputColumnSymbol);
            if (!outputColumnType.isOrderable()) {
                throwNotSupportedException(quantifiedComparison, "Quantified comparison '= ALL' or '<> ANY' for unorderable type " + outputColumnType.getDisplayName());
            }

            List<TypeSignature> outputColumnTypeSignature = ImmutableList.of(outputColumnType.getTypeSignature());
            FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
            QualifiedName min = QualifiedName.of("min");
            QualifiedName max = QualifiedName.of("max");
            Symbol minValue = symbolAllocator.newSymbol(min.toString(), outputColumnType);
            Symbol maxValue = symbolAllocator.newSymbol(max.toString(), outputColumnType);
            List<Expression> outputColumnReferences = ImmutableList.of(outputColumnSymbol.toSymbolReference());
            subqueryPlan = new AggregationNode(
                    idAllocator.getNextId(),
                    subqueryPlan,
                    ImmutableMap.of(
                            minValue, new FunctionCall(min, outputColumnReferences),
                            maxValue, new FunctionCall(max, outputColumnReferences)
                    ),
                    ImmutableMap.of(
                            minValue, functionRegistry.resolveFunction(min, fromTypeSignatures(outputColumnTypeSignature)),
                            maxValue, functionRegistry.resolveFunction(max, fromTypeSignatures(outputColumnTypeSignature))
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
                    ImmutableMap.of(minValue, minValue.toSymbolReference(), maxValue, maxValue.toSymbolReference()),
                    node.getCorrelation());

            Symbol quantifiedComparisonSymbol = getOnlyElement(node.getSubqueryAssignments().keySet());
            LogicalBinaryExpression valueComparedToSubquery = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                    new ComparisonExpression(EQUAL, minValue.toSymbolReference(), maxValue.toSymbolReference()),
                    new ComparisonExpression(EQUAL, quantifiedComparison.getValue(), minValue.toSymbolReference())
            );
            return projectExpressions(applyNode, ImmutableMap.of(quantifiedComparisonSymbol, valueComparedToSubquery));
        }

        private PlanNode rewriteQuantifiedOrderableApplyNode(ApplyNode node, QuantifiedComparisonExpression quantifiedComparison, RewriteContext<PlanNode> context)
        {
            QualifiedName aggregationFunction = chooseAggregationFunction(quantifiedComparison);
            PlanNode subqueryPlan = context.rewrite(node.getSubquery());
            Symbol outputColumn = getOnlyElement(subqueryPlan.getOutputSymbols());
            Type outputColumnType = types.get(outputColumn);
            checkState(outputColumnType.isOrderable(), "Subquery result type must be orderable");

            FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
            Symbol subValue = symbolAllocator.newSymbol(aggregationFunction.toString(), outputColumnType);
            subqueryPlan = new AggregationNode(
                    idAllocator.getNextId(),
                    subqueryPlan,
                    ImmutableMap.of(subValue, new FunctionCall(aggregationFunction, ImmutableList.of(outputColumn.toSymbolReference()))),
                    ImmutableMap.of(subValue, functionRegistry.resolveFunction(aggregationFunction, ImmutableList.of(new TypeSignatureProvider(outputColumnType.getTypeSignature())))),
                    ImmutableMap.of(),
                    ImmutableList.of(ImmutableList.of()),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            PlanNode applyNode = new ApplyNode(
                    node.getId(),
                    context.rewrite(node.getInput()),
                    subqueryPlan,
                    ImmutableMap.of(subValue, subValue.toSymbolReference()),
                    node.getCorrelation());

            ComparisonExpression valueComparedToSubquery = new ComparisonExpression(quantifiedComparison.getComparisonType(), quantifiedComparison.getValue(), subValue.toSymbolReference());
            Symbol quantifiedComparisonSymbol = getOnlyElement(node.getSubqueryAssignments().keySet());
            return projectExpressions(applyNode, ImmutableMap.of(quantifiedComparisonSymbol, valueComparedToSubquery));
        }

        private ProjectNode projectExpressions(PlanNode input, Map<Symbol, Expression> expressions)
        {
            Map<Symbol, Expression> identityProjections = input.getOutputSymbols().stream()
                    .collect(toImmutableMap(symbol -> symbol, Symbol::toSymbolReference));
            return new ProjectNode(
                    idAllocator.getNextId(),
                    input,
                    ImmutableMap.<Symbol, Expression>builder()
                            .putAll(identityProjections)
                            .putAll(expressions)
                            .build());
        }

        private static QualifiedName chooseAggregationFunction(QuantifiedComparisonExpression quantifiedComparison)
        {
            switch (quantifiedComparison.getQuantifier()) {
                case ALL:
                    switch (quantifiedComparison.getComparisonType()) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return QualifiedName.of("min");
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return QualifiedName.of("max");
                    }
                    break;
                case ANY:
                case SOME:
                    switch (quantifiedComparison.getComparisonType()) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return QualifiedName.of("max");
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return QualifiedName.of("min");
                    }
                    break;
            }
            throw new IllegalArgumentException("Unexpected quantifier: " + quantifiedComparison.getQuantifier());
        }
    }
}

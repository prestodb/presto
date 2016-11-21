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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.Predicates.isInstanceOfAny;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Scalar aggregation is aggregation with GROUP BY 'a constant' (or empty GROUP BY).
 * It always returns single row in Presto.
 * <p>
 * This optimizer can rewrite correlated scalar aggregation subquery to left outer join in a way described here:
 * https://github.com/prestodb/presto/wiki/Correlated-subqueries
 * <p>
 * From:
 * <pre>
 * - Apply (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery) Aggregation(GROUP BY (); functions: [sum(F), count(), ...]
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Aggregation(GROUP BY A, B, C, U; functions: [sum(F), count(non_null), ...]
 *   - Join(LEFT_OUTER, D = C)
 *     - AssignUniqueId(adds symbol U)
 *       - (input) plan which produces symbols: [A, B, C]
 *         - Filter(E > 5)
 *           - projection which adds no null symbol used for count() function
 *             - plan which produces symbols: [D, E, F]
 * </pre>
 * <p>
 * Note only conjunction predicates in FilterNode are supported
 */
public class TransformCorrelatedScalarAggregationToJoin
        implements PlanOptimizer
{
    private final Metadata metadata;

    public TransformCorrelatedScalarAggregationToJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            Map<Symbol, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator, symbolAllocator, metadata), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Metadata metadata;
        private final PlanNodeDecorrelator decorrelator;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Metadata metadata)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.decorrelator = new PlanNodeDecorrelator(idAllocator);
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context)
        {
            ApplyNode rewrittenNode = (ApplyNode) context.defaultRewrite(node, context.get());
            if (!rewrittenNode.getCorrelation().isEmpty() && rewrittenNode.isResolvedScalarSubquery()) {
                Optional<AggregationNode> aggregation = searchFrom(rewrittenNode.getSubquery())
                        .where(AggregationNode.class::isInstance)
                        .skipOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class))
                        .findFirst();
                if (aggregation.isPresent() && aggregation.get().getGroupingKeys().isEmpty()) {
                    return rewriteScalarAggregation(rewrittenNode, aggregation.get());
                }
            }
            return rewrittenNode;
        }

        private PlanNode rewriteScalarAggregation(ApplyNode apply, AggregationNode aggregation)
        {
            List<Symbol> correlation = apply.getCorrelation();
            Optional<PlanNodeDecorrelator.DecorrelatedNode> source = decorrelator.decorrelateFilters(aggregation.getSource(), correlation);
            if (!source.isPresent()) {
                return apply;
            }

            Symbol nonNull = symbolAllocator.newSymbol("non_null", BooleanType.BOOLEAN);
            Map<Symbol, Expression> scalarAggregationSourceAssignments = ImmutableMap.<Symbol, Expression>builder()
                    .putAll(toAssignments(source.get().getNode().getOutputSymbols()))
                    .put(nonNull, TRUE_LITERAL)
                    .build();
            ProjectNode scalarAggregationSourceWithNonNullableSymbol = new ProjectNode(
                    idAllocator.getNextId(),
                    source.get().getNode(),
                    scalarAggregationSourceAssignments);

            return rewriteScalarAggregation(
                    apply,
                    aggregation,
                    scalarAggregationSourceWithNonNullableSymbol,
                    source.get().getCorrelatedPredicates(),
                    nonNull);
        }

        private PlanNode rewriteScalarAggregation(
                ApplyNode applyNode,
                AggregationNode scalarAggregation,
                PlanNode scalarAggregationSource,
                Optional<Expression> joinExpression,
                Symbol nonNull)
        {
            AssignUniqueId inputWithUniqueColumns = new AssignUniqueId(
                    idAllocator.getNextId(),
                    applyNode.getInput(),
                    symbolAllocator.newSymbol("unique", BigintType.BIGINT));

            JoinNode leftOuterJoin = new JoinNode(
                    idAllocator.getNextId(),
                    JoinNode.Type.LEFT,
                    inputWithUniqueColumns,
                    scalarAggregationSource,
                    ImmutableList.of(),
                    joinExpression,
                    Optional.empty(),
                    Optional.empty());

            Optional<AggregationNode> aggregationNode = createAggregationNode(
                    scalarAggregation,
                    leftOuterJoin,
                    nonNull);

            if (!aggregationNode.isPresent()) {
                return applyNode;
            }

            Optional<ProjectNode> subqueryProjection = searchFrom(applyNode.getSubquery())
                    .where(ProjectNode.class::isInstance)
                    .findFirst();

            if (subqueryProjection.isPresent()) {
                Map<Symbol, Expression> assignments = ImmutableMap.<Symbol, Expression>builder()
                        .putAll(toAssignments(aggregationNode.get().getOutputSymbols()))
                        .putAll(subqueryProjection.get().getAssignments())
                        .build();

                return new ProjectNode(
                        idAllocator.getNextId(),
                        aggregationNode.get(),
                        assignments);
            }
            else {
                return aggregationNode.get();
            }
        }

        private Optional<AggregationNode> createAggregationNode(
                AggregationNode scalarAggregation,
                JoinNode leftOuterJoin,
                Symbol nonNullableAggregationSourceSymbol)
        {
            ImmutableMap.Builder<Symbol, FunctionCall> aggregations = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Signature> functions = ImmutableMap.builder();
            FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
            for (Map.Entry<Symbol, FunctionCall> entry : scalarAggregation.getAggregations().entrySet()) {
                FunctionCall call = entry.getValue();
                QualifiedName count = QualifiedName.of("count");
                Symbol symbol = entry.getKey();
                if (call.getName().equals(count)) {
                    aggregations.put(symbol, new FunctionCall(
                            count,
                            ImmutableList.of(nonNullableAggregationSourceSymbol.toSymbolReference())));
                    List<TypeSignature> scalarAggregationSourceTypeSignatures = ImmutableList.of(
                            symbolAllocator.getTypes().get(nonNullableAggregationSourceSymbol).getTypeSignature());
                    functions.put(symbol, functionRegistry.resolveFunction(
                            count,
                            fromTypeSignatures(scalarAggregationSourceTypeSignatures)));
                }
                else {
                    aggregations.put(symbol, entry.getValue());
                    functions.put(symbol, scalarAggregation.getFunctions().get(symbol));
                }
            }

            List<Symbol> groupBySymbols = leftOuterJoin.getLeft().getOutputSymbols();
            return Optional.of(new AggregationNode(
                    idAllocator.getNextId(),
                    leftOuterJoin,
                    aggregations.build(),
                    functions.build(),
                    scalarAggregation.getMasks(),
                    ImmutableList.of(groupBySymbols),
                    scalarAggregation.getStep(),
                    scalarAggregation.getHashSymbol(),
                    Optional.empty()));
        }
    }

    private static Map<Symbol, Expression> toAssignments(Collection<Symbol> symbols)
    {
        return symbols.stream()
                .collect(toImmutableMap(s -> s, Symbol::toSymbolReference));
    }
}

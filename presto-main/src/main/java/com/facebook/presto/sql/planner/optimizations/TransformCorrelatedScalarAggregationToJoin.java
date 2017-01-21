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
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.Predicates.isInstanceOfAny;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
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

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Metadata metadata)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
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
            Optional<DecorrelatedNode> source = decorrelateFilters(aggregation.getSource(), correlation);
            if (!source.isPresent()) {
                return apply;
            }

            Symbol nonNull = symbolAllocator.newSymbol("non_null", BooleanType.BOOLEAN);
            Assignments scalarAggregationSourceAssignments = Assignments.builder()
                    .putAll(Assignments.identity(source.get().getNode().getOutputSymbols()))
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
                    ImmutableList.<Symbol>builder()
                            .addAll(inputWithUniqueColumns.getOutputSymbols())
                            .addAll(scalarAggregationSource.getOutputSymbols())
                            .build(),
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
                    .skipOnlyWhen(EnforceSingleRowNode.class::isInstance)
                    .findFirst();

            if (subqueryProjection.isPresent()) {
                Assignments assignments = Assignments.builder()
                        .putAll(Assignments.identity(aggregationNode.get().getOutputSymbols()))
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

        private Optional<DecorrelatedNode> decorrelateFilters(PlanNode node, List<Symbol> correlation)
        {
            PlanNodeSearcher filterNodeSearcher = searchFrom(node)
                    .where(FilterNode.class::isInstance)
                    .skipOnlyWhen(isInstanceOfAny(ProjectNode.class, LimitNode.class));
            List<FilterNode> filterNodes = filterNodeSearcher.findAll();

            if (filterNodes.isEmpty()) {
                return decorrelatedNode(ImmutableList.of(), node, correlation);
            }

            if (filterNodes.size() > 1) {
                return Optional.empty();
            }

            FilterNode filterNode = filterNodes.get(0);
            Expression predicate = filterNode.getPredicate();

            if (!isSupportedPredicate(predicate)) {
                return Optional.empty();
            }

            if (!DependencyExtractor.extractUnique(predicate).containsAll(correlation)) {
                return Optional.empty();
            }

            Map<Boolean, List<Expression>> predicates = ExpressionUtils.extractConjuncts(predicate).stream()
                    .collect(Collectors.partitioningBy(isUsingPredicate(correlation)));
            List<Expression> correlatedPredicates = ImmutableList.copyOf(predicates.get(true));
            List<Expression> uncorrelatedPredicates = ImmutableList.copyOf(predicates.get(false));

            node = updateFilterNode(filterNodeSearcher, uncorrelatedPredicates);

            if (!correlatedPredicates.isEmpty()) {
                // filterNodes condition has changed so Limit node no longer applies for EXISTS subquery
                node = removeLimitNode(node);
            }

            node = ensureJoinSymbolsAreReturned(node, correlatedPredicates);

            return decorrelatedNode(correlatedPredicates, node, correlation);
        }

        private static Optional<DecorrelatedNode> decorrelatedNode(
                List<Expression> correlatedPredicates,
                PlanNode node,
                List<Symbol> correlation)
        {
            if (DependencyExtractor.extractUnique(node).stream().anyMatch(correlation::contains)) {
                // node is still correlated ; /
                return Optional.empty();
            }
            return Optional.of(new DecorrelatedNode(correlatedPredicates, node));
        }

        private static Predicate<Expression> isUsingPredicate(List<Symbol> symbols)
        {
            return expression -> symbols.stream().anyMatch(DependencyExtractor.extractUnique(expression)::contains);
        }

        private PlanNode updateFilterNode(PlanNodeSearcher filterNodeSearcher, List<Expression> newPredicates)
        {
            if (newPredicates.isEmpty()) {
                return filterNodeSearcher.removeAll();
            }
            FilterNode oldFilterNode = Iterables.getOnlyElement(filterNodeSearcher.findAll());
            FilterNode newFilterNode = new FilterNode(
                    idAllocator.getNextId(),
                    oldFilterNode.getSource(),
                    ExpressionUtils.combineConjuncts(newPredicates));
            return filterNodeSearcher.replaceAll(newFilterNode);
        }

        private static PlanNode removeLimitNode(PlanNode node)
        {
            node = searchFrom(node)
                    .where(LimitNode.class::isInstance)
                    .skipOnlyWhen(ProjectNode.class::isInstance)
                    .removeFirst();
            return node;
        }

        private PlanNode ensureJoinSymbolsAreReturned(PlanNode scalarAggregationSource, List<Expression> joinPredicate)
        {
            Set<Symbol> joinExpressionSymbols = DependencyExtractor.extractUnique(joinPredicate);
            ExtendProjectionRewriter extendProjectionRewriter = new ExtendProjectionRewriter(
                    idAllocator,
                    joinExpressionSymbols);
            return rewriteWith(extendProjectionRewriter, scalarAggregationSource);
        }

        private static boolean isSupportedPredicate(Expression predicate)
        {
            AtomicBoolean isSupported = new AtomicBoolean(true);
            new DefaultTraversalVisitor<Void, AtomicBoolean>()
            {
                @Override
                protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, AtomicBoolean context)
                {
                    if (node.getType() != LogicalBinaryExpression.Type.AND) {
                        context.set(false);
                    }
                    return null;
                }
            }.process(predicate, isSupported);
            return isSupported.get();
        }
    }

    private static class DecorrelatedNode
    {
        private final List<Expression> correlatedPredicates;
        private final PlanNode node;

        public DecorrelatedNode(List<Expression> correlatedPredicates, PlanNode node)
        {
            requireNonNull(correlatedPredicates, "correlatedPredicates is null");
            this.correlatedPredicates = ImmutableList.copyOf(correlatedPredicates);
            this.node = requireNonNull(node, "node is null");
        }

        Optional<Expression> getCorrelatedPredicates()
        {
            if (correlatedPredicates.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(ExpressionUtils.and(correlatedPredicates));
        }

        public PlanNode getNode()
        {
            return node;
        }
    }

    private static class ExtendProjectionRewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Set<Symbol> symbols;

        ExtendProjectionRewriter(PlanNodeIdAllocator idAllocator, Set<Symbol> symbols)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbols = requireNonNull(symbols, "symbols is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<PlanNode> context)
        {
            ProjectNode rewrittenNode = (ProjectNode) context.defaultRewrite(node, context.get());

            List<Symbol> symbolsToAdd = symbols.stream()
                    .filter(rewrittenNode.getSource().getOutputSymbols()::contains)
                    .filter(symbol -> !rewrittenNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());

            Assignments assignments = Assignments.builder()
                    .putAll(rewrittenNode.getAssignments())
                    .putAll(Assignments.identity(symbolsToAdd))
                    .build();

            return new ProjectNode(idAllocator.getNextId(), rewrittenNode.getSource(), assignments);
        }
    }
}

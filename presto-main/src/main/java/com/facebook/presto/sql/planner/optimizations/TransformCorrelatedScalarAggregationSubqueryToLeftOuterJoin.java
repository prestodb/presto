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
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.EnforceUniqueColumns;
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
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeUtils.findFirstPlanNodeMatching;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeUtils.removeFirstPlanNodesMatching;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeUtils.replaceAllPlanNodesMatching;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class TransformCorrelatedScalarAggregationSubqueryToLeftOuterJoin
        implements PlanOptimizer
{
    private final Metadata metadata;

    public TransformCorrelatedScalarAggregationSubqueryToLeftOuterJoin(Metadata metadata)
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
        private final ScalarAggregationSourceRewriter scalarSourceRewriter;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Metadata metadata)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.scalarSourceRewriter = new ScalarAggregationSourceRewriter(idAllocator);
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context)
        {
            ApplyNode rewrittenNode = (ApplyNode) context.defaultRewrite(node, context.get());
            if (!rewrittenNode.getCorrelation().isEmpty()) {
                Optional<AggregationNode> scalarAggregation = findFirstPlanNodeMatching(
                        rewrittenNode.getSubquery(),
                        AggregationNode.class::isInstance,
                        Predicates.<PlanNode>isInstance(ProjectNode.class).or(EnforceSingleRowNode.class::isInstance));
                if (scalarAggregation.isPresent() && scalarAggregation.get().getGroupBy().isEmpty()) {
                    return rewriteScalarAggregation(rewrittenNode, scalarAggregation.get());
                }
            }
            return rewrittenNode;
        }

        private PlanNode rewriteScalarAggregation(ApplyNode applyNode, AggregationNode scalarAggregation)
        {
            List<Symbol> correlation = applyNode.getCorrelation();
            Optional<ScalarAggregationSourceRewriter.Result> scalarSourceRewriteResult = scalarSourceRewriter
                    .rewrite(scalarAggregation, correlation);
            if (!scalarSourceRewriteResult.isPresent()) {
                return applyNode;
            }

            Symbol nonNull = symbolAllocator.newSymbol("non_null", BooleanType.BOOLEAN);
            Map<Symbol, Expression> scalarAggregationSourceAssignments = ImmutableMap.<Symbol, Expression>builder()
                    .putAll(toAssignments(scalarSourceRewriteResult.get().getNode().getOutputSymbols()))
                    .put(nonNull, TRUE_LITERAL)
                    .build();
            ProjectNode scalarAggregationSourceWithNonNullableSymbol = new ProjectNode(
                    idAllocator.getNextId(),
                    scalarSourceRewriteResult.get().getNode(),
                    scalarAggregationSourceAssignments);

            return rewriteScalarAggregation(
                    applyNode,
                    scalarAggregation,
                    scalarAggregationSourceWithNonNullableSymbol,
                    scalarSourceRewriteResult.get(),
                    nonNull);
        }

        private PlanNode rewriteScalarAggregation(
                ApplyNode applyNode,
                AggregationNode scalarAggregation,
                PlanNode scalarAggregationSource,
                ScalarAggregationSourceRewriter.Result scalarSourceRewriteResult,
                Symbol nonNull)
        {
            EnforceUniqueColumns inputWithUniqueColumns = new EnforceUniqueColumns(
                    idAllocator.getNextId(),
                    applyNode.getInput(),
                    symbolAllocator.newSymbol("most_sig_bits", BigintType.BIGINT),
                    symbolAllocator.newSymbol("least_sig_bits", BigintType.BIGINT));

            JoinNode leftOuterJoin = new JoinNode(
                    idAllocator.getNextId(),
                    JoinNode.Type.LEFT,
                    inputWithUniqueColumns,
                    scalarAggregationSource,
                    ImmutableList.of(),
                    scalarSourceRewriteResult.getJoinExpressions(),
                    Optional.empty(),
                    Optional.empty());

            Optional<AggregationNode> aggregationNode = createAggregationNode(
                    scalarAggregation,
                    leftOuterJoin,
                    nonNull);

            if (!aggregationNode.isPresent()) {
                return applyNode;
            }

            Optional<ProjectNode> subqueryProjection = findFirstPlanNodeMatching(
                    applyNode.getSubquery(),
                    ProjectNode.class::isInstance);

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
                            scalarAggregationSourceTypeSignatures,
                            false));
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
                    groupBySymbols,
                    aggregations.build(),
                    functions.build(),
                    scalarAggregation.getMasks(),
                    ImmutableList.of(groupBySymbols),
                    scalarAggregation.getStep(),
                    scalarAggregation.getSampleWeight(),
                    scalarAggregation.getConfidence(),
                    scalarAggregation.getHashSymbol()));
        }
    }

    private static class ScalarAggregationSourceRewriter
    {
        private final PlanNodeIdAllocator idAllocator;

        private ScalarAggregationSourceRewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        public Optional<Result> rewrite(AggregationNode scalarAggregation, List<Symbol> correlation)
        {
            ImmutableList.Builder<Expression> joinExpressions = ImmutableList.builder();
            PlanNode scalarAggregationSource = scalarAggregation.getSource();
            Optional<FilterNode> scalarAggregationWhere = findFirstPlanNodeMatching(
                    scalarAggregationSource,
                    FilterNode.class::isInstance,
                    Predicates.<PlanNode>isInstance(ProjectNode.class).or(LimitNode.class::isInstance));
            if (scalarAggregationWhere.isPresent()) {
                Expression predicate = scalarAggregationWhere.get().getPredicate();
                if (!isSupportedPredicate(predicate)) {
                    return Optional.empty();
                }
                ImmutableSet.Builder<Symbol> allPredicateSymbols = ImmutableSet.builder();
                ImmutableList.Builder<Expression> newWherePredicate = ImmutableList.<Expression>builder()
                        .add(TRUE_LITERAL);
                for (Expression conjunct : extractConjuncts(predicate)) {
                    List<Symbol> symbols = DependencyExtractor.extractAll(conjunct);

                    if (correlation.stream().anyMatch(symbols::contains)) {
                        joinExpressions.add(conjunct);
                    }
                    else {
                        newWherePredicate.add(conjunct);
                    }
                    allPredicateSymbols.addAll(symbols);
                }

                if (!allPredicateSymbols.build().containsAll(correlation)) {
                    return Optional.empty();
                }

                FilterNode newFilterNode = new FilterNode(
                        idAllocator.getNextId(),
                        scalarAggregationWhere.get().getSource(),
                        ExpressionUtils.and(newWherePredicate.build()));
                if (!joinExpressions.build().isEmpty()) {
                    // filter condition has changed so Limit node no longer applies for EXISTS subquery
                    scalarAggregationSource = removeFirstPlanNodesMatching(
                            scalarAggregationSource,
                            LimitNode.class::isInstance,
                            ProjectNode.class::isInstance);
                }
                scalarAggregationSource = replaceAllPlanNodesMatching(
                        scalarAggregationSource,
                        newFilterNode,
                        FilterNode.class::isInstance,
                        Predicates.<PlanNode>isInstance(ProjectNode.class).or(LimitNode.class::isInstance));

                scalarAggregationSource = ensureJoinSymbolsAreReturned(scalarAggregationSource, joinExpressions.build());
            }
            return Optional.of(new Result(joinExpressions.build(), scalarAggregationSource));
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

        private static List<Expression> extractConjuncts(Expression predicate)
        {
            ImmutableList.Builder<Expression> conjuncts = ImmutableList.builder();
            extractConjuncts(predicate, conjuncts);
            return conjuncts.build();
        }

        private static void extractConjuncts(Expression predicate, ImmutableList.Builder<Expression> conjuncts)
        {
            if (predicate instanceof LogicalBinaryExpression) {
                extractConjuncts(((LogicalBinaryExpression) predicate).getLeft(), conjuncts);
                extractConjuncts(((LogicalBinaryExpression) predicate).getRight(), conjuncts);
            }
            else {
                conjuncts.add(predicate);
            }
        }

        private static class Result
        {
            private final List<Expression> joinExpressions;
            private final PlanNode node;

            public Result(List<Expression> joinExpressions, PlanNode node)
            {
                requireNonNull(joinExpressions, "joinExpressions is null");
                this.joinExpressions = ImmutableList.copyOf(joinExpressions);
                this.node = requireNonNull(node, "node is null");
            }

            Optional<Expression> getJoinExpressions()
            {
                if (joinExpressions.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(ExpressionUtils.and(joinExpressions));
            }

            public PlanNode getNode()
            {
                return node;
            }
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

            Map<Symbol, Expression> assignments = ImmutableMap.<Symbol, Expression>builder()
                    .putAll(rewrittenNode.getAssignments())
                    .putAll(toAssignments(symbolsToAdd))
                    .build();

            return new ProjectNode(idAllocator.getNextId(), rewrittenNode.getSource(), assignments);
        }
    }

    private static Map<Symbol, Expression> toAssignments(Collection<Symbol> symbols)
    {
        return symbols.stream()
                .collect(toImmutableMap(s -> s, Symbol::toSymbolReference));
    }
}

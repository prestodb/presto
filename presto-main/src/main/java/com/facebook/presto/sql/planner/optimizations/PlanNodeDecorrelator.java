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

import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.Predicates.isInstanceOfAny;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PlanNodeDecorrelator
{
    private final PlanNodeIdAllocator idAllocator;

    public PlanNodeDecorrelator(PlanNodeIdAllocator idAllocator)
    {
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
    }

    public Optional<DecorrelatedNode> decorrelateFilters(PlanNode node, List<Symbol> correlation)
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

    private PlanNode ensureJoinSymbolsAreReturned(PlanNode scalarAggregationSource, List<Expression> joinPredicate)
    {
        Set<Symbol> joinExpressionSymbols = DependencyExtractor.extractUnique(joinPredicate);
        ExtendProjectionRewriter extendProjectionRewriter = new ExtendProjectionRewriter(
                idAllocator,
                joinExpressionSymbols);
        return rewriteWith(extendProjectionRewriter, scalarAggregationSource);
    }

    public static Optional<DecorrelatedNode> decorrelatedNode(
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

    public static Predicate<Expression> isUsingPredicate(List<Symbol> symbols)
    {
        return expression -> symbols.stream().anyMatch(DependencyExtractor.extractUnique(expression)::contains);
    }

    public static PlanNode removeLimitNode(PlanNode node)
    {
        node = searchFrom(node)
                .where(LimitNode.class::isInstance)
                .skipOnlyWhen(ProjectNode.class::isInstance)
                .removeFirst();
        return node;
    }

    public static boolean isSupportedPredicate(Expression predicate)
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

    public static class DecorrelatedNode
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

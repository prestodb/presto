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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PlanNodeDecorrelator
{
    private final PlanNodeIdAllocator idAllocator;
    private final Lookup lookup;

    public PlanNodeDecorrelator(PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    public Optional<DecorrelatedNode> decorrelateFilters(PlanNode node, List<Symbol> correlation)
    {
        PlanNodeSearcher filterNodeSearcher = searchFrom(node, lookup)
                .where(FilterNode.class::isInstance)
                .recurseOnlyWhen(isInstanceOfAny(ProjectNode.class, LimitNode.class));
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

        if (!SymbolsExtractor.extractUnique(predicate).containsAll(correlation)) {
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

    private Predicate<Expression> isUsingPredicate(List<Symbol> symbols)
    {
        return expression -> symbols.stream().anyMatch(SymbolsExtractor.extractUnique(expression)::contains);
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

    private PlanNode removeLimitNode(PlanNode node)
    {
        node = searchFrom(node, lookup)
                .where(LimitNode.class::isInstance)
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .removeFirst();
        return node;
    }

    private PlanNode ensureJoinSymbolsAreReturned(PlanNode scalarAggregationSource, List<Expression> joinPredicate)
    {
        Set<Symbol> joinExpressionSymbols = SymbolsExtractor.extractUnique(joinPredicate);
        ExtendProjectionRewriter extendProjectionRewriter = new ExtendProjectionRewriter(
                idAllocator,
                joinExpressionSymbols);
        return rewriteWith(extendProjectionRewriter, scalarAggregationSource);
    }

    private Optional<DecorrelatedNode> decorrelatedNode(
            List<Expression> correlatedPredicates,
            PlanNode node,
            List<Symbol> correlation)
    {
        if (SymbolsExtractor.extractUnique(node, lookup).stream().anyMatch(correlation::contains)) {
            // node is still correlated ; /
            return Optional.empty();
        }
        return Optional.of(new DecorrelatedNode(correlatedPredicates, node));
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

            Assignments assignments = Assignments.builder()
                    .putAll(rewrittenNode.getAssignments())
                    .putIdentities(symbolsToAdd)
                    .build();

            return new ProjectNode(idAllocator.getNextId(), rewrittenNode.getSource(), assignments);
        }
    }
}

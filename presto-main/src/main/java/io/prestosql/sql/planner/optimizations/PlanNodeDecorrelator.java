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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
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
        // TODO: when correlations list empty this should return immediately. However this isn't correct
        // right now, because for nested subqueries correlation list is empty while there might exists usages
        // of the outer most correlated symbols

        Optional<DecorrelationResult> decorrelationResultOptional = lookup.resolve(node).accept(new DecorrelatingVisitor(correlation), null);
        return decorrelationResultOptional.flatMap(decorrelationResult -> decorrelatedNode(
                decorrelationResult.correlatedPredicates,
                decorrelationResult.node,
                correlation));
    }

    private class DecorrelatingVisitor
            extends PlanVisitor<Optional<DecorrelationResult>, Void>
    {
        final List<Symbol> correlation;

        DecorrelatingVisitor(List<Symbol> correlation)
        {
            this.correlation = requireNonNull(correlation, "correlation is null");
        }

        @Override
        protected Optional<DecorrelationResult> visitPlan(PlanNode node, Void context)
        {
            return Optional.of(new DecorrelationResult(
                    node,
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    ImmutableMultimap.of(),
                    false));
        }

        @Override
        public Optional<DecorrelationResult> visitFilter(FilterNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = Optional.of(new DecorrelationResult(
                    node.getSource(),
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    ImmutableMultimap.of(),
                    false));

            // try to decorrelate filters down the tree
            if (containsCorrelation(node.getSource(), correlation)) {
                childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            }

            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            Expression predicate = node.getPredicate();
            Map<Boolean, List<Expression>> predicates = ExpressionUtils.extractConjuncts(predicate).stream()
                    .collect(Collectors.partitioningBy(PlanNodeDecorrelator.DecorrelatingVisitor.this::isCorrelated));
            List<Expression> correlatedPredicates = ImmutableList.copyOf(predicates.get(true));
            List<Expression> uncorrelatedPredicates = ImmutableList.copyOf(predicates.get(false));

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            FilterNode newFilterNode = new FilterNode(
                    idAllocator.getNextId(),
                    childDecorrelationResult.node,
                    ExpressionUtils.combineConjuncts(uncorrelatedPredicates));

            Set<Symbol> symbolsToPropagate = Sets.difference(SymbolsExtractor.extractUnique(correlatedPredicates), ImmutableSet.copyOf(correlation));
            return Optional.of(new DecorrelationResult(
                    newFilterNode,
                    Sets.union(childDecorrelationResult.symbolsToPropagate, symbolsToPropagate),
                    ImmutableList.<Expression>builder()
                            .addAll(childDecorrelationResult.correlatedPredicates)
                            .addAll(correlatedPredicates)
                            .build(),
                    ImmutableMultimap.<Symbol, Symbol>builder()
                            .putAll(childDecorrelationResult.correlatedSymbolsMapping)
                            .putAll(extractCorrelatedSymbolsMapping(correlatedPredicates))
                            .build(),
                    childDecorrelationResult.atMostSingleRow));
        }

        @Override
        public Optional<DecorrelationResult> visitLimit(LimitNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent() || node.getCount() == 0) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            if (childDecorrelationResult.atMostSingleRow) {
                return childDecorrelationResultOptional;
            }

            if (node.getCount() != 1) {
                return Optional.empty();
            }

            Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();
            PlanNode decorrelatedChildNode = childDecorrelationResult.node;

            if (constantSymbols.isEmpty() || !constantSymbols.containsAll(decorrelatedChildNode.getOutputSymbols())) {
                return Optional.empty();
            }

            // Rewrite limit to aggregation on constant symbols
            AggregationNode aggregationNode = new AggregationNode(
                    idAllocator.getNextId(),
                    decorrelatedChildNode,
                    ImmutableMap.of(),
                    singleGroupingSet(decorrelatedChildNode.getOutputSymbols()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            return Optional.of(new DecorrelationResult(
                    aggregationNode,
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    true));
        }

        @Override
        public Optional<DecorrelationResult> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            return childDecorrelationResultOptional.filter(result -> result.atMostSingleRow);
        }

        @Override
        public Optional<DecorrelationResult> visitAggregation(AggregationNode node, Void context)
        {
            if (node.hasEmptyGroupingSet()) {
                return Optional.empty();
            }

            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();

            AggregationNode decorrelatedAggregation = childDecorrelationResult.getCorrelatedSymbolMapper()
                    .map(node, childDecorrelationResult.node);

            Set<Symbol> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
            List<Symbol> symbolsToAdd = childDecorrelationResult.symbolsToPropagate.stream()
                    .filter(symbol -> !groupingKeys.contains(symbol))
                    .collect(toImmutableList());

            if (!constantSymbols.containsAll(symbolsToAdd)) {
                return Optional.empty();
            }

            AggregationNode newAggregation = new AggregationNode(
                    decorrelatedAggregation.getId(),
                    decorrelatedAggregation.getSource(),
                    decorrelatedAggregation.getAggregations(),
                    AggregationNode.singleGroupingSet(ImmutableList.<Symbol>builder()
                            .addAll(node.getGroupingKeys())
                            .addAll(symbolsToAdd)
                            .build()),
                    ImmutableList.of(),
                    decorrelatedAggregation.getStep(),
                    decorrelatedAggregation.getHashSymbol(),
                    decorrelatedAggregation.getGroupIdSymbol());

            boolean atMostSingleRow = newAggregation.getGroupingSetCount() == 1
                    && constantSymbols.containsAll(newAggregation.getGroupingKeys());

            return Optional.of(new DecorrelationResult(
                    newAggregation,
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    atMostSingleRow));
        }

        @Override
        public Optional<DecorrelationResult> visitProject(ProjectNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            Set<Symbol> nodeOutputSymbols = ImmutableSet.copyOf(node.getOutputSymbols());
            List<Symbol> symbolsToAdd = childDecorrelationResult.symbolsToPropagate.stream()
                    .filter(symbol -> !nodeOutputSymbols.contains(symbol))
                    .collect(toImmutableList());

            Assignments assignments = Assignments.builder()
                    .putAll(node.getAssignments())
                    .putIdentities(symbolsToAdd)
                    .build();

            return Optional.of(new DecorrelationResult(
                    new ProjectNode(idAllocator.getNextId(), childDecorrelationResult.node, assignments),
                    childDecorrelationResult.symbolsToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedSymbolsMapping,
                    childDecorrelationResult.atMostSingleRow));
        }

        private Multimap<Symbol, Symbol> extractCorrelatedSymbolsMapping(List<Expression> correlatedConjuncts)
        {
            // TODO: handle coercions and non-direct column references
            ImmutableMultimap.Builder<Symbol, Symbol> mapping = ImmutableMultimap.builder();
            for (Expression conjunct : correlatedConjuncts) {
                if (!(conjunct instanceof ComparisonExpression)) {
                    continue;
                }

                ComparisonExpression comparison = (ComparisonExpression) conjunct;
                if (!(comparison.getLeft() instanceof SymbolReference
                        && comparison.getRight() instanceof SymbolReference
                        && comparison.getOperator().equals(EQUAL))) {
                    continue;
                }

                Symbol left = Symbol.from(comparison.getLeft());
                Symbol right = Symbol.from(comparison.getRight());

                if (correlation.contains(left) && !correlation.contains(right)) {
                    mapping.put(left, right);
                }

                if (correlation.contains(right) && !correlation.contains(left)) {
                    mapping.put(right, left);
                }
            }

            return mapping.build();
        }

        private boolean isCorrelated(Expression expression)
        {
            return correlation.stream().anyMatch(SymbolsExtractor.extractUnique(expression)::contains);
        }
    }

    private static class DecorrelationResult
    {
        final PlanNode node;
        final Set<Symbol> symbolsToPropagate;
        final List<Expression> correlatedPredicates;

        // mapping from correlated symbols to their uncorrelated equivalence
        final Multimap<Symbol, Symbol> correlatedSymbolsMapping;
        // If a subquery has at most single row for any correlation values?
        final boolean atMostSingleRow;

        DecorrelationResult(PlanNode node, Set<Symbol> symbolsToPropagate, List<Expression> correlatedPredicates, Multimap<Symbol, Symbol> correlatedSymbolsMapping, boolean atMostSingleRow)
        {
            this.node = node;
            this.symbolsToPropagate = symbolsToPropagate;
            this.correlatedPredicates = correlatedPredicates;
            this.atMostSingleRow = atMostSingleRow;
            this.correlatedSymbolsMapping = correlatedSymbolsMapping;
            checkState(symbolsToPropagate.containsAll(correlatedSymbolsMapping.values()), "Expected symbols to propagate to contain all constant symbols");
        }

        SymbolMapper getCorrelatedSymbolMapper()
        {
            return new SymbolMapper(correlatedSymbolsMapping.asMap().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, symbols -> Iterables.getLast(symbols.getValue()))));
        }

        /**
         * @return constant symbols from a perspective of a subquery
         */
        Set<Symbol> getConstantSymbols()
        {
            return ImmutableSet.copyOf(correlatedSymbolsMapping.values());
        }
    }

    private Optional<DecorrelatedNode> decorrelatedNode(
            List<Expression> correlatedPredicates,
            PlanNode node,
            List<Symbol> correlation)
    {
        if (containsCorrelation(node, correlation)) {
            // node is still correlated ; /
            return Optional.empty();
        }
        return Optional.of(new DecorrelatedNode(correlatedPredicates, node));
    }

    private boolean containsCorrelation(PlanNode node, List<Symbol> correlation)
    {
        return Sets.union(SymbolsExtractor.extractUnique(node, lookup), SymbolsExtractor.extractOutputSymbols(node, lookup)).stream().anyMatch(correlation::contains);
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

        public Optional<Expression> getCorrelatedPredicates()
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
}

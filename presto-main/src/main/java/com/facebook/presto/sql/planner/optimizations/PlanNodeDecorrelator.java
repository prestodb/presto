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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PlanNodeDecorrelator
{
    private final PlanNodeIdAllocator idAllocator;
    private final PlanVariableAllocator variableAllocator;
    private final Lookup lookup;

    public PlanNodeDecorrelator(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, Lookup lookup)
    {
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    public Optional<DecorrelatedNode> decorrelateFilters(PlanNode node, List<VariableReferenceExpression> correlation)
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
            extends InternalPlanVisitor<Optional<DecorrelationResult>, Void>
    {
        final List<VariableReferenceExpression> correlation;

        DecorrelatingVisitor(List<VariableReferenceExpression> correlation)
        {
            this.correlation = requireNonNull(correlation, "correlation is null");
        }

        @Override
        public Optional<DecorrelationResult> visitPlan(PlanNode node, Void context)
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

            Expression predicate = castToExpression(node.getPredicate());
            Map<Boolean, List<Expression>> predicates = ExpressionUtils.extractConjuncts(predicate).stream()
                    .collect(Collectors.partitioningBy(PlanNodeDecorrelator.DecorrelatingVisitor.this::isCorrelated));
            List<Expression> correlatedPredicates = ImmutableList.copyOf(predicates.get(true));
            List<Expression> uncorrelatedPredicates = ImmutableList.copyOf(predicates.get(false));

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            FilterNode newFilterNode = new FilterNode(
                    idAllocator.getNextId(),
                    childDecorrelationResult.node,
                    castToRowExpression(ExpressionUtils.combineConjuncts(uncorrelatedPredicates)));

            Set<VariableReferenceExpression> variablesToPropagate = Sets.difference(VariablesExtractor.extractUnique(correlatedPredicates, variableAllocator.getTypes()), ImmutableSet.copyOf(correlation));
            return Optional.of(new DecorrelationResult(
                    newFilterNode,
                    Sets.union(childDecorrelationResult.variablesToPropagate, variablesToPropagate),
                    ImmutableList.<Expression>builder()
                            .addAll(childDecorrelationResult.correlatedPredicates)
                            .addAll(correlatedPredicates)
                            .build(),
                    ImmutableMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                            .putAll(childDecorrelationResult.correlatedVariablesMapping)
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

            Set<VariableReferenceExpression> constantVariables = childDecorrelationResult.getConstantVariables();
            PlanNode decorrelatedChildNode = childDecorrelationResult.node;

            if (constantVariables.isEmpty() ||
                    !constantVariables.containsAll(decorrelatedChildNode.getOutputVariables())) {
                return Optional.empty();
            }

            // Rewrite limit to aggregation on constant symbols
            AggregationNode aggregationNode = new AggregationNode(
                    idAllocator.getNextId(),
                    decorrelatedChildNode,
                    ImmutableMap.of(),
                    singleGroupingSet(decorrelatedChildNode.getOutputVariables()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            return Optional.of(new DecorrelationResult(
                    aggregationNode,
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
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
            Set<VariableReferenceExpression> constantVariables = childDecorrelationResult.getConstantVariables();

            AggregationNode decorrelatedAggregation = childDecorrelationResult.getCorrelatedSymbolMapper()
                    .map(node, childDecorrelationResult.node);

            Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
            List<VariableReferenceExpression> variablesToAdd = childDecorrelationResult.variablesToPropagate.stream()
                    .filter(variable -> !groupingKeys.contains(variable))
                    .collect(toImmutableList());

            if (!constantVariables.containsAll(variablesToAdd)) {
                return Optional.empty();
            }

            AggregationNode newAggregation = new AggregationNode(
                    decorrelatedAggregation.getId(),
                    decorrelatedAggregation.getSource(),
                    decorrelatedAggregation.getAggregations(),
                    AggregationNode.singleGroupingSet(ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(node.getGroupingKeys())
                            .addAll(variablesToAdd)
                            .build()),
                    ImmutableList.of(),
                    decorrelatedAggregation.getStep(),
                    decorrelatedAggregation.getHashVariable(),
                    decorrelatedAggregation.getGroupIdVariable());

            boolean atMostSingleRow = newAggregation.getGroupingSetCount() == 1
                    && constantVariables.containsAll(newAggregation.getGroupingKeys());

            return Optional.of(new DecorrelationResult(
                    newAggregation,
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
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
            Set<VariableReferenceExpression> nodeOutputVariables = ImmutableSet.copyOf(node.getOutputVariables());
            List<VariableReferenceExpression> variablesToAdd = childDecorrelationResult.variablesToPropagate.stream()
                    .filter(variable -> !nodeOutputVariables.contains(variable))
                    .collect(toImmutableList());

            Assignments assignments = Assignments.builder()
                    .putAll(node.getAssignments())
                    .putAll(identitiesAsSymbolReferences(variablesToAdd))
                    .build();

            return Optional.of(new DecorrelationResult(
                    new ProjectNode(idAllocator.getNextId(), childDecorrelationResult.node, assignments),
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
                    childDecorrelationResult.atMostSingleRow));
        }

        private Multimap<VariableReferenceExpression, VariableReferenceExpression> extractCorrelatedSymbolsMapping(List<Expression> correlatedConjuncts)
        {
            ImmutableMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> mapping = ImmutableMultimap.builder();
            for (Expression conjunct : correlatedConjuncts) {
                if (!(conjunct instanceof ComparisonExpression)) {
                    continue;
                }

                ComparisonExpression comparison = (ComparisonExpression) conjunct;
                // handle coercions and non-direct column references
                if (comparison.getOperator().equals(EQUAL)) {
                    List<VariableReferenceExpression> left = extractUniqueExpression(comparison.getLeft());
                    List<VariableReferenceExpression> right = extractUniqueExpression(comparison.getRight());

                    for (VariableReferenceExpression leftVariableExpression : left) {
                        for (VariableReferenceExpression rightVariableExpression : right) {
                            if ((correlation.contains(leftVariableExpression) && !correlation.contains(rightVariableExpression))) {
                                mapping.put(leftVariableExpression, rightVariableExpression);
                            }
                            if (correlation.contains(rightVariableExpression) && !correlation.contains(leftVariableExpression)) {
                                mapping.put(rightVariableExpression, leftVariableExpression);
                            }
                        }
                    }
                }
            }

            return mapping.build();
        }

        private List<VariableReferenceExpression> extractUniqueExpression(Expression expression)
        {
            return VariablesExtractor.extractUnique(expression, variableAllocator.getTypes()).stream().collect(toImmutableList());
        }

        private boolean isCorrelated(Expression expression)
        {
            return correlation.stream().anyMatch(VariablesExtractor.extractUnique(expression, variableAllocator.getTypes())::contains);
        }
    }

    private static class DecorrelationResult
    {
        final PlanNode node;
        final Set<VariableReferenceExpression> variablesToPropagate;
        final List<Expression> correlatedPredicates;

        // mapping from correlated symbols to their uncorrelated equivalence
        final Multimap<VariableReferenceExpression, VariableReferenceExpression> correlatedVariablesMapping;
        // If a subquery has at most single row for any correlation values?
        final boolean atMostSingleRow;

        DecorrelationResult(
                PlanNode node,
                Set<VariableReferenceExpression> variablesToPropagate,
                List<Expression> correlatedPredicates,
                Multimap<VariableReferenceExpression, VariableReferenceExpression> correlatedVariablesMapping,
                boolean atMostSingleRow)
        {
            this.node = node;
            this.variablesToPropagate = variablesToPropagate;
            this.correlatedPredicates = correlatedPredicates;
            this.atMostSingleRow = atMostSingleRow;
            this.correlatedVariablesMapping = correlatedVariablesMapping;
            checkState(variablesToPropagate.containsAll(correlatedVariablesMapping.values()), "Expected symbols to propagate to contain all constant symbols");
        }

        SymbolMapper getCorrelatedSymbolMapper()
        {
            SymbolMapper.Builder builder = SymbolMapper.builder();
            correlatedVariablesMapping.forEach(builder::put);
            return builder.build();
        }

        /**
         * @return constant symbols from a perspective of a subquery
         */
        Set<VariableReferenceExpression> getConstantVariables()
        {
            return ImmutableSet.copyOf(correlatedVariablesMapping.values());
        }
    }

    private Optional<DecorrelatedNode> decorrelatedNode(
            List<Expression> correlatedPredicates,
            PlanNode node,
            List<VariableReferenceExpression> correlation)
    {
        if (containsCorrelation(node, correlation)) {
            // node is still correlated ; /
            return Optional.empty();
        }
        return Optional.of(new DecorrelatedNode(correlatedPredicates, node));
    }

    private boolean containsCorrelation(PlanNode node, List<VariableReferenceExpression> correlation)
    {
        return Sets.union(VariablesExtractor.extractUnique(node, lookup, variableAllocator.getTypes()), VariablesExtractor.extractOutputVariables(node, lookup)).stream().anyMatch(correlation::contains);
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

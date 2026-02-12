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

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
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

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.relational.Expressions.isComparison;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PlanNodeDecorrelator
{
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;
    private final Lookup lookup;
    private final LogicalRowExpressions logicalRowExpressions;

    public PlanNodeDecorrelator(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, Lookup lookup, LogicalRowExpressions logicalRowExpressions)
    {
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.logicalRowExpressions = requireNonNull(logicalRowExpressions, "logicalRowExpressions is null");
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

            RowExpression predicate = node.getPredicate();
            Map<Boolean, List<RowExpression>> predicates = LogicalRowExpressions.extractConjuncts(predicate).stream()
                    .collect(Collectors.partitioningBy(PlanNodeDecorrelator.DecorrelatingVisitor.this::isCorrelated));
            List<RowExpression> correlatedPredicates = ImmutableList.copyOf(predicates.get(true));
            List<RowExpression> uncorrelatedPredicates = ImmutableList.copyOf(predicates.get(false));

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            FilterNode newFilterNode = new FilterNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    childDecorrelationResult.node,
                    logicalRowExpressions.combineConjuncts(uncorrelatedPredicates));

            Set<VariableReferenceExpression> variablesToPropagate = Sets.difference(VariablesExtractor.extractUnique(correlatedPredicates), ImmutableSet.copyOf(correlation));
            return Optional.of(new DecorrelationResult(
                    newFilterNode,
                    Sets.union(childDecorrelationResult.variablesToPropagate, variablesToPropagate),
                    ImmutableList.<RowExpression>builder()
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

            if (node.getCount() == 1) {
                return rewriteLimitWithRowCountOne(childDecorrelationResult, node.getId());
            }
            return rewriteLimitWithRowCountGreaterThanOne(childDecorrelationResult, node);
        }

        // TODO Limit (1) could be decorrelated by the method rewriteLimitWithRowCountGreaterThanOne() as well.
        // The current decorrelation method for Limit (1) cannot deal with subqueries outputting other symbols
        // than constants.
        //
        // An example query that is currently not supported:
        // SELECT (
        //      SELECT a+b
        //      FROM (VALUES (1, 2), (1, 2)) inner_relation(a, b)
        //      WHERE a=x
        //      LIMIT 1)
        // FROM (VALUES (1)) outer_relation(x)
        //
        // Switching the decorrelation method would change the way that queries with EXISTS are executed,
        // and thus it needs benchmarking.
        private Optional<DecorrelationResult> rewriteLimitWithRowCountOne(DecorrelationResult childDecorrelationResult, PlanNodeId nodeId)
        {
            Set<VariableReferenceExpression> constantVariables = childDecorrelationResult.getConstantVariables();
            PlanNode decorrelatedChildNode = childDecorrelationResult.node;

            if (constantVariables.isEmpty() ||
                    !constantVariables.containsAll(decorrelatedChildNode.getOutputVariables())) {
                return Optional.empty();
            }

            // Rewrite limit to aggregation on constant symbols
            AggregationNode aggregationNode = new AggregationNode(
                    decorrelatedChildNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    decorrelatedChildNode,
                    ImmutableMap.of(),
                    singleGroupingSet(decorrelatedChildNode.getOutputVariables()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            return Optional.of(new DecorrelationResult(
                    aggregationNode,
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
                    true));
        }

        private Optional<DecorrelationResult> rewriteLimitWithRowCountGreaterThanOne(DecorrelationResult childDecorrelationResult, LimitNode node)
        {
            PlanNode decorrelatedChildNode = childDecorrelationResult.node;

            // no rewrite needed (no symbols to partition by)
            if (childDecorrelationResult.variablesToPropagate.isEmpty()) {
                return Optional.of(new DecorrelationResult(
                        node.replaceChildren(ImmutableList.of(decorrelatedChildNode)),
                        childDecorrelationResult.variablesToPropagate,
                        childDecorrelationResult.correlatedPredicates,
                        childDecorrelationResult.correlatedVariablesMapping,
                        false));
            }

            Set<VariableReferenceExpression> constantVariables = childDecorrelationResult.getConstantVariables();
            if (!constantVariables.containsAll(childDecorrelationResult.variablesToPropagate)) {
                return Optional.empty();
            }

            // rewrite Limit to RowNumberNode partitioned by constant symbols
            RowNumberNode rowNumberNode = new RowNumberNode(
                    decorrelatedChildNode.getSourceLocation(),
                    node.getId(),
                    decorrelatedChildNode,
                    ImmutableList.copyOf(childDecorrelationResult.variablesToPropagate),
                    variableAllocator.newVariable("row_number", BIGINT),
                    Optional.of(toIntExact(node.getCount())),
                    false,
                    Optional.empty());

            return Optional.of(new DecorrelationResult(
                    rowNumberNode,
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
                    false));
        }

        @Override
        public Optional<DecorrelationResult> visitTopN(TopNNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            if (childDecorrelationResult.atMostSingleRow) {
                return childDecorrelationResultOptional;
            }

            PlanNode decorrelatedChildNode = childDecorrelationResult.node;
            Set<VariableReferenceExpression> constantVariables = childDecorrelationResult.getConstantVariables();
            Optional<OrderingScheme> decorrelatedOrderingScheme = decorrelateOrderingScheme(node.getOrderingScheme(), constantVariables);

            // no partitioning needed (no symbols to partition by)
            if (childDecorrelationResult.variablesToPropagate.isEmpty()) {
                return decorrelatedOrderingScheme
                        .map(orderingScheme -> Optional.of(new DecorrelationResult(
                                // ordering symbols are present - return decorrelated TopNNode
                                new TopNNode(decorrelatedChildNode.getSourceLocation(), node.getId(), decorrelatedChildNode, node.getCount(), orderingScheme, node.getStep()),
                                childDecorrelationResult.variablesToPropagate,
                                childDecorrelationResult.correlatedPredicates,
                                childDecorrelationResult.correlatedVariablesMapping,
                                node.getCount() == 1)))
                        .orElseGet(() -> Optional.of(new DecorrelationResult(
                                // no ordering symbols are left - convert to LimitNode
                                new LimitNode(decorrelatedChildNode.getSourceLocation(), node.getId(), decorrelatedChildNode, node.getCount(), LimitNode.Step.FINAL),
                                childDecorrelationResult.variablesToPropagate,
                                childDecorrelationResult.correlatedPredicates,
                                childDecorrelationResult.correlatedVariablesMapping,
                                node.getCount() == 1)));
            }

            if (!constantVariables.containsAll(childDecorrelationResult.variablesToPropagate)) {
                return Optional.empty();
            }

            return decorrelatedOrderingScheme
                    .map(orderingScheme -> {
                        // ordering symbols are present - rewrite TopN to TopNRowNumberNode partitioned by constant symbols
                        TopNRowNumberNode topNRowNumberNode = new TopNRowNumberNode(
                                decorrelatedChildNode.getSourceLocation(),
                                node.getId(),
                                decorrelatedChildNode,
                                new DataOrganizationSpecification(
                                        ImmutableList.copyOf(childDecorrelationResult.variablesToPropagate),
                                        Optional.of(orderingScheme)),
                                TopNRowNumberNode.RankingFunction.ROW_NUMBER,
                                variableAllocator.newVariable("row_number", BIGINT),
                                toIntExact(node.getCount()),
                                false,
                                Optional.empty());

                        return Optional.of(new DecorrelationResult(
                                topNRowNumberNode,
                                childDecorrelationResult.variablesToPropagate,
                                childDecorrelationResult.correlatedPredicates,
                                childDecorrelationResult.correlatedVariablesMapping,
                                node.getCount() == 1));
                    })
                    .orElseGet(() -> {
                        // no ordering symbols are left - rewrite TopN to RowNumberNode partitioned by constant symbols
                        RowNumberNode rowNumberNode = new RowNumberNode(
                                decorrelatedChildNode.getSourceLocation(),
                                node.getId(),
                                decorrelatedChildNode,
                                ImmutableList.copyOf(childDecorrelationResult.variablesToPropagate),
                                variableAllocator.newVariable("row_number", BIGINT),
                                Optional.of(toIntExact(node.getCount())),
                                false,
                                Optional.empty());

                        return Optional.of(new DecorrelationResult(
                                rowNumberNode,
                                childDecorrelationResult.variablesToPropagate,
                                childDecorrelationResult.correlatedPredicates,
                                childDecorrelationResult.correlatedVariablesMapping,
                                node.getCount() == 1));
                    });
        }

        private Optional<OrderingScheme> decorrelateOrderingScheme(OrderingScheme orderingScheme, Set<VariableReferenceExpression> constantVariables)
        {
            // remove local and remote constant sort symbols from the OrderingScheme
            ImmutableList.Builder<Ordering> nonConstantOrderings = ImmutableList.builder();

            for (Ordering ordering : orderingScheme.getOrderBy()) {
                if (!constantVariables.contains(ordering.getVariable()) && !correlation.contains(ordering.getVariable())) {
                    nonConstantOrderings.add(ordering);
                }
            }

            if (nonConstantOrderings.build().isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new OrderingScheme(nonConstantOrderings.build()));
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
                    decorrelatedAggregation.getSourceLocation(),
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
                    decorrelatedAggregation.getGroupIdVariable(),
                    decorrelatedAggregation.getAggregationId());

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
                    .putAll(identityAssignments(variablesToAdd))
                    .build();

            return Optional.of(new DecorrelationResult(
                    new ProjectNode(idAllocator.getNextId(), childDecorrelationResult.node, assignments),
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
                    childDecorrelationResult.atMostSingleRow));
        }

        private Multimap<VariableReferenceExpression, VariableReferenceExpression> extractCorrelatedSymbolsMapping(List<RowExpression> correlatedConjuncts)
        {
            ImmutableMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> mapping = ImmutableMultimap.builder();
            for (RowExpression conjunct : correlatedConjuncts) {
                if (!(conjunct instanceof CallExpression)) {
                    continue;
                }
                CallExpression comparison = (CallExpression) conjunct;
                if (!isComparison(comparison)) {
                    continue;
                }
                checkArgument(comparison.getArguments().size() == 2, "Unexpected comparison function: %s", comparison);

                // handle coercions and non-direct column references
                if (comparison.getFunctionHandle().getName().equals(EQUAL.getFunctionName().toString())) {
                    List<VariableReferenceExpression> left = extractUniqueExpression(comparison.getArguments().get(0));
                    List<VariableReferenceExpression> right = extractUniqueExpression(comparison.getArguments().get(1));

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

        private List<VariableReferenceExpression> extractUniqueExpression(RowExpression expression)
        {
            return VariablesExtractor.extractUnique(expression).stream().collect(toImmutableList());
        }

        private boolean isCorrelated(RowExpression expression)
        {
            return correlation.stream().anyMatch(VariablesExtractor.extractUnique(expression)::contains);
        }
    }

    private static class DecorrelationResult
    {
        final PlanNode node;
        final Set<VariableReferenceExpression> variablesToPropagate;
        final List<RowExpression> correlatedPredicates;

        // mapping from correlated symbols to their uncorrelated equivalence
        final Multimap<VariableReferenceExpression, VariableReferenceExpression> correlatedVariablesMapping;
        // If a subquery has at most single row for any correlation values?
        final boolean atMostSingleRow;

        DecorrelationResult(
                PlanNode node,
                Set<VariableReferenceExpression> variablesToPropagate,
                List<RowExpression> correlatedPredicates,
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
            List<RowExpression> correlatedPredicates,
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
        return Sets.union(VariablesExtractor.extractUnique(node, lookup), VariablesExtractor.extractOutputVariables(node, lookup)).stream().anyMatch(correlation::contains);
    }

    public static class DecorrelatedNode
    {
        private final List<RowExpression> correlatedPredicates;
        private final PlanNode node;

        public DecorrelatedNode(List<RowExpression> correlatedPredicates, PlanNode node)
        {
            requireNonNull(correlatedPredicates, "correlatedPredicates is null");
            this.correlatedPredicates = ImmutableList.copyOf(correlatedPredicates);
            this.node = requireNonNull(node, "node is null");
        }

        public Optional<RowExpression> getCorrelatedPredicates()
        {
            if (correlatedPredicates.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(and(correlatedPredicates));
        }

        public PlanNode getNode()
        {
            return node;
        }
    }
}

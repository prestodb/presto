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
package com.facebook.presto.sql.planner;

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils.outputMap;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class EffectivePredicateExtractor
{
    private final RowExpressionDomainTranslator domainTranslator;
    private final FunctionAndTypeManager functionAndTypeManager;

    public EffectivePredicateExtractor(RowExpressionDomainTranslator domainTranslator, FunctionAndTypeManager functionAndTypeManager)
    {
        this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        this.functionAndTypeManager = functionAndTypeManager;
    }

    public RowExpression extract(PlanNode node)
    {
        return node.accept(new Visitor(domainTranslator, functionAndTypeManager), null);
    }

    private static class Visitor
            extends InternalPlanVisitor<RowExpression, Void>
    {
        private final RowExpressionDomainTranslator domainTranslator;
        private final LogicalRowExpressions logicalRowExpressions;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;
        private final FunctionAndTypeManager functionManger;

        public Visitor(RowExpressionDomainTranslator domainTranslator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
            this.functionManger = requireNonNull(functionAndTypeManager);
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
            this.logicalRowExpressions = new LogicalRowExpressions(determinismEvaluator, new FunctionResolution(functionAndTypeManager), functionAndTypeManager);
        }

        @Override
        public RowExpression visitPlan(PlanNode node, Void context)
        {
            return TRUE_CONSTANT;
        }

        @Override
        public RowExpression visitAggregation(AggregationNode node, Void context)
        {
            // GROUP BY () always produces a group, regardless of whether there's any
            // input (unlike the case where there are group by keys, which produce
            // no output if there's no input).
            // Therefore, we can't say anything about the effective predicate of the
            // output of such an aggregation.
            if (node.getGroupingKeys().isEmpty()) {
                return TRUE_CONSTANT;
            }

            RowExpression underlyingPredicate = node.getSource().accept(this, context);

            return pullExpressionThroughVariables(underlyingPredicate, node.getGroupingKeys());
        }

        @Override
        public RowExpression visitFilter(FilterNode node, Void context)
        {
            RowExpression underlyingPredicate = node.getSource().accept(this, context);

            RowExpression predicate = node.getPredicate();

            // Remove non-deterministic conjuncts
            predicate = logicalRowExpressions.filterDeterministicConjuncts(predicate);

            return logicalRowExpressions.combineConjuncts(predicate, underlyingPredicate);
        }

        @Override
        public RowExpression visitExchange(ExchangeNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> {
                Map<VariableReferenceExpression, VariableReferenceExpression> mappings = new HashMap<>();
                for (int i = 0; i < node.getInputs().get(source).size(); i++) {
                    mappings.put(
                            node.getOutputVariables().get(i),
                            node.getInputs().get(source).get(i));
                }
                return mappings.entrySet();
            });
        }

        @Override
        public RowExpression visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            if (node.getSource() instanceof ProjectNode) {
                return node.getSource().accept(this, context);
            }
            return TRUE_CONSTANT;
        }

        @Override
        public RowExpression visitProject(ProjectNode node, Void context)
        {
            // TODO: add simple algebraic solver for projection translation (right now only considers identity projections)

            RowExpression underlyingPredicate = node.getSource().accept(this, context);

            List<RowExpression> projectionEqualities = node.getAssignments().getMap().entrySet().stream()
                    .filter(this::notIdentityAssignment)
                    .filter(this::canCompareEquity)
                    .map(this::toEquality)
                    .collect(toImmutableList());

            return pullExpressionThroughVariables(logicalRowExpressions.combineConjuncts(
                    ImmutableList.<RowExpression>builder()
                            .addAll(projectionEqualities)
                            .add(underlyingPredicate)
                            .build()),
                    node.getOutputVariables());
        }

        @Override
        public RowExpression visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitTableScan(TableScanNode node, Void context)
        {
            Map<ColumnHandle, VariableReferenceExpression> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
            return domainTranslator.toPredicate(node.getCurrentConstraint().simplify().transform(column -> assignments.containsKey(column) ? assignments.get(column) : null));
        }

        @Override
        public RowExpression visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitUnion(UnionNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> outputMap(node, source).entries());
        }

        @Override
        public RowExpression visitJoin(JoinNode node, Void context)
        {
            RowExpression leftPredicate = node.getLeft().accept(this, context);
            RowExpression rightPredicate = node.getRight().accept(this, context);

            List<RowExpression> joinConjuncts = node.getCriteria().stream()
                    .map(this::toRowExpression)
                    .collect(toImmutableList());

            switch (node.getType()) {
                case INNER:
                    return pullExpressionThroughVariables(logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(leftPredicate)
                            .add(rightPredicate)
                            .add(logicalRowExpressions.combineConjuncts(joinConjuncts))
                            .add(node.getFilter().orElse(TRUE_CONSTANT))
                            .build()), node.getOutputVariables());
                case LEFT:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, node.getOutputVariables()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputVariables(), node.getRight().getOutputVariables()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputVariables(), node.getRight().getOutputVariables()::contains))
                            .build());
                case RIGHT:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(pullExpressionThroughVariables(rightPredicate, node.getOutputVariables()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), node.getOutputVariables(), node.getLeft().getOutputVariables()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputVariables(), node.getLeft().getOutputVariables()::contains))
                            .build());
                case FULL:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), node.getOutputVariables(), node.getLeft().getOutputVariables()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputVariables(), node.getRight().getOutputVariables()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputVariables(), node.getLeft().getOutputVariables()::contains, node.getRight().getOutputVariables()::contains))
                            .build());
                default:
                    throw new UnsupportedOperationException("Unknown join type: " + node.getType());
            }
        }

        private Iterable<RowExpression> pullNullableConjunctsThroughOuterJoin(List<RowExpression> conjuncts, Collection<VariableReferenceExpression> outputVariables, Predicate<VariableReferenceExpression>... nullVariableScopes)
        {
            // Conjuncts without any symbol dependencies cannot be applied to the effective predicate (e.g. FALSE literal)
            return conjuncts.stream()
                    .map(expression -> pullExpressionThroughVariables(expression, outputVariables))
                    .map(expression -> VariablesExtractor.extractAll(expression).isEmpty() ? TRUE_CONSTANT : expression)
                    .map(expressionOrNullVariables(nullVariableScopes))
                    .collect(toImmutableList());
        }

        public Function<RowExpression, RowExpression> expressionOrNullVariables(final Predicate<VariableReferenceExpression>... nullVariableScopes)
        {
            return expression -> {
                ImmutableList.Builder<RowExpression> resultDisjunct = ImmutableList.builder();
                resultDisjunct.add(expression);

                for (Predicate<VariableReferenceExpression> nullVariableScope : nullVariableScopes) {
                    List<VariableReferenceExpression> variables = VariablesExtractor.extractUnique(expression).stream()
                            .filter(nullVariableScope)
                            .collect(toImmutableList());

                    if (Iterables.isEmpty(variables)) {
                        continue;
                    }

                    ImmutableList.Builder<RowExpression> nullConjuncts = ImmutableList.builder();
                    for (VariableReferenceExpression variable : variables) {
                        nullConjuncts.add(specialForm(IS_NULL, BOOLEAN, variable));
                    }

                    resultDisjunct.add(logicalRowExpressions.and(nullConjuncts.build()));
                }

                return logicalRowExpressions.or(resultDisjunct.build());
            };
        }

        @Override
        public RowExpression visitSemiJoin(SemiJoinNode node, Void context)
        {
            // Filtering source does not change the effective predicate over the output symbols
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            RowExpression leftPredicate = node.getLeft().accept(this, context);
            RowExpression rightPredicate = node.getRight().accept(this, context);

            switch (node.getType()) {
                case INNER:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, node.getOutputVariables()))
                            .add(pullExpressionThroughVariables(rightPredicate, node.getOutputVariables()))
                            .build());
                case LEFT:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, node.getOutputVariables()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputVariables(), node.getRight().getOutputVariables()::contains))
                            .build());
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }
        }

        private RowExpression toRowExpression(JoinNode.EquiJoinClause equiJoinClause)
        {
            return buildEqualsExpression(functionManger, equiJoinClause.getLeft(), equiJoinClause.getRight());
        }

        private RowExpression deriveCommonPredicates(PlanNode node, Function<Integer, Collection<Map.Entry<VariableReferenceExpression, VariableReferenceExpression>>> mapping)
        {
            // Find the predicates that can be pulled up from each source
            List<Set<RowExpression>> sourceOutputConjuncts = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                RowExpression underlyingPredicate = node.getSources().get(i).accept(this, null);

                List<RowExpression> equalities = mapping.apply(i).stream()
                        .filter(this::notIdentityAssignment)
                        .filter(this::canCompareEquity)
                        .map(this::toEquality)
                        .collect(toImmutableList());

                sourceOutputConjuncts.add(ImmutableSet.copyOf(extractConjuncts(pullExpressionThroughVariables(logicalRowExpressions.combineConjuncts(
                        ImmutableList.<RowExpression>builder()
                                .addAll(equalities)
                                .add(underlyingPredicate)
                                .build()),
                        node.getOutputVariables()))));
            }

            // Find the intersection of predicates across all sources
            // TODO: use a more precise way to determine overlapping conjuncts (e.g. commutative predicates)
            Iterator<Set<RowExpression>> iterator = sourceOutputConjuncts.iterator();
            Set<RowExpression> potentialOutputConjuncts = iterator.next();
            while (iterator.hasNext()) {
                potentialOutputConjuncts = Sets.intersection(potentialOutputConjuncts, iterator.next());
            }

            return logicalRowExpressions.combineConjuncts(potentialOutputConjuncts);
        }

        private boolean notIdentityAssignment(Map.Entry<VariableReferenceExpression, ? extends RowExpression> entry)
        {
            return !entry.getKey().equals(entry.getValue());
        }

        private boolean canCompareEquity(Map.Entry<VariableReferenceExpression, ? extends RowExpression> entry)
        {
            try {
                functionManger.resolveOperator(EQUAL, fromTypes(entry.getKey().getType(), entry.getValue().getType()));
                return true;
            }
            catch (OperatorNotFoundException e) {
                return false;
            }
        }

        private RowExpression toEquality(Map.Entry<VariableReferenceExpression, ? extends RowExpression> entry)
        {
            return buildEqualsExpression(functionManger, entry.getKey(), entry.getValue());
        }

        private static CallExpression buildEqualsExpression(FunctionAndTypeManager functionAndTypeManager, RowExpression left, RowExpression right)
        {
            return call(
                    EQUAL.getFunctionName().getObjectName(),
                    functionAndTypeManager.resolveOperator(EQUAL, fromTypes(left.getType(), right.getType())),
                    BOOLEAN,
                    left,
                    right);
        }

        private RowExpression pullExpressionThroughVariables(RowExpression expression, Collection<VariableReferenceExpression> variables)
        {
            EqualityInference equalityInference = new EqualityInference.Builder(functionManger)
                    .addEqualityInference(expression)
                    .build();

            ImmutableList.Builder<RowExpression> effectiveConjuncts = ImmutableList.builder();
            for (RowExpression conjunct : new EqualityInference.Builder(functionManger).nonInferrableConjuncts(expression)) {
                if (determinismEvaluator.isDeterministic(conjunct)) {
                    RowExpression rewritten = equalityInference.rewriteExpression(conjunct, in(variables));
                    if (rewritten != null) {
                        effectiveConjuncts.add(rewritten);
                    }
                }
            }

            effectiveConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(in(variables)).getScopeEqualities());

            return logicalRowExpressions.combineConjuncts(effectiveConjuncts.build());
        }
    }
}

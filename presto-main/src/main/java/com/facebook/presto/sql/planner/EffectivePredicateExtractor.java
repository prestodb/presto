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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.optimizations.JoinNodeUtils;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
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

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.expressionOrNullVariables;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.filterDeterministicConjuncts;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.transformValues;
import static java.util.Objects.requireNonNull;

/**
 * Computes the effective predicate at the top of the specified PlanNode
 * <p>
 * Note: non-deterministic predicates can not be pulled up (so they will be ignored)
 */
public class EffectivePredicateExtractor
{
    private static final Predicate<Map.Entry<VariableReferenceExpression, ? extends Expression>> VARIABLE_MATCHES_EXPRESSION =
            entry -> entry.getValue().equals(new SymbolReference(entry.getKey().getName()));

    private static final Function<Map.Entry<VariableReferenceExpression, ? extends Expression>, Expression> VARIABLE_ENTRY_TO_EQUALITY =
            entry -> {
                SymbolReference reference = new SymbolReference(entry.getKey().getName());
                Expression expression = entry.getValue();
                // TODO: this is not correct with respect to NULLs ('reference IS NULL' would be correct, rather than 'reference = NULL')
                // TODO: switch this to 'IS NOT DISTINCT FROM' syntax when EqualityInference properly supports it
                return new ComparisonExpression(ComparisonExpression.Operator.EQUAL, reference, expression);
            };

    private final ExpressionDomainTranslator domainTranslator;

    public EffectivePredicateExtractor(ExpressionDomainTranslator domainTranslator)
    {
        this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
    }

    public Expression extract(PlanNode node, TypeProvider types)
    {
        return node.accept(new Visitor(domainTranslator, types), null);
    }

    private static class Visitor
            extends InternalPlanVisitor<Expression, Void>
    {
        private final ExpressionDomainTranslator domainTranslator;
        private final TypeProvider types;

        public Visitor(ExpressionDomainTranslator domainTranslator, TypeProvider types)
        {
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public Expression visitPlan(PlanNode node, Void context)
        {
            return TRUE_LITERAL;
        }

        @Override
        public Expression visitAggregation(AggregationNode node, Void context)
        {
            // GROUP BY () always produces a group, regardless of whether there's any
            // input (unlike the case where there are group by keys, which produce
            // no output if there's no input).
            // Therefore, we can't say anything about the effective predicate of the
            // output of such an aggregation.
            if (node.getGroupingKeys().isEmpty()) {
                return TRUE_LITERAL;
            }

            Expression underlyingPredicate = node.getSource().accept(this, context);

            return pullExpressionThroughVariables(underlyingPredicate, node.getGroupingKeys());
        }

        @Override
        public Expression visitFilter(FilterNode node, Void context)
        {
            Expression underlyingPredicate = node.getSource().accept(this, context);

            Expression predicate = castToExpression(node.getPredicate());

            // Remove non-deterministic conjuncts
            predicate = filterDeterministicConjuncts(predicate);

            return combineConjuncts(predicate, underlyingPredicate);
        }

        @Override
        public Expression visitExchange(ExchangeNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> {
                Map<VariableReferenceExpression, SymbolReference> mappings = new HashMap<>();
                for (int i = 0; i < node.getInputs().get(source).size(); i++) {
                    mappings.put(
                            node.getOutputVariables().get(i),
                            new SymbolReference(node.getInputs().get(source).get(i).getName()));
                }
                return mappings.entrySet();
            });
        }

        @Override
        public Expression visitProject(ProjectNode node, Void context)
        {
            // TODO: add simple algebraic solver for projection translation (right now only considers identity projections)

            Expression underlyingPredicate = node.getSource().accept(this, context);

            List<Expression> projectionEqualities = transformValues(node.getAssignments().getMap(), OriginalExpressionUtils::castToExpression).entrySet().stream()
                    .filter(VARIABLE_MATCHES_EXPRESSION.negate())
                    .map(VARIABLE_ENTRY_TO_EQUALITY)
                    .collect(toImmutableList());

            return pullExpressionThroughVariables(combineConjuncts(
                    ImmutableList.<Expression>builder()
                            .addAll(projectionEqualities)
                            .add(underlyingPredicate)
                            .build()),
                    node.getOutputVariables());
        }

        @Override
        public Expression visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitTableScan(TableScanNode node, Void context)
        {
            Map<ColumnHandle, VariableReferenceExpression> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
            return domainTranslator.toPredicate(node.getCurrentConstraint().simplify().transform(column -> assignments.containsKey(column) ? assignments.get(column).getName() : null));
        }

        @Override
        public Expression visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitUnion(UnionNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> Multimaps.transformValues(node.outputMap(source), variable -> new SymbolReference(variable.getName())).entries());
        }

        @Override
        public Expression visitJoin(JoinNode node, Void context)
        {
            Expression leftPredicate = node.getLeft().accept(this, context);
            Expression rightPredicate = node.getRight().accept(this, context);

            List<Expression> joinConjuncts = node.getCriteria().stream()
                    .map(JoinNodeUtils::toExpression)
                    .collect(toImmutableList());

            switch (node.getType()) {
                case INNER:
                    return pullExpressionThroughVariables(combineConjuncts(ImmutableList.<Expression>builder()
                            .add(leftPredicate)
                            .add(rightPredicate)
                            .add(combineConjuncts(joinConjuncts))
                            .add(node.getFilter().map(OriginalExpressionUtils::castToExpression).orElse(TRUE_LITERAL))
                            .build()), node.getOutputVariables());
                case LEFT:
                    return combineConjuncts(ImmutableList.<Expression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, node.getOutputVariables()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputVariables(), node.getRight().getOutputVariables()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputVariables(), node.getRight().getOutputVariables()::contains))
                            .build());
                case RIGHT:
                    return combineConjuncts(ImmutableList.<Expression>builder()
                            .add(pullExpressionThroughVariables(rightPredicate, node.getOutputVariables()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), node.getOutputVariables(), node.getLeft().getOutputVariables()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputVariables(), node.getLeft().getOutputVariables()::contains))
                            .build());
                case FULL:
                    return combineConjuncts(ImmutableList.<Expression>builder()
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), node.getOutputVariables(), node.getLeft().getOutputVariables()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputVariables(), node.getRight().getOutputVariables()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputVariables(), node.getLeft().getOutputVariables()::contains, node.getRight().getOutputVariables()::contains))
                            .build());
                default:
                    throw new UnsupportedOperationException("Unknown join type: " + node.getType());
            }
        }

        private Iterable<Expression> pullNullableConjunctsThroughOuterJoin(List<Expression> conjuncts, Collection<VariableReferenceExpression> outputVariables, Predicate<VariableReferenceExpression>... nullVariableScopes)
        {
            // Conjuncts without any symbol dependencies cannot be applied to the effective predicate (e.g. FALSE literal)
            return conjuncts.stream()
                    .map(expression -> pullExpressionThroughVariables(expression, outputVariables))
                    .map(expression -> VariablesExtractor.extractAll(expression, types).isEmpty() ? TRUE_LITERAL : expression)
                    .map(expressionOrNullVariables(types, nullVariableScopes))
                    .collect(toImmutableList());
        }

        @Override
        public Expression visitSemiJoin(SemiJoinNode node, Void context)
        {
            // Filtering source does not change the effective predicate over the output symbols
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            Expression leftPredicate = node.getLeft().accept(this, context);
            Expression rightPredicate = node.getRight().accept(this, context);

            switch (node.getType()) {
                case INNER:
                    return combineConjuncts(ImmutableList.<Expression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, node.getOutputVariables()))
                            .add(pullExpressionThroughVariables(rightPredicate, node.getOutputVariables()))
                            .build());
                case LEFT:
                    return combineConjuncts(ImmutableList.<Expression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, node.getOutputVariables()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputVariables(), node.getRight().getOutputVariables()::contains))
                            .build());
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }
        }

        private Expression deriveCommonPredicates(PlanNode node, Function<Integer, Collection<Map.Entry<VariableReferenceExpression, SymbolReference>>> mapping)
        {
            // Find the predicates that can be pulled up from each source
            List<Set<Expression>> sourceOutputConjuncts = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                Expression underlyingPredicate = node.getSources().get(i).accept(this, null);

                List<Expression> equalities = mapping.apply(i).stream()
                        .filter(VARIABLE_MATCHES_EXPRESSION.negate())
                        .map(VARIABLE_ENTRY_TO_EQUALITY)
                        .collect(toImmutableList());

                sourceOutputConjuncts.add(ImmutableSet.copyOf(extractConjuncts(pullExpressionThroughVariables(combineConjuncts(
                        ImmutableList.<Expression>builder()
                                .addAll(equalities)
                                .add(underlyingPredicate)
                                .build()),
                        node.getOutputVariables()))));
            }

            // Find the intersection of predicates across all sources
            // TODO: use a more precise way to determine overlapping conjuncts (e.g. commutative predicates)
            Iterator<Set<Expression>> iterator = sourceOutputConjuncts.iterator();
            Set<Expression> potentialOutputConjuncts = iterator.next();
            while (iterator.hasNext()) {
                potentialOutputConjuncts = Sets.intersection(potentialOutputConjuncts, iterator.next());
            }

            return combineConjuncts(potentialOutputConjuncts);
        }

        private Expression pullExpressionThroughVariables(Expression expression, Collection<VariableReferenceExpression> variables)
        {
            EqualityInference equalityInference = createEqualityInference(expression);

            ImmutableList.Builder<Expression> effectiveConjuncts = ImmutableList.builder();
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(expression)) {
                if (ExpressionDeterminismEvaluator.isDeterministic(conjunct)) {
                    Expression rewritten = equalityInference.rewriteExpression(conjunct, in(variables), types);
                    if (rewritten != null) {
                        effectiveConjuncts.add(rewritten);
                    }
                }
            }

            effectiveConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(in(variables), types).getScopeEqualities());

            return combineConjuncts(effectiveConjuncts.build());
        }
    }
}

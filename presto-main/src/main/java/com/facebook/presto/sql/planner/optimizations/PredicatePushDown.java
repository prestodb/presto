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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.EffectivePredicateExtractor;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.ExpressionSymbolInliner;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.util.maps.IdentityLinkedHashMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.expressionOrNullSymbols;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.DeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class PredicatePushDown
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(PredicatePushDown.class);

    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PredicatePushDown(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(symbolAllocator, idAllocator, metadata, sqlParser, session, types), plan, BooleanLiteral.TRUE_LITERAL);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Expression>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final Session session;
        private final Map<Symbol, Type> types;
        private final ExpressionEquivalence expressionEquivalence;

        private Rewriter(
                SymbolAllocator symbolAllocator,
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                SqlParser sqlParser,
                Session session,
                Map<Symbol, Type> types)
        {
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.expressionEquivalence = new ExpressionEquivalence(metadata, sqlParser);
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Expression> context)
        {
            PlanNode rewrittenNode = context.defaultRewrite(node, BooleanLiteral.TRUE_LITERAL);
            if (!context.get().equals(BooleanLiteral.TRUE_LITERAL)) {
                // Drop in a FilterNode b/c we cannot push our predicate down any further
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, context.get());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Expression> context)
        {
            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                Map<Symbol, SymbolReference> outputsToInputs = new HashMap<>();
                for (int index = 0; index < node.getInputs().get(i).size(); index++) {
                    outputsToInputs.put(
                            node.getOutputSymbols().get(index),
                            node.getInputs().get(i).get(index).toSymbolReference());
                }

                Expression sourcePredicate = ExpressionTreeRewriter.rewriteWith(new ExpressionSymbolInliner(outputsToInputs), context.get());
                PlanNode source = node.getSources().get(i);
                PlanNode rewrittenSource = context.rewrite(source, sourcePredicate);
                if (rewrittenSource != source) {
                    modified = true;
                }
                builder.add(rewrittenSource);
            }

            if (modified) {
                return new ExchangeNode(
                        node.getId(),
                        node.getType(),
                        node.getScope(),
                        node.getPartitioningScheme(),
                        builder.build(),
                        node.getInputs());
            }

            return node;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Expression> context)
        {
            Set<Symbol> deterministicSymbols = node.getAssignments().entrySet().stream()
                    .filter(entry -> DeterminismEvaluator.isDeterministic(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Predicate<Expression> deterministic = conjunct -> DependencyExtractor.extractUnique(conjunct).stream()
                    .allMatch(deterministicSymbols::contains);

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(deterministic));

            // Push down conjuncts from the inherited predicate that don't depend on non-deterministic assignments
            PlanNode rewrittenNode = context.defaultRewrite(node,
                    ExpressionTreeRewriter.rewriteWith(new ExpressionSymbolInliner(node.getAssignments().getMap()), combineConjuncts(conjuncts.get(true))));

            // All non-deterministic conjuncts, if any, will be in the filter node.
            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(conjuncts.get(false)));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Expression> context)
        {
            checkState(!DependencyExtractor.extractUnique(context.get()).contains(node.getGroupIdSymbol()), "groupId symbol cannot be referenced in predicate");

            Map<Symbol, SymbolReference> commonGroupingSymbolMapping = node.getGroupingSetMappings().entrySet().stream()
                    .filter(entry -> node.getCommonGroupingColumns().contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));

            Predicate<Expression> pushdownEligiblePredicate = conjunct -> DependencyExtractor.extractUnique(conjunct).stream()
                    .allMatch(commonGroupingSymbolMapping.keySet()::contains);

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(pushdownEligiblePredicate));

            // Push down conjuncts from the inherited predicate that apply to common grouping symbols
            PlanNode rewrittenNode = context.defaultRewrite(node,
                    ExpressionTreeRewriter.rewriteWith(new ExpressionSymbolInliner(commonGroupingSymbolMapping), combineConjuncts(conjuncts.get(true))));

            // All other conjuncts, if any, will be in the filter node.
            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(conjuncts.get(false)));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Expression> context)
        {
            checkState(!DependencyExtractor.extractUnique(context.get()).contains(node.getMarkerSymbol()), "predicate depends on marker symbol");
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Expression> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Expression> context)
        {
            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                Expression sourcePredicate = ExpressionTreeRewriter.rewriteWith(new ExpressionSymbolInliner(node.sourceSymbolMap(i)), context.get());
                PlanNode source = node.getSources().get(i);
                PlanNode rewrittenSource = context.rewrite(source, sourcePredicate);
                if (rewrittenSource != source) {
                    modified = true;
                }
                builder.add(rewrittenSource);
            }

            if (modified) {
                return new UnionNode(node.getId(), builder.build(), node.getSymbolMapping(), node.getOutputSymbols());
            }

            return node;
        }

        @Deprecated
        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Expression> context)
        {
            return context.rewrite(node.getSource(), combineConjuncts(node.getPredicate(), context.get()));
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();

            // See if we can rewrite outer joins in terms of a plain inner join
            node = tryNormalizeToOuterToInnerJoin(node, inheritedPredicate);

            Expression leftEffectivePredicate = EffectivePredicateExtractor.extract(node.getLeft(), symbolAllocator.getTypes());
            Expression rightEffectivePredicate = EffectivePredicateExtractor.extract(node.getRight(), symbolAllocator.getTypes());
            Expression joinPredicate = extractJoinPredicate(node);

            Expression leftPredicate;
            Expression rightPredicate;
            Expression postJoinPredicate;
            Expression newJoinPredicate;

            switch (node.getType()) {
                case INNER:
                    InnerJoinPushDownResult innerJoinPushDownResult = processInnerJoin(inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputSymbols());
                    leftPredicate = innerJoinPushDownResult.getLeftPredicate();
                    rightPredicate = innerJoinPushDownResult.getRightPredicate();
                    postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
                    break;
                case LEFT:
                    OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputSymbols());
                    leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
                    rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
                    postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = leftOuterJoinPushDownResult.getJoinPredicate();
                    break;
                case RIGHT:
                    OuterJoinPushDownResult rightOuterJoinPushDownResult = processLimitedOuterJoin(inheritedPredicate,
                            rightEffectivePredicate,
                            leftEffectivePredicate,
                            joinPredicate,
                            node.getRight().getOutputSymbols());
                    leftPredicate = rightOuterJoinPushDownResult.getInnerJoinPredicate();
                    rightPredicate = rightOuterJoinPushDownResult.getOuterJoinPredicate();
                    postJoinPredicate = rightOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = rightOuterJoinPushDownResult.getJoinPredicate();
                    break;
                case FULL:
                    leftPredicate = BooleanLiteral.TRUE_LITERAL;
                    rightPredicate = BooleanLiteral.TRUE_LITERAL;
                    postJoinPredicate = inheritedPredicate;
                    newJoinPredicate = joinPredicate;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);
            // TODO: find a better way to directly optimize FALSE LITERAL in join predicate
            if (newJoinPredicate.equals(BooleanLiteral.FALSE_LITERAL)) {
                newJoinPredicate = new ComparisonExpression(ComparisonExpressionType.EQUAL, new LongLiteral("0"), new LongLiteral("1"));
            }

            PlanNode leftSource = context.rewrite(node.getLeft(), leftPredicate);
            PlanNode rightSource = context.rewrite(node.getRight(), rightPredicate);

            PlanNode output = node;
            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !expressionEquivalence.areExpressionsEquivalent(session, newJoinPredicate, joinPredicate, types)) {
                // Create identity projections for all existing symbols
                Assignments.Builder leftProjections = Assignments.builder();
                leftProjections.putAll(node.getLeft()
                        .getOutputSymbols().stream()
                        .collect(Collectors.toMap(key -> key, Symbol::toSymbolReference)));

                Assignments.Builder rightProjections = Assignments.builder();
                rightProjections.putAll(node.getRight()
                        .getOutputSymbols().stream()
                        .collect(Collectors.toMap(key -> key, Symbol::toSymbolReference)));

                // Create new projections for the new join clauses
                ImmutableList.Builder<JoinNode.EquiJoinClause> joinConditionBuilder = ImmutableList.builder();
                ImmutableList.Builder<Expression> joinFilterBuilder = ImmutableList.builder();
                for (Expression conjunct : extractConjuncts(newJoinPredicate)) {
                    if (joinEqualityExpression(node.getLeft().getOutputSymbols()).test(conjunct)) {
                        ComparisonExpression equality = (ComparisonExpression) conjunct;

                        boolean alignedComparison = Iterables.all(DependencyExtractor.extractUnique(equality.getLeft()), in(node.getLeft().getOutputSymbols()));
                        Expression leftExpression = (alignedComparison) ? equality.getLeft() : equality.getRight();
                        Expression rightExpression = (alignedComparison) ? equality.getRight() : equality.getLeft();

                        Symbol leftSymbol = symbolForExpression(leftExpression);
                        if (!node.getLeft().getOutputSymbols().contains(leftSymbol)) {
                            leftProjections.put(leftSymbol, leftExpression);
                        }

                        Symbol rightSymbol = symbolForExpression(rightExpression);
                        if (!node.getRight().getOutputSymbols().contains(rightSymbol)) {
                            rightProjections.put(rightSymbol, rightExpression);
                        }

                        joinConditionBuilder.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                    }
                    else {
                        joinFilterBuilder.add(conjunct);
                    }
                }

                Optional<Expression> newJoinFilter = Optional.of(combineConjuncts(joinFilterBuilder.build()));
                if (newJoinFilter.get() == BooleanLiteral.TRUE_LITERAL) {
                    newJoinFilter = Optional.empty();
                }

                leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());

                output = createJoinNodeWithExpectedOutputs(
                        node.getOutputSymbols(), idAllocator,
                        node.getType(),
                        leftSource,
                        rightSource,
                        newJoinFilter,
                        joinConditionBuilder.build(),
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType());
            }
            if (!postJoinPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                output = new FilterNode(idAllocator.getNextId(), output, postJoinPredicate);
            }
            return output;
        }

        private Symbol symbolForExpression(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return Symbol.from(expression);
            }

            return symbolAllocator.newSymbol(expression, extractType(expression));
        }

        private static PlanNode createJoinNodeWithExpectedOutputs(
                List<Symbol> expectedOutputs,
                PlanNodeIdAllocator idAllocator,
                JoinNode.Type type,
                PlanNode left,
                PlanNode right,
                Optional<Expression> filter,
                List<JoinNode.EquiJoinClause> conditions,
                Optional<Symbol> leftHashSymbol,
                Optional<Symbol> rightHashSymbol,
                Optional<JoinNode.DistributionType> distributionType)
        {
            // TODO: this should be removed once join nodes with output column pruning is supported for cross join
            if (conditions.isEmpty() && !filter.isPresent()) {
                PlanNode output = new JoinNode(
                        idAllocator.getNextId(),
                        type,
                        left,
                        right,
                        conditions,
                        ImmutableList.<Symbol>builder()
                                .addAll(left.getOutputSymbols())
                                .addAll(right.getOutputSymbols())
                                .build(),
                        filter,
                        leftHashSymbol,
                        rightHashSymbol,
                        distributionType);

                if (!output.getOutputSymbols().equals(expectedOutputs)) {
                    // Introduce a projection to constrain the outputs to what was originally expected
                    // Some nodes are sensitive to what's produced (e.g., DistinctLimit node)
                    output = new ProjectNode(
                            idAllocator.getNextId(),
                            output,
                            Assignments.identity(expectedOutputs));
                }
                return output;
            }
            else {
                return new JoinNode(idAllocator.getNextId(), type, left, right, conditions, expectedOutputs, filter, leftHashSymbol, rightHashSymbol, distributionType);
            }
        }

        private static OuterJoinPushDownResult processLimitedOuterJoin(Expression inheritedPredicate, Expression outerEffectivePredicate, Expression innerEffectivePredicate, Expression joinPredicate, Collection<Symbol> outerSymbols)
        {
            checkArgument(Iterables.all(DependencyExtractor.extractUnique(outerEffectivePredicate), in(outerSymbols)), "outerEffectivePredicate must only contain symbols from outerSymbols");
            checkArgument(Iterables.all(DependencyExtractor.extractUnique(innerEffectivePredicate), not(in(outerSymbols))), "innerEffectivePredicate must not contain symbols from outerSymbols");

            ImmutableList.Builder<Expression> outerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> innerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> postJoinConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            postJoinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(DeterminismEvaluator::isDeterministic)));
            inheritedPredicate = stripNonDeterministicConjuncts(inheritedPredicate);

            outerEffectivePredicate = stripNonDeterministicConjuncts(outerEffectivePredicate);
            innerEffectivePredicate = stripNonDeterministicConjuncts(innerEffectivePredicate);
            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(DeterminismEvaluator::isDeterministic)));
            joinPredicate = stripNonDeterministicConjuncts(joinPredicate);

            // Generate equality inferences
            EqualityInference inheritedInference = createEqualityInference(inheritedPredicate);
            EqualityInference outerInference = createEqualityInference(inheritedPredicate, outerEffectivePredicate);

            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(in(outerSymbols));
            Expression outerOnlyInheritedEqualities = combineConjuncts(equalityPartition.getScopeEqualities());
            EqualityInference potentialNullSymbolInference = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, innerEffectivePredicate, joinPredicate);

            // See if we can push inherited predicates down
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression outerRewritten = outerInference.rewriteExpression(conjunct, in(outerSymbols));
                if (outerRewritten != null) {
                    outerPushdownConjuncts.add(outerRewritten);

                    // A conjunct can only be pushed down into an inner side if it can be rewritten in terms of the outer side
                    Expression innerRewritten = potentialNullSymbolInference.rewriteExpression(outerRewritten, not(in(outerSymbols)));
                    if (innerRewritten != null) {
                        innerPushdownConjuncts.add(innerRewritten);
                    }
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }
            // Add the equalities from the inferences back in
            outerPushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            // See if we can push down any outer effective predicates to the inner side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(outerEffectivePredicate)) {
                Expression rewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerSymbols)));
                if (rewritten != null) {
                    innerPushdownConjuncts.add(rewritten);
                }
            }

            // See if we can push down join predicates to the inner side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(joinPredicate)) {
                Expression innerRewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerSymbols)));
                if (innerRewritten != null) {
                    innerPushdownConjuncts.add(innerRewritten);
                }
                else {
                    joinConjuncts.add(conjunct);
                }
            }

            // Push outer and join equalities into the inner side. For example:
            // SELECT * FROM nation LEFT OUTER JOIN region ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'

            EqualityInference potentialNullSymbolInferenceWithoutInnerInferred = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, joinPredicate);
            innerPushdownConjuncts.addAll(potentialNullSymbolInferenceWithoutInnerInferred.generateEqualitiesPartitionedBy(not(in(outerSymbols))).getScopeEqualities());

            // TODO: we can further improve simplifying the equalities by considering other relationships from the outer side
            EqualityInference.EqualityPartition joinEqualityPartition = createEqualityInference(joinPredicate).generateEqualitiesPartitionedBy(not(in(outerSymbols)));
            innerPushdownConjuncts.addAll(joinEqualityPartition.getScopeEqualities());
            joinConjuncts.addAll(joinEqualityPartition.getScopeComplementEqualities())
                    .addAll(joinEqualityPartition.getScopeStraddlingEqualities());

            return new OuterJoinPushDownResult(combineConjuncts(outerPushdownConjuncts.build()),
                    combineConjuncts(innerPushdownConjuncts.build()),
                    combineConjuncts(joinConjuncts.build()),
                    combineConjuncts(postJoinConjuncts.build()));
        }

        private static class OuterJoinPushDownResult
        {
            private final Expression outerJoinPredicate;
            private final Expression innerJoinPredicate;
            private final Expression joinPredicate;
            private final Expression postJoinPredicate;

            private OuterJoinPushDownResult(Expression outerJoinPredicate, Expression innerJoinPredicate, Expression joinPredicate, Expression postJoinPredicate)
            {
                this.outerJoinPredicate = outerJoinPredicate;
                this.innerJoinPredicate = innerJoinPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private Expression getOuterJoinPredicate()
            {
                return outerJoinPredicate;
            }

            private Expression getInnerJoinPredicate()
            {
                return innerJoinPredicate;
            }

            public Expression getJoinPredicate()
            {
                return joinPredicate;
            }

            private Expression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private static InnerJoinPushDownResult processInnerJoin(Expression inheritedPredicate, Expression leftEffectivePredicate, Expression rightEffectivePredicate, Expression joinPredicate, Collection<Symbol> leftSymbols)
        {
            checkArgument(Iterables.all(DependencyExtractor.extractUnique(leftEffectivePredicate), in(leftSymbols)), "leftEffectivePredicate must only contain symbols from leftSymbols");
            checkArgument(Iterables.all(DependencyExtractor.extractUnique(rightEffectivePredicate), not(in(leftSymbols))), "rightEffectivePredicate must not contain symbols from leftSymbols");

            ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            joinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(DeterminismEvaluator::isDeterministic)));
            inheritedPredicate = stripNonDeterministicConjuncts(inheritedPredicate);

            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(DeterminismEvaluator::isDeterministic)));
            joinPredicate = stripNonDeterministicConjuncts(joinPredicate);

            leftEffectivePredicate = stripNonDeterministicConjuncts(leftEffectivePredicate);
            rightEffectivePredicate = stripNonDeterministicConjuncts(rightEffectivePredicate);

            // Generate equality inferences
            EqualityInference allInference = createEqualityInference(inheritedPredicate, leftEffectivePredicate, rightEffectivePredicate, joinPredicate);
            EqualityInference allInferenceWithoutLeftInferred = createEqualityInference(inheritedPredicate, rightEffectivePredicate, joinPredicate);
            EqualityInference allInferenceWithoutRightInferred = createEqualityInference(inheritedPredicate, leftEffectivePredicate, joinPredicate);

            // Sort through conjuncts in inheritedPredicate that were not used for inference
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression leftRewrittenConjunct = allInference.rewriteExpression(conjunct, in(leftSymbols));
                if (leftRewrittenConjunct != null) {
                    leftPushDownConjuncts.add(leftRewrittenConjunct);
                }

                Expression rightRewrittenConjunct = allInference.rewriteExpression(conjunct, not(in(leftSymbols)));
                if (rightRewrittenConjunct != null) {
                    rightPushDownConjuncts.add(rightRewrittenConjunct);
                }

                // Drop predicate after join only if unable to push down to either side
                if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                    joinConjuncts.add(conjunct);
                }
            }

            // See if we can push the right effective predicate to the left side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(rightEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, in(leftSymbols));
                if (rewritten != null) {
                    leftPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push the left effective predicate to the right side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(leftEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, not(in(leftSymbols)));
                if (rewritten != null) {
                    rightPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push any parts of the join predicates to either side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(joinPredicate)) {
                Expression leftRewritten = allInference.rewriteExpression(conjunct, in(leftSymbols));
                if (leftRewritten != null) {
                    leftPushDownConjuncts.add(leftRewritten);
                }

                Expression rightRewritten = allInference.rewriteExpression(conjunct, not(in(leftSymbols)));
                if (rightRewritten != null) {
                    rightPushDownConjuncts.add(rightRewritten);
                }

                if (leftRewritten == null && rightRewritten == null) {
                    joinConjuncts.add(conjunct);
                }
            }

            // Add equalities from the inference back in
            leftPushDownConjuncts.addAll(allInferenceWithoutLeftInferred.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeEqualities());
            rightPushDownConjuncts.addAll(allInferenceWithoutRightInferred.generateEqualitiesPartitionedBy(not(in(leftSymbols))).getScopeEqualities());
            joinConjuncts.addAll(allInference.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as part of the join predicate

            return new InnerJoinPushDownResult(combineConjuncts(leftPushDownConjuncts.build()), combineConjuncts(rightPushDownConjuncts.build()), combineConjuncts(joinConjuncts.build()), BooleanLiteral.TRUE_LITERAL);
        }

        private static class InnerJoinPushDownResult
        {
            private final Expression leftPredicate;
            private final Expression rightPredicate;
            private final Expression joinPredicate;
            private final Expression postJoinPredicate;

            private InnerJoinPushDownResult(Expression leftPredicate, Expression rightPredicate, Expression joinPredicate, Expression postJoinPredicate)
            {
                this.leftPredicate = leftPredicate;
                this.rightPredicate = rightPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private Expression getLeftPredicate()
            {
                return leftPredicate;
            }

            private Expression getRightPredicate()
            {
                return rightPredicate;
            }

            private Expression getJoinPredicate()
            {
                return joinPredicate;
            }

            private Expression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private static Expression extractJoinPredicate(JoinNode joinNode)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
                builder.add(equalsExpression(equiJoinClause.getLeft(), equiJoinClause.getRight()));
            }
            joinNode.getFilter().ifPresent(builder::add);
            return combineConjuncts(builder.build());
        }

        private static Expression equalsExpression(Symbol symbol1, Symbol symbol2)
        {
            return new ComparisonExpression(ComparisonExpressionType.EQUAL,
                    symbol1.toSymbolReference(),
                    symbol2.toSymbolReference());
        }

        private Type extractType(Expression expression)
        {
            return getExpressionTypes(session, metadata, sqlParser, symbolAllocator.getTypes(), expression, emptyList() /* parameters have already been replaced */).get(expression);
        }

        private JoinNode tryNormalizeToOuterToInnerJoin(JoinNode node, Expression inheritedPredicate)
        {
            checkArgument(EnumSet.of(INNER, RIGHT, LEFT, FULL).contains(node.getType()), "Unsupported join type: %s", node.getType());

            if (node.getType() == JoinNode.Type.INNER) {
                return node;
            }

            if (node.getType() == JoinNode.Type.FULL) {
                boolean canConvertToLeftJoin = canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate);
                boolean canConvertToRightJoin = canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate);
                if (!canConvertToLeftJoin && !canConvertToRightJoin) {
                    return node;
                }
                if (canConvertToLeftJoin && canConvertToRightJoin) {
                    return new JoinNode(node.getId(), INNER, node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(), node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType());
                }
                else {
                    return new JoinNode(node.getId(), canConvertToLeftJoin ? LEFT : RIGHT,
                            node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(), node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType());
                }
            }

            if (node.getType() == JoinNode.Type.LEFT && !canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate) ||
                    node.getType() == JoinNode.Type.RIGHT && !canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate)) {
                return node;
            }
            return new JoinNode(node.getId(), JoinNode.Type.INNER, node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(), node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType());
        }

        private boolean canConvertOuterToInner(List<Symbol> innerSymbolsForOuterJoin, Expression inheritedPredicate)
        {
            Set<Symbol> innerSymbols = ImmutableSet.copyOf(innerSymbolsForOuterJoin);
            for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
                if (DeterminismEvaluator.isDeterministic(conjunct)) {
                    // Ignore a conjunct for this test if we can not deterministically get responses from it
                    Object response = nullInputEvaluator(innerSymbols, conjunct);
                    if (response == null || response instanceof NullLiteral || Boolean.FALSE.equals(response)) {
                        // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for the inner side symbols of an outer join
                        // then this conjunct removes all effects of the outer join, and effectively turns this into an equivalent of an inner join.
                        // So, let's just rewrite this join as an INNER join
                        return true;
                    }
                }
            }
            return false;
        }

        // Temporary implementation for joins because the SimplifyExpressions optimizers can not run properly on join clauses
        private Expression simplifyExpression(Expression expression)
        {
            IdentityLinkedHashMap<Expression, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    symbolAllocator.getTypes(),
                    expression,
                    emptyList() /* parameters have already been replaced */);
            ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
            return LiteralInterpreter.toExpression(optimizer.optimize(NoOpSymbolResolver.INSTANCE), expressionTypes.get(expression));
        }

        /**
         * Evaluates an expression's response to binding the specified input symbols to NULL
         */
        private Object nullInputEvaluator(final Collection<Symbol> nullSymbols, Expression expression)
        {
            IdentityLinkedHashMap<Expression, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    symbolAllocator.getTypes(),
                    expression,
                    emptyList() /* parameters have already been replaced */);
            return ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes)
                    .optimize(symbol -> nullSymbols.contains(symbol) ? null : symbol.toSymbolReference());
        }

        private static Predicate<Expression> joinEqualityExpression(final Collection<Symbol> leftSymbols)
        {
            return expression -> {
                // At this point in time, our join predicates need to be deterministic
                if (isDeterministic(expression) && expression instanceof ComparisonExpression) {
                    ComparisonExpression comparison = (ComparisonExpression) expression;
                    if (comparison.getType() == ComparisonExpressionType.EQUAL) {
                        Set<Symbol> symbols1 = DependencyExtractor.extractUnique(comparison.getLeft());
                        Set<Symbol> symbols2 = DependencyExtractor.extractUnique(comparison.getRight());
                        if (symbols1.isEmpty() || symbols2.isEmpty()) {
                            return false;
                        }
                        return (Iterables.all(symbols1, in(leftSymbols)) && Iterables.all(symbols2, not(in(leftSymbols)))) ||
                                (Iterables.all(symbols2, in(leftSymbols)) && Iterables.all(symbols1, not(in(leftSymbols))));
                    }
                }
                return false;
            };
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();

            Expression sourceEffectivePredicate = EffectivePredicateExtractor.extract(node.getSource(), symbolAllocator.getTypes());

            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> filteringSourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // TODO: see if there are predicates that can be inferred from the semi join output

            // Push inherited and source predicates to filtering source via a contrived join predicate (but needs to avoid touching NULL values in the filtering source)
            Expression joinPredicate = equalsExpression(node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol());
            EqualityInference joinInference = createEqualityInference(inheritedPredicate, sourceEffectivePredicate, joinPredicate);
            for (Expression conjunct : Iterables.concat(EqualityInference.nonInferrableConjuncts(inheritedPredicate), EqualityInference.nonInferrableConjuncts(sourceEffectivePredicate))) {
                Expression rewrittenConjunct = joinInference.rewriteExpression(conjunct, equalTo(node.getFilteringSourceJoinSymbol()));
                if (rewrittenConjunct != null && DeterminismEvaluator.isDeterministic(rewrittenConjunct)) {
                    // Alter conjunct to include an OR filteringSourceJoinSymbol IS NULL disjunct
                    Expression rewrittenConjunctOrNull = expressionOrNullSymbols(Predicate.isEqual(node.getFilteringSourceJoinSymbol())).apply(rewrittenConjunct);
                    filteringSourceConjuncts.add(rewrittenConjunctOrNull);
                }
            }
            EqualityInference.EqualityPartition joinInferenceEqualityPartition = joinInference.generateEqualitiesPartitionedBy(equalTo(node.getFilteringSourceJoinSymbol()));

            filteringSourceConjuncts.addAll(joinInferenceEqualityPartition.getScopeEqualities().stream()
                    .map(expressionOrNullSymbols(Predicate.isEqual(node.getFilteringSourceJoinSymbol())))
                    .collect(Collectors.toList()));

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            EqualityInference inheritedInference = createEqualityInference(inheritedPredicate);
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = inheritedInference.rewriteExpression(conjunct, in(node.getSource().getOutputSymbols()));
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }

            // Add the inherited equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(in(node.getSource().getOutputSymbols()));
            sourceConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(sourceConjuncts));
            PlanNode rewrittenFilteringSource = context.rewrite(node.getFilteringSource(), combineConjuncts(filteringSourceConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource()) {
                output = new SemiJoinNode(node.getId(), rewrittenSource, rewrittenFilteringSource, node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput(), node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol(), node.getDistributionType());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postJoinConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Expression> context)
        {
            if (node.getGroupingKeys().isEmpty()) {
                // cannot push predicates down through aggregations without any grouping columns
                return visitPlan(node, context);
            }

            Expression inheritedPredicate = context.get();

            EqualityInference equalityInference = createEqualityInference(inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postAggregationConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            postAggregationConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(DeterminismEvaluator::isDeterministic))));
            inheritedPredicate = stripNonDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(node.getGroupingKeys()));
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postAggregationConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(node.getGroupingKeys()));
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new AggregationNode(node.getId(),
                        rewrittenSource,
                        node.getAggregations(),
                        node.getFunctions(),
                        node.getMasks(),
                        node.getGroupingSets(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol());
            }
            if (!postAggregationConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postAggregationConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();

            EqualityInference equalityInference = createEqualityInference(inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postUnnestConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            postUnnestConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(DeterminismEvaluator::isDeterministic))));
            inheritedPredicate = stripNonDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(node.getReplicateSymbols()));
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postUnnestConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(node.getReplicateSymbols()));
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postUnnestConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postUnnestConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new UnnestNode(node.getId(), rewrittenSource, node.getReplicateSymbols(), node.getUnnestSymbols(), node.getOrdinalitySymbol());
            }
            if (!postUnnestConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postUnnestConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode visitSample(SampleNode node, RewriteContext<Expression> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Expression> context)
        {
            Expression predicate = simplifyExpression(context.get());

            if (!BooleanLiteral.TRUE_LITERAL.equals(predicate)) {
                return new FilterNode(idAllocator.getNextId(), node, predicate);
            }

            return node;
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Expression> context)
        {
            Set<Symbol> predicateSymbols = DependencyExtractor.extractUnique(context.get());
            checkState(!predicateSymbols.contains(node.getIdColumn()), "UniqueId in predicate is not yet supported");
            return context.defaultRewrite(node, context.get());
        }
    }
}

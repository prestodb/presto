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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.EffectivePredicateExtractor;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.ExpressionDeterminismEvaluator;
import com.facebook.presto.sql.planner.ExpressionDomainTranslator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.ExpressionVariableInliner;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.util.AstUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

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
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.filterDeterministicConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.facebook.presto.sql.planner.ExpressionDeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.ExpressionVariableInliner.inlineVariables;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.filter;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PredicatePushDown
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final LiteralEncoder literalEncoder;
    private final EffectivePredicateExtractor effectivePredicateExtractor;
    private final SqlParser sqlParser;

    public PredicatePushDown(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        this.effectivePredicateExtractor = new EffectivePredicateExtractor(new ExpressionDomainTranslator(literalEncoder));
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(
                new Rewriter(variableAllocator, idAllocator, metadata, literalEncoder, effectivePredicateExtractor, sqlParser, session, types),
                plan,
                TRUE_LITERAL);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Expression>
    {
        private final PlanVariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final LiteralEncoder literalEncoder;
        private final EffectivePredicateExtractor effectivePredicateExtractor;
        private final SqlParser sqlParser;
        private final Session session;
        private final TypeProvider types;
        private final ExpressionEquivalence expressionEquivalence;

        private Rewriter(
                PlanVariableAllocator variableAllocator,
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                LiteralEncoder literalEncoder,
                EffectivePredicateExtractor effectivePredicateExtractor,
                SqlParser sqlParser,
                Session session,
                TypeProvider types)
        {
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
            this.effectivePredicateExtractor = requireNonNull(effectivePredicateExtractor, "effectivePredicateExtractor is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.expressionEquivalence = new ExpressionEquivalence(metadata, sqlParser);
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Expression> context)
        {
            PlanNode rewrittenNode = context.defaultRewrite(node, TRUE_LITERAL);
            if (!context.get().equals(TRUE_LITERAL)) {
                // Drop in a FilterNode b/c we cannot push our predicate down any further
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, castToRowExpression(context.get()));
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Expression> context)
        {
            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                Map<VariableReferenceExpression, SymbolReference> outputsToInputs = new HashMap<>();
                for (int index = 0; index < node.getInputs().get(i).size(); index++) {
                    outputsToInputs.put(
                            node.getOutputVariables().get(index),
                            new SymbolReference(node.getInputs().get(i).get(index).getName()));
                }

                Expression sourcePredicate = inlineVariables(outputsToInputs, context.get(), types);
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
                        node.getInputs(),
                        node.getOrderingScheme());
            }

            return node;
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Expression> context)
        {
            // TODO: This could be broader. We can push down conjucts if they are constant for all rows in a window partition.
            // The simplest way to guarantee this is if the conjucts are deterministic functions of the partitioning symbols.
            // This can leave out cases where they're both functions of some set of common expressions and the partitioning
            // function is injective, but that's a rare case. The majority of window nodes are expected to be partitioned by
            // pre-projected symbols.
            Predicate<Expression> isSupported = conjunct ->
                    ExpressionDeterminismEvaluator.isDeterministic(conjunct) &&
                    VariablesExtractor.extractUnique(conjunct, types).stream()
                            .allMatch(node.getPartitionBy()::contains);

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(isSupported));

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(conjuncts.get(true)));

            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, castToRowExpression(combineConjuncts(conjuncts.get(false))));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Expression> context)
        {
            Set<VariableReferenceExpression> deterministicVariables = node.getAssignments().entrySet().stream()
                    .filter(entry -> ExpressionDeterminismEvaluator.isDeterministic(castToExpression(entry.getValue())))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Predicate<Expression> deterministic = conjunct -> deterministicVariables.containsAll(VariablesExtractor.extractUnique(conjunct, types));

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(deterministic));

            // Push down conjuncts from the inherited predicate that only depend on deterministic assignments with
            // certain limitations.
            List<Expression> deterministicConjuncts = conjuncts.get(true);

            // We partition the expressions in the deterministicConjuncts into two lists, and only inline the
            // expressions that are in the inlining targets list.
            Map<Boolean, List<Expression>> inlineConjuncts = deterministicConjuncts.stream()
                    .collect(Collectors.partitioningBy(expression -> isInliningCandidate(expression, node)));

            List<Expression> inlinedDeterministicConjuncts = inlineConjuncts.get(true).stream()
                    .map(entry -> ExpressionVariableInliner.inlineVariables(Maps.transformValues(node.getAssignments().getMap(), OriginalExpressionUtils::castToExpression), entry, types))
                    .collect(Collectors.toList());

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(inlinedDeterministicConjuncts));

            // All deterministic conjuncts that contains non-inlining targets, and non-deterministic conjuncts,
            // if any, will be in the filter node.
            List<Expression> nonInliningConjuncts = inlineConjuncts.get(false);
            nonInliningConjuncts.addAll(conjuncts.get(false));

            if (!nonInliningConjuncts.isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, castToRowExpression(combineConjuncts(nonInliningConjuncts)));
            }

            return rewrittenNode;
        }

        private boolean isInliningCandidate(Expression expression, ProjectNode node)
        {
            // TryExpressions should not be pushed down. However they are now being handled as lambda
            // passed to a FunctionCall now and should not affect predicate push down. So we want to make
            // sure the conjuncts are not TryExpressions.
            verify(AstUtils.preOrder(expression).noneMatch(TryExpression.class::isInstance));

            // candidate symbols for inlining are
            //   1. references to simple constants
            //   2. references to complex expressions that appear only once
            // which come from the node, as opposed to an enclosing scope.
            Set<VariableReferenceExpression> childOutputSet = ImmutableSet.copyOf(node.getOutputVariables());
            Map<VariableReferenceExpression, Long> dependencies = VariablesExtractor.extractAll(expression, types).stream()
                    .filter(childOutputSet::contains)
                    .collect(Collectors.groupingBy(identity(), Collectors.counting()));

            return dependencies.entrySet().stream()
                    .allMatch(entry -> entry.getValue() == 1 || castToExpression(node.getAssignments().get(entry.getKey())) instanceof Literal);
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Expression> context)
        {
            Map<VariableReferenceExpression, SymbolReference> commonGroupingVariableMapping = node.getGroupingColumns().entrySet().stream()
                    .filter(entry -> node.getCommonGroupingColumns().contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> new SymbolReference(entry.getValue().getName())));

            Predicate<Expression> pushdownEligiblePredicate = conjunct -> VariablesExtractor.extractUnique(conjunct, types).stream()
                    .allMatch(commonGroupingVariableMapping.keySet()::contains);

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(pushdownEligiblePredicate));

            // Push down conjuncts from the inherited predicate that apply to common grouping symbols
            PlanNode rewrittenNode = context.defaultRewrite(node, inlineVariables(commonGroupingVariableMapping, combineConjuncts(conjuncts.get(true)), types));

            // All other conjuncts, if any, will be in the filter node.
            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, castToRowExpression(combineConjuncts(conjuncts.get(false))));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Expression> context)
        {
            Set<VariableReferenceExpression> pushDownableVariables = ImmutableSet.copyOf(node.getDistinctVariables());
            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream()
                    .collect(Collectors.partitioningBy(conjunct -> pushDownableVariables.containsAll(VariablesExtractor.extractUnique(conjunct, types))));

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(conjuncts.get(true)));

            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, castToRowExpression(combineConjuncts(conjuncts.get(false))));
            }
            return rewrittenNode;
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
                Expression sourcePredicate = inlineVariables(Maps.transformValues(node.sourceVariableMap(i), variable -> new SymbolReference(variable.getName())), context.get(), types);
                PlanNode source = node.getSources().get(i);
                PlanNode rewrittenSource = context.rewrite(source, sourcePredicate);
                if (rewrittenSource != source) {
                    modified = true;
                }
                builder.add(rewrittenSource);
            }

            if (modified) {
                return new UnionNode(node.getId(), builder.build(), node.getVariableMapping());
            }

            return node;
        }

        @Deprecated
        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Expression> context)
        {
            PlanNode rewrittenPlan = context.rewrite(node.getSource(), combineConjuncts(castToExpression(node.getPredicate()), context.get()));
            if (!(rewrittenPlan instanceof FilterNode)) {
                return rewrittenPlan;
            }

            FilterNode rewrittenFilterNode = (FilterNode) rewrittenPlan;
            if (!areExpressionsEquivalent(castToExpression(rewrittenFilterNode.getPredicate()), castToExpression(node.getPredicate()))
                    || node.getSource() != rewrittenFilterNode.getSource()) {
                return rewrittenPlan;
            }

            return node;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();

            // See if we can rewrite outer joins in terms of a plain inner join
            node = tryNormalizeToOuterToInnerJoin(node, inheritedPredicate);

            Expression leftEffectivePredicate = effectivePredicateExtractor.extract(node.getLeft(), types);
            Expression rightEffectivePredicate = effectivePredicateExtractor.extract(node.getRight(), types);
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
                            node.getLeft().getOutputVariables());
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
                            node.getLeft().getOutputVariables());
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
                            node.getRight().getOutputVariables());
                    leftPredicate = rightOuterJoinPushDownResult.getInnerJoinPredicate();
                    rightPredicate = rightOuterJoinPushDownResult.getOuterJoinPredicate();
                    postJoinPredicate = rightOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = rightOuterJoinPushDownResult.getJoinPredicate();
                    break;
                case FULL:
                    leftPredicate = TRUE_LITERAL;
                    rightPredicate = TRUE_LITERAL;
                    postJoinPredicate = inheritedPredicate;
                    newJoinPredicate = joinPredicate;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);
            // TODO: find a better way to directly optimize FALSE LITERAL in join predicate
            if (newJoinPredicate.equals(BooleanLiteral.FALSE_LITERAL)) {
                newJoinPredicate = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new LongLiteral("0"), new LongLiteral("1"));
            }

            PlanNode leftSource = context.rewrite(node.getLeft(), leftPredicate);
            PlanNode rightSource = context.rewrite(node.getRight(), rightPredicate);

            PlanNode output = node;

            // Create identity projections for all existing symbols
            Assignments.Builder leftProjections = Assignments.builder()
                    .putAll(identityAssignmentsAsSymbolReferences(node.getLeft().getOutputVariables()));

            Assignments.Builder rightProjections = Assignments.builder()
                    .putAll(identityAssignmentsAsSymbolReferences(node.getRight().getOutputVariables()));

            // Create new projections for the new join clauses
            List<JoinNode.EquiJoinClause> equiJoinClauses = new ArrayList<>();
            ImmutableList.Builder<Expression> joinFilterBuilder = ImmutableList.builder();
            for (Expression conjunct : extractConjuncts(newJoinPredicate)) {
                if (joinEqualityExpression(node.getLeft().getOutputVariables()).test(conjunct)) {
                    ComparisonExpression equality = (ComparisonExpression) conjunct;

                    boolean alignedComparison = Iterables.all(VariablesExtractor.extractUnique(equality.getLeft(), types), in(node.getLeft().getOutputVariables()));
                    Expression leftExpression = (alignedComparison) ? equality.getLeft() : equality.getRight();
                    Expression rightExpression = (alignedComparison) ? equality.getRight() : equality.getLeft();

                    VariableReferenceExpression leftVariable = variableForExpression(leftExpression);
                    if (!node.getLeft().getOutputVariables().contains(leftVariable)) {
                        leftProjections.put(leftVariable, castToRowExpression(leftExpression));
                    }

                    VariableReferenceExpression rightVariable = variableForExpression(rightExpression);
                    if (!node.getRight().getOutputVariables().contains(rightVariable)) {
                        rightProjections.put(rightVariable, castToRowExpression(rightExpression));
                    }

                    equiJoinClauses.add(new JoinNode.EquiJoinClause(leftVariable, rightVariable));
                }
                else {
                    joinFilterBuilder.add(conjunct);
                }
            }

            Optional<Expression> newJoinFilter = Optional.of(combineConjuncts(joinFilterBuilder.build()));
            if (newJoinFilter.get() == TRUE_LITERAL) {
                newJoinFilter = Optional.empty();
            }

            if (node.getType() == INNER && newJoinFilter.isPresent() && equiJoinClauses.isEmpty()) {
                // if we do not have any equi conjunct we do not pushdown non-equality condition into
                // inner join, so we plan execution as nested-loops-join followed by filter instead
                // hash join.
                // todo: remove the code when we have support for filter function in nested loop join
                postJoinPredicate = combineConjuncts(postJoinPredicate, newJoinFilter.get());
                newJoinFilter = Optional.empty();
            }

            boolean filtersEquivalent =
                    newJoinFilter.isPresent() == node.getFilter().isPresent() &&
                            (!newJoinFilter.isPresent() || areExpressionsEquivalent(newJoinFilter.get(), castToExpression(node.getFilter().get())));

            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !filtersEquivalent ||
                    !ImmutableSet.copyOf(equiJoinClauses).equals(ImmutableSet.copyOf(node.getCriteria()))) {
                leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());

                // if the distribution type is already set, make sure that changes from PredicatePushDown
                // don't make the join node invalid.
                Optional<JoinNode.DistributionType> distributionType = node.getDistributionType();
                if (node.getDistributionType().isPresent()) {
                    if (node.getType().mustPartition()) {
                        distributionType = Optional.of(PARTITIONED);
                    }
                    if (node.getType().mustReplicate(equiJoinClauses)) {
                        distributionType = Optional.of(REPLICATED);
                    }
                }

                output = new JoinNode(
                        node.getId(),
                        node.getType(),
                        leftSource,
                        rightSource,
                        equiJoinClauses,
                        ImmutableList.<VariableReferenceExpression>builder()
                                .addAll(leftSource.getOutputVariables())
                                .addAll(rightSource.getOutputVariables())
                                .build(),
                        newJoinFilter.map(OriginalExpressionUtils::castToRowExpression),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable(),
                        distributionType);
            }

            if (!postJoinPredicate.equals(TRUE_LITERAL)) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(postJoinPredicate));
            }

            if (!node.getOutputVariables().equals(output.getOutputVariables())) {
                output = new ProjectNode(idAllocator.getNextId(), output, identityAssignmentsAsSymbolReferences(node.getOutputVariables()));
            }

            return output;
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();

            // See if we can rewrite left join in terms of a plain inner join
            if (node.getType() == SpatialJoinNode.Type.LEFT && canConvertOuterToInner(node.getRight().getOutputVariables(), inheritedPredicate)) {
                node = new SpatialJoinNode(
                        node.getId(),
                        SpatialJoinNode.Type.INNER,
                        node.getLeft(),
                        node.getRight(),
                        node.getOutputVariables(),
                        node.getFilter(),
                        node.getLeftPartitionVariable(),
                        node.getRightPartitionVariable(),
                        node.getKdbTree());
            }

            Expression leftEffectivePredicate = effectivePredicateExtractor.extract(node.getLeft(), types);
            Expression rightEffectivePredicate = effectivePredicateExtractor.extract(node.getRight(), types);
            Expression joinPredicate = castToExpression(node.getFilter());

            Expression leftPredicate;
            Expression rightPredicate;
            Expression postJoinPredicate;
            Expression newJoinPredicate;

            switch (node.getType()) {
                case INNER:
                    InnerJoinPushDownResult innerJoinPushDownResult = processInnerJoin(
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputVariables());
                    leftPredicate = innerJoinPushDownResult.getLeftPredicate();
                    rightPredicate = innerJoinPushDownResult.getRightPredicate();
                    postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
                    break;
                case LEFT:
                    OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputVariables());
                    leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
                    rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
                    postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = leftOuterJoinPushDownResult.getJoinPredicate();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);
            verify(!newJoinPredicate.equals(BooleanLiteral.FALSE_LITERAL), "Spatial join predicate is missing");

            PlanNode leftSource = context.rewrite(node.getLeft(), leftPredicate);
            PlanNode rightSource = context.rewrite(node.getRight(), rightPredicate);

            PlanNode output = node;
            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !areExpressionsEquivalent(newJoinPredicate, joinPredicate)) {
                // Create identity projections for all existing symbols
                Assignments.Builder leftProjections = Assignments.builder()
                        .putAll(identityAssignmentsAsSymbolReferences(node.getLeft().getOutputVariables()));

                Assignments.Builder rightProjections = Assignments.builder()
                        .putAll(identityAssignmentsAsSymbolReferences(node.getRight().getOutputVariables()));

                leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());

                output = new SpatialJoinNode(
                        node.getId(),
                        node.getType(),
                        leftSource,
                        rightSource,
                        node.getOutputVariables(),
                        castToRowExpression(newJoinPredicate),
                        node.getLeftPartitionVariable(),
                        node.getRightPartitionVariable(),
                        node.getKdbTree());
            }

            if (!postJoinPredicate.equals(TRUE_LITERAL)) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(postJoinPredicate));
            }

            return output;
        }

        private VariableReferenceExpression variableForExpression(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return new VariableReferenceExpression(((SymbolReference) expression).getName(), extractType(expression));
            }

            return variableAllocator.newVariable(expression, extractType(expression));
        }

        private OuterJoinPushDownResult processLimitedOuterJoin(Expression inheritedPredicate, Expression outerEffectivePredicate, Expression innerEffectivePredicate, Expression joinPredicate, Collection<VariableReferenceExpression> outerVariables)
        {
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(outerEffectivePredicate, types), in(outerVariables)), "outerEffectivePredicate must only contain variables from outerVariables");
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(innerEffectivePredicate, types), not(in(outerVariables))), "innerEffectivePredicate must not contain variables from outerVariables");

            ImmutableList.Builder<Expression> outerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> innerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> postJoinConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            postJoinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(ExpressionDeterminismEvaluator::isDeterministic)));
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            outerEffectivePredicate = filterDeterministicConjuncts(outerEffectivePredicate);
            innerEffectivePredicate = filterDeterministicConjuncts(innerEffectivePredicate);
            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(ExpressionDeterminismEvaluator::isDeterministic)));
            joinPredicate = filterDeterministicConjuncts(joinPredicate);

            // Generate equality inferences
            EqualityInference inheritedInference = createEqualityInference(inheritedPredicate);
            EqualityInference outerInference = createEqualityInference(inheritedPredicate, outerEffectivePredicate);

            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(in(outerVariables), types);
            Expression outerOnlyInheritedEqualities = combineConjuncts(equalityPartition.getScopeEqualities());
            EqualityInference potentialNullSymbolInference = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, innerEffectivePredicate, joinPredicate);

            // See if we can push inherited predicates down
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression outerRewritten = outerInference.rewriteExpression(conjunct, in(outerVariables), types);
                if (outerRewritten != null) {
                    outerPushdownConjuncts.add(outerRewritten);

                    // A conjunct can only be pushed down into an inner side if it can be rewritten in terms of the outer side
                    Expression innerRewritten = potentialNullSymbolInference.rewriteExpression(outerRewritten, not(in(outerVariables)), types);
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
                Expression rewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerVariables)), types);
                if (rewritten != null) {
                    innerPushdownConjuncts.add(rewritten);
                }
            }

            // See if we can push down join predicates to the inner side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(joinPredicate)) {
                Expression innerRewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerVariables)), types);
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
            innerPushdownConjuncts.addAll(potentialNullSymbolInferenceWithoutInnerInferred.generateEqualitiesPartitionedBy(not(in(outerVariables)), types).getScopeEqualities());

            // TODO: we can further improve simplifying the equalities by considering other relationships from the outer side
            EqualityInference.EqualityPartition joinEqualityPartition = createEqualityInference(joinPredicate).generateEqualitiesPartitionedBy(not(in(outerVariables)), types);
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

        private InnerJoinPushDownResult processInnerJoin(Expression inheritedPredicate, Expression leftEffectivePredicate, Expression rightEffectivePredicate, Expression joinPredicate, Collection<VariableReferenceExpression> leftVariables)
        {
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(leftEffectivePredicate, types), in(leftVariables)), "leftEffectivePredicate must only contain variables from leftVariables");
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(rightEffectivePredicate, types), not(in(leftVariables))), "rightEffectivePredicate must not contain variables from leftVariables");

            ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            joinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(ExpressionDeterminismEvaluator::isDeterministic)));
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(ExpressionDeterminismEvaluator::isDeterministic)));
            joinPredicate = filterDeterministicConjuncts(joinPredicate);

            leftEffectivePredicate = filterDeterministicConjuncts(leftEffectivePredicate);
            rightEffectivePredicate = filterDeterministicConjuncts(rightEffectivePredicate);

            // Generate equality inferences
            EqualityInference allInference = createEqualityInference(inheritedPredicate, leftEffectivePredicate, rightEffectivePredicate, joinPredicate);
            EqualityInference allInferenceWithoutLeftInferred = createEqualityInference(inheritedPredicate, rightEffectivePredicate, joinPredicate);
            EqualityInference allInferenceWithoutRightInferred = createEqualityInference(inheritedPredicate, leftEffectivePredicate, joinPredicate);

            // Sort through conjuncts in inheritedPredicate that were not used for inference
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression leftRewrittenConjunct = allInference.rewriteExpression(conjunct, in(leftVariables), types);
                if (leftRewrittenConjunct != null) {
                    leftPushDownConjuncts.add(leftRewrittenConjunct);
                }

                Expression rightRewrittenConjunct = allInference.rewriteExpression(conjunct, not(in(leftVariables)), types);
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
                Expression rewritten = allInference.rewriteExpression(conjunct, in(leftVariables), types);
                if (rewritten != null) {
                    leftPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push the left effective predicate to the right side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(leftEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, not(in(leftVariables)), types);
                if (rewritten != null) {
                    rightPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push any parts of the join predicates to either side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(joinPredicate)) {
                Expression leftRewritten = allInference.rewriteExpression(conjunct, in(leftVariables), types);
                if (leftRewritten != null) {
                    leftPushDownConjuncts.add(leftRewritten);
                }

                Expression rightRewritten = allInference.rewriteExpression(conjunct, not(in(leftVariables)), types);
                if (rightRewritten != null) {
                    rightPushDownConjuncts.add(rightRewritten);
                }

                if (leftRewritten == null && rightRewritten == null) {
                    joinConjuncts.add(conjunct);
                }
            }

            // Add equalities from the inference back in
            leftPushDownConjuncts.addAll(allInferenceWithoutLeftInferred.generateEqualitiesPartitionedBy(in(leftVariables), types).getScopeEqualities());
            rightPushDownConjuncts.addAll(allInferenceWithoutRightInferred.generateEqualitiesPartitionedBy(not(in(leftVariables)), types).getScopeEqualities());
            joinConjuncts.addAll(allInference.generateEqualitiesPartitionedBy(in(leftVariables)::apply, types).getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as part of the join predicate

            return new InnerJoinPushDownResult(combineConjuncts(leftPushDownConjuncts.build()), combineConjuncts(rightPushDownConjuncts.build()), combineConjuncts(joinConjuncts.build()), TRUE_LITERAL);
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
                builder.add(JoinNodeUtils.toExpression(equiJoinClause));
            }
            joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).ifPresent(builder::add);
            return combineConjuncts(builder.build());
        }

        private Type extractType(Expression expression)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, types, expression, emptyList(), /* parameters have already been replaced */WarningCollector.NOOP);
            return expressionTypes.get(NodeRef.of(expression));
        }

        private JoinNode tryNormalizeToOuterToInnerJoin(JoinNode node, Expression inheritedPredicate)
        {
            checkArgument(EnumSet.of(INNER, RIGHT, LEFT, FULL).contains(node.getType()), "Unsupported join type: %s", node.getType());

            if (node.getType() == JoinNode.Type.INNER) {
                return node;
            }

            if (node.getType() == JoinNode.Type.FULL) {
                boolean canConvertToLeftJoin = canConvertOuterToInner(node.getLeft().getOutputVariables(), inheritedPredicate);
                boolean canConvertToRightJoin = canConvertOuterToInner(node.getRight().getOutputVariables(), inheritedPredicate);
                if (!canConvertToLeftJoin && !canConvertToRightJoin) {
                    return node;
                }
                if (canConvertToLeftJoin && canConvertToRightJoin) {
                    return new JoinNode(node.getId(), INNER, node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputVariables(), node.getFilter(), node.getLeftHashVariable(), node.getRightHashVariable(), node.getDistributionType());
                }
                else {
                    return new JoinNode(node.getId(), canConvertToLeftJoin ? LEFT : RIGHT,
                            node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputVariables(), node.getFilter(), node.getLeftHashVariable(), node.getRightHashVariable(), node.getDistributionType());
                }
            }

            if (node.getType() == JoinNode.Type.LEFT && !canConvertOuterToInner(node.getRight().getOutputVariables(), inheritedPredicate) ||
                    node.getType() == JoinNode.Type.RIGHT && !canConvertOuterToInner(node.getLeft().getOutputVariables(), inheritedPredicate)) {
                return node;
            }
            return new JoinNode(node.getId(), JoinNode.Type.INNER, node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputVariables(), node.getFilter(), node.getLeftHashVariable(), node.getRightHashVariable(), node.getDistributionType());
        }

        private boolean canConvertOuterToInner(List<VariableReferenceExpression> innerVariablesForOuterJoin, Expression inheritedPredicate)
        {
            Set<VariableReferenceExpression> innerVariables = ImmutableSet.copyOf(innerVariablesForOuterJoin);
            for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
                if (ExpressionDeterminismEvaluator.isDeterministic(conjunct)) {
                    // Ignore a conjunct for this test if we can not deterministically get responses from it
                    Object response = nullInputEvaluator(innerVariables, conjunct);
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
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    types,
                    expression,
                    emptyList(), /* parameters have already been replaced */
                    WarningCollector.NOOP);
            ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
            return literalEncoder.toExpression(optimizer.optimize(NoOpSymbolResolver.INSTANCE), expressionTypes.get(NodeRef.of(expression)));
        }

        private boolean areExpressionsEquivalent(Expression leftExpression, Expression rightExpression)
        {
            return expressionEquivalence.areExpressionsEquivalent(session, leftExpression, rightExpression, types);
        }

        /**
         * Evaluates an expression's response to binding the specified input symbols to NULL
         */
        private Object nullInputEvaluator(final Collection<VariableReferenceExpression> nullVariables, Expression expression)
        {
            Set<String> nullVariableNames = nullVariables.stream()
                    .map(VariableReferenceExpression::getName)
                    .collect(toImmutableSet());
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    types,
                    expression,
                    emptyList(), /* parameters have already been replaced */
                    WarningCollector.NOOP);
            return ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes)
                    .optimize(symbol -> nullVariableNames.contains(symbol.getName()) ? null : symbol.toSymbolReference());
        }

        private Predicate<Expression> joinEqualityExpression(final Collection<VariableReferenceExpression> leftVariables)
        {
            return expression -> {
                // At this point in time, our join predicates need to be deterministic
                if (isDeterministic(expression) && expression instanceof ComparisonExpression) {
                    ComparisonExpression comparison = (ComparisonExpression) expression;
                    if (comparison.getOperator() == ComparisonExpression.Operator.EQUAL) {
                        Set<VariableReferenceExpression> variables1 = VariablesExtractor.extractUnique(comparison.getLeft(), types);
                        Set<VariableReferenceExpression> variables2 = VariablesExtractor.extractUnique(comparison.getRight(), types);
                        if (variables1.isEmpty() || variables2.isEmpty()) {
                            return false;
                        }
                        return (Iterables.all(variables1, in(leftVariables)) && Iterables.all(variables2, not(in(leftVariables)))) ||
                                (Iterables.all(variables2, in(leftVariables)) && Iterables.all(variables1, not(in(leftVariables))));
                    }
                }
                return false;
            };
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            if (!extractConjuncts(inheritedPredicate).contains(new SymbolReference(node.getSemiJoinOutput().getName()))) {
                return visitNonFilteringSemiJoin(node, context);
            }
            return visitFilteringSemiJoin(node, context);
        }

        private PlanNode visitNonFilteringSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // TODO: see if there are predicates that can be inferred from the semi join output

            PlanNode rewrittenFilteringSource = context.defaultRewrite(node.getFilteringSource(), TRUE_LITERAL);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            EqualityInference inheritedInference = createEqualityInference(inheritedPredicate);
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = inheritedInference.rewriteExpressionAllowNonDeterministic(conjunct, in(node.getSource().getOutputVariables()), types);
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }

            // Add the inherited equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(in(node.getSource()
                    .getOutputVariables())::apply, types);
            sourceConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(sourceConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource()) {
                output = new SemiJoinNode(node.getId(), rewrittenSource, rewrittenFilteringSource, node.getSourceJoinVariable(), node.getFilteringSourceJoinVariable(), node.getSemiJoinOutput(), node.getSourceHashVariable(), node.getFilteringSourceHashVariable(), node.getDistributionType());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(combineConjuncts(postJoinConjuncts)));
            }
            return output;
        }

        private PlanNode visitFilteringSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            Expression deterministicInheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);
            Expression sourceEffectivePredicate = filterDeterministicConjuncts(effectivePredicateExtractor.extract(node.getSource(), types));
            Expression filteringSourceEffectivePredicate = filterDeterministicConjuncts(effectivePredicateExtractor.extract(node.getFilteringSource(), types));
            Expression joinExpression = new ComparisonExpression(
                    ComparisonExpression.Operator.EQUAL,
                    new SymbolReference(node.getSourceJoinVariable().getName()),
                    new SymbolReference(node.getFilteringSourceJoinVariable().getName()));

            List<VariableReferenceExpression> sourceVariables = node.getSource().getOutputVariables();
            List<VariableReferenceExpression> filteringSourceVariables = node.getFilteringSource().getOutputVariables();

            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> filteringSourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // Generate equality inferences
            EqualityInference allInference = createEqualityInference(deterministicInheritedPredicate, sourceEffectivePredicate, filteringSourceEffectivePredicate, joinExpression);
            EqualityInference allInferenceWithoutSourceInferred = createEqualityInference(deterministicInheritedPredicate, filteringSourceEffectivePredicate, joinExpression);
            EqualityInference allInferenceWithoutFilteringSourceInferred = createEqualityInference(deterministicInheritedPredicate, sourceEffectivePredicate, joinExpression);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = allInference.rewriteExpressionAllowNonDeterministic(conjunct, in(sourceVariables), types);
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }

            // Push inheritedPredicates down to the filtering source if possible
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(deterministicInheritedPredicate)) {
                Expression rewrittenConjunct = allInference.rewriteExpression(conjunct, in(filteringSourceVariables), types);
                // We cannot push non-deterministic predicates to filtering side. Each filtering side row have to be
                // logically reevaluated for each source row.
                if (rewrittenConjunct != null) {
                    filteringSourceConjuncts.add(rewrittenConjunct);
                }
            }

            // move effective predicate conjuncts source <-> filter
            // See if we can push the filtering source effective predicate to the source side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(filteringSourceEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, in(sourceVariables), types);
                if (rewritten != null) {
                    sourceConjuncts.add(rewritten);
                }
            }

            // See if we can push the source effective predicate to the filtering soruce side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(sourceEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, in(filteringSourceVariables), types);
                if (rewritten != null) {
                    filteringSourceConjuncts.add(rewritten);
                }
            }

            // Add equalities from the inference back in
            sourceConjuncts.addAll(allInferenceWithoutSourceInferred.generateEqualitiesPartitionedBy(in(sourceVariables), types).getScopeEqualities());
            filteringSourceConjuncts.addAll(allInferenceWithoutFilteringSourceInferred.generateEqualitiesPartitionedBy(in(filteringSourceVariables), types).getScopeEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(sourceConjuncts));
            PlanNode rewrittenFilteringSource = context.rewrite(node.getFilteringSource(), combineConjuncts(filteringSourceConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource()) {
                output = new SemiJoinNode(
                        node.getId(),
                        rewrittenSource,
                        rewrittenFilteringSource,
                        node.getSourceJoinVariable(),
                        node.getFilteringSourceJoinVariable(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashVariable(),
                        node.getFilteringSourceHashVariable(),
                        node.getDistributionType());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(combineConjuncts(postJoinConjuncts)));
            }
            return output;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Expression> context)
        {
            if (node.hasEmptyGroupingSet()) {
                // TODO: in case of grouping sets, we should be able to push the filters over grouping keys below the aggregation
                // and also preserve the filter above the aggregation if it has an empty grouping set
                return visitPlan(node, context);
            }

            Expression inheritedPredicate = context.get();

            EqualityInference equalityInference = createEqualityInference(inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postAggregationConjuncts = new ArrayList<>();

            List<VariableReferenceExpression> groupingKeyVariables = node.getGroupingKeys();

            // Strip out non-deterministic conjuncts
            postAggregationConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(ExpressionDeterminismEvaluator::isDeterministic))));
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                if (node.getGroupIdVariable().isPresent() && VariablesExtractor.extractUnique(conjunct, types).contains(node.getGroupIdVariable().get())) {
                    // aggregation operator synthesizes outputs for group ids corresponding to the global grouping set (i.e., ()), so we
                    // need to preserve any predicates that evaluate the group id to run after the aggregation
                    // TODO: we should be able to infer if conditions on grouping() correspond to global grouping sets to determine whether
                    // we need to do this for each specific case
                    postAggregationConjuncts.add(conjunct);
                    continue;
                }

                Expression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(groupingKeyVariables), types);
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postAggregationConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(groupingKeyVariables)::apply, types);
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new AggregationNode(node.getId(),
                        rewrittenSource,
                        node.getAggregations(),
                        node.getGroupingSets(),
                        ImmutableList.of(),
                        node.getStep(),
                        node.getHashVariable(),
                        node.getGroupIdVariable());
            }
            if (!postAggregationConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(combineConjuncts(postAggregationConjuncts)));
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
            postUnnestConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(ExpressionDeterminismEvaluator::isDeterministic))));
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(node.getReplicateVariables()), types);
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postUnnestConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(node.getReplicateVariables())::apply, types);
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postUnnestConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postUnnestConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new UnnestNode(node.getId(), rewrittenSource, node.getReplicateVariables(), node.getUnnestVariables(), node.getOrdinalityVariable());
            }
            if (!postUnnestConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(combineConjuncts(postUnnestConjuncts)));
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

            if (!TRUE_LITERAL.equals(predicate)) {
                return new FilterNode(idAllocator.getNextId(), node, castToRowExpression(predicate));
            }

            return node;
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Expression> context)
        {
            Set<VariableReferenceExpression> predicateVariables = VariablesExtractor.extractUnique(context.get(), types);
            checkState(!predicateVariables.contains(node.getIdVariable()), "UniqueId in predicate is not yet supported");
            return context.defaultRewrite(node, context.get());
        }
    }
}

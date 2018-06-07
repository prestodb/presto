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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinReorderingStrategy;
import static com.facebook.presto.SystemSessionProperties.getMaxReorderedTables;
import static com.facebook.presto.cost.PlanNodeCostEstimate.INFINITE_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.DeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.facebook.presto.sql.planner.EqualityInference.nonInferrableConjuncts;
import static com.facebook.presto.sql.planner.iterative.rule.MultiJoinNode.toMultiJoinNode;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.INFINITE_COST_RESULT;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.UNKNOWN_COST_RESULT;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.createJoinEnumerationResult;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.powerSet;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public class ReorderJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(ReorderJoins.class);
    private static final Pattern<JoinNode> PATTERN = join().matching(
            joinNode -> !joinNode.getDistributionType().isPresent()
                    && joinNode.getType() == INNER
                    && isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL)));

    private final CostComparator costComparator;

    public ReorderJoins(CostComparator costComparator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinReorderingStrategy(session) == COST_BASED;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        // We check that join distribution type is absent because we only want to do this transformation once (reordered joins will have distribution type already set).
        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), getMaxReorderedTables(context.getSession()));
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                context.getSession(),
                context.getCostProvider(),
                costComparator,
                context.getIdAllocator(),
                multiJoinNode.getFilter(),
                context.getLookup(),
                context);
        JoinEnumerationResult result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols());
        if (!result.getPlanNode().isPresent()) {
            return Result.empty();
        }
        return Result.ofPlanNode(result.getPlanNode().get());
    }

    @VisibleForTesting
    static class JoinEnumerator
    {
        private final Session session;
        private final CostProvider costProvider;
        // Using Ordering to facilitate rule determinism
        private final Ordering<JoinEnumerationResult> resultComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final Expression allFilter;
        private final EqualityInference allFilterInference;
        private final Lookup lookup;
        private final Context context;

        private final Map<Set<PlanNode>, JoinEnumerationResult> memo = new HashMap<>();

        @VisibleForTesting
        JoinEnumerator(Session session, CostProvider costProvider, CostComparator costComparator, PlanNodeIdAllocator idAllocator, Expression filter, Lookup lookup, Context context)
        {
            this.session = requireNonNull(session, "session is null");
            this.costProvider = requireNonNull(costProvider, "costProvider is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.allFilter = requireNonNull(filter, "filter is null");
            this.allFilterInference = createEqualityInference(filter);
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.context = context;
        }

        private JoinEnumerationResult chooseJoinOrder(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols)
        {
            context.checkTimeoutNotExhausted();

            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            JoinEnumerationResult bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();
                Set<Set<Integer>> partitions = generatePartitions(sources.size());
                for (Set<Integer> partition : partitions) {
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(sources, outputSymbols, partition);
                    if (result.equals(UNKNOWN_COST_RESULT)) {
                        memo.put(multiJoinKey, result);
                        return result;
                    }
                    if (!result.equals(INFINITE_COST_RESULT)) {
                        resultBuilder.add(result);
                    }
                }

                List<JoinEnumerationResult> results = resultBuilder.build();
                if (results.isEmpty()) {
                    memo.put(multiJoinKey, INFINITE_COST_RESULT);
                    return INFINITE_COST_RESULT;
                }

                bestResult = resultComparator.min(results);
                memo.put(multiJoinKey, bestResult);
            }

            bestResult.planNode.ifPresent((planNode) -> log.debug("Least cost join was: %s", planNode));
            return bestResult;
        }

        /**
         * This method generates all the ways of dividing totalNodes into two sets
         * each containing at least one node. It will generate one set for each
         * possible partitioning. The other partition is implied in the absent values.
         * In order not to generate the inverse of any set, we always include the 0th
         * node in our sets.
         *
         * @return A set of sets each of which defines a partitioning of totalNodes
         */
        @VisibleForTesting
        static Set<Set<Integer>> generatePartitions(int totalNodes)
        {
            checkArgument(totalNodes > 1, "totalNodes must be greater than 1");
            Set<Integer> numbers = IntStream.range(0, totalNodes)
                    .boxed()
                    .collect(toImmutableSet());
            return powerSet(numbers).stream()
                    .filter(subSet -> subSet.contains(0))
                    .filter(subSet -> subSet.size() < numbers.size())
                    .collect(toImmutableSet());
        }

        JoinEnumerationResult createJoinAccordingToPartitioning(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols, Set<Integer> partitioning)
        {
            List<PlanNode> sourceList = ImmutableList.copyOf(sources);
            LinkedHashSet<PlanNode> leftSources = partitioning.stream()
                    .map(sourceList::get)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            LinkedHashSet<PlanNode> rightSources = sources.stream()
                    .filter(source -> !leftSources.contains(source))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            return createJoin(leftSources, rightSources, outputSymbols);
        }

        private JoinEnumerationResult createJoin(LinkedHashSet<PlanNode> leftSources, LinkedHashSet<PlanNode> rightSources, List<Symbol> outputSymbols)
        {
            Set<Symbol> leftSymbols = leftSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());
            Set<Symbol> rightSymbols = rightSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());

            List<Expression> joinPredicates = getJoinPredicates(leftSymbols, rightSymbols);
            List<JoinNode.EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(JoinEnumerator::isJoinEqualityCondition)
                    .map(predicate -> toEquiJoinClause((ComparisonExpression) predicate, leftSymbols))
                    .collect(toImmutableList());
            if (joinConditions.isEmpty()) {
                return INFINITE_COST_RESULT;
            }
            List<Expression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<Symbol> requiredJoinSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(outputSymbols)
                    .addAll(SymbolsExtractor.extractUnique(joinPredicates))
                    .build();

            JoinEnumerationResult leftResult = getJoinSource(
                    leftSources,
                    requiredJoinSymbols.stream().filter(leftSymbols::contains).collect(toImmutableList()));
            if (leftResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (leftResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            PlanNode left = leftResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            JoinEnumerationResult rightResult = getJoinSource(
                    rightSources,
                    requiredJoinSymbols.stream()
                            .filter(rightSymbols::contains)
                            .collect(toImmutableList()));
            if (rightResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (rightResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            PlanNode right = rightResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            // sort output symbols so that the left input symbols are first
            List<Symbol> sortedOutputSymbols = Stream.concat(left.getOutputSymbols().stream(), right.getOutputSymbols().stream())
                    .filter(outputSymbols::contains)
                    .collect(toImmutableList());

            return setJoinNodeProperties(new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    joinConditions,
                    sortedOutputSymbols,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(and(joinFilters)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        private List<Expression> getJoinPredicates(Set<Symbol> leftSymbols, Set<Symbol> rightSymbols)
        {
            ImmutableList.Builder<Expression> joinPredicatesBuilder = ImmutableList.builder();

            // This takes all conjuncts that were part of allFilters that
            // could not be used for equality inference.
            // If they use both the left and right symbols, we add them to the list of joinPredicates
            stream(nonInferrableConjuncts(allFilter))
                    .map(conjunct -> allFilterInference.rewriteExpression(conjunct, symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right symbols
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, leftSymbols::contains) == null)
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, rightSymbols::contains) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available symbols
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            List<Expression> joinEqualities = allFilterInference.generateEqualitiesPartitionedBy(symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)).getScopeEqualities();
            EqualityInference joinInference = createEqualityInference(joinEqualities.toArray(new Expression[0]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeStraddlingEqualities());

            return joinPredicatesBuilder.build();
        }

        private JoinEnumerationResult getJoinSource(LinkedHashSet<PlanNode> nodes, List<Symbol> outputSymbols)
        {
            if (nodes.size() == 1) {
                PlanNode planNode = getOnlyElement(nodes);
                ImmutableList.Builder<Expression> predicates = ImmutableList.builder();
                predicates.addAll(allFilterInference.generateEqualitiesPartitionedBy(outputSymbols::contains).getScopeEqualities());
                stream(nonInferrableConjuncts(allFilter))
                        .map(conjunct -> allFilterInference.rewriteExpression(conjunct, outputSymbols::contains))
                        .filter(Objects::nonNull)
                        .forEach(predicates::add);
                Expression filter = combineConjuncts(predicates.build());
                if (!TRUE_LITERAL.equals(filter)) {
                    planNode = new FilterNode(idAllocator.getNextId(), planNode, filter);
                }
                return createJoinEnumerationResult(Optional.of(planNode), costProvider.getCumulativeCost(planNode));
            }
            return chooseJoinOrder(nodes, outputSymbols);
        }

        private static boolean isJoinEqualityCondition(Expression expression)
        {
            return expression instanceof ComparisonExpression
                    && ((ComparisonExpression) expression).getType() == EQUAL
                    && ((ComparisonExpression) expression).getLeft() instanceof SymbolReference
                    && ((ComparisonExpression) expression).getRight() instanceof SymbolReference;
        }

        private static JoinNode.EquiJoinClause toEquiJoinClause(ComparisonExpression equality, Set<Symbol> leftSymbols)
        {
            Symbol leftSymbol = Symbol.from(equality.getLeft());
            Symbol rightSymbol = Symbol.from(equality.getRight());
            JoinNode.EquiJoinClause equiJoinClause = new JoinNode.EquiJoinClause(leftSymbol, rightSymbol);
            return leftSymbols.contains(leftSymbol) ? equiJoinClause : equiJoinClause.flip();
        }

        private JoinEnumerationResult setJoinNodeProperties(JoinNode joinNode)
        {
            // TODO avoid stat (but not cost) recalculation for all considered (distribution,flip) pairs, since resulting relation is the same in all case

            if (isAtMostScalar(joinNode.getRight(), lookup)) {
                JoinNode node = joinNode.withDistributionType(REPLICATED);
                return createJoinEnumerationResult(Optional.of(node), costProvider.getCumulativeCost(node));
            }
            if (isAtMostScalar(joinNode.getLeft(), lookup)) {
                JoinNode node = joinNode.flipChildren().withDistributionType(REPLICATED);
                return createJoinEnumerationResult(Optional.of(node), costProvider.getCumulativeCost(node));
            }

            List<JoinEnumerationResult> possibleJoinNodes = new ArrayList<>();
            FeaturesConfig.JoinDistributionType joinDistributionType = getJoinDistributionType(session);
            if (joinDistributionType.canRepartition() && !joinNode.isCrossJoin()) {
                JoinNode node = joinNode.withDistributionType(PARTITIONED);
                possibleJoinNodes.add(createJoinEnumerationResult(Optional.of(node), costProvider.getCumulativeCost(node)));
                node = node.flipChildren();
                possibleJoinNodes.add(createJoinEnumerationResult(Optional.of(node), costProvider.getCumulativeCost(node)));
            }
            if (joinDistributionType.canReplicate()) {
                JoinNode node = joinNode.withDistributionType(REPLICATED);
                possibleJoinNodes.add(createJoinEnumerationResult(Optional.of(node), costProvider.getCumulativeCost(node)));
                node = node.flipChildren();
                possibleJoinNodes.add(createJoinEnumerationResult(Optional.of(node), costProvider.getCumulativeCost(node)));
            }
            if (possibleJoinNodes.stream().anyMatch(result -> result.equals(UNKNOWN_COST_RESULT))) {
                return UNKNOWN_COST_RESULT;
            }
            return resultComparator.min(possibleJoinNodes);
        }
    }

    @VisibleForTesting
    static class JoinEnumerationResult
    {
        public static final JoinEnumerationResult UNKNOWN_COST_RESULT = new JoinEnumerationResult(Optional.empty(), UNKNOWN_COST);
        public static final JoinEnumerationResult INFINITE_COST_RESULT = new JoinEnumerationResult(Optional.empty(), INFINITE_COST);

        private final Optional<PlanNode> planNode;
        private final PlanNodeCostEstimate cost;

        private JoinEnumerationResult(Optional<PlanNode> planNode, PlanNodeCostEstimate cost)
        {
            this.planNode = requireNonNull(planNode);
            this.cost = requireNonNull(cost);
            checkArgument((cost.hasUnknownComponents() || cost.equals(INFINITE_COST)) && !planNode.isPresent()
                            || (!cost.hasUnknownComponents() || !cost.equals(INFINITE_COST)) && planNode.isPresent(),
                    "planNode should be present if and only if cost is known");
        }

        public Optional<PlanNode> getPlanNode()
        {
            return planNode;
        }

        public PlanNodeCostEstimate getCost()
        {
            return cost;
        }

        static JoinEnumerationResult createJoinEnumerationResult(Optional<PlanNode> planNode, PlanNodeCostEstimate cost)
        {
            if (cost.hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (cost.equals(INFINITE_COST)) {
                return INFINITE_COST_RESULT;
            }
            return new JoinEnumerationResult(planNode, cost);
        }
    }
}

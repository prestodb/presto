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
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.RowExpressionEqualityInference;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.DistributionType;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.StandardExpressions;
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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinReorderingStrategy;
import static com.facebook.presto.SystemSessionProperties.getMaxReorderedJoins;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.and;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.ExpressionDeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.isBelowMaxBroadcastSize;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.INFINITE_COST_RESULT;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.UNKNOWN_COST_RESULT;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.relational.StandardExpressions.getLeft;
import static com.facebook.presto.sql.relational.StandardExpressions.getRight;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.powerSet;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class ReorderJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(ReorderJoins.class);

    // We check that join distribution type is absent because we only want
    // to do this transformation once (reordered joins will have distribution type already set).
    private static final Pattern<JoinNode> PATTERN = join().matching(
            joinNode -> !joinNode.getDistributionType().isPresent()
                    && joinNode.getType() == INNER
                    && isDeterministic(joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).orElse(TRUE_LITERAL)));

    private final CostComparator costComparator;
    private final Metadata metadata;
    private final StandardExpressions standardExpressions;
    private final LogicalRowExpressions logicalRowExpressions;

    public ReorderJoins(Metadata metadata, CostComparator costComparator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.standardExpressions = new StandardExpressions(metadata.getFunctionManager());
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata.getFunctionManager()), new FunctionResolution(metadata.getFunctionManager()));
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinReorderingStrategy(session) == AUTOMATIC;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), getMaxReorderedJoins(context.getSession()));
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                costComparator,
                multiJoinNode.getFilter(),
                context);
        JoinEnumerationResult result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputVariables());
        if (!result.getPlanNode().isPresent()) {
            return Result.empty();
        }
        return Result.ofPlanNode(result.getPlanNode().get());
    }

    public JoinEnumerator createJoinEnumerator(RowExpression filter, Context context)
    {
        return new JoinEnumerator(
                costComparator,
                filter,
                context);
    }

    @VisibleForTesting
    class JoinEnumerator
    {
        private final Session session;
        private final CostProvider costProvider;
        // Using Ordering to facilitate rule determinism
        private final Ordering<JoinEnumerationResult> resultComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final RowExpression allFilter;
        private final RowExpressionEqualityInference allFilterInference;
        private final Lookup lookup;
        private final Context context;
        private final Map<Set<PlanNode>, JoinEnumerationResult> memo = new HashMap<>();

        private JoinEnumerator(CostComparator costComparator, RowExpression filter, Context context)
        {
            this.context = requireNonNull(context);
            this.session = requireNonNull(context.getSession(), "session is null");
            this.costProvider = requireNonNull(context.getCostProvider(), "costProvider is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(context.getIdAllocator(), "idAllocator is null");
            this.allFilter = requireNonNull(filter, "filter is null");
            this.allFilterInference = RowExpressionEqualityInference.createEqualityInference(metadata, filter);
            this.lookup = requireNonNull(context.getLookup(), "lookup is null");
        }

        private JoinEnumerationResult chooseJoinOrder(LinkedHashSet<PlanNode> sources, List<VariableReferenceExpression> outputVariables)
        {
            context.checkTimeoutNotExhausted();

            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            JoinEnumerationResult bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();
                Set<Set<Integer>> partitions = generatePartitions(sources.size());
                for (Set<Integer> partition : partitions) {
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(sources, outputVariables, partition);
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

        @VisibleForTesting
        JoinEnumerationResult createJoinAccordingToPartitioning(LinkedHashSet<PlanNode> sources, List<VariableReferenceExpression> outputVariables, Set<Integer> partitioning)
        {
            List<PlanNode> sourceList = ImmutableList.copyOf(sources);
            LinkedHashSet<PlanNode> leftSources = partitioning.stream()
                    .map(sourceList::get)
                    .collect(toCollection(LinkedHashSet::new));
            LinkedHashSet<PlanNode> rightSources = sources.stream()
                    .filter(source -> !leftSources.contains(source))
                    .collect(toCollection(LinkedHashSet::new));
            return createJoin(leftSources, rightSources, outputVariables);
        }

        private JoinEnumerationResult createJoin(LinkedHashSet<PlanNode> leftSources, LinkedHashSet<PlanNode> rightSources, List<VariableReferenceExpression> outputVariables)
        {
            Set<VariableReferenceExpression> leftVariables = leftSources.stream()
                    .flatMap(node -> node.getOutputVariables().stream())
                    .collect(toImmutableSet());
            Set<VariableReferenceExpression> rightVariables = rightSources.stream()
                    .flatMap(node -> node.getOutputVariables().stream())
                    .collect(toImmutableSet());

            List<RowExpression> joinPredicates = getJoinPredicates(leftVariables, rightVariables);
            List<EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(ReorderJoins.this::isJoinEqualityCondition)
                    .map(predicate -> toEquiJoinClause(predicate, leftVariables))
                    .collect(toImmutableList());
            if (joinConditions.isEmpty()) {
                return INFINITE_COST_RESULT;
            }
            List<RowExpression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<VariableReferenceExpression> requiredJoinVariables = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(outputVariables)
                    .addAll(VariablesExtractor.extractUnique(joinPredicates))
                    .build();

            JoinEnumerationResult leftResult = getJoinSource(
                    leftSources,
                    requiredJoinVariables.stream()
                            .filter(leftVariables::contains)
                            .collect(toImmutableList()));
            if (leftResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (leftResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            PlanNode left = leftResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            JoinEnumerationResult rightResult = getJoinSource(
                    rightSources,
                    requiredJoinVariables.stream()
                            .filter(rightVariables::contains)
                            .collect(toImmutableList()));
            if (rightResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (rightResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            PlanNode right = rightResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            // sort output variables so that the left input variables are first
            List<VariableReferenceExpression> sortedOutputVariables = Stream.concat(left.getOutputVariables().stream(), right.getOutputVariables().stream())
                    .filter(outputVariables::contains)
                    .collect(toImmutableList());

            return setJoinNodeProperties(new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    joinConditions,
                    sortedOutputVariables,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(logicalRowExpressions.and(joinFilters)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        private List<RowExpression> getJoinPredicates(Set<VariableReferenceExpression> leftVariables, Set<VariableReferenceExpression> rightVariables)
        {
            ImmutableList.Builder<RowExpression> joinPredicatesBuilder = ImmutableList.builder();
            // This takes all conjuncts that were part of allFilters that
            // could not be used for equality inference.
            // If they use both the left and right variables, we add them to the list of joinPredicates
            RowExpressionEqualityInference.Builder builder = new RowExpressionEqualityInference.Builder(metadata.getFunctionManager(), metadata.getTypeManager());

            stream(builder.nonInferrableConjuncts(allFilter))
                    .map(conjunct -> allFilterInference.rewriteExpression(
                            conjunct,
                            variable -> leftVariables.contains(variable) || rightVariables.contains(variable)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right variables
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, leftVariables::contains) == null)
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, rightVariables::contains) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available variables
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            List<RowExpression> joinEqualities = allFilterInference.generateEqualitiesPartitionedBy(
                    variable -> leftVariables.contains(variable) || rightVariables.contains(variable))
                    .getScopeEqualities();
            RowExpressionEqualityInference joinInference = RowExpressionEqualityInference.createEqualityInference(metadata, joinEqualities.toArray(new RowExpression[0]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftVariables)).getScopeStraddlingEqualities());

            return joinPredicatesBuilder.build();
        }

        private JoinEnumerationResult getJoinSource(LinkedHashSet<PlanNode> nodes, List<VariableReferenceExpression> outputVariables)
        {
            if (nodes.size() == 1) {
                PlanNode planNode = getOnlyElement(nodes);
                ImmutableList.Builder<RowExpression> predicates = ImmutableList.builder();
                predicates.addAll(allFilterInference.generateEqualitiesPartitionedBy(outputVariables::contains).getScopeEqualities());
                RowExpressionEqualityInference.Builder builder = new RowExpressionEqualityInference.Builder(metadata.getFunctionManager(), metadata.getTypeManager());
                stream(builder.nonInferrableConjuncts(allFilter))
                        .map(conjunct -> allFilterInference.rewriteExpression(conjunct, outputVariables::contains))
                        .filter(Objects::nonNull)
                        .forEach(predicates::add);
                RowExpression filter = logicalRowExpressions.combineConjuncts(predicates.build());
                if (!TRUE_CONSTANT.equals(filter)) {
                    planNode = new FilterNode(idAllocator.getNextId(), planNode, filter);
                }
                return createJoinEnumerationResult(planNode);
            }
            return chooseJoinOrder(nodes, outputVariables);
        }

        private JoinEnumerationResult setJoinNodeProperties(JoinNode joinNode)
        {
            // TODO avoid stat (but not cost) recalculation for all considered (distribution,flip) pairs, since resulting relation is the same in all case
            if (isAtMostScalar(joinNode.getRight(), lookup)) {
                return createJoinEnumerationResult(joinNode.withDistributionType(REPLICATED));
            }
            if (isAtMostScalar(joinNode.getLeft(), lookup)) {
                return createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(REPLICATED));
            }
            List<JoinEnumerationResult> possibleJoinNodes = getPossibleJoinNodes(joinNode, getJoinDistributionType(session));
            verify(!possibleJoinNodes.isEmpty(), "possibleJoinNodes is empty");
            if (possibleJoinNodes.stream().anyMatch(UNKNOWN_COST_RESULT::equals)) {
                return UNKNOWN_COST_RESULT;
            }
            return resultComparator.min(possibleJoinNodes);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, JoinDistributionType distributionType)
        {
            checkArgument(joinNode.getType() == INNER, "unexpected join node type: %s", joinNode.getType());

            if (joinNode.isCrossJoin()) {
                return getPossibleJoinNodes(joinNode, REPLICATED);
            }

            switch (distributionType) {
                case PARTITIONED:
                    return getPossibleJoinNodes(joinNode, PARTITIONED);
                case BROADCAST:
                    return getPossibleJoinNodes(joinNode, REPLICATED);
                case AUTOMATIC:
                    ImmutableList.Builder<JoinEnumerationResult> result = ImmutableList.builder();
                    result.addAll(getPossibleJoinNodes(joinNode, PARTITIONED));
                    if (isBelowMaxBroadcastSize(joinNode, context)) {
                        result.addAll(getPossibleJoinNodes(joinNode, REPLICATED));
                    }
                    return result.build();
                default:
                    throw new IllegalArgumentException("unexpected join distribution type: " + distributionType);
            }
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType)
        {
            return ImmutableList.of(
                    createJoinEnumerationResult(joinNode.withDistributionType(distributionType)),
                    createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(distributionType)));
        }

        private JoinEnumerationResult createJoinEnumerationResult(PlanNode planNode)
        {
            return JoinEnumerationResult.createJoinEnumerationResult(Optional.of(planNode), costProvider.getCost(planNode));
        }
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

    private boolean isJoinEqualityCondition(RowExpression expression)
    {
        return standardExpressions.getOperation(expression).map(OperatorType.EQUAL::equals).orElse(false)
                && getLeft(expression) instanceof VariableReferenceExpression
                && getRight(expression) instanceof VariableReferenceExpression;
    }

    private RowExpression toRowExpression(EquiJoinClause clause)
    {
        return standardExpressions.compare(OperatorType.EQUAL, clause.getLeft(), clause.getRight());
    }

    private EquiJoinClause toEquiJoinClause(RowExpression equality, Set<VariableReferenceExpression> leftVariables)
    {
        VariableReferenceExpression leftVariable = (VariableReferenceExpression) getLeft(equality);
        VariableReferenceExpression rightVariable = (VariableReferenceExpression) getRight(equality);
        EquiJoinClause equiJoinClause = new EquiJoinClause(leftVariable, rightVariable);
        return leftVariables.contains(leftVariable) ? equiJoinClause : equiJoinClause.flip();
    }

    @VisibleForTesting
    MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup, int joinLimit)
    {
        // the number of sources is the number of joins + 1
        return new JoinNodeFlattener(joinNode, lookup, joinLimit + 1).toMultiJoinNode();
    }

    @VisibleForTesting
    MultiJoinNode createMultiJoinNode(LinkedHashSet<PlanNode> sources, RowExpression filter, List<VariableReferenceExpression> outputVariables)
    {
        return new MultiJoinNode(sources, filter, outputVariables);
    }

    public Builder multiNodeBuilder()
    {
        return new Builder();
    }

    /**
     * This class represents a set of inner joins that can be executed in any order.
     */
    @VisibleForTesting
    class MultiJoinNode
    {
        // Use a linked hash set to ensure optimizer is deterministic
        private final LinkedHashSet<PlanNode> sources;
        private final RowExpression filter;
        private final List<VariableReferenceExpression> outputVariables;

        public MultiJoinNode(LinkedHashSet<PlanNode> sources, RowExpression filter, List<VariableReferenceExpression> outputVariables)
        {
            requireNonNull(sources, "sources is null");
            checkArgument(sources.size() > 1, "sources size is <= 1");
            requireNonNull(filter, "filter is null");
            requireNonNull(outputVariables, "outputVariables is null");

            this.sources = sources;
            this.filter = filter;
            this.outputVariables = ImmutableList.copyOf(outputVariables);

            List<VariableReferenceExpression> inputVariables = sources.stream().flatMap(source -> source.getOutputVariables().stream()).collect(toImmutableList());
            checkArgument(inputVariables.containsAll(outputVariables), "inputs do not contain all output variables");
        }

        public RowExpression getFilter()
        {
            return filter;
        }

        public LinkedHashSet<PlanNode> getSources()
        {
            return sources;
        }

        public List<VariableReferenceExpression> getOutputVariables()
        {
            return outputVariables;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sources, ImmutableSet.copyOf(logicalRowExpressions.extractConjuncts(filter)), outputVariables);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof MultiJoinNode)) {
                return false;
            }

            MultiJoinNode other = (MultiJoinNode) obj;
            return this.sources.equals(other.sources)
                    && ImmutableSet.copyOf(logicalRowExpressions.extractConjuncts(this.filter)).equals(ImmutableSet.copyOf(logicalRowExpressions.extractConjuncts(other.filter)))
                    && this.outputVariables.equals(other.outputVariables);
        }
    }

    class JoinNodeFlattener
    {
        private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
        private final List<RowExpression> filters = new ArrayList<>();
        private final List<VariableReferenceExpression> outputVariables;
        private final Lookup lookup;

        JoinNodeFlattener(JoinNode node, Lookup lookup, int sourceLimit)
        {
            requireNonNull(node, "node is null");
            checkState(node.getType() == INNER, "join type must be INNER");
            this.outputVariables = node.getOutputVariables();
            this.lookup = requireNonNull(lookup, "lookup is null");
            flattenNode(node, sourceLimit);
        }

        private void flattenNode(PlanNode node, int limit)
        {
            PlanNode resolved = lookup.resolve(node);

            // (limit - 2) because you need to account for adding left and right side
            if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {
                sources.add(node);
                return;
            }

            JoinNode joinNode = (JoinNode) resolved;
            if (joinNode.getType() != INNER || !isDeterministic(joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).orElse(TRUE_LITERAL)) || joinNode.getDistributionType().isPresent()) {
                sources.add(node);
                return;
            }

            // we set the left limit to limit - 1 to account for the node on the right
            flattenNode(joinNode.getLeft(), limit - 1);
            flattenNode(joinNode.getRight(), limit);
            joinNode.getCriteria().stream()
                    .map(ReorderJoins.this::toRowExpression)
                    .forEach(filters::add);
            joinNode.getFilter().ifPresent(filter -> filters.add(filter));
        }

        MultiJoinNode toMultiJoinNode()
        {
            return new MultiJoinNode(sources, and(filters), outputVariables);
        }
    }

    class Builder
    {
        private List<PlanNode> sources;
        private RowExpression filter;
        private List<VariableReferenceExpression> outputVariables;

        public Builder setSources(PlanNode... sources)
        {
            this.sources = ImmutableList.copyOf(sources);
            return this;
        }

        public Builder setFilter(RowExpression filter)
        {
            this.filter = filter;
            return this;
        }

        public Builder setOutputVariables(VariableReferenceExpression... outputVariables)
        {
            this.outputVariables = ImmutableList.copyOf(outputVariables);
            return this;
        }

        public MultiJoinNode build()
        {
            return new MultiJoinNode(new LinkedHashSet<>(sources), filter, outputVariables);
        }
    }

    @VisibleForTesting
    static class JoinEnumerationResult
    {
        public static final JoinEnumerationResult UNKNOWN_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.unknown());
        public static final JoinEnumerationResult INFINITE_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.infinite());

        private final Optional<PlanNode> planNode;
        private final PlanCostEstimate cost;

        private JoinEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            this.planNode = requireNonNull(planNode, "planNode is null");
            this.cost = requireNonNull(cost, "cost is null");
            checkArgument((cost.hasUnknownComponents() || cost.equals(PlanCostEstimate.infinite())) && !planNode.isPresent()
                            || (!cost.hasUnknownComponents() || !cost.equals(PlanCostEstimate.infinite())) && planNode.isPresent(),
                    "planNode should be present if and only if cost is known");
        }

        static JoinEnumerationResult createJoinEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            if (cost.hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (cost.equals(PlanCostEstimate.infinite())) {
                return INFINITE_COST_RESULT;
            }
            return new JoinEnumerationResult(planNode, cost);
        }

        public Optional<PlanNode> getPlanNode()
        {
            return planNode;
        }

        public PlanCostEstimate getCost()
        {
            return cost;
        }
    }
}

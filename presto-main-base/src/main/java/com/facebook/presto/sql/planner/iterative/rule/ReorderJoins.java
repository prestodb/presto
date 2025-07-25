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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.MultiJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.facebook.presto.SystemSessionProperties.confidenceBasedBroadcastEnabled;
import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinReorderingStrategy;
import static com.facebook.presto.SystemSessionProperties.getMaxReorderedJoins;
import static com.facebook.presto.SystemSessionProperties.shouldHandleComplexEquiJoins;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.iterative.ConfidenceBasedBroadcastUtil.confidenceBasedBroadcast;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.isBelowMaxBroadcastSize;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.mustPartition;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.INFINITE_COST_RESULT;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.UNKNOWN_COST_RESULT;
import static com.facebook.presto.sql.planner.optimizations.JoinNodeUtils.toRowExpression;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.getNonIdentityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.powerSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class ReorderJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(ReorderJoins.class);

    // We check that join distribution type is absent because we only want
    // to do this transformation once (reordered joins will have distribution type already set).
    private final Pattern<JoinNode> joinNodePattern;

    private final CostComparator costComparator;
    private final Metadata metadata;
    private final FunctionResolution functionResolution;
    private final DeterminismEvaluator determinismEvaluator;
    private String statsSource;

    public ReorderJoins(CostComparator costComparator, Metadata metadata)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());

        this.joinNodePattern = join().matching(
                joinNode -> !joinNode.getDistributionType().isPresent()
                        && joinNode.getType() == INNER
                        && determinismEvaluator.isDeterministic(joinNode.getFilter().orElse(TRUE_CONSTANT)));
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return joinNodePattern;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinReorderingStrategy(session) == AUTOMATIC;
    }

    @Override
    public boolean isCostBased(Session session)
    {
        // when enabled, join order is always cost-based
        return isEnabled(session);
    }

    public String getStatsSource()
    {
        return statsSource;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), getMaxReorderedJoins(context.getSession()), shouldHandleComplexEquiJoins(context.getSession()),
                functionResolution, determinismEvaluator);
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                costComparator,
                multiJoinNode.getFilter(),
                context,
                determinismEvaluator,
                functionResolution,
                metadata);

        JoinEnumerationResult result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputVariables());

        if (!result.getPlanNode().isPresent()) {
            return Result.empty();
        }

        statsSource = context.getStatsProvider().getStats(joinNode).getSourceInfo().getSourceInfoName();

        PlanNode transformedPlan = result.getPlanNode().get();
        if (!multiJoinNode.getAssignments().isEmpty()) {
            transformedPlan = new ProjectNode(
                    transformedPlan.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    transformedPlan,
                    multiJoinNode.getAssignments(),
                    LOCAL);
        }

        return Result.ofPlanNode(transformedPlan);
    }

    @VisibleForTesting
    static class JoinEnumerator
    {
        private final Session session;
        private final CostProvider costProvider;
        // Using Ordering to facilitate rule determinism
        private final Ordering<JoinEnumerationResult> resultComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final RowExpression allFilter;
        private final EqualityInference allFilterInference;
        private final LogicalRowExpressions logicalRowExpressions;
        private final Lookup lookup;
        private final Context context;

        private final Map<Set<PlanNode>, JoinEnumerationResult> memo = new HashMap<>();
        private final FunctionResolution functionResolution;

        @VisibleForTesting
        JoinEnumerator(CostComparator costComparator, RowExpression filter, Context context, DeterminismEvaluator determinismEvaluator, FunctionResolution functionResolution, Metadata metadata)
        {
            this.context = requireNonNull(context);
            this.session = requireNonNull(context.getSession(), "session is null");
            this.costProvider = requireNonNull(context.getCostProvider(), "costProvider is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(context.getIdAllocator(), "idAllocator is null");
            this.allFilter = requireNonNull(filter, "filter is null");
            this.lookup = requireNonNull(context.getLookup(), "lookup is null");

            this.metadata = requireNonNull(metadata, "metadata is null");
            this.allFilterInference = createEqualityInference(metadata, filter);
            this.logicalRowExpressions = new LogicalRowExpressions(determinismEvaluator, functionResolution, metadata.getFunctionAndTypeManager());
            this.functionResolution = functionResolution;
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
            HashSet<VariableReferenceExpression> leftVariables = leftSources.stream()
                    .flatMap(node -> node.getOutputVariables().stream())
                    .collect(toCollection(HashSet::new));
            HashSet<VariableReferenceExpression> rightVariables = rightSources.stream()
                    .flatMap(node -> node.getOutputVariables().stream())
                    .collect(toCollection(HashSet::new));

            List<RowExpression> joinPredicates = getJoinPredicates(leftVariables, rightVariables);

            VariableAllocator variableAllocator = context.getVariableAllocator();
            JoinCondition joinConditions = extractJoinConditions(joinPredicates, leftVariables, rightVariables, variableAllocator);
            List<EquiJoinClause> joinClauses = joinConditions.getJoinClauses();
            List<RowExpression> joinFilters = joinConditions.getJoinFilters();

            //Update the left & right variable sets with any new variables generated
            leftVariables.addAll(joinConditions.getNewLeftAssignments().keySet());
            rightVariables.addAll(joinConditions.getNewRightAssignments().keySet());

            if (joinClauses.isEmpty()) {
                return INFINITE_COST_RESULT;
            }

            Set<VariableReferenceExpression> requiredJoinVariables = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(outputVariables)
                    .addAll(extractUnique(joinPredicates))
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
            if (!joinConditions.getNewLeftAssignments().isEmpty()) {
                ImmutableMap.Builder<VariableReferenceExpression, RowExpression> assignments = ImmutableMap.builder();
                left.getOutputVariables().forEach(outputVariable -> assignments.put(outputVariable, outputVariable));
                assignments.putAll(joinConditions.getNewLeftAssignments());

                left = addProjections(left, idAllocator, assignments.build());
            }

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
            if (!joinConditions.getNewRightAssignments().isEmpty()) {
                ImmutableMap.Builder<VariableReferenceExpression, RowExpression> assignments = ImmutableMap.builder();
                right.getOutputVariables().forEach(outputVariable -> assignments.put(outputVariable, outputVariable));
                assignments.putAll(joinConditions.getNewRightAssignments());

                right = addProjections(right, idAllocator, assignments.build());
            }

            // sort output variables so that the left input variables are first
            List<VariableReferenceExpression> sortedOutputVariables = Stream.concat(left.getOutputVariables().stream(), right.getOutputVariables().stream())
                    .filter(outputVariables::contains)
                    .collect(toImmutableList());

            return setJoinNodeProperties(new JoinNode(
                    left.getSourceLocation(),
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    joinClauses,
                    sortedOutputVariables,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(and(joinFilters)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of()));
        }

        private List<RowExpression> getJoinPredicates(Set<VariableReferenceExpression> leftVariables, Set<VariableReferenceExpression> rightVariables)
        {
            ImmutableList.Builder<RowExpression> joinPredicatesBuilder = ImmutableList.builder();
            // This takes all conjuncts that were part of allFilters that
            // could not be used for equality inference.
            // If they use both the left and right variables, we add them to the list of joinPredicates
            EqualityInference.Builder builder = new EqualityInference.Builder(metadata);
            StreamSupport.stream(builder.nonInferableConjuncts(allFilter).spliterator(), false)
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
                    variable -> leftVariables.contains(variable) || rightVariables.contains(variable)).getScopeEqualities();
            EqualityInference joinInference = createEqualityInference(metadata, joinEqualities.toArray(new RowExpression[0]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftVariables)).getScopeStraddlingEqualities());

            return joinPredicatesBuilder.build();
        }

        private JoinEnumerationResult getJoinSource(LinkedHashSet<PlanNode> nodes, List<VariableReferenceExpression> outputVariables)
        {
            if (nodes.size() == 1) {
                PlanNode planNode = getOnlyElement(nodes);
                ImmutableList.Builder<RowExpression> predicates = ImmutableList.builder();
                predicates.addAll(allFilterInference.generateEqualitiesPartitionedBy(outputVariables::contains).getScopeEqualities());
                EqualityInference.Builder builder = new EqualityInference.Builder(metadata);
                StreamSupport.stream(builder.nonInferableConjuncts(allFilter).spliterator(), false)
                        .map(conjunct -> allFilterInference.rewriteExpression(conjunct, outputVariables::contains))
                        .filter(Objects::nonNull)
                        .forEach(predicates::add);
                RowExpression filter = logicalRowExpressions.combineConjuncts(predicates.build());
                if (!TRUE_CONSTANT.equals(filter)) {
                    planNode = new FilterNode(planNode.getSourceLocation(), idAllocator.getNextId(), planNode, filter);
                }
                return createJoinEnumerationResult(planNode);
            }
            return chooseJoinOrder(nodes, outputVariables);
        }

        @VisibleForTesting
        JoinCondition extractJoinConditions(List<RowExpression> joinPredicates,
                Set<VariableReferenceExpression> leftVariables,
                Set<VariableReferenceExpression> rightVariables,
                VariableAllocator variableAllocator)
        {
            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> newLeftAssignments = ImmutableMap.builder();
            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> newRightAssignments = ImmutableMap.builder();

            ImmutableList.Builder<EquiJoinClause> joinClauses = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> joinFilters = ImmutableList.builder();

            for (RowExpression predicate : joinPredicates) {
                if (predicate instanceof CallExpression
                        && functionResolution.isEqualsFunction(((CallExpression) predicate).getFunctionHandle())
                        && ((CallExpression) predicate).getArguments().size() == 2) {
                    RowExpression argument0 = ((CallExpression) predicate).getArguments().get(0);
                    RowExpression argument1 = ((CallExpression) predicate).getArguments().get(1);

                    // First check if arguments refer to different sides of join
                    Set<VariableReferenceExpression> argument0Vars = extractUnique(argument0);
                    Set<VariableReferenceExpression> argument1Vars = extractUnique(argument1);
                    if (!((leftVariables.containsAll(argument0Vars) && rightVariables.containsAll(argument1Vars))
                            || (rightVariables.containsAll(argument0Vars) && leftVariables.containsAll(argument1Vars)))) {
                        // Arguments have a mix of join sides, use this predicate as a filter
                        joinFilters.add(predicate);
                        continue;
                    }

                    // Next, check to see if first argument refers to left side and second argument to the right side
                    // If not, swap the arguments
                    if (leftVariables.containsAll(argument1Vars)) {
                        RowExpression temp = argument1;
                        argument1 = argument0;
                        argument0 = temp;
                    }

                    // Next, check if we need to create new assignments for complex equi-join clauses
                    // E.g. leftVar = ADD(rightVar1, rightVar2)
                    if (!(argument0 instanceof VariableReferenceExpression)) {
                        VariableReferenceExpression newLeft = variableAllocator.newVariable(argument0);
                        newLeftAssignments.put(newLeft, argument0);
                        argument0 = newLeft;
                    }

                    if (!(argument1 instanceof VariableReferenceExpression)) {
                        VariableReferenceExpression newRight = variableAllocator.newVariable(argument1);
                        newRightAssignments.put(newRight, argument1);
                        argument1 = newRight;
                    }

                    joinClauses.add(new EquiJoinClause((VariableReferenceExpression) argument0, (VariableReferenceExpression) argument1));
                }
                else {
                    joinFilters.add(predicate);
                }
            }

            return new JoinCondition(joinClauses.build(), joinFilters.build(), newLeftAssignments.build(), newRightAssignments.build());
        }

        @VisibleForTesting
        static class JoinCondition
        {
            List<EquiJoinClause> joinClauses;
            List<RowExpression> joinFilters;
            Map<VariableReferenceExpression, RowExpression> newLeftAssignments;
            Map<VariableReferenceExpression, RowExpression> newRightAssignments;

            public JoinCondition(List<EquiJoinClause> joinClauses, List<RowExpression> joinFilters,
                    Map<VariableReferenceExpression, RowExpression> left, Map<VariableReferenceExpression, RowExpression> right)
            {
                this.joinClauses = joinClauses;
                this.joinFilters = joinFilters;
                this.newLeftAssignments = left;
                this.newRightAssignments = right;
            }

            public List<EquiJoinClause> getJoinClauses()
            {
                return joinClauses;
            }

            public List<RowExpression> getJoinFilters()
            {
                return joinFilters;
            }

            public Map<VariableReferenceExpression, RowExpression> getNewLeftAssignments()
            {
                return newLeftAssignments;
            }

            public Map<VariableReferenceExpression, RowExpression> getNewRightAssignments()
            {
                return newRightAssignments;
            }
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

            if (isBelowMaxBroadcastSize(joinNode, context) && isBelowMaxBroadcastSize(joinNode.flipChildren(), context) && !mustPartition(joinNode) && confidenceBasedBroadcastEnabled(context.getSession())) {
                Optional<JoinNode> result = confidenceBasedBroadcast(joinNode, context);
                if (result.isPresent()) {
                    return createJoinEnumerationResult(result.get());
                }
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
                    result.addAll(getPossibleJoinNodes(joinNode, REPLICATED, node -> isBelowMaxBroadcastSize(node, context)));
                    return result.build();
                default:
                    throw new IllegalArgumentException("unexpected join distribution type: " + distributionType);
            }
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, com.facebook.presto.spi.plan.JoinDistributionType distributionType)
        {
            return getPossibleJoinNodes(joinNode, distributionType, (node) -> true);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, com.facebook.presto.spi.plan.JoinDistributionType distributionType, Predicate<JoinNode> isAllowed)
        {
            List<JoinNode> nodes = ImmutableList.of(
                    joinNode.withDistributionType(distributionType),
                    joinNode.flipChildren().withDistributionType(distributionType));
            return nodes.stream().filter(isAllowed).map(this::createJoinEnumerationResult).collect(toImmutableList());
        }

        private JoinEnumerationResult createJoinEnumerationResult(PlanNode planNode)
        {
            return JoinEnumerationResult.createJoinEnumerationResult(Optional.of(planNode), costProvider.getCost(planNode));
        }
    }

    public static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup, int joinLimit, boolean handleComplexEquiJoins, FunctionResolution functionResolution, DeterminismEvaluator determinismEvaluator)
    {
        // the number of sources is the number of joins + 1
        return new JoinNodeFlattener(joinNode, lookup, joinLimit + 1, handleComplexEquiJoins, functionResolution, determinismEvaluator).toMultiJoinNode();
    }

    @VisibleForTesting
    private static class JoinNodeFlattener
    {
        private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
        private final Assignments intermediateAssignments;
        private final boolean handleComplexEquiJoins;
        private List<RowExpression> filters = new ArrayList<>();
        private final List<VariableReferenceExpression> outputVariables;
        private final FunctionResolution functionResolution;
        private final DeterminismEvaluator determinismEvaluator;
        private final Lookup lookup;

        JoinNodeFlattener(JoinNode node, Lookup lookup, int sourceLimit, boolean handleComplexEquiJoins, FunctionResolution functionResolution,
                DeterminismEvaluator determinismEvaluator)
        {
            requireNonNull(node, "node is null");
            checkState(node.getType() == INNER, "join type must be INNER");
            this.outputVariables = node.getOutputVariables();
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
            this.handleComplexEquiJoins = handleComplexEquiJoins;

            Map<VariableReferenceExpression, RowExpression> intermediateAssignments = new HashMap<>();
            flattenNode(node, sourceLimit, intermediateAssignments);

            // We resolve the intermediate assignments to only inputs of the flattened join node
            ImmutableSet<VariableReferenceExpression> inputVariables = sources.stream().flatMap(s -> s.getOutputVariables().stream()).collect(toImmutableSet());
            this.intermediateAssignments = resolveAssignments(intermediateAssignments, inputVariables);
            rewriteFilterWithInlinedAssignments(this.intermediateAssignments);
        }

        private Assignments resolveAssignments(Map<VariableReferenceExpression, RowExpression> assignments, Set<VariableReferenceExpression> availableVariables)
        {
            HashSet<VariableReferenceExpression> resolvedVariables = new HashSet<>();
            ImmutableList.copyOf(assignments.keySet()).forEach(variable -> resolveVariable(variable, resolvedVariables, assignments, availableVariables));

            return Assignments.builder().putAll(assignments).build();
        }

        private void resolveVariable(VariableReferenceExpression variable, HashSet<VariableReferenceExpression> resolvedVariables, Map<VariableReferenceExpression,
                RowExpression> assignments, Set<VariableReferenceExpression> availableVariables)
        {
            RowExpression expression = assignments.get(variable);
            Sets.SetView<VariableReferenceExpression> variablesToResolve = Sets.difference(Sets.difference(extractUnique(expression), availableVariables), resolvedVariables);

            // Recursively resolve any unresolved variables
            variablesToResolve.forEach(variableToResolve -> resolveVariable(variableToResolve, resolvedVariables, assignments, availableVariables));

            // Modify the assignment for the variable : Replace it with the now resolved constituent variables
            assignments.put(variable, replaceExpression(expression, assignments));
            // Mark this variable as resolved
            resolvedVariables.add(variable);
        }

        private void rewriteFilterWithInlinedAssignments(Assignments assignments)
        {
            ImmutableList.Builder<RowExpression> modifiedFilters = ImmutableList.builder();
            filters.forEach(filter -> modifiedFilters.add(replaceExpression(filter, assignments.getMap())));
            filters = modifiedFilters.build();
        }

        private void flattenNode(PlanNode node, int limit, Map<VariableReferenceExpression, RowExpression> assignmentsBuilder)
        {
            PlanNode resolved = lookup.resolve(node);

            if (resolved instanceof ProjectNode) {
                ProjectNode projectNode = (ProjectNode) resolved;
                // A ProjectNode could be 'hiding' a join source by building an assignment of a complex equi-join criteria like `left.key = right1.key1 + right1.key2`
                // We open up the join space by tracking the assignments from this Project node; these will be inlined into the overall filters once we finish
                // traversing the join graph
                // We only do this if the ProjectNode assignments are deterministic
                if (handleComplexEquiJoins && lookup.resolve(projectNode.getSource()) instanceof JoinNode &&
                        projectNode.getAssignments().getExpressions().stream().allMatch(determinismEvaluator::isDeterministic)) {
                    //We keep track of only the non-identity assignments since these are the ones that will be inlined into the overall filters
                    assignmentsBuilder.putAll(getNonIdentityAssignments(projectNode.getAssignments()));
                    flattenNode(projectNode.getSource(), limit, assignmentsBuilder);
                }
                else {
                    sources.add(node);
                }
                return;
            }

            // (limit - 2) because you need to account for adding left and right side
            if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {
                sources.add(node);
                return;
            }

            JoinNode joinNode = (JoinNode) resolved;
            if (joinNode.getType() != INNER || !determinismEvaluator.isDeterministic(joinNode.getFilter().orElse(TRUE_CONSTANT)) || joinNode.getDistributionType().isPresent()) {
                sources.add(node);
                return;
            }

            // we set the left limit to limit - 1 to account for the node on the right
            flattenNode(joinNode.getLeft(), limit - 1, assignmentsBuilder);
            flattenNode(joinNode.getRight(), limit, assignmentsBuilder);
            joinNode.getCriteria().stream()
                    .map(criteria -> toRowExpression(criteria, functionResolution))
                    .forEach(filters::add);
            joinNode.getFilter().ifPresent(filters::add);
        }

        MultiJoinNode toMultiJoinNode()
        {
            ImmutableSet<VariableReferenceExpression> inputVariables = sources.stream().flatMap(source -> source.getOutputVariables().stream()).collect(toImmutableSet());

            // We could have some output variables that were possibly generated from intermediate assignments
            // For each of these variables, use the intermediate assignments to replace this variable with the set of input variables it uses

            // Additionally, we build an overall set of assignments for the reordered Join node - this is used to add a wrapper Project over the updated output variables
            // We do this to satisfy the invariant that the rewritten Join node must produce the same output variables as the input Join node
            ImmutableSet.Builder<VariableReferenceExpression> updatedOutputVariables = ImmutableSet.builder();
            Assignments.Builder overallAssignments = Assignments.builder();
            boolean nonIdentityAssignmentsFound = false;

            for (VariableReferenceExpression outputVariable : outputVariables) {
                if (inputVariables.contains(outputVariable)) {
                    overallAssignments.put(outputVariable, outputVariable);
                    updatedOutputVariables.add(outputVariable);
                    continue;
                }

                checkState(intermediateAssignments.getMap().containsKey(outputVariable),
                        "Output variable [%s] not found in input variables or in intermediate assignments", outputVariable);
                nonIdentityAssignmentsFound = true;
                overallAssignments.put(outputVariable, intermediateAssignments.get(outputVariable));
                updatedOutputVariables.addAll(extractUnique(intermediateAssignments.get(outputVariable)));
            }

            return new MultiJoinNode(sources,
                    and(filters),
                    updatedOutputVariables.build().asList(),
                    nonIdentityAssignmentsFound ? overallAssignments.build() : Assignments.of(), false, Optional.empty());
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

        public Optional<PlanNode> getPlanNode()
        {
            return planNode;
        }

        public PlanCostEstimate getCost()
        {
            return cost;
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
    }
}

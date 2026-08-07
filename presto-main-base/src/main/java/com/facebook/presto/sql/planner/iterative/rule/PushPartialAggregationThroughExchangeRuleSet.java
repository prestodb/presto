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
import com.facebook.presto.cost.PartialAggregationStatsEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getPartialAggregationByteReductionThreshold;
import static com.facebook.presto.SystemSessionProperties.getPartialAggregationStrategy;
import static com.facebook.presto.SystemSessionProperties.isStreamingForPartialAggregationEnabled;
import static com.facebook.presto.SystemSessionProperties.usePartialAggregationHistory;
import static com.facebook.presto.cost.PartialAggregationStatsEstimate.isUnknown;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.operator.aggregation.AggregationUtils.isDecomposable;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.LOW;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.NEVER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Transforms either:
 * <pre>
 *   Aggregation (PARTIAL or SINGLE)
 *       Exchange
 * </pre>
 * or:
 * <pre>
 *   Aggregation (PARTIAL or SINGLE)
 *       Project
 *           Exchange
 * </pre>
 *
 * For the first form (base rule), the partial aggregation is pushed below each source
 * branch of the exchange.
 *
 * For the second form (projection rule), the projection's assignments are first pushed into
 * each source branch of the exchange (with expressions remapped to each source's variable
 * names), and then the same aggregation push-down logic is applied. The projection rule only
 * fires when the base push-down would also succeed — all guards (cost-based, structural, and
 * session-property checks) are evaluated against the original exchange before the transformation
 * is applied.
 */
public class PushPartialAggregationThroughExchangeRuleSet
{
    private static final Capture<ExchangeNode> EXCHANGE_NODE = newCapture();
    private static final Capture<ProjectNode> PROJECT_NODE = newCapture();

    // Pattern: Agg -> Exchange (no ordering scheme)
    private static final Pattern<AggregationNode> WITHOUT_PROJECTION =
            aggregation()
                    .with(source().matching(
                            exchange()
                                    .matching(node -> !node.getOrderingScheme().isPresent())
                                    .capturedAs(EXCHANGE_NODE)));

    // Pattern: Agg -> Project -> Exchange (no ordering scheme)
    private static final Pattern<AggregationNode> WITH_PROJECTION =
            aggregation()
                    .with(source().matching(
                            project().capturedAs(PROJECT_NODE)
                                    .with(source().matching(
                                            exchange()
                                                    .matching(node -> !node.getOrderingScheme().isPresent())
                                                    .capturedAs(EXCHANGE_NODE)))));

    private final FunctionAndTypeManager functionAndTypeManager;
    private final boolean nativeExecution;

    public PushPartialAggregationThroughExchangeRuleSet(FunctionAndTypeManager functionAndTypeManager, boolean nativeExecution)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.nativeExecution = nativeExecution;
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(withoutProjectionRule(), withProjectionRule());
    }

    @VisibleForTesting
    PushPartialAggregationThroughExchange withoutProjectionRule()
    {
        return new PushPartialAggregationThroughExchange();
    }

    @VisibleForTesting
    PushPartialAggregationWithProjectThroughExchange withProjectionRule()
    {
        return new PushPartialAggregationWithProjectThroughExchange();
    }

    /**
     * Base rule: pushes a partial aggregation directly below an exchange.
     */
    @VisibleForTesting
    class PushPartialAggregationThroughExchange
            extends BaseRule
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return WITHOUT_PROJECTION;
        }

        @Override
        public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
        {
            ExchangeNode exchangeNode = captures.get(EXCHANGE_NODE);
            // exchangeNode is both the structural target and the stats source.
            return applyPushdown(aggregationNode, exchangeNode, exchangeNode, context);
        }
    }

    /**
     * Projection rule: handles the case where a ProjectNode sits between the aggregation and the
     * exchange.
     *
     * <p>The rule only fires if the base push-down (without the project) would also succeed.
     * All guards are evaluated against the <em>original</em> exchange so that cost-based checks
     * use pre-computed stats. When the guards pass, the project's computed assignments are pushed
     * into each source branch of the exchange (with expressions remapped to each source's
     * variables via the exchange's own per-branch input mapping), and the aggregation is then
     * pushed through the resulting exchange.
     */
    @VisibleForTesting
    class PushPartialAggregationWithProjectThroughExchange
            extends BaseRule
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return WITH_PROJECTION;
        }

        @Override
        public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
        {
            ProjectNode projectNode = captures.get(PROJECT_NODE);
            ExchangeNode exchangeNode = captures.get(EXCHANGE_NODE);

            // Identify the projection's computed outputs: variables introduced by the project that
            // are not already produced by the exchange.
            ImmutableSet<VariableReferenceExpression> exchangeOutputSet = ImmutableSet.copyOf(exchangeNode.getOutputVariables());
            List<VariableReferenceExpression> computedOutputs = projectNode.getAssignments().getOutputs().stream()
                    .filter(v -> !exchangeOutputSet.contains(v))
                    .collect(toImmutableList());

            if (computedOutputs.isEmpty()) {
                // The project is a pure identity pass-through (RemoveRedundantIdentityProjections would normally eliminate these)
                return applyPushdown(aggregationNode, exchangeNode, exchangeNode, context);
            }

            // Build a new exchange that has the project pushed into each source branch.
            // The original exchange is preserved for guard/stats evaluation (see applyPushdown).
            ExchangeNode newExchange = buildExchangeWithPushedProject(projectNode, computedOutputs, exchangeNode, context);

            // Use the original exchange for guards/stats, the new exchange for the actual push.
            return applyPushdown(aggregationNode, newExchange, exchangeNode, context);
        }
    }

    /**
     * Shared base class that holds all push-down logic and helpers accessible to both inner rules.
     */
    private abstract class BaseRule
            implements Rule<AggregationNode>
    {
        private String statsSource;

        @Override
        public boolean isCostBased(Session session)
        {
            return getPartialAggregationStrategy(session) == AUTOMATIC;
        }

        @Override
        public String getStatsSource()
        {
            return statsSource;
        }

        /**
         * Core push-down entry point.
         *
         * @param exchangeForPush  the exchange (possibly with project pushed in) used for structural
         *                         checks and as the target of the actual push ({@code pushPartial}).
         * @param exchangeForStats the exchange whose pre-computed stats are used for cost-based
         *                         decisions. For the base rule this is the same as
         *                         {@code exchangeForPush}; for the projection rule it is the
         *                         original exchange (before any project was stripped).
         */
        protected Result applyPushdown(
                AggregationNode aggregationNode,
                ExchangeNode exchangeForPush,
                ExchangeNode exchangeForStats,
                Context context)
        {
            boolean decomposable = isDecomposable(aggregationNode, functionAndTypeManager);

            // Special case: SINGLE-step aggregation with mixed grouping sets in a REPARTITION stage.
            // This fixes a broken plan emitted by AddExchanges and always performs a split.
            if (aggregationNode.getStep().equals(SINGLE) &&
                    aggregationNode.hasEmptyGroupingSet() &&
                    aggregationNode.hasNonEmptyGroupingSet() &&
                    exchangeForPush.getType() == REPARTITION) {
                checkState(
                        decomposable,
                        "Distributed aggregation with empty grouping set requires partial but functions are not decomposable");
                return Result.ofPlanNode(split(rebaseAggregation(aggregationNode, exchangeForPush), context));
            }

            PartialAggregationStrategy partialAggregationStrategy = getPartialAggregationStrategy(context.getSession());
            if (!decomposable ||
                    partialAggregationStrategy == NEVER ||
                    partialAggregationStrategy == AUTOMATIC &&
                            partialAggregationNotUseful(aggregationNode, exchangeForStats, context, aggregationNode.getGroupingKeys().size())) {
                return Result.empty();
            }

            // Partial aggregation can only be pushed through exchanges that preserve stream
            // cardinality (gather or repartition).
            if ((exchangeForPush.getType() != GATHER && exchangeForPush.getType() != REPARTITION) ||
                    exchangeForPush.getPartitioningScheme().isReplicateNullsAndAny()) {
                return Result.empty();
            }

            if (exchangeForPush.getType() == REPARTITION) {
                // If partitioning columns are not a subset of grouping keys we cannot push through.
                List<VariableReferenceExpression> partitioningColumns = exchangeForPush.getPartitioningScheme()
                        .getPartitioning()
                        .getArguments()
                        .stream()
                        .filter(VariableReferenceExpression.class::isInstance)
                        .map(VariableReferenceExpression.class::cast)
                        .collect(Collectors.toList());

                if (!aggregationNode.getGroupingKeys().containsAll(partitioningColumns)) {
                    return Result.empty();
                }
            }

            // Pre-computed hash functions are not supported.
            if (aggregationNode.getHashVariable().isPresent() || exchangeForPush.getPartitioningScheme().getHashColumn().isPresent()) {
                return Result.empty();
            }

            // For native execution: do not push past a gather exchange that sits directly on a
            // system-table scan (the partial result from the Java coordinator is not compatible
            // with native workers).
            if (nativeExecution
                    && exchangeForPush.getType() == GATHER
                    && PlannerUtils.directlyOnSystemTableScan(exchangeForPush, context.getLookup())) {
                return Result.empty();
            }

            PlanNode resultNode;
            switch (aggregationNode.getStep()) {
                case SINGLE:
                    // Split into FINAL on top of PARTIAL; the PARTIAL's source must be the
                    // (potentially rewritten) exchange.
                    resultNode = split(rebaseAggregation(aggregationNode, exchangeForPush), context);
                    storeStatsSourceInfo(context, partialAggregationStrategy, aggregationNode);
                    return Result.ofPlanNode(resultNode);
                case PARTIAL:
                    // Push the partial aggregation underneath each branch of the exchange.
                    resultNode = pushPartial(aggregationNode, exchangeForPush, context);
                    storeStatsSourceInfo(context, partialAggregationStrategy, aggregationNode);
                    return Result.ofPlanNode(resultNode);
                default:
                    return Result.empty();
            }
        }

        private void storeStatsSourceInfo(Context context, PartialAggregationStrategy partialAggregationStrategy, PlanNode resultNode)
        {
            if (partialAggregationStrategy == AUTOMATIC) {
                statsSource = context.getStatsProvider().getStats(resultNode).getSourceInfo().getSourceInfoName();
            }
        }

        private PlanNode pushPartial(AggregationNode aggregation, ExchangeNode exchange, Context context)
        {
            List<PlanNode> partials = new ArrayList<>();
            for (int i = 0; i < exchange.getSources().size(); i++) {
                PlanNode source = exchange.getSources().get(i);

                SymbolMapper.Builder mappingsBuilder = SymbolMapper.builder();
                for (int outputIndex = 0; outputIndex < exchange.getOutputVariables().size(); outputIndex++) {
                    VariableReferenceExpression output = exchange.getOutputVariables().get(outputIndex);
                    VariableReferenceExpression input = exchange.getInputs().get(i).get(outputIndex);
                    if (!output.equals(input)) {
                        mappingsBuilder.put(output, input);
                    }
                }

                SymbolMapper symbolMapper = mappingsBuilder.build();
                AggregationNode mappedPartial = symbolMapper.map(aggregation, source, context.getIdAllocator());

                Assignments.Builder assignments = Assignments.builder();
                for (VariableReferenceExpression output : aggregation.getOutputVariables()) {
                    VariableReferenceExpression input = symbolMapper.map(output);
                    assignments.put(output, input);
                }
                partials.add(new ProjectNode(exchange.getSourceLocation(), context.getIdAllocator().getNextId(), mappedPartial, assignments.build(), LOCAL));
            }

            for (PlanNode node : partials) {
                verify(aggregation.getOutputVariables().equals(node.getOutputVariables()));
            }
            // Since this exchange source is now guaranteed to have the same symbols as the inputs to the partial
            // aggregation, we don't need to rewrite symbols in the partitioning function
            List<VariableReferenceExpression> aggregationOutputs = aggregation.getOutputVariables();
            PartitioningScheme partitioning = new PartitioningScheme(
                    exchange.getPartitioningScheme().getPartitioning(),
                    aggregationOutputs,
                    exchange.getPartitioningScheme().getHashColumn(),
                    exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                    exchange.getPartitioningScheme().isScaleWriters(),
                    exchange.getPartitioningScheme().getEncoding(),
                    exchange.getPartitioningScheme().getBucketToPartition());

            return new ExchangeNode(
                    aggregation.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    exchange.getType(),
                    exchange.getScope(),
                    partitioning,
                    partials,
                    ImmutableList.copyOf(Collections.nCopies(partials.size(), aggregationOutputs)),
                    exchange.isEnsureSourceOrdering(),
                    Optional.empty());
        }

        private PlanNode split(AggregationNode node, Context context)
        {
            // Add a partial and final with an exchange in between
            Map<VariableReferenceExpression, AggregationNode.Aggregation> intermediateAggregation = new LinkedHashMap<>();
            Map<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregation = new LinkedHashMap<>();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation originalAggregation = entry.getValue();
                String functionName = functionAndTypeManager.getFunctionMetadata(originalAggregation.getFunctionHandle()).getName().getObjectName();
                FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
                AggregationFunctionImplementation function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);
                VariableReferenceExpression intermediateVariable = context.getVariableAllocator().newVariable(entry.getValue().getCall().getSourceLocation(), functionName, function.getIntermediateType());

                checkState(!originalAggregation.getOrderBy().isPresent(), "Aggregate with ORDER BY does not support partial aggregation");
                intermediateAggregation.put(intermediateVariable, new AggregationNode.Aggregation(
                        new CallExpression(
                                originalAggregation.getCall().getSourceLocation(),
                                functionName,
                                functionHandle,
                                function.getIntermediateType(),
                                originalAggregation.getArguments()),
                        originalAggregation.getFilter(),
                        originalAggregation.getOrderBy(),
                        originalAggregation.isDistinct(),
                        originalAggregation.getMask()));

                // rewrite final aggregation in terms of intermediate function
                finalAggregation.put(entry.getKey(),
                        new AggregationNode.Aggregation(
                                new CallExpression(
                                        originalAggregation.getCall().getSourceLocation(),
                                        functionName,
                                        functionHandle,
                                        function.getFinalType(),
                                        ImmutableList.<RowExpression>builder()
                                                .add(intermediateVariable)
                                                .addAll(originalAggregation.getArguments()
                                                        .stream()
                                                        .filter(PushPartialAggregationThroughExchangeRuleSet::isLambda)
                                                        .collect(toImmutableList()))
                                                .build()),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.empty()));
            }

            // We can always enable streaming aggregation for partial aggregations. But if the table is not pre-group by the groupby columns, it may have regressions.
            // This session property is just a solution to force enabling when we know the execution would benefit from partial streaming aggregation.
            // We can work on determining it based on the input table properties later.
            List<VariableReferenceExpression> preGroupedSymbols = ImmutableList.of();
            if (isStreamingForPartialAggregationEnabled(context.getSession())) {
                preGroupedSymbols = ImmutableList.copyOf(node.getGroupingSets().getGroupingKeys());
            }

            Integer aggregationId = Integer.parseInt(context.getIdAllocator().getNextId().getId());
            PlanNode partial = new AggregationNode(
                    node.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    node.getSource(),
                    intermediateAggregation,
                    node.getGroupingSets(),
                    // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                    // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                    preGroupedSymbols,
                    PARTIAL,
                    node.getHashVariable(),
                    node.getGroupIdVariable(),
                    Optional.of(aggregationId));

            return new AggregationNode(
                    node.getSourceLocation(),
                    node.getId(),
                    partial,
                    finalAggregation,
                    node.getGroupingSets(),
                    // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                    // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                    ImmutableList.of(),
                    FINAL,
                    node.getHashVariable(),
                    node.getGroupIdVariable(),
                    Optional.of(aggregationId));
        }

        private boolean partialAggregationNotUseful(AggregationNode aggregationNode, ExchangeNode exchangeNode, Context context, int numAggregationKeys)
        {
            StatsProvider stats = context.getStatsProvider();
            PlanNodeStatsEstimate exchangeStats = stats.getStats(exchangeNode);
            PlanNodeStatsEstimate aggregationStats = stats.getStats(aggregationNode);
            double inputSize = exchangeStats.getOutputSizeInBytes(exchangeNode);
            double outputSize = aggregationStats.getOutputSizeInBytes(aggregationNode);
            PartialAggregationStatsEstimate partialAggregationStatsEstimate = aggregationStats.getPartialAggregationStatsEstimate();
            ConfidenceLevel confidenceLevel = exchangeStats.confidenceLevel();
            // keep old behavior of skipping partial aggregation only for single-key aggregations
            boolean numberOfKeyCheck = usePartialAggregationHistory(context.getSession()) || numAggregationKeys == 1;
            if (!isUnknown(partialAggregationStatsEstimate) && usePartialAggregationHistory(context.getSession())) {
                confidenceLevel = aggregationStats.confidenceLevel();
                // use rows instead of bytes when use_partial_aggregation_history flag is on
                inputSize = partialAggregationStatsEstimate.getInputRowCount();
                outputSize = partialAggregationStatsEstimate.getOutputRowCount();
            }
            double byteReductionThreshold = getPartialAggregationByteReductionThreshold(context.getSession());

            // calling this function means we are using a cost-based strategy for this optimization
            return numberOfKeyCheck && confidenceLevel != LOW && outputSize > inputSize * byteReductionThreshold;
        }
    }

    /**
     * Rebuilds an aggregation node pointing at a new source, preserving all other properties.
     * Used to ensure that {@code split()} uses the correct (possibly rewritten) exchange as the
     * source of the PARTIAL aggregation it emits.
     */
    private static AggregationNode rebaseAggregation(AggregationNode aggregation, PlanNode newSource)
    {
        return new AggregationNode(
                aggregation.getSourceLocation(),
                aggregation.getId(),
                newSource,
                aggregation.getAggregations(),
                aggregation.getGroupingSets(),
                aggregation.getPreGroupedVariables(),
                aggregation.getStep(),
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable(),
                aggregation.getAggregationId());
    }

    /**
     * Pushes {@code projectNode}'s computed assignments into every source branch of
     * {@code originalExchange}, returning a new ExchangeNode whose output-variable list and
     * per-source input lists include the newly computed variables.
     *
     * <p>For each source branch {@code i} the projection expressions are remapped from the
     * exchange's output-variable names to that branch's own input-variable names using the
     * exchange's existing per-branch mapping ({@code exchange.getInputs().get(i)}).
     */
    private ExchangeNode buildExchangeWithPushedProject(
            ProjectNode projectNode,
            List<VariableReferenceExpression> computedOutputs,
            ExchangeNode originalExchange,
            Rule.Context context)
    {
        List<PlanNode> newSources = new ArrayList<>();
        List<List<VariableReferenceExpression>> newInputs = new ArrayList<>();

        for (int i = 0; i < originalExchange.getSources().size(); i++) {
            PlanNode source = originalExchange.getSources().get(i);

            // Build the per-source variable mapping: outputVar[j] → inputVar[i][j]
            SymbolMapper.Builder mappingsBuilder = SymbolMapper.builder();
            for (int j = 0; j < originalExchange.getOutputVariables().size(); j++) {
                VariableReferenceExpression output = originalExchange.getOutputVariables().get(j);
                VariableReferenceExpression input = originalExchange.getInputs().get(i).get(j);
                if (!output.equals(input)) {
                    mappingsBuilder.put(output, input);
                }
            }
            SymbolMapper symbolMapper = mappingsBuilder.build();

            // Build the ProjectNode assignments for this source:
            //  1. Identity pass-throughs for every variable the source already produces.
            //  2. Remapped computed assignments (expressions remapped to source variable names).
            Assignments.Builder assignments = Assignments.builder();
            for (VariableReferenceExpression var : source.getOutputVariables()) {
                assignments.put(var, var);
            }
            for (VariableReferenceExpression computedVar : computedOutputs) {
                RowExpression expr = projectNode.getAssignments().get(computedVar);
                assignments.put(computedVar, symbolMapper.map(expr));
            }

            newSources.add(new ProjectNode(
                    projectNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    source,
                    assignments.build(),
                    projectNode.getLocality()));

            // This source now also produces the computed variables.
            newInputs.add(ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(originalExchange.getInputs().get(i))
                    .addAll(computedOutputs)
                    .build());
        }

        List<VariableReferenceExpression> newOutputVariables = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(originalExchange.getOutputVariables())
                .addAll(computedOutputs)
                .build();

        PartitioningScheme newPartitioningScheme = new PartitioningScheme(
                originalExchange.getPartitioningScheme().getPartitioning(),
                newOutputVariables,
                originalExchange.getPartitioningScheme().getHashColumn(),
                originalExchange.getPartitioningScheme().isReplicateNullsAndAny(),
                originalExchange.getPartitioningScheme().isScaleWriters(),
                originalExchange.getPartitioningScheme().getEncoding(),
                originalExchange.getPartitioningScheme().getBucketToPartition());

        return new ExchangeNode(
                originalExchange.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                originalExchange.getType(),
                originalExchange.getScope(),
                newPartitioningScheme,
                newSources,
                newInputs,
                originalExchange.isEnsureSourceOrdering(),
                Optional.empty());
    }

    private static boolean isLambda(RowExpression rowExpression)
    {
        return rowExpression instanceof LambdaDefinitionExpression;
    }
}

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
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getPartialAggregationByteReductionThreshold;
import static com.facebook.presto.SystemSessionProperties.getPartialAggregationStrategy;
import static com.facebook.presto.SystemSessionProperties.isStreamingForPartialAggregationEnabled;
import static com.facebook.presto.SystemSessionProperties.usePartialAggregationHistory;
import static com.facebook.presto.cost.PartialAggregationStatsEstimate.isUnknown;
import static com.facebook.presto.operator.aggregation.AggregationUtils.isDecomposable;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.LOW;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.NEVER;
import static com.facebook.presto.sql.planner.PlannerUtils.containsSystemTableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PushPartialAggregationThroughExchange
        implements Rule<AggregationNode>
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final boolean nativeExecution;
    private String statsSource;

    public PushPartialAggregationThroughExchange(FunctionAndTypeManager functionAndTypeManager, boolean nativeExecution)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.nativeExecution = nativeExecution;
    }

    private static final Capture<ExchangeNode> EXCHANGE_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(
                    exchange()
                            .matching(node -> !node.getOrderingScheme().isPresent())
                            .capturedAs(EXCHANGE_NODE)));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

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

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        ExchangeNode exchangeNode = captures.get(EXCHANGE_NODE);

        boolean decomposable = isDecomposable(aggregationNode, functionAndTypeManager);

        if (aggregationNode.getStep().equals(SINGLE) &&
                aggregationNode.hasEmptyGroupingSet() &&
                aggregationNode.hasNonEmptyGroupingSet() &&
                exchangeNode.getType() == REPARTITION) {
            // single-step aggregation w/ empty grouping sets in a partitioned stage, so we need a partial that will produce
            // the default intermediates for the empty grouping set that will be routed to the appropriate final aggregation.
            // TODO: technically, AddExchanges generates a broken plan that this rule "fixes"
            checkState(
                    decomposable,
                    "Distributed aggregation with empty grouping set requires partial but functions are not decomposable");
            return Result.ofPlanNode(split(aggregationNode, context));
        }

        PartialAggregationStrategy partialAggregationStrategy = getPartialAggregationStrategy(context.getSession());
        if (!decomposable ||
                partialAggregationStrategy == NEVER ||
                partialAggregationStrategy == AUTOMATIC &&
                        partialAggregationNotUseful(aggregationNode, exchangeNode, context, aggregationNode.getGroupingKeys().size())) {
            return Result.empty();
        }

        // partial aggregation can only be pushed through exchange that doesn't change
        // the cardinality of the stream (i.e., gather or repartition)
        if ((exchangeNode.getType() != GATHER && exchangeNode.getType() != REPARTITION) ||
                exchangeNode.getPartitioningScheme().isReplicateNullsAndAny()) {
            return Result.empty();
        }

        if (exchangeNode.getType() == REPARTITION) {
            // if partitioning columns are not a subset of grouping keys,
            // we can't push this through
            List<VariableReferenceExpression> partitioningColumns = exchangeNode.getPartitioningScheme()
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

        // currently, we only support plans that don't use pre-computed hash functions
        if (aggregationNode.getHashVariable().isPresent() || exchangeNode.getPartitioningScheme().getHashColumn().isPresent()) {
            return Result.empty();
        }

        // System table scan must be run in Java on coordinator and partial aggregation output may not be compatible with Velox
        if (nativeExecution && containsSystemTableScan(exchangeNode, context.getLookup())) {
            return Result.empty();
        }

        PlanNode resultNode = null;
        switch (aggregationNode.getStep()) {
            case SINGLE:
                // Split it into a FINAL on top of a PARTIAL and
                resultNode = split(aggregationNode, context);
                storeStatsSourceInfo(context, partialAggregationStrategy, aggregationNode);
                return Result.ofPlanNode(resultNode);
            case PARTIAL:
                // Push it underneath each branch of the exchange
                resultNode = pushPartial(aggregationNode, exchangeNode, context);
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
        // otherwise, add a partial and final with an exchange in between
        Map<VariableReferenceExpression, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
        Map<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
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
                                                    .filter(PushPartialAggregationThroughExchange::isLambda)
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

    private static boolean isLambda(RowExpression rowExpression)
    {
        return rowExpression instanceof LambdaDefinitionExpression;
    }
}

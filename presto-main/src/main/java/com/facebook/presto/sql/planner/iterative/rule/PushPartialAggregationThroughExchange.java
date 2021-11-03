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

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy;
import com.facebook.presto.sql.planner.PartitioningScheme;
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
import static com.facebook.presto.operator.aggregation.AggregationUtils.isDecomposable;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.NEVER;
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

    public PushPartialAggregationThroughExchange(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
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
                        partialAggregationNotUseful(aggregationNode, exchangeNode, context) &&
                        aggregationNode.getGroupingKeys().size() == 1) {
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

        switch (aggregationNode.getStep()) {
            case SINGLE:
                // Split it into a FINAL on top of a PARTIAL and
                return Result.ofPlanNode(split(aggregationNode, context));
            case PARTIAL:
                // Push it underneath each branch of the exchange
                return Result.ofPlanNode(pushPartial(aggregationNode, exchangeNode, context));
            default:
                return Result.empty();
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
            partials.add(new ProjectNode(context.getIdAllocator().getNextId(), mappedPartial, assignments.build(), LOCAL));
        }

        for (PlanNode node : partials) {
            verify(aggregation.getOutputVariables().equals(node.getOutputVariables()));
        }
        // Since this exchange source is now guaranteed to have the same symbols as the inputs to the the partial
        // aggregation, we don't need to rewrite symbols in the partitioning function
        List<VariableReferenceExpression> aggregationOutputs = aggregation.getOutputVariables();
        PartitioningScheme partitioning = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                aggregationOutputs,
                exchange.getPartitioningScheme().getHashColumn(),
                exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                exchange.getPartitioningScheme().getBucketToPartition());

        return new ExchangeNode(
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
            InternalAggregationFunction function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);
            VariableReferenceExpression intermediateVariable = context.getVariableAllocator().newVariable(functionName, function.getIntermediateType());

            checkState(!originalAggregation.getOrderBy().isPresent(), "Aggregate with ORDER BY does not support partial aggregation");
            intermediateAggregation.put(intermediateVariable, new AggregationNode.Aggregation(
                    new CallExpression(
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

        PlanNode partial = new AggregationNode(
                context.getIdAllocator().getNextId(),
                node.getSource(),
                intermediateAggregation,
                node.getGroupingSets(),
                // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                ImmutableList.of(),
                PARTIAL,
                node.getHashVariable(),
                node.getGroupIdVariable());

        return new AggregationNode(
                node.getId(),
                partial,
                finalAggregation,
                node.getGroupingSets(),
                // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                ImmutableList.of(),
                FINAL,
                node.getHashVariable(),
                node.getGroupIdVariable());
    }

    private boolean partialAggregationNotUseful(AggregationNode aggregationNode, ExchangeNode exchangeNode, Context context)
    {
        StatsProvider stats = context.getStatsProvider();
        PlanNodeStatsEstimate exchangeStats = stats.getStats(exchangeNode);
        PlanNodeStatsEstimate aggregationStats = stats.getStats(aggregationNode);
        double inputBytes = exchangeStats.getOutputSizeInBytes(exchangeNode.getOutputVariables());
        double outputBytes = aggregationStats.getOutputSizeInBytes(aggregationNode.getOutputVariables());
        double byteReductionThreshold = getPartialAggregationByteReductionThreshold(context.getSession());

        return exchangeStats.isConfident() && outputBytes > inputBytes * byteReductionThreshold;
    }

    private static boolean isLambda(RowExpression rowExpression)
    {
        return rowExpression instanceof LambdaDefinitionExpression;
    }
}

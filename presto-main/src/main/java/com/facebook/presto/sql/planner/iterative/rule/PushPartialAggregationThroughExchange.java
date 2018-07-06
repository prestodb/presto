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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.preferPartialAggregation;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PushPartialAggregationThroughExchange
        implements Rule<AggregationNode>
{
    private final FunctionRegistry functionRegistry;

    public PushPartialAggregationThroughExchange(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
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

        boolean decomposable = aggregationNode.isDecomposable(functionRegistry);

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

        if (!decomposable || !preferPartialAggregation(context.getSession())) {
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
            List<Symbol> partitioningColumns = exchangeNode.getPartitioningScheme()
                    .getPartitioning()
                    .getArguments()
                    .stream()
                    .filter(Partitioning.ArgumentBinding::isVariable)
                    .map(Partitioning.ArgumentBinding::getColumn)
                    .collect(Collectors.toList());

            if (!aggregationNode.getGroupingKeys().containsAll(partitioningColumns)) {
                return Result.empty();
            }
        }

        // currently, we only support plans that don't use pre-computed hash functions
        if (aggregationNode.getHashSymbol().isPresent() || exchangeNode.getPartitioningScheme().getHashColumn().isPresent()) {
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
            for (int outputIndex = 0; outputIndex < exchange.getOutputSymbols().size(); outputIndex++) {
                Symbol output = exchange.getOutputSymbols().get(outputIndex);
                Symbol input = exchange.getInputs().get(i).get(outputIndex);
                if (!output.equals(input)) {
                    mappingsBuilder.put(output, input);
                }
            }

            SymbolMapper symbolMapper = mappingsBuilder.build();
            AggregationNode mappedPartial = symbolMapper.map(aggregation, source, context.getIdAllocator());

            Assignments.Builder assignments = Assignments.builder();

            for (Symbol output : aggregation.getOutputSymbols()) {
                Symbol input = symbolMapper.map(output);
                assignments.put(output, input.toSymbolReference());
            }
            partials.add(new ProjectNode(context.getIdAllocator().getNextId(), mappedPartial, assignments.build()));
        }

        for (PlanNode node : partials) {
            verify(aggregation.getOutputSymbols().equals(node.getOutputSymbols()));
        }

        // Since this exchange source is now guaranteed to have the same symbols as the inputs to the the partial
        // aggregation, we don't need to rewrite symbols in the partitioning function
        PartitioningScheme partitioning = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                aggregation.getOutputSymbols(),
                exchange.getPartitioningScheme().getHashColumn(),
                exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                exchange.getPartitioningScheme().getBucketToPartition());

        return new ExchangeNode(
                context.getIdAllocator().getNextId(),
                exchange.getType(),
                exchange.getScope(),
                partitioning,
                partials,
                ImmutableList.copyOf(Collections.nCopies(partials.size(), aggregation.getOutputSymbols())),
                Optional.empty());
    }

    private PlanNode split(AggregationNode node, Context context)
    {
        // otherwise, add a partial and final with an exchange in between
        Map<Symbol, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
        Map<Symbol, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            AggregationNode.Aggregation originalAggregation = entry.getValue();
            Signature signature = originalAggregation.getSignature();
            InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(signature);
            Symbol intermediateSymbol = context.getSymbolAllocator().newSymbol(signature.getName(), function.getIntermediateType());

            checkState(!originalAggregation.getCall().getOrderBy().isPresent(), "Aggregate with ORDER BY does not support partial aggregation");
            intermediateAggregation.put(intermediateSymbol, new AggregationNode.Aggregation(originalAggregation.getCall(), signature, originalAggregation.getMask()));

            // rewrite final aggregation in terms of intermediate function
            finalAggregation.put(entry.getKey(),
                    new AggregationNode.Aggregation(
                            new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.of(intermediateSymbol.toSymbolReference())),
                            signature,
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
                node.getHashSymbol(),
                node.getGroupIdSymbol());

        return new AggregationNode(
                node.getId(),
                partial,
                finalAggregation,
                node.getGroupingSets(),
                // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                ImmutableList.of(),
                FINAL,
                node.getHashSymbol(),
                node.getGroupIdSymbol());
    }
}

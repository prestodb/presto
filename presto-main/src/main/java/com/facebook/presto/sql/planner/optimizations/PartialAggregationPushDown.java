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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PartialAggregationPushDown
        implements PlanOptimizer
{
    private final FunctionRegistry functionRegistry;

    public PartialAggregationPushDown(FunctionRegistry registry)
    {
        requireNonNull(registry, "registry is null");

        this.functionRegistry = registry;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(symbolAllocator, idAllocator), plan, null);
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final SymbolAllocator allocator;
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(SymbolAllocator allocator, PlanNodeIdAllocator idAllocator)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode child = node.getSource();

            if (!(child instanceof ExchangeNode)) {
                return context.defaultRewrite(node);
            }

            // partial aggregation can only be pushed through exchange that doesn't change
            // the cardinality of the stream (i.e., gather or repartition)
            ExchangeNode exchange = (ExchangeNode) child;
            if ((exchange.getType() != GATHER && exchange.getType() != REPARTITION) ||
                    exchange.getPartitioningScheme().isReplicateNulls()) {
                return context.defaultRewrite(node);
            }

            if (exchange.getType() == REPARTITION) {
                // if partitioning columns are not a subset of grouping keys,
                // we can't push this through
                List<Symbol> partitioningColumns = exchange.getPartitioningScheme()
                        .getPartitioning()
                        .getArguments()
                        .stream()
                        .filter(Partitioning.ArgumentBinding::isVariable)
                        .map(Partitioning.ArgumentBinding::getColumn)
                        .collect(Collectors.toList());

                if (!node.getGroupingKeys().containsAll(partitioningColumns)) {
                    return context.defaultRewrite(node);
                }
            }

            // currently, we only support plans that don't use pre-computed hash functions
            if (node.getHashSymbol().isPresent() || exchange.getPartitioningScheme().getHashColumn().isPresent()) {
                return context.defaultRewrite(node);
            }

            boolean decomposable = node.getFunctions().values().stream()
                    .map(functionRegistry::getAggregateFunctionImplementation)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            if (!decomposable) {
                return context.defaultRewrite(node);
            }

            switch (node.getStep()) {
                case SINGLE:
                    // Split it into a FINAL on top of a PARTIAL and
                    // reprocess the resulting plan to push the partial
                    // below the exchange (see case below).
                    return context.rewrite(split(node));
                case PARTIAL:
                    // Push it underneath each branch of the exchange
                    // and reprocess in case it can be pushed further down
                    // (e.g., if there are local/remote exchanges stacked)
                    return context.rewrite(pushPartial(node, exchange));
                default:
                    return context.defaultRewrite(node);
            }
        }

        private PlanNode pushPartial(AggregationNode partial, ExchangeNode exchange)
        {
            List<PlanNode> partials = new ArrayList<>();
            for (int i = 0; i < exchange.getSources().size(); i++) {
                PlanNode source = exchange.getSources().get(i);

                if (!exchange.getOutputSymbols().equals(exchange.getInputs().get(i))) {
                    // Add an identity projection to preserve the inputs to the aggregation, if necessary.
                    // This allows us to avoid having to rewrite the symbols in the aggregation node below.
                    ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
                    for (int outputIndex = 0; outputIndex < exchange.getOutputSymbols().size(); outputIndex++) {
                        Symbol output = exchange.getOutputSymbols().get(outputIndex);
                        Symbol input = exchange.getInputs().get(i).get(outputIndex);
                        assignments.put(output, input.toSymbolReference());
                    }

                    source = new ProjectNode(idAllocator.getNextId(), source, assignments.build());
                }

                // Since this exchange source is now guaranteed to have the same symbols as the inputs to the the partial
                // aggregation, we can build a new AggregationNode without any further symbol rewrites
                partials.add(new AggregationNode(
                        idAllocator.getNextId(),
                        source,
                        partial.getAggregations(),
                        partial.getFunctions(),
                        partial.getMasks(),
                        partial.getGroupingSets(),
                        partial.getStep(),
                        partial.getHashSymbol(),
                        partial.getGroupIdSymbol()));
            }

            for (PlanNode node : partials) {
                verify(partial.getOutputSymbols().equals(node.getOutputSymbols()));
            }

            // Since this exchange source is now guaranteed to have the same symbols as the inputs to the the partial
            // aggregation, we don't need to rewrite symbols in the partitioning function
            PartitioningScheme partitioning = new PartitioningScheme(
                    exchange.getPartitioningScheme().getPartitioning(),
                    partial.getOutputSymbols(),
                    exchange.getPartitioningScheme().getHashColumn(),
                    exchange.getPartitioningScheme().isReplicateNulls(),
                    exchange.getPartitioningScheme().getBucketToPartition());

            return new ExchangeNode(
                    idAllocator.getNextId(),
                    exchange.getType(),
                    exchange.getScope(),
                    partitioning,
                    partials,
                    ImmutableList.copyOf(Collections.nCopies(partials.size(), partial.getOutputSymbols())));
        }

        private PlanNode split(AggregationNode node)
        {
            // otherwise, add a partial and final with an exchange in between
            Map<Symbol, Symbol> masks = node.getMasks();

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
            Map<Symbol, Symbol> intermediateMask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());
                InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(signature);

                Symbol intermediateSymbol = allocator.newSymbol(signature.getName(), function.getIntermediateType());
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    intermediateMask.put(intermediateSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.of(intermediateSymbol.toSymbolReference())));
            }

            PlanNode partial = new AggregationNode(
                    idAllocator.getNextId(),
                    node.getSource(),
                    intermediateCalls,
                    intermediateFunctions,
                    intermediateMask,
                    node.getGroupingSets(),
                    PARTIAL,
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());

            return new AggregationNode(
                    node.getId(),
                    partial,
                    finalCalls,
                    node.getFunctions(),
                    ImmutableMap.of(),
                    node.getGroupingSets(),
                    FINAL,
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }
    }
}

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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PartialAggregationPushDown
        implements PlanOptimizer
{
    private final FunctionRegistry functionRegistry;

    public PartialAggregationPushDown(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.functionRegistry = metadata.getFunctionRegistry();
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(symbolAllocator, idAllocator), plan, null);
    }

    private class Rewriter
            extends SimplePlanRewriter<AggregationNode>
    {
        private final SymbolAllocator allocator;
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(SymbolAllocator allocator, PlanNodeIdAllocator idAllocator)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<AggregationNode> context)
        {
            boolean decomposable = node.getFunctions().values().stream()
                    .map(functionRegistry::getAggregateFunctionImplementation)
                    .allMatch(InternalAggregationFunction::isDecomposable);
            checkState(node.getStep() == SINGLE, "aggregation should be SINGLE, but it is %s", node.getStep());
            checkState(context.get() == null, "context is not null: %s", context);
            if (!decomposable || !allowPushThrough(node.getSource())) {
                return context.defaultRewrite(node);
            }

            Map<Symbol, Symbol> masks = node.getMasks();

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
            Map<Symbol, Symbol> intermediateMask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());

                Symbol intermediateSymbol = generateIntermediateSymbol(signature);
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    intermediateMask.put(intermediateSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.of(intermediateSymbol.toSymbolReference())));
            }

            AggregationNode partial = new AggregationNode(
                    idAllocator.getNextId(),
                    node.getSource(),
                    intermediateCalls,
                    intermediateFunctions,
                    intermediateMask,
                    node.getGroupingSets(),
                    PARTIAL,
                    node.getSampleWeight(),
                    node.getConfidence(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());

            return new AggregationNode(
                    node.getId(),
                    context.rewrite(node.getSource(), partial),
                    finalCalls,
                    node.getFunctions(),
                    ImmutableMap.of(),
                    node.getGroupingSets(),
                    FINAL,
                    Optional.empty(),
                    node.getConfidence(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<AggregationNode> context)
        {
            AggregationNode partial = context.get();
            if (partial == null) {
                return context.defaultRewrite(node);
            }

            List<PlanNode> newChildren = new ArrayList<>();
            List<List<Symbol>> inputs = new ArrayList<>();

            boolean allowPushThroughChildren = node.getSources().stream().allMatch(this::allowPushThrough);
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode currentSource = node.getSources().get(i);
                Map<Symbol, Symbol> exchangeMap = buildExchangeMap(node.getOutputSymbols(), node.getInputs().get(i));
                AggregationWithLayout childPartial = generateNewPartial(partial, currentSource, exchangeMap);
                inputs.add(childPartial.getLayout());
                PlanNode child;
                if (allowPushThroughChildren) {
                    child = context.rewrite(currentSource, childPartial.getAggregationNode());
                }
                else {
                    child = context.defaultRewrite(childPartial.getAggregationNode());
                }
                newChildren.add(child);
            }
            PartitioningScheme partitioningScheme = new PartitioningScheme(
                    node.getPartitioningScheme().getPartitioning(),
                    partial.getOutputSymbols(),
                    partial.getHashSymbol());
            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    partitioningScheme,
                    newChildren,
                    inputs);
        }

        private boolean allowPushThrough(PlanNode node)
        {
            if (node instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) node;
                return exchangeNode.getType() != REPLICATE && !exchangeNode.getPartitioningScheme().isReplicateNulls();
            }
            return false;
        }

        private Symbol generateIntermediateSymbol(Signature signature)
        {
            InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(signature);
            return allocator.newSymbol(signature.getName(), function.getIntermediateType());
        }

        private Map<Symbol, Symbol> buildExchangeMap(List<Symbol> exchangeOutput, List<Symbol> sourceOutput)
        {
            checkState(exchangeOutput.size() == sourceOutput.size(), "exchange output length doesn't match source output length");
            ImmutableMap.Builder<Symbol, Symbol> builder = ImmutableMap.builder();
            for (int i = 0; i < exchangeOutput.size(); i++) {
                builder.put(exchangeOutput.get(i), sourceOutput.get(i));
            }
            return builder.build();
        }

        private List<Expression> replaceArguments(List<Expression> arguments, Map<Symbol, Symbol> exchangeMap)
        {
            Map<SymbolReference, SymbolReference> symbolReferenceSymbolMap = new HashMap<>();
            for (Map.Entry<Symbol, Symbol> entry : exchangeMap.entrySet()) {
                symbolReferenceSymbolMap.put(entry.getKey().toSymbolReference(), entry.getValue().toSymbolReference());
            }
            return arguments.stream()
                    .map(expression -> {
                        if (symbolReferenceSymbolMap.containsKey(expression)) {
                            return symbolReferenceSymbolMap.get(expression);
                        }
                        return expression;
                    })
                    .collect(toList());
        }

        // generate new partial aggregation for each exchange branch with renamed symbols
        private AggregationWithLayout generateNewPartial(AggregationNode node, PlanNode source, Map<Symbol, Symbol> exchangeMap)
        {
            checkState(!node.getHashSymbol().isPresent(), "PartialAggregationPushDown optimizer must run before HashGenerationOptimizer");

            // Store the symbol mapping from old aggregation output to new aggregation output
            Map<Symbol, Symbol> layoutMap = new HashMap<>();

            Map<Symbol, FunctionCall> functionCallMap = new HashMap<>();
            Map<Symbol, Signature> signatureMap = new HashMap<>();
            Map<Symbol, Symbol> mask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());
                Symbol symbol = generateIntermediateSymbol(signature);

                signatureMap.put(symbol, node.getFunctions().get(entry.getKey()));

                List<Expression> arguments = replaceArguments(entry.getValue().getArguments(), exchangeMap);
                functionCallMap.put(symbol, new FunctionCall(entry.getValue().getName(), arguments));
                if (node.getMasks().containsKey(entry.getKey())) {
                    mask.put(symbol, exchangeMap.get(node.getMasks().get(entry.getKey())));
                }

                layoutMap.put(entry.getKey(), symbol);
            }

            // put group by keys in map
            for (Symbol groupBySymbol : node.getGroupingKeys()) {
                Symbol newGroupBySymbol = exchangeMap.get(groupBySymbol);
                layoutMap.put(groupBySymbol, newGroupBySymbol);
            }

            // translate grouping sets
            ImmutableList.Builder<List<Symbol>> groupingSets = ImmutableList.builder();
            for (List<Symbol> symbols : node.getGroupingSets()) {
                ImmutableList.Builder<Symbol> symbolList = ImmutableList.builder();
                for (Symbol symbol : symbols) {
                    Symbol translated = exchangeMap.get(symbol);
                    symbolList.add(translated);
                }
                groupingSets.add(symbolList.build());
            }

            AggregationNode partial = new AggregationNode(
                    idAllocator.getNextId(),
                    source,
                    functionCallMap,
                    signatureMap,
                    mask,
                    groupingSets.build(),
                    PARTIAL,
                    node.getSampleWeight(),
                    node.getConfidence(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol().map(exchangeMap::get));

            // generate the output layout according to the order of pre-pushed aggregation's output
            List<Symbol> layout = node.getOutputSymbols().stream()
                    .map(layoutMap::get)
                    .collect(toList());

            return new AggregationWithLayout(partial, layout);
        }
    }

    private static class AggregationWithLayout
    {
        private final AggregationNode aggregationNode;
        private final List<Symbol> layout;

        public AggregationWithLayout(AggregationNode aggregationNode, List<Symbol> layout)
        {
            this.aggregationNode = requireNonNull(aggregationNode, "aggregationNode is null");
            this.layout = ImmutableList.copyOf(requireNonNull(layout, "layout is null"));
        }

        public AggregationNode getAggregationNode()
        {
            return aggregationNode;
        }

        public List<Symbol> getLayout()
        {
            return layout;
        }
    }
}

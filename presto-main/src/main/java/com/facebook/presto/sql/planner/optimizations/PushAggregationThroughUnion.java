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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PushAggregationThroughUnion
        implements PlanOptimizer
{
    private final FunctionRegistry functionRegistry;

    public PushAggregationThroughUnion(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = functionRegistry;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan);
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        private Map<Symbol, PlanNode> buildOutputSymbolToSourceMap(PlanNode node)
        {
            Map<Symbol, PlanNode> result = new HashMap<>();
            for (PlanNode source : node.getSources()) {
                for (Symbol outputSymbol : source.getOutputSymbols()) {
                    result.put(outputSymbol, source);
                }
            }
            return result;
        }

        private Map<PlanNode, Map<SymbolReference, Symbol>> buildOutputMappingForUnionSources(UnionNode node)
        {
            Map<PlanNode, Map<SymbolReference, Symbol>> result = new HashMap<>();
            Map<Symbol, PlanNode> outputSymbolToSourceMap = buildOutputSymbolToSourceMap(node);
            ListMultimap<Symbol, Symbol> symbolMapping = node.getSymbolMapping();
            for (PlanNode source : node.getSources()) {
                result.put(source, new HashMap<>());
            }
            for (Symbol outputSymbol : symbolMapping.keySet()) {
                List<Symbol> symbols = symbolMapping.get(outputSymbol);
                for (Symbol symbol : symbols) {
                    result.get(outputSymbolToSourceMap.get(symbol)).put(outputSymbol.toSymbolReference(), symbol);
                }
            }
            return result;
        }

        private Map<Symbol, Symbol> buildFinalAggregationSymbolMapping(AggregationNode node)
        {
            Map<Symbol, Symbol> result = new HashMap<>();
            Set<Symbol> groupByKeys = new HashSet<>(node.getGroupBy());
            for (Symbol outputSymbol : node.getOutputSymbols()) {
                Symbol newSymbol = outputSymbol;
                if (!groupByKeys.contains(outputSymbol)) {
                    newSymbol = symbolAllocator.newSymbol(outputSymbol.getName(), symbolAllocator.getTypes().get(outputSymbol));
                }
                result.put(outputSymbol, newSymbol);
            }
            return result;
        }

        private Symbol generateIntermediateSymbol(Signature signature)
        {
            InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(signature);
            return symbolAllocator.newSymbol(signature.getName(), function.getIntermediateType());
        }

        private List<Expression> replaceArguments(List<Expression> arguments, Map<SymbolReference, Symbol> nodeSymbolMap)
        {
            return arguments.stream().map(expression -> {
                if (expression instanceof SymbolReference && nodeSymbolMap.containsKey(expression)) {
                    return nodeSymbolMap.get(expression).toSymbolReference();
                }
                return expression;
            }).collect(toList());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            boolean decomposable = node.getFunctions()
                    .values().stream()
                    .map(functionRegistry::getAggregateFunctionImplementation)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            PlanNode sourceNode = context.rewrite(node.getSource());

            // Only apply this optimization when aggregation over union and aggregations are decomposable
            if (!decomposable || !(sourceNode instanceof UnionNode)) {
                return replaceChildren(node, ImmutableList.of(sourceNode));
            }

            UnionNode unionNode = (UnionNode) sourceNode;

            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();

            // map for {[source node] -> {[union output qualified name reference] -> [source output symbol]}}
            Map<PlanNode, Map<SymbolReference, Symbol>> outputMappingForUnionSources = buildOutputMappingForUnionSources(unionNode);

            // map from symbols in old aggregation node to symbols in new final aggregation node
            Map<Symbol, Symbol> finalAggregationSymbolMapping = buildFinalAggregationSymbolMapping(node);

            Map<Symbol, FunctionCall> aggregations = node.getAggregations();

            // 1. for each union input, create a partial aggregation node upon it
            for (PlanNode unionOriginalSource : unionNode.getSources()) {
                Map<SymbolReference, Symbol> nodeSymbolMap = outputMappingForUnionSources.get(unionOriginalSource);
                Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
                Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
                Map<Symbol, Symbol> intermediateMask = new HashMap<>();
                for (Map.Entry<Symbol, FunctionCall> entry : aggregations.entrySet()) {
                    Signature signature = node.getFunctions().get(entry.getKey());
                    Symbol intermediateSymbol = generateIntermediateSymbol(signature);

                    // Replace the old arguments using the newly created symbols
                    List<Expression> arguments = replaceArguments(entry.getValue().getArguments(), nodeSymbolMap);

                    intermediateCalls.put(intermediateSymbol, new FunctionCall(entry.getValue().getName(), arguments));
                    intermediateFunctions.put(intermediateSymbol, signature);

                    if (node.getMasks().containsKey(entry.getKey())) {
                        intermediateMask.put(intermediateSymbol, node.getMasks().get(entry.getKey()));
                    }
                    mappings.put(finalAggregationSymbolMapping.get(entry.getKey()), intermediateSymbol);
                }

                List<List<Symbol>> intermediateGroupingSets = node.getGroupingSets()
                        .stream()
                        .map(symbolList ->
                                symbolList.stream()
                                        .map(symbol -> nodeSymbolMap.get(symbol.toSymbolReference()))
                                        .collect(toList()))
                        .collect(toList());

                List<Symbol> intermediateGroupBy = node.getGroupBy()
                        .stream()
                        .map(key -> nodeSymbolMap.get(key.toSymbolReference()))
                        .collect(toList());

                for (Symbol groupBySymbol : node.getGroupBy()) {
                    mappings.put(finalAggregationSymbolMapping.get(groupBySymbol), nodeSymbolMap.get(groupBySymbol.toSymbolReference()));
                }

                rewrittenSources.add(new AggregationNode(
                        idAllocator.getNextId(),
                        unionOriginalSource,
                        intermediateGroupBy,
                        intermediateCalls,
                        intermediateFunctions,
                        intermediateMask,
                        intermediateGroupingSets,
                        PARTIAL,
                        node.getSampleWeight(),
                        node.getConfidence(),
                        node.getHashSymbol()));
            }

            // 2. create a union node on top of the aggregations
            UnionNode pushedUnionNode = new UnionNode(idAllocator.getNextId(), rewrittenSources.build(), mappings.build(), ImmutableList.copyOf(mappings.build().keySet()));

            // 3. build final aggregation functions for final aggregation node
            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            for (Symbol symbol : aggregations.keySet()) {
                FunctionCall functionCall = node.getAggregations().get(symbol);
                finalCalls.put(symbol, new FunctionCall(functionCall.getName(), ImmutableList.of(finalAggregationSymbolMapping.get(symbol).toSymbolReference())));
            }

            // 4. create final aggregation node on top of the above union node
            return new AggregationNode(
                    node.getId(),
                    pushedUnionNode,
                    node.getGroupBy(),
                    finalCalls,
                    node.getFunctions(),
                    ImmutableMap.of(),
                    node.getGroupingSets(),
                    FINAL,
                    Optional.empty(),
                    node.getConfidence(),
                    node.getHashSymbol());
        }
    }
}

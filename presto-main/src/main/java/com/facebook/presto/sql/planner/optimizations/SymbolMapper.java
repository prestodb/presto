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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SymbolMapper
{
    private final Map<Symbol, Symbol> mapping;

    public SymbolMapper(Map<Symbol, Symbol> mapping)
    {
        this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
    }

    public Symbol map(Symbol symbol)
    {
        Symbol canonical = symbol;
        while (mapping.containsKey(canonical) && !mapping.get(canonical).equals(canonical)) {
            canonical = mapping.get(canonical);
        }
        return canonical;
    }

    public Expression map(Expression value)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Symbol canonical = map(Symbol.from(node));
                return canonical.toSymbolReference();
            }
        }, value);
    }

    public AggregationNode map(AggregationNode node, PlanNode source)
    {
        ImmutableMap.Builder<Symbol, Signature> functionInfos = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Symbol> masks = ImmutableMap.builder();
        for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
            Symbol symbol = entry.getKey();
            Symbol canonical = map(symbol);
            FunctionCall canonicalCall = (FunctionCall) map(entry.getValue());
            functionCalls.put(canonical, canonicalCall);
            functionInfos.put(canonical, node.getFunctions().get(symbol));
        }
        for (Map.Entry<Symbol, Symbol> entry : node.getMasks().entrySet()) {
            masks.put(map(entry.getKey()), map(entry.getValue()));
        }

        List<List<Symbol>> groupingSets = node.getGroupingSets().stream()
                .map(this::mapAndDistinct)
                .collect(toImmutableList());

        return new AggregationNode(
                node.getId(),
                source,
                functionCalls.build(),
                functionInfos.build(),
                masks.build(),
                groupingSets,
                node.getStep(),
                node.getHashSymbol().map(this::map),
                node.getGroupIdSymbol().map(this::map));
    }

    private List<Symbol> mapAndDistinct(List<Symbol> outputs)
    {
        Set<Symbol> added = new HashSet<>();
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        for (Symbol symbol : outputs) {
            Symbol canonical = map(symbol);
            if (added.add(canonical)) {
                builder.add(canonical);
            }
        }
        return builder.build();
    }

    public static SymbolMapper.Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();

        public SymbolMapper build()
        {
            return new SymbolMapper(mappings.build());
        }

        public void put(Symbol from, Symbol to)
        {
            mappings.put(from, to);
        }
    }
}

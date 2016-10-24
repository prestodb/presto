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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.AND;
import static java.util.Objects.requireNonNull;

/**
 * Generate masks to mask rows if filter clauses exist in aggregation
 * <p>
 * The optimizer goes through all FunctionCall in an AggregationNode to check if they have
 * filters associated with.  If so, the filter expression symbol will be put into masks.
 * In case the FunctionCall already has masks, the optimizer will generate a boolean expression
 * which is AND of filter expression and existing marker expression and put that one
 * into masks.
 * <p>
 * The final output has two cases:
 * <ol>
 * <li>The input AggregationNode has no existing masks, the output is a new AggregationNode
 * with filter expressions as masks.</li>
 * <li>The input AggregationNode has existing masks, A ProjectNode will be created to output the
 * new boolean expression and a new AggregationNode with new masks will be created over the ProjectNode.</li>
 * </ol>
 */
public class FilterOptimizer
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Optimizer(idAllocator, symbolAllocator), plan, Optional.empty());
    }

    private static class Optimizer
            extends SimplePlanRewriter<Optional<Symbol>>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        private Optimizer(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Optional<Symbol>> context)
        {
            Map<Symbol, FunctionCall> functionCallMap = node.getAggregations();
            List<Symbol> sourceOutputSymbols = node.getSource().getOutputSymbols();
            Map<Symbol, Symbol> filterMasks = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                FunctionCall aggregate = entry.getValue();
                if (aggregate.getFilter().isPresent()) {
                    for (Symbol symbol : sourceOutputSymbols) {
                        Expression filter = aggregate.getFilter().get();
                        if (symbol.toSymbolReference().equals(filter)) {
                            filterMasks.put(entry.getKey(), symbol);
                            break;
                        }
                    }
                }
            }

            if (filterMasks.size() < 1) {
                return context.defaultRewrite(node, Optional.empty());
            }

            ProjectNode projectNode = null;
            Map<Symbol, Symbol> existingMasks = node.getMasks();
            Set<Symbol> intersectedSymbols = Sets.intersection(filterMasks.keySet(), existingMasks.keySet());
            if (!intersectedSymbols.isEmpty()) {
                Map<Symbol, Expression> combinedOutputSymbols = new HashMap<>();
                for (Symbol symbol : intersectedSymbols) {
                    Symbol filterSymbol = filterMasks.get(symbol);
                    Symbol otherSymbol = existingMasks.get(symbol);
                    LogicalBinaryExpression expression = new LogicalBinaryExpression(AND, filterSymbol.toSymbolReference(), otherSymbol.toSymbolReference());
                    Symbol combinedSymbol = symbolAllocator.newSymbol(expression, BOOLEAN);
                    combinedOutputSymbols.put(combinedSymbol, expression);
                    filterMasks.put(symbol, combinedSymbol);
                }

                ImmutableMap.Builder<Symbol, Expression> outputSymbols = ImmutableMap.builder();
                for (Symbol symbol : node.getSource().getOutputSymbols()) {
                    // Unused symbols will be pruned later
                    outputSymbols.put(symbol, symbol.toSymbolReference());
                }
                outputSymbols.putAll(combinedOutputSymbols);

                projectNode = new ProjectNode(idAllocator.getNextId(),
                        node.getSource(),
                        outputSymbols.build());
            }

            return new AggregationNode(idAllocator.getNextId(),
                    projectNode == null ? node.getSource() : projectNode,
                    functionCallMap,
                    node.getFunctions(),
                    filterMasks,
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getSampleWeight(),
                    node.getConfidence(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }
    }
}

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
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

/**
 * Implements filtered aggregations by transforming plans of the following shape:
 *
 * <pre>
 * - Aggregation
 *        F1(...) FILTER (WHERE C1(...)),
 *        F2(...) FILTER (WHERE C2(...))
 *     - X
 * </pre>
 *
 * into
 *
 * <pre>
 * - Aggregation
 *        F1(...) mask ($0)
 *        F2(...) mask ($1)
 *     - Project
 *            &lt;identity projections for existing fields&gt;
 *            $0 = C1(...)
 *            $1 = C2(...)
 *         - X
 * </pre>
 */
public class ImplementFilteredAggregations
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
            boolean hasFilters = false;
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol output = entry.getKey();
                FunctionCall call = entry.getValue();

                if (call.getFilter().isPresent()) {
                    if (node.getMasks().containsKey(output)) {
                        // can't handle filtered aggregations with DISTINCT (conservatively, if they have a mask)
                        return context.defaultRewrite(node, Optional.empty());
                    }

                    hasFilters = true;
                }
            }

            if (!hasFilters) {
                return context.defaultRewrite(node, Optional.empty());
            }

            ImmutableMap.Builder<Symbol, Expression> newProjections = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Symbol> masks = ImmutableMap.<Symbol, Symbol>builder()
                    .putAll(node.getMasks());

            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol output = entry.getKey();
                if (entry.getValue().getFilter().isPresent()) {
                    Expression filter = entry.getValue().getFilter().get();
                    Symbol symbol = symbolAllocator.newSymbol(filter, BOOLEAN);
                    newProjections.put(symbol, filter);
                    masks.put(output, symbol);
                }
            }

            // identity projection for all existing inputs
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                newProjections.put(symbol, symbol.toSymbolReference());
            }

            // strip the filters
            ImmutableMap.Builder<Symbol, FunctionCall> calls = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                FunctionCall call = entry.getValue();
                calls.put(entry.getKey(), new FunctionCall(
                        call.getName(),
                        call.getWindow(),
                        Optional.empty(),
                        call.isDistinct(),
                        call.getArguments()));
            }

            return new AggregationNode(
                    idAllocator.getNextId(),
                    new ProjectNode(
                            idAllocator.getNextId(),
                            node.getSource(),
                            newProjections.build()),
                    calls.build(),
                    node.getFunctions(),
                    masks.build(),
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }
    }
}

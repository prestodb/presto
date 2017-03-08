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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

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
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) node;

        boolean hasFilters = aggregation.getAggregations()
                .entrySet().stream()
                .anyMatch(e -> e.getValue().getFilter().isPresent() &&
                        !aggregation.getMasks().containsKey(e.getKey())); // can't handle filtered aggregations with DISTINCT (conservatively, if they have a mask)

        if (!hasFilters) {
            return Optional.empty();
        }

        Assignments.Builder newAssignments = Assignments.builder();
        ImmutableMap.Builder<Symbol, Symbol> masks = ImmutableMap.<Symbol, Symbol>builder()
                .putAll(aggregation.getMasks());
        ImmutableMap.Builder<Symbol, FunctionCall> calls = ImmutableMap.builder();

        for (Map.Entry<Symbol, FunctionCall> entry : aggregation.getAggregations().entrySet()) {
            Symbol output = entry.getKey();

            // strip the filters
            FunctionCall call = entry.getValue();
            calls.put(output, new FunctionCall(
                    call.getName(),
                    call.getWindow(),
                    Optional.empty(),
                    call.isDistinct(),
                    call.getArguments()));

            if (call.getFilter().isPresent()) {
                Expression filter = entry.getValue().getFilter().get();
                Symbol symbol = symbolAllocator.newSymbol(filter, BOOLEAN);
                newAssignments.put(symbol, filter);
                masks.put(output, symbol);
            }
        }

        // identity projection for all existing inputs
        newAssignments.putIdentities(aggregation.getSource().getOutputSymbols());

        return Optional.of(
                new AggregationNode(
                        idAllocator.getNextId(),
                        new ProjectNode(
                                idAllocator.getNextId(),
                                aggregation.getSource(),
                                newAssignments.build()),
                        calls.build(),
                        aggregation.getFunctions(),
                        masks.build(),
                        aggregation.getGroupingSets(),
                        aggregation.getStep(),
                        aggregation.getHashSymbol(),
                        aggregation.getGroupIdSymbol()));
    }
}

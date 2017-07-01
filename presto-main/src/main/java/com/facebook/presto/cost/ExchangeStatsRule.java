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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStats;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

// WIP
public class ExchangeStatsRule
        implements ComposableStatsCalculator.Rule
{
    private static final Pattern PATTERN = Pattern.typeOf(ExchangeNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        ExchangeNode exchangeNode = (ExchangeNode) node;
        // QUESTION should I check partitioning schema?

        Optional<PlanNodeStatsEstimate> estimate = Optional.empty();
        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);
            PlanNodeStatsEstimate sourceStats = lookup.getStats(source, session, types);

            PlanNodeStatsEstimate sourceStatsWithMappedSymbols = mapToOutputSymbols(sourceStats, exchangeNode.getInputs().get(i), exchangeNode.getOutputSymbols());

            if (estimate.isPresent()) {
                estimate = Optional.of(addStats(estimate.get(), sourceStatsWithMappedSymbols));
            }
            else {
                estimate = Optional.of(sourceStatsWithMappedSymbols);
            }
        }

        checkState(estimate.isPresent());
        return estimate;
    }

    private PlanNodeStatsEstimate mapToOutputSymbols(PlanNodeStatsEstimate estimate, List<Symbol> inputs, List<Symbol> outputs)
    {
        checkArgument(inputs.size() == outputs.size(), "Inputs does not match outputs");
        PlanNodeStatsEstimate.Builder mapped = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(estimate.getOutputRowCount());

        for (int i = 0; i < inputs.size(); i++) {
            mapped.addSymbolStatistics(outputs.get(i), estimate.getSymbolStatistics(inputs.get(i)));
        }

        return mapped.build();
    }
}

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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

// WIP
public class ExchangeStatsRule
        implements ComposableStatsCalculator.Rule
{
    @Override
    public Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!(node instanceof ExchangeNode)) {
            return Optional.empty();
        }
        ExchangeNode exchangeNode = (ExchangeNode) node;

        if (exchangeNode.getSources().size() > 1) {
            return Optional.of(PlanNodeStatsEstimate.UNKNOWN_STATS);
        }

        PlanNode source = exchangeNode.getSources().get(0);
        PlanNodeStatsEstimate sourceStats = lookup.getStats(source, session, types);
        List<Symbol> sourceSymbols = exchangeNode.getInputs().get(0);

        PlanNodeStatsEstimate.Builder outputStats = PlanNodeStatsEstimate.builder();
        outputStats.setOutputRowCount(sourceStats.getOutputRowCount());
        for (int symbolIndex = 0; symbolIndex < sourceSymbols.size(); ++symbolIndex) {
            Symbol sourceSymbol = sourceSymbols.get(symbolIndex);
            Symbol outputSymbol = exchangeNode.getOutputSymbols().get(symbolIndex);
            outputStats.addSymbolStatistics(outputSymbol, sourceStats.getSymbolStatistics(sourceSymbol));
        }
        return Optional.of(outputStats.build());
    }

    /*
        // we do not replicate logic for rows replication here as stats
        // are used mostly in phase where exchanges are not in plan anyway
        List<Symbol> outputSymbols = exchangeNode.getOutputSymbols();
        Map<Symbol, ColumnStatistics> resultSymbolStatistics = new HashMap<>();
        Estimate resultOutputRowCount = Estimate.zeroValue();
        for (int sourceIndex = 0; sourceIndex < exchangeNode.getSources().size(); ++sourceIndex) {
            PlanNode source = exchangeNode.getSources().get(sourceIndex);
            PlanNodeStatsEstimate sourceStats = lookup.getStats(source, session, types);
            for (int outputIndex = 0; outputIndex < outputSymbols.size(); outputIndex++) {
                Symbol outputSymbol = exchangeNode.getOutputSymbols().get(outputIndex);
                Symbol sourceSymbol = exchangeNode.getInputs().get(sourceIndex).get(outputIndex);

                Optional<ColumnStatistics> outputSymbolStatistics = Optional.ofNullable(resultSymbolStatistics.get(outputSymbol));
                ColumnStatistics sourceSymbolStatistics = sourceStats.getSymbolStatistics(sourceSymbol);
                if (outputSymbolStatistics.isPresent()) {
                    resultSymbolStatistics.put(outputSymbol, mergeSymbolStatistics(outputSymbolStatistics.get(), sourceSymbolStatistics));
                }
                else {
                    resultSymbolStatistics.put(outputSymbol, sourceSymbolStatistics);
                }
            }
            resultOutputRowCount = resultOutputRowCount.add(sourceStats.getOutputRowCount());
        }

        return Optional.of(PlanNodeStatsEstimate.builder()
                .setOutputRowCount(resultOutputRowCount)
                .setSymbolStatistics(resultSymbolStatistics)
                .build());
    }

    private ColumnStatistics mergeSymbolStatistics(ColumnStatistics left, ColumnStatistics right)
    {
        return ColumnStatistics.builder()
                .setNullsFraction(left.getNullsFraction().add(right.getNullsFraction()))
                .addRange(mergeRange(left.getOnlyRangeColumnStatistics(), right.getOnlyRangeColumnStatistics()))
                .build();
    }

    private RangeColumnStatistics mergeRange(RangeColumnStatistics left, RangeColumnStatistics right)
    {
        return RangeColumnStatistics.builder()
                .set
    }
    */
}

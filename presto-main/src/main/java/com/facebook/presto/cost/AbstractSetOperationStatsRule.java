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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SetOperationNode;
import com.google.common.collect.ListMultimap;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

abstract class AbstractSetOperationStatsRule
        extends SimpleStatsRule
{
    public AbstractSetOperationStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    protected final Optional<PlanNodeStatsEstimate> doCalculate(PlanNode node, StatsProvider statsProvider, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        SetOperationNode unionNode = (SetOperationNode) node;

        Optional<PlanNodeStatsEstimate> estimate = Optional.empty();
        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);
            PlanNodeStatsEstimate sourceStats = statsProvider.getStats(source);

            PlanNodeStatsEstimate sourceStatsWithMappedSymbols = mapToOutputSymbols(sourceStats, unionNode.getSymbolMapping(), i);

            if (estimate.isPresent()) {
                estimate = Optional.of(operate(estimate.get(), sourceStatsWithMappedSymbols));
            }
            else {
                estimate = Optional.of(sourceStatsWithMappedSymbols);
            }
        }

        checkState(estimate.isPresent());
        return estimate;
    }

    private PlanNodeStatsEstimate mapToOutputSymbols(PlanNodeStatsEstimate estimate, ListMultimap<Symbol, Symbol> mapping, int index)
    {
        PlanNodeStatsEstimate.Builder mapped = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(estimate.getOutputRowCount());

        mapping.keySet().stream()
                .forEach(symbol -> mapped.addSymbolStatistics(symbol, estimate.getSymbolStatistics(mapping.get(symbol).get(index))));

        return mapped.build();
    }

    protected abstract PlanNodeStatsEstimate operate(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second);
}

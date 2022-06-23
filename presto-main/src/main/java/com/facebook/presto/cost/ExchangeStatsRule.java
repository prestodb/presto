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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.ExchangeNode;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.buildFrom;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndMaxDistinctValues;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;

public class ExchangeStatsRule
        extends SimpleStatsRule<ExchangeNode>
{
    private static final Pattern<ExchangeNode> PATTERN = exchange();

    public ExchangeStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(ExchangeNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        Optional<PlanNodeStatsEstimate> estimate = Optional.empty();
        double totalSize = 0;
        boolean confident = true;
        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);
            PlanNodeStatsEstimate sourceStats = statsProvider.getStats(source);
            totalSize += sourceStats.getOutputSizeInBytes();
            if (!sourceStats.isConfident()) {
                confident = false;
            }

            PlanNodeStatsEstimate sourceStatsWithMappedSymbols = mapToOutputVariables(sourceStats, node.getInputs().get(i), node.getOutputVariables());

            if (estimate.isPresent()) {
                estimate = Optional.of(addStatsAndMaxDistinctValues(estimate.get(), sourceStatsWithMappedSymbols));
            }
            else {
                estimate = Optional.of(sourceStatsWithMappedSymbols);
            }
        }

        verify(estimate.isPresent());
        return Optional.of(buildFrom(estimate.get())
                .setTotalSize(totalSize)
                .setConfident(confident)
                .build());
    }

    private PlanNodeStatsEstimate mapToOutputVariables(PlanNodeStatsEstimate estimate, List<VariableReferenceExpression> inputs, List<VariableReferenceExpression> outputs)
    {
        checkArgument(inputs.size() == outputs.size(), "Input symbols count does not match output symbols count");
        PlanNodeStatsEstimate.Builder mapped = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(estimate.getOutputRowCount());

        for (int i = 0; i < inputs.size(); i++) {
            mapped.addVariableStatistics(outputs.get(i), estimate.getVariableStatistics(inputs.get(i)));
        }

        return mapped.build();
    }
}

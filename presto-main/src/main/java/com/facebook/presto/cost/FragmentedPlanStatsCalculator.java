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
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;

import java.util.List;

import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndMaxDistinctValues;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class FragmentedPlanStatsCalculator
        implements StatsCalculator
{
    private final StatsCalculator delegate;
    private final FragmentedPlanSourceProvider sourceProvider;

    public FragmentedPlanStatsCalculator(StatsCalculator delegate, FragmentedPlanSourceProvider sourceProvider)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.sourceProvider = requireNonNull(sourceProvider, "source provider is null");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        if (node instanceof RemoteSourceNode) {
            return calculateRemoteSourceStats((RemoteSourceNode) node, sourceStats);
        }
        return delegate.calculateStats(node, sourceStats, lookup, session, types);
    }

    private PlanNodeStatsEstimate calculateRemoteSourceStats(RemoteSourceNode node, StatsProvider statsProvider)
    {
        PlanNodeStatsEstimate estimate = null;
        for (PlanFragment sourceFragment : sourceProvider.getSourceFragments(node)) {
            PlanNodeStatsEstimate sourceStatsWithMappedSymbols = mapToOutputSymbols(statsProvider.getStats(sourceFragment.getRoot()), sourceFragment.getPartitioningScheme().getOutputLayout(), node.getOutputSymbols());

            if (estimate != null) {
                estimate = addStatsAndMaxDistinctValues(estimate, sourceStatsWithMappedSymbols);
            }
            else {
                estimate = sourceStatsWithMappedSymbols;
            }
        }

        verify(estimate != null, "estimate is null");
        return estimate;
    }

    private PlanNodeStatsEstimate mapToOutputSymbols(PlanNodeStatsEstimate estimate, List<Symbol> inputs, List<Symbol> outputs)
    {
        checkArgument(inputs.size() == outputs.size(), "Input symbols count does not match output symbols count");
        PlanNodeStatsEstimate.Builder mapped = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(estimate.getOutputRowCount());

        for (int i = 0; i < inputs.size(); i++) {
            mapped.addSymbolStatistics(outputs.get(i), estimate.getSymbolStatistics(inputs.get(i)));
        }

        return mapped.build();
    }
}

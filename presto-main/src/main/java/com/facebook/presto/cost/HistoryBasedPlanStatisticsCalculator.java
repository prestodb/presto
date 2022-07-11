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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.statistics.ExternalPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.useExternalPlanStatisticsEnabled;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsCalculator
        implements StatsCalculator
{
    private final Supplier<ExternalPlanStatisticsProvider> externalPlanStatisticsProvider;
    private final Metadata metadata;
    private final StatsCalculator delegate;

    public HistoryBasedPlanStatisticsCalculator(Supplier<ExternalPlanStatisticsProvider> externalPlanStatisticsProvider, Metadata metadata, StatsCalculator delegate)
    {
        this.externalPlanStatisticsProvider = requireNonNull(externalPlanStatisticsProvider, "externalPlanStatisticsProvider is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        return delegate.calculateStats(node, sourceStats, lookup, session, types)
                .combineStats(getStatistics(node, session, types, lookup));
    }

    private PlanNode removeGroupReferences(PlanNode planNode, Lookup lookup)
    {
        if (planNode instanceof GroupReference) {
            return removeGroupReferences(lookup.resolve(planNode), lookup);
        }
        List<PlanNode> children = planNode.getSources().stream().map(node -> removeGroupReferences(node, lookup)).collect(toImmutableList());
        return planNode.replaceChildren(children);
    }

    private PlanStatistics getStatistics(PlanNode planNode, Session session, TypeProvider types, Lookup lookup)
    {
        planNode = removeGroupReferences(planNode, lookup);
        ExternalPlanStatisticsProvider externalStatisticsProvider = externalPlanStatisticsProvider.get();
        if (!useExternalPlanStatisticsEnabled(session)) {
            return PlanStatistics.empty();
        }
        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics =
                externalStatisticsProvider.getStats(ImmutableList.of(new PlanNodeWithHash(planNode, Optional.empty())));

        if (statistics.size() == 1) {
            return statistics.values().iterator().next().getLastRunStatistics();
        }
        return PlanStatistics.empty();
    }
}

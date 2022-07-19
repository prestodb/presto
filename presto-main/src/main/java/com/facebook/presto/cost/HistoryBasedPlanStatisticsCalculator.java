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
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.statistics.ExternalPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.collect.ImmutableList;

import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.useExternalPlanStatisticsEnabled;
import static com.facebook.presto.SystemSessionProperties.useHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonLogicalPlan;
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
        return resolveGroupReferences(planNode, lookup);
    }

    private PlanStatistics getStatistics(PlanNode planNode, Session session, TypeProvider types, Lookup lookup)
    {
        planNode = removeGroupReferences(planNode, lookup);
        ExternalPlanStatisticsProvider externalStatisticsProvider = externalPlanStatisticsProvider.get();
        if (useExternalPlanStatisticsEnabled(session)) {
            return externalStatisticsProvider.getStats(
                    planNode,
                    node -> jsonLogicalPlan(node, types, metadata.getFunctionAndTypeManager(), StatsAndCosts.empty(), session),
                    tableScanNode -> metadata.getTableStatistics(
                            session,
                            tableScanNode.getTable(),
                            ImmutableList.copyOf(tableScanNode.getAssignments().values()),
                            new Constraint<>(tableScanNode.getCurrentConstraint())));
        }
        if (!useHistoryBasedPlanStatisticsEnabled(session)) {
            return PlanStatistics.empty();
        }
        // Unimplemented
        return PlanStatistics.empty();
    }
}

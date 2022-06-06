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
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonLogicalPlan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsCalculator
        implements StatsCalculator
{
    private final Optional<ExternalPlanStatisticsProvider> externalPlanStatisticsProvider;
    private final Metadata metadata;
    private final StatsCalculator delegate;

    public HistoryBasedPlanStatisticsCalculator(Optional<ExternalPlanStatisticsProvider> externalPlanStatisticsProvider, Metadata metadata, StatsCalculator delegate)
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
            return lookup.resolve(planNode);
        }
        List<PlanNode> children = planNode.getSources().stream().map(node -> removeGroupReferences(node, lookup)).collect(toImmutableList());
        return planNode.replaceChildren(children);
    }

    private PlanStatistics getStatistics(PlanNode planNode, Session session, TypeProvider types, Lookup lookup)
    {
        planNode = removeGroupReferences(planNode, lookup);
        if (externalPlanStatisticsProvider.isPresent()) {
            return externalPlanStatisticsProvider.get().getStats(
                    planNode,
                    node -> jsonLogicalPlan(node, types, metadata.getFunctionAndTypeManager(), StatsAndCosts.empty(), session),
                    tableScanNode -> metadata.getTableStatistics(
                            session,
                            tableScanNode.getTable(),
                            ImmutableList.copyOf(tableScanNode.getAssignments().values()),
                            new Constraint<>(tableScanNode.getCurrentConstraint())));
        }
        return PlanStatistics.empty();
    }
}

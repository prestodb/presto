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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class StatsCalculatorAssertion
{
    private final StatsCalculator statsCalculator;
    private final Session session;
    private final PlanNode planNode;
    private final Map<Symbol, Type> types;

    private Map<PlanNode, PlanNodeStatsEstimate> sourcesStats;

    public StatsCalculatorAssertion(StatsCalculator statsCalculator, Session session, PlanNode planNode, Map<Symbol, Type> types)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator can not be null");
        this.session = requireNonNull(session, "sesssion can not be null");
        this.planNode = requireNonNull(planNode, "planNode is null");
        this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));

        sourcesStats = new HashMap<>();
        planNode.getSources().forEach(child -> sourcesStats.put(child, UNKNOWN_STATS));
    }

    public StatsCalculatorAssertion withSourceStats(PlanNodeStatsEstimate sourceStats)
    {
        checkState(planNode.getSources().size() == 1, "expected single source");
        return withSourceStats(0, sourceStats);
    }

    public StatsCalculatorAssertion withSourceStats(int sourceIndex, PlanNodeStatsEstimate sourceStats)
    {
        checkArgument(sourceIndex < planNode.getSources().size(), "invalid sourceIndex %s; planNode has %s sources", sourceIndex, planNode.getSources().size());
        sourcesStats.put(planNode.getSources().get(sourceIndex), sourceStats);
        return this;
    }

    public StatsCalculatorAssertion check(Consumer<PlanNodeStatsAssertion> statisticsAssertionConsumer)
    {
        PlanNodeStatsEstimate statsEstimate = statsCalculator.calculateStats(planNode, this::getSourceStats, noLookup(), session, types);
        statisticsAssertionConsumer.accept(PlanNodeStatsAssertion.assertThat(statsEstimate));
        return this;
    }

    public StatsCalculatorAssertion check(ComposableStatsCalculator.Rule rule, Consumer<PlanNodeStatsAssertion> statisticsAssertionConsumer)
    {
        Optional<PlanNodeStatsEstimate> statsEstimate = rule.calculate(planNode, this::getSourceStats, noLookup(), session, types);
        checkState(statsEstimate.isPresent(), "Expected stats estimates to be present");
        statisticsAssertionConsumer.accept(PlanNodeStatsAssertion.assertThat(statsEstimate.get()));
        return this;
    }

    private PlanNodeStatsEstimate getSourceStats(PlanNode sourceNode)
    {
        checkArgument(sourcesStats.containsKey(sourceNode), "stats not found for source %s", sourceNode);
        return sourcesStats.get(sourceNode);
    }
}

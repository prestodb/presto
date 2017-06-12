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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.facebook.presto.cost.PlanNodeCostEstimate.INFINITE_COST;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class StatsCalculatorAssertion
{
    private final StatsCalculator statsCalculator;
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private final Metadata metadata;
    private final Session session;
    private PlanNode planNode;
    private Map<PlanNode, PlanNodeStatsEstimate> sourcesStats;
    private Map<Symbol, Type> types;

    public StatsCalculatorAssertion(StatsCalculator statsCalculator, Metadata metadata, Session session)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator can not be null");
        this.metadata = requireNonNull(metadata, "metadata can not be null");
        this.session = requireNonNull(session, "sesssion can not be null");
    }

    public StatsCalculatorAssertion on(Function<PlanBuilder, PlanNode> planProvider)
    {
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, metadata);
        this.planNode = planProvider.apply(planBuilder);
        this.sourcesStats = new HashMap<>();
        this.planNode.getSources().forEach(child -> sourcesStats.put(child, UNKNOWN_STATS));
        this.types = planBuilder.getSymbols();
        return this;
    }

    public StatsCalculatorAssertion withSourceStats(Consumer<PlanNodeStatsEstimate.Builder> sourceStatsBuilderConsumer)
    {
        checkPlanNodeSet();
        checkState(planNode.getSources().size() == 1, "expected single source");
        return withSourceStats(0, sourceStatsBuilderConsumer);
    }

    public StatsCalculatorAssertion withSourceStats(PlanNodeStatsEstimate sourceStats)
    {
        checkPlanNodeSet();
        checkState(planNode.getSources().size() == 1, "expected single source");
        return withSourceStats(0, sourceStats);
    }

    public StatsCalculatorAssertion withSourceStats(int sourceIndex, Consumer<PlanNodeStatsEstimate.Builder> sourceStatsBuilderConsumer)
    {
        PlanNodeStatsEstimate.Builder sourceStatsBuilder = PlanNodeStatsEstimate.builder();
        sourceStatsBuilderConsumer.accept(sourceStatsBuilder);
        return withSourceStats(sourceIndex, sourceStatsBuilder.build());
    }

    public StatsCalculatorAssertion withSourceStats(int sourceIndex, PlanNodeStatsEstimate sourceStats)
    {
        checkPlanNodeSet();
        checkArgument(sourceIndex < planNode.getSources().size(), "invalid sourceIndex %s; planNode has %s sources", sourceIndex, planNode.getSources().size());
        sourcesStats.put(planNode.getSources().get(sourceIndex), sourceStats);
        return this;
    }

    public StatsCalculatorAssertion check(Consumer<PlanNodeStatsAssertion> statisticsAssertionConsumer)
    {
        PlanNodeStatsEstimate statsEstimate = statsCalculator.calculateStats(planNode, mockLookup(), session, types);
        statisticsAssertionConsumer.accept(PlanNodeStatsAssertion.assertThat(statsEstimate));
        return this;
    }

    private void checkPlanNodeSet()
    {
        checkState(planNode != null, "tested planNode not set yet");
    }

    private Lookup mockLookup()
    {
        return new Lookup()
        {
            @Override
            public PlanNode resolve(PlanNode node)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public PlanNodeStatsEstimate getStats(PlanNode node, Session session, Map<Symbol, Type> types)
            {
                checkArgument(sourcesStats.containsKey(node), "stats not found for source %s", node);
                return sourcesStats.get(node);
            }

            @Override
            public PlanNodeCostEstimate getCumulativeCost(PlanNode node, Session session, Map<Symbol, Type> types)
            {
                return INFINITE_COST;
            }
        };
    }
}

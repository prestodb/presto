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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_CARDINALITY_RATIO_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_STRATEGY;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestAddDynamicFilters
        extends BasePlanTest
{
    private Metadata metadata;
    private PlanNodeIdAllocator idAllocator;
    private PlanBuilder builder;
    private VariableReferenceExpression probeKey;
    private VariableReferenceExpression buildKey;
    private TableScanNode probeTableScan;
    private TableScanNode buildTableScan;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        idAllocator = new PlanNodeIdAllocator();
        builder = new PlanBuilder(getQueryRunner().getDefaultSession(), idAllocator, metadata);
        ConnectorId connectorId = getCurrentConnectorId();

        TableHandle ordersHandle = new TableHandle(
                connectorId,
                new TpchTableHandle("orders", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());
        probeKey = builder.variable("probeKey", BIGINT);
        probeTableScan = builder.tableScan(ordersHandle, ImmutableList.of(probeKey),
                ImmutableMap.of(probeKey, new TpchColumnHandle("orderkey", BIGINT)));

        TableHandle lineitemHandle = new TableHandle(
                connectorId,
                new TpchTableHandle("lineitem", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());
        buildKey = builder.variable("buildKey", BIGINT);
        buildTableScan = builder.tableScan(lineitemHandle, ImmutableList.of(buildKey),
                ImmutableMap.of(buildKey, new TpchColumnHandle("orderkey", BIGINT)));
    }

    @Test
    public void testAlwaysCreatesFilter()
    {
        PlanNode join = buildJoin();
        JoinNode result = (JoinNode) optimize(join, "ALWAYS");
        assertEquals(result.getDynamicFilters().size(), 1);
        assertTrue(result.getDynamicFilters().values().iterator().next().getName().equals("buildKey"));
    }

    @Test
    public void testAlwaysCreatesTwoClauses()
    {
        VariableReferenceExpression probeKey2 = builder.variable("probeKey2", BIGINT);
        VariableReferenceExpression buildKey2 = builder.variable("buildKey2", BIGINT);
        PlanNode probeValues = builder.values(probeKey, probeKey2);
        PlanNode buildValues = builder.values(buildKey, buildKey2);
        PlanNode join = builder.join(
                JoinType.INNER,
                probeValues,
                buildValues,
                ImmutableList.of(
                        new EquiJoinClause(probeKey, buildKey),
                        new EquiJoinClause(probeKey2, buildKey2)),
                ImmutableList.of(probeKey, probeKey2, buildKey, buildKey2),
                Optional.empty());

        JoinNode result = (JoinNode) optimize(join, "ALWAYS");
        assertEquals(result.getDynamicFilters().size(), 2);
    }

    @Test
    public void testDisabledDoesNotModify()
    {
        PlanNode join = buildJoin();
        JoinNode result = (JoinNode) optimize(join, "DISABLED");
        assertTrue(result.getDynamicFilters().isEmpty());
    }

    @Test
    public void testSkipsExistingDynamicFilters()
    {
        PlanNode join = builder.join(
                JoinType.INNER,
                probeTableScan,
                buildTableScan,
                ImmutableList.of(new EquiJoinClause(probeKey, buildKey)),
                ImmutableList.of(probeKey, buildKey),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("existing", buildKey));

        JoinNode result = (JoinNode) optimize(join, "ALWAYS");
        assertEquals(result.getDynamicFilters().size(), 1);
        assertEquals(result.getDynamicFilters().get("existing"), buildKey);
    }

    @Test
    public void testSkipsLeftJoin()
    {
        PlanNode join = builder.join(
                JoinType.LEFT,
                probeTableScan,
                buildTableScan,
                new EquiJoinClause(probeKey, buildKey));

        JoinNode result = (JoinNode) optimize(join, "ALWAYS");
        assertTrue(result.getDynamicFilters().isEmpty());
    }

    @Test
    public void testCostBasedSkipsHighRatio()
    {
        PlanNode join = buildJoin();
        // Both sides have 1M rows → ratio = 1.0 → skip
        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());

        JoinNode result = (JoinNode) optimizeCostBased(join, overrides, sessionWithStrategy("COST_BASED"));
        assertTrue(result.getDynamicFilters().isEmpty());
    }

    @Test
    public void testCostBasedCreatesFilterForGoodRatio()
    {
        PlanNode join = buildJoin();
        // Probe 1M, build 100 → ratio = 0.0001 → create
        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(100).build());

        JoinNode result = (JoinNode) optimizeCostBased(join, overrides, sessionWithStrategy("COST_BASED"));
        assertEquals(result.getDynamicFilters().size(), 1);
    }

    @Test
    public void testCostBasedCreatesFilterForUnknownStats()
    {
        PlanNode join = buildJoin();
        // No overrides → unknown stats → should create filter
        JoinNode result = (JoinNode) optimizeCostBased(join, new HashMap<>(), sessionWithStrategy("COST_BASED"));
        assertEquals(result.getDynamicFilters().size(), 1);
    }

    @Test
    public void testCostBasedRespectsCustomThreshold()
    {
        PlanNode join = buildJoin();
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_CARDINALITY_RATIO_THRESHOLD, "0.9")
                .setRuntimeStats(new RuntimeStats())
                .build();
        // Probe 1M, build 800K → ratio = 0.8 < 0.9 → create
        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(800_000).build());

        JoinNode result = (JoinNode) optimizeCostBased(join, overrides, session);
        assertEquals(result.getDynamicFilters().size(), 1);
    }

    @Test
    public void testExtendedMetricsCreated()
    {
        PlanNode join = buildJoin();
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "true")
                .setRuntimeStats(new RuntimeStats())
                .build();
        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(100).build());

        optimizeCostBased(join, overrides, session);

        String metricKey = format("%s[%s]", DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO, "probeKey");
        assertNotNull(session.getRuntimeStats().getMetrics().get(metricKey));
        assertEquals(session.getRuntimeStats().getMetrics().get(metricKey).getSum(), 1);
    }

    @Test
    public void testExtendedMetricsSkipped()
    {
        PlanNode join = buildJoin();
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "true")
                .setRuntimeStats(new RuntimeStats())
                .build();
        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());

        optimizeCostBased(join, overrides, session);

        String metricKey = format("%s[%s]", DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY, "probeKey");
        assertNotNull(session.getRuntimeStats().getMetrics().get(metricKey));
    }

    @Test
    public void testExtendedMetricsNotEmittedWhenDisabled()
    {
        PlanNode join = buildJoin();
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "false")
                .setRuntimeStats(new RuntimeStats())
                .build();
        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(100).build());

        optimizeCostBased(join, overrides, session);

        assertNull(session.getRuntimeStats().getMetrics().get(
                format("%s[%s]", DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO, "probeKey")));
    }

    @Test
    public void testCostBasedMixedClauses()
    {
        VariableReferenceExpression probeKey2 = builder.variable("probeKey2", BIGINT);
        VariableReferenceExpression buildKey2 = builder.variable("buildKey2", BIGINT);
        PlanNode probeValues = builder.values(probeKey, probeKey2);
        PlanNode buildValues = builder.values(buildKey, buildKey2);
        PlanNode join = builder.join(
                JoinType.INNER,
                probeValues,
                buildValues,
                ImmutableList.of(
                        new EquiJoinClause(probeKey, buildKey),
                        new EquiJoinClause(probeKey2, buildKey2)),
                ImmutableList.of(probeKey, probeKey2, buildKey, buildKey2),
                Optional.empty());

        // Both clauses share the same build/probe ratio (100/1M), so both pass
        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeValues.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildValues.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(100).build());

        JoinNode result = (JoinNode) optimizeCostBased(join, overrides, sessionWithStrategy("COST_BASED"));
        assertEquals(result.getDynamicFilters().size(), 2);
    }

    @Test
    public void testSemiJoinAlways()
    {
        VariableReferenceExpression semiJoinOutput = builder.variable("semiJoinOutput", BIGINT);
        PlanNode semiJoin = builder.semiJoin(
                probeKey,
                buildKey,
                semiJoinOutput,
                Optional.empty(),
                Optional.empty(),
                probeTableScan,
                buildTableScan);

        SemiJoinNode result = (SemiJoinNode) optimize(semiJoin, "ALWAYS");
        assertEquals(result.getDynamicFilters().size(), 1);
    }

    @Test
    public void testSemiJoinCostBasedSkipsHighRatio()
    {
        VariableReferenceExpression semiJoinOutput = builder.variable("semiJoinOutput2", BIGINT);
        PlanNode semiJoin = builder.semiJoin(
                probeKey,
                buildKey,
                semiJoinOutput,
                Optional.empty(),
                Optional.empty(),
                probeTableScan,
                buildTableScan);

        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());

        SemiJoinNode result = (SemiJoinNode) optimizeCostBased(semiJoin, overrides, sessionWithStrategy("COST_BASED"));
        assertTrue(result.getDynamicFilters().isEmpty());
    }

    @Test
    public void testSemiJoinCostBasedCreatesForGoodRatio()
    {
        VariableReferenceExpression semiJoinOutput = builder.variable("semiJoinOutput3", BIGINT);
        PlanNode semiJoin = builder.semiJoin(
                probeKey,
                buildKey,
                semiJoinOutput,
                Optional.empty(),
                Optional.empty(),
                probeTableScan,
                buildTableScan);

        Map<PlanNodeId, PlanNodeStatsEstimate> overrides = new HashMap<>();
        overrides.put(probeTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build());
        overrides.put(buildTableScan.getId(),
                PlanNodeStatsEstimate.builder().setOutputRowCount(100).build());

        SemiJoinNode result = (SemiJoinNode) optimizeCostBased(semiJoin, overrides, sessionWithStrategy("COST_BASED"));
        assertEquals(result.getDynamicFilters().size(), 1);
    }

    private PlanNode buildJoin()
    {
        return builder.join(
                JoinType.INNER,
                probeTableScan,
                buildTableScan,
                new EquiJoinClause(probeKey, buildKey));
    }

    private PlanNode optimize(PlanNode plan, String strategy)
    {
        Session session = sessionWithStrategy(strategy);
        return new AddDynamicFilters(getQueryRunner().getStatsCalculator()).optimize(
                plan,
                session,
                TypeProvider.viewOf(new VariableAllocator().getVariables()),
                new VariableAllocator(),
                idAllocator,
                WarningCollector.NOOP).getPlanNode();
    }

    private Session sessionWithStrategy(String strategy)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, strategy)
                .setRuntimeStats(new RuntimeStats())
                .build();
    }

    private PlanNode optimizeCostBased(PlanNode plan, Map<PlanNodeId, PlanNodeStatsEstimate> statsOverrides, Session session)
    {
        StatsCalculator overridingCalculator = (node, statsProvider, lookup, s, types) -> {
            PlanNodeStatsEstimate override = statsOverrides.get(node.getId());
            if (override != null) {
                return override;
            }
            return PlanNodeStatsEstimate.unknown();
        };
        return new AddDynamicFilters(overridingCalculator).optimize(
                plan,
                session,
                TypeProvider.viewOf(new VariableAllocator().getVariables()),
                new VariableAllocator(),
                idAllocator,
                WarningCollector.NOOP).getPlanNode();
    }
}

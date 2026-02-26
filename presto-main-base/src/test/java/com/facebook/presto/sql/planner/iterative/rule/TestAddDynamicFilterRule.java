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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.metadata.DelegatingMetadataManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_CARDINALITY_RATIO_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_DISCRETE_VALUES_LIMIT;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestAddDynamicFilterRule
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(ImmutableList.of(), ImmutableMap.of(), Optional.of(4));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
    }

    @Test
    public void testAlwaysCreatesAllFilters()
    {
        tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .on(p -> {
                    VariableReferenceExpression probeKey = p.variable("probeKey", BIGINT);
                    VariableReferenceExpression buildKey = p.variable("buildKey", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey),
                            p.values(buildKey),
                            new EquiJoinClause(probeKey, buildKey));
                })
                .matches(join(JoinType.INNER,
                        ImmutableList.of(equiJoinClause("probeKey", "buildKey")),
                        Optional.empty(),
                        values("probeKey"),
                        values("buildKey")));
    }

    @Test
    public void testAlwaysCreatesTwoClauses()
    {
        tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .on(p -> {
                    VariableReferenceExpression probeKey1 = p.variable("probeKey1", BIGINT);
                    VariableReferenceExpression probeKey2 = p.variable("probeKey2", BIGINT);
                    VariableReferenceExpression buildKey1 = p.variable("buildKey1", BIGINT);
                    VariableReferenceExpression buildKey2 = p.variable("buildKey2", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey1, probeKey2),
                            p.values(buildKey1, buildKey2),
                            ImmutableList.of(
                                    new EquiJoinClause(probeKey1, buildKey1),
                                    new EquiJoinClause(probeKey2, buildKey2)),
                            ImmutableList.of(probeKey1, probeKey2, buildKey1, buildKey2),
                            Optional.empty());
                })
                .matches(join(JoinType.INNER,
                        ImmutableList.of(
                                equiJoinClause("probeKey1", "buildKey1"),
                                equiJoinClause("probeKey2", "buildKey2")),
                        Optional.empty(),
                        values("probeKey1", "probeKey2"),
                        values("buildKey1", "buildKey2")));
    }

    @Test
    public void testCostBasedSkipsHighRatioAndHighNdv()
    {
        tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .on(p -> {
                    VariableReferenceExpression probeKey = p.variable("probeKey", BIGINT);
                    VariableReferenceExpression buildKey = p.variable("buildKey", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey),
                            p.values(buildKey),
                            new EquiJoinClause(probeKey, buildKey));
                })
                .overrideStats("0", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000_000)
                        .build())
                .overrideStats("1", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000_000)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression(Optional.empty(), "buildKey", BIGINT),
                                new VariableStatsEstimate(0, 1_000_000, 0, 8, 1_000_000)))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testCostBasedCreatesFilterForLowNdv()
    {
        tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .on(p -> {
                    VariableReferenceExpression probeKey = p.variable("probeKey", BIGINT);
                    VariableReferenceExpression buildKey = p.variable("buildKey", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey),
                            p.values(buildKey),
                            new EquiJoinClause(probeKey, buildKey));
                })
                .overrideStats("0", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000_000)
                        .build())
                .overrideStats("1", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000_000)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression(Optional.empty(), "buildKey", BIGINT),
                                new VariableStatsEstimate(0, 100, 0, 8, 100)))
                        .build())
                .matches(join(JoinType.INNER,
                        ImmutableList.of(equiJoinClause("probeKey", "buildKey")),
                        Optional.empty(),
                        values("probeKey"),
                        values("buildKey")));
    }

    @Test
    public void testCostBasedCreatesFilterForGoodRatio()
    {
        tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .on(p -> {
                    VariableReferenceExpression probeKey = p.variable("probeKey", BIGINT);
                    VariableReferenceExpression buildKey = p.variable("buildKey", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey),
                            p.values(buildKey),
                            new EquiJoinClause(probeKey, buildKey));
                })
                .overrideStats("0", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000_000)
                        .build())
                .overrideStats("1", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression(Optional.empty(), "buildKey", BIGINT),
                                new VariableStatsEstimate(0, 100_000, 0, 8, 100_000)))
                        .build())
                .matches(join(JoinType.INNER,
                        ImmutableList.of(equiJoinClause("probeKey", "buildKey")),
                        Optional.empty(),
                        values("probeKey"),
                        values("buildKey")));
    }

    @Test
    public void testCostBasedMixedClauses()
    {
        PlanNode result = tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .on(p -> {
                    VariableReferenceExpression probeKey1 = p.variable("probeKey1", BIGINT);
                    VariableReferenceExpression probeKey2 = p.variable("probeKey2", BIGINT);
                    VariableReferenceExpression buildKey1 = p.variable("buildKey1", BIGINT);
                    VariableReferenceExpression buildKey2 = p.variable("buildKey2", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey1, probeKey2),
                            p.values(buildKey1, buildKey2),
                            ImmutableList.of(
                                    new EquiJoinClause(probeKey1, buildKey1),
                                    new EquiJoinClause(probeKey2, buildKey2)),
                            ImmutableList.of(probeKey1, probeKey2, buildKey1, buildKey2),
                            Optional.empty());
                })
                .overrideStats("0", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000_000)
                        .build())
                .overrideStats("1", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000_000)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression(Optional.empty(), "buildKey1", BIGINT),
                                new VariableStatsEstimate(0, 100, 0, 8, 100),
                                new VariableReferenceExpression(Optional.empty(), "buildKey2", BIGINT),
                                new VariableStatsEstimate(0, 1_000_000, 0, 8, 1_000_000)))
                        .build())
                .get();

        JoinNode joinResult = (JoinNode) result;
        assertEquals(joinResult.getDynamicFilters().size(), 1);
        assertFalse(joinResult.getDynamicFilters().isEmpty());
    }

    @Test
    public void testCostBasedSkipsUnknownStats()
    {
        tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .on(p -> {
                    VariableReferenceExpression probeKey = p.variable("probeKey", BIGINT);
                    VariableReferenceExpression buildKey = p.variable("buildKey", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey),
                            p.values(buildKey),
                            new EquiJoinClause(probeKey, buildKey));
                })
                .overrideStats("0", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(Double.NaN)
                        .build())
                .overrideStats("1", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(Double.NaN)
                        .build())
                .doesNotFire();
    }

    @Test
    public void testCostBasedRespectsCustomThresholds()
    {
        tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_CARDINALITY_RATIO_THRESHOLD, "0.9")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_DISCRETE_VALUES_LIMIT, "10")
                .on(p -> {
                    VariableReferenceExpression probeKey = p.variable("probeKey", BIGINT);
                    VariableReferenceExpression buildKey = p.variable("buildKey", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey),
                            p.values(buildKey),
                            new EquiJoinClause(probeKey, buildKey));
                })
                .overrideStats("0", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000_000)
                        .build())
                .overrideStats("1", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(800_000)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression(Optional.empty(), "buildKey", BIGINT),
                                new VariableStatsEstimate(0, 800_000, 0, 8, 800_000)))
                        .build())
                .matches(join(JoinType.INNER,
                        ImmutableList.of(equiJoinClause("probeKey", "buildKey")),
                        Optional.empty(),
                        values("probeKey"),
                        values("buildKey")));
    }

    @Test
    public void testDisabledDoesNotFire()
    {
        tester.assertThat(new AddDynamicFilterRule(tester.getMetadata()))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "DISABLED")
                .on(p -> {
                    VariableReferenceExpression probeKey = p.variable("probeKey", BIGINT);
                    VariableReferenceExpression buildKey = p.variable("buildKey", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(probeKey),
                            p.values(buildKey),
                            new EquiJoinClause(probeKey, buildKey));
                })
                .doesNotFire();
    }

    @Test
    public void testCostBasedCreatesFilterForPartitionColumn()
    {
        // Create a metadata wrapper that returns discrete predicates for the probe column,
        // simulating a partitioned table (like Iceberg or Hive)
        TpchColumnHandle partitionColumn = new TpchColumnHandle("nationkey", BIGINT);
        Metadata metadataWithDiscretePredicates = new DelegatingMetadataManager((MetadataManager) tester.getMetadata())
        {
            @Override
            public MetadataResolver getMetadataResolver(Session session)
            {
                return ((MetadataManager) tester.getMetadata()).getMetadataResolver(session);
            }

            @Override
            public TableLayout getLayout(Session session, TableHandle handle)
            {
                DiscretePredicates discretePredicates = new DiscretePredicates(
                        ImmutableList.of(partitionColumn),
                        ImmutableList.of(TupleDomain.all()));
                ConnectorTableLayout connectorLayout = new ConnectorTableLayout(
                        handle.getLayout().get(),
                        Optional.empty(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(discretePredicates),
                        ImmutableList.of());
                return TableLayout.fromConnectorLayout(
                        handle.getConnectorId(),
                        handle.getConnectorHandle(),
                        handle.getTransaction(),
                        connectorLayout);
            }
        };

        TpchTableHandle tpchTableHandle = new TpchTableHandle("nation", 1.0);
        TpchTableLayoutHandle layoutHandle = new TpchTableLayoutHandle(tpchTableHandle, TupleDomain.all());
        TableHandle tableHandle = new TableHandle(
                tester.getCurrentConnectorId(),
                tpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(layoutHandle));

        // Unknown row counts — cost-based would normally reject (no stats).
        // But the probe column is a partition column, so the partition column
        // fallback creates the filter (runtime max-tuple-domain-size caps
        // oversized domains).
        tester.assertThat(new AddDynamicFilterRule(metadataWithDiscretePredicates))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .on(p -> {
                    VariableReferenceExpression probeKey = p.variable("probeKey", BIGINT);
                    VariableReferenceExpression buildKey = p.variable("buildKey", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(tableHandle, ImmutableList.of(probeKey), ImmutableMap.of(probeKey, partitionColumn)),
                            p.values(buildKey),
                            new EquiJoinClause(probeKey, buildKey));
                })
                .overrideStats("0", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(Double.NaN)
                        .build())
                .overrideStats("1", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(Double.NaN)
                        .build())
                .matches(join(JoinType.INNER,
                        ImmutableList.of(equiJoinClause("probeKey", "buildKey")),
                        Optional.empty(),
                        tableScan("nation", ImmutableMap.of("probeKey", "nationkey")),
                        values("buildKey")));
    }
}

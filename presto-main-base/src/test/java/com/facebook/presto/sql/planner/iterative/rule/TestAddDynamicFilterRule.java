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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_CARDINALITY_RATIO_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_STRATEGY;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

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
        tester.assertThat(new AddDynamicFilterRule())
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
        tester.assertThat(new AddDynamicFilterRule())
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
        tester.assertThat(new AddDynamicFilterRule())
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
                        .build())
                .doesNotFire();
    }

    @Test
    public void testCostBasedCreatesFilterForGoodRatio()
    {
        tester.assertThat(new AddDynamicFilterRule())
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
        PlanNode result = tester.assertThat(new AddDynamicFilterRule())
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
                        .setOutputRowCount(100)
                        .build())
                .get();

        // Both clauses share the same build/probe ratio (100/1M), so both pass
        JoinNode joinResult = (JoinNode) result;
        assertEquals(joinResult.getDynamicFilters().size(), 2);
    }

    @Test
    public void testCostBasedCreatesFilterForUnknownStats()
    {
        tester.assertThat(new AddDynamicFilterRule())
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
                .matches(join(JoinType.INNER,
                        ImmutableList.of(equiJoinClause("probeKey", "buildKey")),
                        Optional.empty(),
                        values("probeKey"),
                        values("buildKey")));
    }

    @Test
    public void testCostBasedRespectsCustomThresholds()
    {
        tester.assertThat(new AddDynamicFilterRule())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_CARDINALITY_RATIO_THRESHOLD, "0.9")
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
        tester.assertThat(new AddDynamicFilterRule())
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
    public void testExtendedMetricsCreatedFavorableRatio()
    {
        Session session = Session.builder(tester.getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "true")
                .setRuntimeStats(new RuntimeStats())
                .build();

        tester.assertThat(new AddDynamicFilterRule())
                .withSession(session)
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
                        .build())
                .matches(join(JoinType.INNER,
                        ImmutableList.of(equiJoinClause("probeKey", "buildKey")),
                        Optional.empty(),
                        values("probeKey"),
                        values("buildKey")));

        String metricKey = format("%s[%s]", DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO, "probeKey");
        assertNotNull(session.getRuntimeStats().getMetrics().get(metricKey),
                "Should emit PLAN_CREATED_FAVORABLE_RATIO metric for probeKey");
        assertEquals(session.getRuntimeStats().getMetrics().get(metricKey).getSum(), 1);
    }

    @Test
    public void testExtendedMetricsSkippedHighCardinality()
    {
        Session session = Session.builder(tester.getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "true")
                .setRuntimeStats(new RuntimeStats())
                .build();

        tester.assertThat(new AddDynamicFilterRule())
                .withSession(session)
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
                        .build())
                .doesNotFire();

        String metricKey = format("%s[%s]", DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY, "probeKey");
        assertNotNull(session.getRuntimeStats().getMetrics().get(metricKey),
                "Should emit PLAN_SKIPPED_HIGH_CARDINALITY metric for probeKey");
        assertEquals(session.getRuntimeStats().getMetrics().get(metricKey).getSum(), 1);
    }

    @Test
    public void testExtendedMetricsNotEmittedWhenDisabled()
    {
        Session session = Session.builder(tester.getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "COST_BASED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "false")
                .setRuntimeStats(new RuntimeStats())
                .build();

        tester.assertThat(new AddDynamicFilterRule())
                .withSession(session)
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
                        .build())
                .matches(join(JoinType.INNER,
                        ImmutableList.of(equiJoinClause("probeKey", "buildKey")),
                        Optional.empty(),
                        values("probeKey"),
                        values("buildKey")));

        // No plan decision metrics should be emitted when extended metrics is disabled
        assertNull(session.getRuntimeStats().getMetrics().get(
                        format("%s[%s]", DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO, "probeKey")),
                "Should NOT emit plan decision metrics when extended metrics is disabled");
    }
}

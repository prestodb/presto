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
import com.facebook.presto.cost.PartialAggregationStatsEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.execution.warnings.DefaultWarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_BYTE_REDUCTION_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.USE_PARTIAL_AGGREGATION_HISTORY;
import static com.facebook.presto.common.WarningHandlingLevel.NORMAL;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.StandardWarningCode.FORCE_PUSH_PARTIAL_AGGREGATION_UNKNOWN_STATS;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.FACT;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.LOW;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static org.testng.Assert.assertTrue;

public class TestPushPartialAggregationThroughExchange
        extends BasePlanTest
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
    }

    private RuleAssert assertAutomaticPushPartialAggregationThroughExchange()
    {
        return tester.assertThat(new PushPartialAggregationThroughExchange(tester.getMetadata().getFunctionAndTypeManager(), false))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC");
    }

    @Test
    public void testPartialAggregationAdded()
    {
        assertAutomaticPushPartialAggregationThroughExchange()
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(a))
                                            .addInputsSet(a)
                                            .singleDistributionPartitioningScheme(a)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .matches(exchange(
                        project(
                                aggregation(
                                        ImmutableMap.of("SUM", functionCall("sum", ImmutableList.of("a"))),
                                        PARTIAL,
                                        values("a")))));
    }

    @Test
    public void testNoPartialAggregationWhenDisabled()
    {
        assertAutomaticPushPartialAggregationThroughExchange()
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "NEVER") // Override the AUTOMATIC
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(a))
                                            .addInputsSet(a)
                                            .singleDistributionPartitioningScheme(a)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .doesNotFire();
    }

    @Test
    public void testNoPartialAggregationWhenReductionBelowThreshold()
    {
        assertAutomaticPushPartialAggregationThroughExchange()
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", DOUBLE);
                    VariableReferenceExpression b = p.variable("b", DOUBLE);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(new PlanNodeId("values"), a, b))
                                            .addInputsSet(a, b)
                                            .singleDistributionPartitioningScheme(a, b)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .singleGroupingSet(b)
                            .step(SINGLE));
                })
                .overrideStats("values", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfidence(FACT)
                        .build())
                .doesNotFire();
    }

    @Test
    public void testNoPartialAggregationWhenReductionBelowThresholdUsingPartialAggregationStats()
    {
        assertAutomaticPushPartialAggregationThroughExchange()
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .on(p -> constructAggregation(p))
                .overrideStats("aggregation", PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfidence(FACT)
                        .setPartialAggregationStatsEstimate(new PartialAggregationStatsEstimate(1000, 800, 10, 10))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testNoPartialAggregationWhenReductionAboveThresholdUsingPartialAggregationStats()
    {
        // when use_partial_aggregation_history=true, we use row count reduction (instead of bytes) to decide if partial aggregation is useful
        assertAutomaticPushPartialAggregationThroughExchange()
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .on(p -> constructAggregation(p))
                .overrideStats("aggregation", PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfidence(FACT)
                        .setPartialAggregationStatsEstimate(new PartialAggregationStatsEstimate(1000, 300, 10, 10))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testNoPartialAggregationWhenRowReductionBelowThreshold()
    {
        assertAutomaticPushPartialAggregationThroughExchange()
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .on(p -> constructAggregation(p))
                .overrideStats("aggregation", PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfidence(FACT)
                        .setPartialAggregationStatsEstimate(new PartialAggregationStatsEstimate(0, 300, 10, 8))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testPartialAggregationWhenRowReductionAboveThreshold()
    {
        assertAutomaticPushPartialAggregationThroughExchange()
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .on(p -> constructAggregation(p))
                .overrideStats("aggregation", PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfidence(FACT)
                        .setPartialAggregationStatsEstimate(new PartialAggregationStatsEstimate(0, 300, 10, 1))
                        .build())
                .matches(aggregation(ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("sum0"))),
                        aggregation(
                                ImmutableMap.of("sum0", functionCall("sum", ImmutableList.of("a"))),
                                exchange(
                                        values("a", "b")))));
    }

    @Test
    public void testPartialAggregationEnabledWhenNotConfident()
    {
        assertAutomaticPushPartialAggregationThroughExchange()
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", DOUBLE);
                    VariableReferenceExpression b = p.variable("b", DOUBLE);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(new PlanNodeId("values"), a, b))
                                            .addInputsSet(a, b)
                                            .singleDistributionPartitioningScheme(a, b)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .singleGroupingSet(b)
                            .step(PARTIAL));
                })
                .overrideStats("values", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfidence(LOW)
                        .build())
                .matches(exchange(
                        project(
                                aggregation(
                                        ImmutableMap.of("SUM", functionCall("sum", ImmutableList.of("a"))),
                                        PARTIAL,
                                        values("a", "b")))));
    }

    @Test
    public void testPartialAggregationPushedBelowExchangeWhenAlreadySplit()
    {
        // An already-split PARTIAL aggregation must always be pushed below the exchange, even when the byte reduction
        // is below the threshold. These are the same stats as testNoPartialAggregationWhenReductionBelowThreshold (which
        // does not fire for a SINGLE step); the only difference is the PARTIAL step, which forces the push.
        assertAutomaticPushPartialAggregationThroughExchange()
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", DOUBLE);
                    VariableReferenceExpression b = p.variable("b", DOUBLE);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(new PlanNodeId("values"), a, b))
                                            .addInputsSet(a, b)
                                            .singleDistributionPartitioningScheme(a, b)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .singleGroupingSet(b)
                            .step(PARTIAL));
                })
                .overrideStats("values", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfidence(FACT)
                        .build())
                .matches(exchange(
                        project(
                                aggregation(
                                        ImmutableMap.of("SUM", functionCall("sum", ImmutableList.of("a"))),
                                        PARTIAL,
                                        values("a", "b")))));
    }

    @Test
    public void testForcePartialAggregationBelowExchangeWhenStatsAreUnknown()
    {
        // When the stats needed to decide if partial aggregation is useful are unknown (NaN), the rule should force the
        // push below the exchange and register a warning explaining why the cost-based decision was skipped.
        WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig(), NORMAL);
        Session session = Session.builder(tester.getSession())
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .setWarningCollector(warningCollector)
                .build();
        assertAutomaticPushPartialAggregationThroughExchange()
                .withSession(session)
                .on(p -> constructAggregation(p))
                .overrideStats("values", PlanNodeStatsEstimate.unknown())
                .matches(aggregation(ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("sum0"))),
                        aggregation(
                                ImmutableMap.of("sum0", functionCall("sum", ImmutableList.of("a"))),
                                exchange(
                                        values("a", "b")))));

        assertTrue(
                warningCollector.getWarnings().stream()
                        .anyMatch(warning -> warning.getWarningCode().equals(FORCE_PUSH_PARTIAL_AGGREGATION_UNKNOWN_STATS.toWarningCode())),
                "Expected a FORCE_PUSH_PARTIAL_AGGREGATION_UNKNOWN_STATS warning to be registered");
    }

    @Test
    public void testPartialAggregationSplitOverRemoteExchange()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .build();
        assertDistributedPlan(
                "select l.suppkey, sum(l.quantity) from lineitem AS l group by l.suppkey",
                session,
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        project(exchange(REMOTE_STREAMING, REPARTITION,
                                                project(aggregation(
                                                        ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("quantity"))), PARTIAL,
                                                        tableScan("lineitem", ImmutableMap.of("quantity", "quantity", "suppkey", "suppkey"))))))))));

        session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                // TPCH tiny has suppkey NDV = 100, total row count = 60175, 100/60175 ~= 0.00166. Any value below this will not trigger the partial agg pushdown
                .setSystemProperty(PARTIAL_AGGREGATION_BYTE_REDUCTION_THRESHOLD, "0.001")
                .build();

        assertDistributedPlan(
                "select l.suppkey, sum(l.quantity) from lineitem AS l group by l.suppkey",
                session,
                anyTree(
                        aggregation(
                                ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("quantity"))),
                                SINGLE,
                                exchange(LOCAL, GATHER,
                                        project(exchange(REMOTE_STREAMING, REPARTITION,
                                                project(tableScan("lineitem", ImmutableMap.of("quantity", "quantity", "suppkey", "suppkey")))))))));
    }

    private static AggregationNode constructAggregation(PlanBuilder p)
    {
        VariableReferenceExpression a = p.variable("a", DOUBLE);
        VariableReferenceExpression b = p.variable("b", DOUBLE);
        return p.aggregation(ab -> ab
                .source(
                        p.exchange(e -> e
                                .addSource(p.values(new PlanNodeId("values"), a, b))
                                .addInputsSet(a, b)
                                .singleDistributionPartitioningScheme(
                                        ImmutableList.of(a, b))))
                .addAggregation(p.variable("sum", DOUBLE), p.rowExpression("sum(a)"))
                .singleGroupingSet(b)
                .setPlanNodeId(new PlanNodeId("aggregation")));
    }
}

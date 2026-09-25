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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PARALLELIZE_CHAINED_AGGREGATION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;

public class TestParallelizeChainedAggregation
        extends BaseRuleTest
{
    @Test
    public void testFiresWhenOuterKeysSubsetOfInnerKeys()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.aggregation(inner -> inner
                                            .singleGroupingSet(k1, k2)
                                            .step(FINAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(k1, k2, x))))));
                })
                .matches(
                        // outer PARTIAL -> LocalRR (new) -> original Exchange -> inner FINAL
                        node(AggregationNode.class,
                                node(ExchangeNode.class,
                                        node(ExchangeNode.class,
                                                aggregation(ImmutableMap.of(), FINAL, values("k1", "k2", "x"))))));
    }

    @Test
    public void testFiresWhenOuterIsGlobalAggregation()
    {
        // Global outer aggregation (no grouping keys) — empty set is trivially a subset of inner
        // grouping keys, and the local round-robin gives the otherwise serially-executed outer
        // PARTIAL fanout across local drivers.
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .globalGrouping()
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.aggregation(inner -> inner
                                            .singleGroupingSet(k1, k2)
                                            .step(FINAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(k1, k2, x))))));
                })
                .matches(
                        // outer global PARTIAL -> LocalRR (new) -> original Exchange -> inner FINAL
                        node(AggregationNode.class,
                                node(ExchangeNode.class,
                                        node(ExchangeNode.class,
                                                aggregation(ImmutableMap.of(), FINAL, values("k1", "k2", "x"))))));
    }

    @Test
    public void testFiresWithProjectBetweenOuterAndExchange()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    Assignments identity = identityAssignments(k2, innerSum);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.project(
                                    identity,
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.aggregation(inner -> inner
                                                    .singleGroupingSet(k1, k2)
                                                    .step(FINAL)
                                                    .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                                    .source(p.values(k1, k2, x)))))));
                })
                .matches(
                        // outer PARTIAL -> LocalRR (new) -> Project -> Exchange -> inner FINAL
                        node(AggregationNode.class,
                                node(ExchangeNode.class,
                                        node(com.facebook.presto.spi.plan.ProjectNode.class,
                                                node(ExchangeNode.class,
                                                        aggregation(ImmutableMap.of(), FINAL, values("k1", "k2", "x")))))));
    }

    @Test
    public void testFiresWithProjectBetweenExchangeAndInner()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    Assignments identity = identityAssignments(k1, k2, innerSum);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.project(
                                            identity,
                                            p.aggregation(inner -> inner
                                                    .singleGroupingSet(k1, k2)
                                                    .step(FINAL)
                                                    .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                                    .source(p.values(k1, k2, x)))))));
                })
                .matches(
                        // outer PARTIAL -> LocalRR (new) -> Exchange -> Project -> inner FINAL
                        node(AggregationNode.class,
                                node(ExchangeNode.class,
                                        node(ExchangeNode.class,
                                                node(com.facebook.presto.spi.plan.ProjectNode.class,
                                                        aggregation(ImmutableMap.of(), FINAL, values("k1", "k2", "x")))))));
    }

    @Test
    public void testFiresWithProjectsOnBothSides()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.project(
                                    identityAssignments(k2, innerSum),
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.project(
                                                    identityAssignments(k1, k2, innerSum),
                                                    p.aggregation(inner -> inner
                                                            .singleGroupingSet(k1, k2)
                                                            .step(FINAL)
                                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                                            .source(p.values(k1, k2, x))))))));
                })
                .matches(
                        // outer PARTIAL -> LocalRR (new) -> Project -> Exchange -> Project -> inner FINAL
                        node(AggregationNode.class,
                                node(ExchangeNode.class,
                                        node(com.facebook.presto.spi.plan.ProjectNode.class,
                                                node(ExchangeNode.class,
                                                        node(com.facebook.presto.spi.plan.ProjectNode.class,
                                                                aggregation(ImmutableMap.of(), FINAL, values("k1", "k2", "x"))))))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "false")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.aggregation(inner -> inner
                                            .singleGroupingSet(k1, k2)
                                            .step(FINAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(k1, k2, x))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenOuterKeysEqualInnerKeys()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k1)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.aggregation(inner -> inner
                                            .singleGroupingSet(k1)
                                            .step(FINAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(k1, x))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenOuterKeysNotSubsetOfInnerKeys()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression k3 = p.variable("k3", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k3)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.aggregation(inner -> inner
                                            .singleGroupingSet(k1, k2)
                                            .step(FINAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(k1, k2, k3, x))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenBothAreGlobal()
    {
        // Both global: outerKeys.equals(innerKeys) (both empty) — not a cascading shape.
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .globalGrouping()
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.aggregation(inner -> inner
                                            .globalGrouping()
                                            .step(FINAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(x))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenOuterIsNotPartial()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(FINAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.aggregation(inner -> inner
                                            .singleGroupingSet(k1, k2)
                                            .step(FINAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(k1, k2, x))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenInnerIsNotFinal()
    {
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.aggregation(inner -> inner
                                            .singleGroupingSet(k1, k2)
                                            .step(PARTIAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(k1, k2, x))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenIntermediateExchangeHasMultipleSources()
    {
        // A multi-source ExchangeNode (e.g. UNION ALL) above the inner aggregation is not safe to
        // traverse — picking source[0] would silently skip the other branches and cause the rule
        // to fire incorrectly when only one branch happens to be the matching inner aggregation.
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    PlanNode innerAgg = p.aggregation(inner -> inner
                            .singleGroupingSet(k1, k2)
                            .step(FINAL)
                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                            .source(p.values(k1, k2, x)));

                    PlanNode otherBranch = p.values(k1, k2, innerSum);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.exchange(e -> e
                                    .type(ExchangeNode.Type.GATHER)
                                    .scope(ExchangeNode.Scope.LOCAL)
                                    .singleDistributionPartitioningScheme(k1, k2, innerSum)
                                    .addSource(innerAgg)
                                    .addInputsSet(k1, k2, innerSum)
                                    .addSource(otherBranch)
                                    .addInputsSet(k1, k2, innerSum))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenNonPassthroughNodeIntervenes()
    {
        // A FilterNode between outer and inner is NOT a pass-through node (it changes which rows
        // reach the inner aggregation). The walker only crosses Projects and Exchanges, so the
        // rule must not fire here.
        tester().assertThat(new ParallelizeChainedAggregation())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression innerSum = p.variable("inner_sum", BIGINT);
                    VariableReferenceExpression outerSum = p.variable("outer_sum", BIGINT);

                    return p.aggregation(outer -> outer
                            .singleGroupingSet(k2)
                            .step(PARTIAL)
                            .addAggregation(outerSum, p.rowExpression("sum(inner_sum)"))
                            .source(p.filter(
                                    p.rowExpression("inner_sum > 0"),
                                    p.aggregation(inner -> inner
                                            .singleGroupingSet(k1, k2)
                                            .step(FINAL)
                                            .addAggregation(innerSum, p.rowExpression("sum(x)"))
                                            .source(p.values(k1, k2, x))))));
                })
                .doesNotFire();
    }
}

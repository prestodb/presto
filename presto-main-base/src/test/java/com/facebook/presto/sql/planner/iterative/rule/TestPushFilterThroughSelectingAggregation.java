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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_FILTER_THROUGH_SELECTING_AGGREGATION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushFilterThroughSelectingAggregation
        extends BaseRuleTest
{
    @Test
    public void testPushesMaxGreaterThanOrEqual()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression maxX = p.variable("max_x", BIGINT);
                    return p.filter(
                            p.rowExpression("max_x >= BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(maxX, p.rowExpression("max(x)"))
                                    .singleGroupingSet(g)));
                })
                .matches(
                        aggregation(
                                singleGroupingSet("g"),
                                ImmutableMap.of(Optional.of("max_x"), functionCall("max", ImmutableList.of("x"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                filter(
                                        "x >= BIGINT '5'",
                                        values("x", "g"))));
    }

    @Test
    public void testPushesMaxGreaterThan()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression maxX = p.variable("max_x", BIGINT);
                    return p.filter(
                            p.rowExpression("max_x > BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(maxX, p.rowExpression("max(x)"))
                                    .singleGroupingSet(g)));
                })
                .matches(
                        aggregation(
                                singleGroupingSet("g"),
                                ImmutableMap.of(Optional.of("max_x"), functionCall("max", ImmutableList.of("x"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                filter(
                                        "x > BIGINT '5'",
                                        values("x", "g"))));
    }

    @Test
    public void testPushesMinLessThanOrEqual()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression minX = p.variable("min_x", BIGINT);
                    return p.filter(
                            p.rowExpression("min_x <= BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(minX, p.rowExpression("min(x)"))
                                    .singleGroupingSet(g)));
                })
                .matches(
                        aggregation(
                                singleGroupingSet("g"),
                                ImmutableMap.of(Optional.of("min_x"), functionCall("min", ImmutableList.of("x"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                filter(
                                        "x <= BIGINT '5'",
                                        values("x", "g"))));
    }

    @Test
    public void testPushesMinLessThan()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression minX = p.variable("min_x", BIGINT);
                    return p.filter(
                            p.rowExpression("min_x < BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(minX, p.rowExpression("min(x)"))
                                    .singleGroupingSet(g)));
                })
                .matches(
                        aggregation(
                                singleGroupingSet("g"),
                                ImmutableMap.of(Optional.of("min_x"), functionCall("min", ImmutableList.of("x"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                filter(
                                        "x < BIGINT '5'",
                                        values("x", "g"))));
    }

    @Test
    public void testPushesArbitraryEquals()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression arbX = p.variable("arb_x", BIGINT);
                    return p.filter(
                            p.rowExpression("arb_x = BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(arbX, p.rowExpression("arbitrary(x)"))
                                    .singleGroupingSet(g)));
                })
                .matches(
                        aggregation(
                                singleGroupingSet("g"),
                                ImmutableMap.of(Optional.of("arb_x"), functionCall("arbitrary", ImmutableList.of("x"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                filter(
                                        "x = BIGINT '5'",
                                        values("x", "g"))));
    }

    @Test
    public void testDoesNotPushMaxLessThan()
    {
        // MAX with < / <= would change which groups appear in the result — never safe to pushdown.
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression maxX = p.variable("max_x", BIGINT);
                    return p.filter(
                            p.rowExpression("max_x <= BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(maxX, p.rowExpression("max(x)"))
                                    .singleGroupingSet(g)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushMinGreaterThan()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression minX = p.variable("min_x", BIGINT);
                    return p.filter(
                            p.rowExpression("min_x >= BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(minX, p.rowExpression("min(x)"))
                                    .singleGroupingSet(g)));
                })
                .doesNotFire();
    }

    @Test
    public void testMaxEqualsAddsPreFilterAndKeepsHaving()
    {
        // MAX(x) = c → push x >= c below (relaxed pre-filter), KEEP the original HAVING above.
        // A direct x = c rewrite would accept spurious groups.
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression maxX = p.variable("max_x", BIGINT);
                    return p.filter(
                            p.rowExpression("max_x = BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(maxX, p.rowExpression("max(x)"))
                                    .singleGroupingSet(g)));
                })
                .matches(
                        filter(
                                "max_x = BIGINT '5'",
                                aggregation(
                                        singleGroupingSet("g"),
                                        ImmutableMap.of(Optional.of("max_x"), functionCall("max", ImmutableList.of("x"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        filter(
                                                "x >= BIGINT '5'",
                                                values("x", "g")))));
    }

    @Test
    public void testMinEqualsAddsPreFilterAndKeepsHaving()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression minX = p.variable("min_x", BIGINT);
                    return p.filter(
                            p.rowExpression("min_x = BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(minX, p.rowExpression("min(x)"))
                                    .singleGroupingSet(g)));
                })
                .matches(
                        filter(
                                "min_x = BIGINT '5'",
                                aggregation(
                                        singleGroupingSet("g"),
                                        ImmutableMap.of(Optional.of("min_x"), functionCall("min", ImmutableList.of("x"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        filter(
                                                "x <= BIGINT '5'",
                                                values("x", "g")))));
    }

    @Test
    public void testDoesNotPushWhenMultipleAggregates()
    {
        // Pushing the filter below the aggregation would change the row set seen by sum.
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression maxX = p.variable("max_x", BIGINT);
                    VariableReferenceExpression sumY = p.variable("sum_y", BIGINT);
                    return p.filter(
                            p.rowExpression("max_x >= BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, y, g))
                                    .addAggregation(maxX, p.rowExpression("max(x)"))
                                    .addAggregation(sumY, p.rowExpression("sum(y)"))
                                    .singleGroupingSet(g)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushWhenSessionPropertyDisabled()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "false")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression maxX = p.variable("max_x", BIGINT);
                    return p.filter(
                            p.rowExpression("max_x >= BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(maxX, p.rowExpression("max(x)"))
                                    .singleGroupingSet(g)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushWhenAggregateArgumentIsExpression()
    {
        // The aggregate argument must be a direct VariableReferenceExpression — max(x + 1) is not eligible.
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression maxX = p.variable("max_x", BIGINT);
                    return p.filter(
                            p.rowExpression("max_x >= BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(maxX, p.rowExpression("max(x + BIGINT '1')"))
                                    .singleGroupingSet(g)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushSumAggregate()
    {
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression sumX = p.variable("sum_x", BIGINT);
                    return p.filter(
                            p.rowExpression("sum_x >= BIGINT '5'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(sumX, p.rowExpression("sum(x)"))
                                    .singleGroupingSet(g)));
                })
                .doesNotFire();
    }

    @Test
    public void testPartialPushdownSplitsIntoTwoFilters()
    {
        // The pushable conjunct goes below the aggregation; the unpushable one stays above.
        tester().assertThat(new PushFilterThroughSelectingAggregation(tester().getMetadata()))
                .setSystemProperty(PUSH_FILTER_THROUGH_SELECTING_AGGREGATION, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression g = p.variable("g", BIGINT);
                    VariableReferenceExpression maxX = p.variable("max_x", BIGINT);
                    return p.filter(
                            p.rowExpression("max_x >= BIGINT '5' AND g <> BIGINT '0'"),
                            p.aggregation(ab -> ab
                                    .source(p.values(x, g))
                                    .addAggregation(maxX, p.rowExpression("max(x)"))
                                    .singleGroupingSet(g)));
                })
                .matches(
                        filter(
                                "g <> BIGINT '0'",
                                aggregation(
                                        singleGroupingSet("g"),
                                        ImmutableMap.of(Optional.of("max_x"), functionCall("max", ImmutableList.of("x"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        filter(
                                                "x >= BIGINT '5'",
                                                values("x", "g")))));
    }
}

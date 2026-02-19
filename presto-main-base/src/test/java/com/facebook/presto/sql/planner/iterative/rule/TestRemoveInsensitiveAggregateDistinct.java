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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.symbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveInsensitiveAggregateDistinct
        extends BaseRuleTest
{
    @Test
    public void testNoDistinct()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output"), p.rowExpression("max(input1)"))))
                .doesNotFire();
    }

    @Test
    public void testSensitiveDistinct()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output"), p.rowExpression("count(input1)"), true)))
                .doesNotFire();

        assertRuleApplication()
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output"), p.rowExpression("min(input1, 2)"), true)))
                .doesNotFire();
    }

    @Test
    public void testSensitiveOrder()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output"), p.rowExpression("set_agg(input1)"), true)))
                .doesNotFire();

        assertRuleApplication()
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1", new ArrayType(BIGINT)), p.variable("input2")))
                        .addAggregation(p.variable("output"), p.rowExpression("set_union(input1)"), true)))
                .doesNotFire();
    }

    @Test
    public void testInsensitiveDistinct()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output"), p.rowExpression("max(input1)"), true)))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("output"),
                                        functionCall("max", false, ImmutableList.of(symbol("input1")))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                Step.SINGLE,
                                values("input1", "input2")));
    }

    @Test
    public void testMultipleInsensitiveDistinct()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2", BOOLEAN)))
                        .addAggregation(p.variable("output1"), p.rowExpression("any_value(input1)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("arbitrary(input1)"), true)
                        .addAggregation(p.variable("output3"), p.rowExpression("bitwise_and_agg(input1)"), true)
                        .addAggregation(p.variable("output4"), p.rowExpression("bitwise_or_agg(input1)"), true)
                        .addAggregation(p.variable("output5"), p.rowExpression("bool_and(input2)"), true)
                        .addAggregation(p.variable("output6"), p.rowExpression("bool_or(input2)"), true)
                        .addAggregation(p.variable("output7"), p.rowExpression("every(input2)"), true)
                        .addAggregation(p.variable("output8"), p.rowExpression("max(input1)"), true)
                        .addAggregation(p.variable("output9"), p.rowExpression("min(input1)"), true)))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                        .put(Optional.of("output1"), functionCall("any_value", false, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output2"), functionCall("arbitrary", false, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output3"), functionCall("bitwise_and_agg", false, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output4"), functionCall("bitwise_or_agg", false, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output5"), functionCall("bool_and", false, ImmutableList.of(symbol("input2"))))
                                        .put(Optional.of("output6"), functionCall("bool_or", false, ImmutableList.of(symbol("input2"))))
                                        .put(Optional.of("output7"), functionCall("every", false, ImmutableList.of(symbol("input2"))))
                                        .put(Optional.of("output8"), functionCall("max", false, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output9"), functionCall("min", false, ImmutableList.of(symbol("input1"))))
                                        .build(),
                                ImmutableMap.of(),
                                Optional.empty(),
                                Step.SINGLE,
                                values("input1", "input2")));
    }

    @Test
    public void testMixedDistinct()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2", BOOLEAN)))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(input1)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("sum(input1)"), true)
                        .addAggregation(p.variable("output3"), p.rowExpression("avg(input1)"), true)
                        .addAggregation(p.variable("output4"), p.rowExpression("max(input1)"), true)
                        .addAggregation(p.variable("output5"), p.rowExpression("min(input1)"), true)))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                        .put(Optional.of("output1"), functionCall("count", true, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output2"), functionCall("sum", true, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output3"), functionCall("avg", true, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output4"), functionCall("max", false, ImmutableList.of(symbol("input1"))))
                                        .put(Optional.of("output5"), functionCall("min", false, ImmutableList.of(symbol("input1"))))
                                        .build(),
                                ImmutableMap.of(),
                                Optional.empty(),
                                Step.SINGLE,
                                values("input1", "input2")));
    }

    private RuleAssert assertRuleApplication()
    {
        return tester().assertThat(new RemoveInsensitiveAggregateDistinct(getFunctionManager()));
    }
}

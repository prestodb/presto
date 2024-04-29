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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.groupingSets;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPullConstantsAboveGroupBy
        extends BaseRuleTest
{
    @Test
    public void testNoConstGroupingKeysDoesNotFire()
    {
        tester().assertThat(new PullConstantsAboveGroupBy())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.variable("COL"), p.rowExpression("COL"))
                                                .put(p.variable("CONST_COL"), p.rowExpression("1"))
                                                .build(),
                                        p.values(p.variable("COL"))))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL)"))
                        .singleGroupingSet(p.variable("COL"))))
                .doesNotFire();
    }

    @Test
    public void testMultipleGroupingSetsDoesNotFire()
    {
        tester().assertThat(new PullConstantsAboveGroupBy())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.variable("COL"), p.rowExpression("COL"))
                                                .put(p.variable("CONST_COL"), p.rowExpression("1"))
                                                .build(),
                                        p.values(p.variable("COL"))))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL)"))
                        .groupingSets(
                                groupingSets(ImmutableList.of(p.variable("COL")), 2, ImmutableSet.of(0)))))
                .doesNotFire();
    }

    @Test
    public void testRuleDisabledDoesNotFire()
    {
        RuleTester tester = new RuleTester(ImmutableList.of(), ImmutableMap.of("optimize_constant_grouping_keys", "false", "rewrite_expression_with_constant_expression", "false"));

        tester.assertThat(new PullConstantsAboveGroupBy())
            .on(p -> p.aggregation(ab -> ab
                    .source(
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("COL"), p.rowExpression("COL"))
                                            .put(p.variable("CONST_COL"), p.rowExpression("1"))
                                            .build(),
                                    p.values(p.variable("COL"))))
                    .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL)"))
                    .singleGroupingSet(p.variable("CONST_COL"), p.variable("COL"))))
            .doesNotFire();
    }

    @Test
    public void testOnlyConstantKeysDoesNotFire()
    {
        tester().assertThat(new PullConstantsAboveGroupBy())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.variable("COL"), p.rowExpression("COL"))
                                                .put(p.variable("CONST_COL"), p.rowExpression("1"))
                                                .build(),
                                        p.values(p.variable("COL"))))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL)"))
                        .singleGroupingSet(p.variable("CONST_COL"))))
                .doesNotFire();
    }

    @Test
    public void testSingleConstColumn()
    {
        tester().assertThat(new PullConstantsAboveGroupBy())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.variable("COL"), p.rowExpression("COL"))
                                                .put(p.variable("CONST_COL"), p.rowExpression("1"))
                                                .build(),
                                        p.values(p.variable("COL"))))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL)"))
                        .singleGroupingSet(p.variable("CONST_COL"), p.variable("COL"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "CONST_COL", expression("1")),
                                aggregation(
                                        singleGroupingSet("COL"),
                                        ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                                .put(Optional.of("AVG"), functionCall("avg", ImmutableList.of("col")))
                                                .build(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(ImmutableMap.of(
                                                        "CONST_COL", expression("1")),
                                        values("COL")))));
    }

    @Test
    public void testMultipleConstCols()
    {
        tester().assertThat(new PullConstantsAboveGroupBy())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.variable("COL"), p.rowExpression("COL"))
                                                .put(p.variable("CONST_COL1"), p.rowExpression("1"))
                                                .put(p.variable("CONST_COL2"), p.rowExpression("2"))
                                                .build(),
                                        p.values(p.variable("COL"))))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL)"))
                        .singleGroupingSet(p.variable("CONST_COL1"), p.variable("COL"), p.variable("CONST_COL2"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "CONST_COL1", expression("1"),
                                        "CONST_COL2", expression("2")),
                                aggregation(
                                        singleGroupingSet("COL"),
                                        ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                                .put(Optional.of("AVG"), functionCall("avg", ImmutableList.of("col")))
                                                .build(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(ImmutableMap.of(
                                                        "CONST_COL1", expression("1"),
                                                        "CONST_COL2", expression("2")),
                                                values("COL")))));
    }
}

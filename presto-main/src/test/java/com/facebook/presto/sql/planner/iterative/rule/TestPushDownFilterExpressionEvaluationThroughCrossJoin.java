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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushDownFilterExpressionEvaluationThroughCrossJoin
        extends BaseRuleTest
{
    @Test
    public void testTriggerWithAddition()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("left_k1+left_k2 = right_k1+right_k2"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                })
                .matches(
                        project(
                                filter(
                                        "add_1 = add_0",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(),
                                                project(
                                                        ImmutableMap.of("add_1", expression("left_k1+left_k2")),
                                                        values("left_k1", "left_k2")),
                                                project(
                                                        ImmutableMap.of("add_0", expression("right_k1+right_k2")),
                                                        values("right_k1", "right_k2"))))));
    }

    @Test
    public void testNotTriggerWithAddition()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("left_k1+right_k1 = left_k2+right_k2"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                }).doesNotFire();
    }

    @Test
    public void testTriggerWithArrayCardinality()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("right_k1", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("left_k1 = cardinality(right_k1)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1")),
                                    p.values(p.variable("right_k1", new ArrayType(BIGINT)))));
                })
                .matches(
                        project(
                                filter(
                                        "left_k1 = card",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(),
                                                values("left_k1"),
                                                project(
                                                        ImmutableMap.of("card", expression("cardinality(right_k1)")),
                                                        values("right_k1"))))));
    }

    @Test
    public void testCast()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", VARCHAR);
                    p.variable("left_k2", VARCHAR);
                    p.variable("right_k1", VARCHAR);
                    p.variable("right_k2", VARCHAR);
                    return p.filter(
                            p.rowExpression("left_k1 = right_k1 or CAST(left_k2 AS BIGINT) = CAST(right_k2 AS BIGINT)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", VARCHAR), p.variable("left_k2", VARCHAR)),
                                    p.values(p.variable("right_k1", VARCHAR), p.variable("right_k2", VARCHAR))));
                })
                .matches(
                        project(
                                filter(
                                        "left_k1 = right_k1 OR cast_1 = cast_0",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(),
                                                project(
                                                        ImmutableMap.of("cast_1", expression("CAST(left_k2 AS bigint)")),
                                                        values("left_k1", "left_k2")),
                                                project(
                                                        ImmutableMap.of("cast_0", expression("CAST(right_k2 AS bigint)")),
                                                        values("right_k1", "right_k2"))))));
    }

    @Test
    public void testCoalesce()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", VARCHAR);
                    p.variable("right_k3", VARCHAR);
                    return p.filter(
                            p.rowExpression("left_k1 = right_k1 or CAST(left_k1 AS VARCHAR) = COALESCE(right_k2, right_k3)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2", VARCHAR), p.variable("right_k3", VARCHAR))));
                })
                .matches(
                        project(
                                filter(
                                        "left_k1 = right_k1 OR cast_1 = expr",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(),
                                                project(
                                                        ImmutableMap.of("cast_1", expression("CAST(left_k1 AS varchar)")),
                                                        values("left_k1")),
                                                project(
                                                        ImmutableMap.of("expr", expression("COALESCE(right_k2, right_k3)")),
                                                        values("right_k1", "right_k2", "right_k3"))))));
    }

    @Test
    public void testTriggerWithArrayContains()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", VARCHAR);
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("contains(right_array_k1, cast(left_k1 as BIGINT))"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", VARCHAR)),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)))));
                })
                .matches(
                        project(
                                filter(
                                        "contains(right_array_k1, cast_l)",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(),
                                                project(
                                                        ImmutableMap.of("cast_l", expression("CAST(left_k1 AS bigint)")),
                                                        values("left_k1")),
                                                values("right_array_k1")))));
    }

    @Test
    public void testTriggerWithArrayContains2()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", VARCHAR);
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("contains(cast(right_array_k1 as array<varchar>), left_k1)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", VARCHAR)),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)))));
                })
                .matches(
                        project(
                                filter(
                                        "contains(cast_arr, left_k1)",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(),
                                                values("left_k1"),
                                                project(
                                                        ImmutableMap.of("cast_arr", expression("cast(right_array_k1 as array<varchar>)")),
                                                        values("right_array_k1"))))));
    }

    @Test
    public void testNotTriggerWithArrayContains()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", VARCHAR);
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("contains(right_array_k1, cast(left_k1 as BIGINT)) or cast(left_k1 as BIGINT) > 2"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", VARCHAR)),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerWithArrayContains2()
    {
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", DOUBLE);
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("contains(cast(right_array_k1 as array<DOUBLE>), left_k1)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", DOUBLE)),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)))));
                }).doesNotFire();
    }
}

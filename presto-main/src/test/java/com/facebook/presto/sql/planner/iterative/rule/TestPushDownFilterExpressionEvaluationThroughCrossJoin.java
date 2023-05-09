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
                            p.rowExpression("left_k1 = right_k1 or CAST(left_k2 AS DOUBLE) = CAST(right_k2 AS DOUBLE)"),
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
                                                        ImmutableMap.of("cast_1", expression("CAST(left_k2 AS double)")),
                                                        values("left_k1", "left_k2")),
                                                project(
                                                        ImmutableMap.of("cast_0", expression("CAST(right_k2 AS double)")),
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
}

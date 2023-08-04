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
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestCrossJoinWithArrayContainsToInnerJoin
        extends BaseRuleTest
{
    @Test
    public void testTriggerForBigInt()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, right_k1)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT))),
                                    p.values(p.variable("right_k1"))));
                })
                .matches(
                        join(
                                JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("field", "right_k1")),
                                unnest(
                                        ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                        project(
                                                ImmutableMap.of("array_distinct", expression("array_distinct(left_array_k1)")),
                                                values("left_array_k1"))),
                                values("right_k1")));
    }

    @Test
    public void testTriggerForBigIntArrayRightSide()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("contains(right_array_k1, left_k1)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1")),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)))));
                })
                .matches(
                        join(
                                JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("left_k1", "field")),
                                values("left_k1"),
                                unnest(
                                        ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                        project(
                                                ImmutableMap.of("array_distinct", expression("array_distinct(right_array_k1)")),
                                                values("right_array_k1")))));
    }

    @Test
    public void testMultipleArrayContainsConditions()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_array_k2", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, right_k1) and contains(right_array_k2, left_k2)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_array_k2", new ArrayType(BIGINT)))));
                })
                .matches(
                        filter(
                                "contains(right_array_k2, left_k2)",
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("field", "right_k1")),
                                        unnest(
                                                ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                                project(
                                                        ImmutableMap.of("array_distinct", expression("array_distinct(left_array_k1)")),
                                                        values("left_array_k1", "left_k2"))),
                                        values("right_k1", "right_array_k2"))));
    }

    @Test
    public void testMultipleInvalidArrayContainsConditions()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_array_k2", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, right_k1) or contains(right_array_k2, left_k2)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_array_k2", new ArrayType(BIGINT)))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerForDouble()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(DOUBLE));
                    p.variable("right_k1", DOUBLE);
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, right_k1)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(DOUBLE))),
                                    p.values(p.variable("right_k1", DOUBLE))));
                }).doesNotFire();
    }

    @Test
    public void testArrayContainsWithCast()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", VARCHAR);
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, CAST(right_k1 AS BIGINT))"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", new ArrayType(BIGINT))),
                                    p.values(p.variable("right_k1", VARCHAR))));
                }).doesNotFire();
    }

    @Test
    public void testArrayContainsWithCastBothRules()
    {
        tester().assertThat(
                ImmutableSet.of(
                        new PushDownFilterExpressionEvaluationThroughCrossJoin(getFunctionManager()),
                        new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager())))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", VARCHAR);
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, CAST(right_k1 AS BIGINT))"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT))),
                                    p.values(p.variable("right_k1", VARCHAR))));
                })
                .matches(
                        project(
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("field", "cast_r")),
                                        unnest(
                                                ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                                project(
                                                        ImmutableMap.of("array_distinct", expression("array_distinct(left_array_k1)")),
                                                        values("left_array_k1"))),
                                        project(
                                                ImmutableMap.of("cast_r", expression("CAST(right_k1 AS bigint)")),
                                                values("right_k1")))));
    }

    @Test
    public void testArrayContainsWithCastBothRules2()
    {
        tester().assertThat(
                ImmutableSet.of(
                        new PushDownFilterExpressionEvaluationThroughCrossJoin(getFunctionManager()),
                        new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager())))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", VARCHAR);
                    return p.filter(
                            p.rowExpression("contains(CAST(left_array_k1 AS ARRAY<VARCHAR>), right_k1)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT))),
                                    p.values(p.variable("right_k1", VARCHAR))));
                })
                .matches(
                        project(
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("field", "right_k1")),
                                        unnest(
                                                ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                                project(
                                                        ImmutableMap.of("array_distinct", expression("array_distinct(cast_array)")),
                                                        project(
                                                                ImmutableMap.of("cast_array", expression("cast(left_array_k1 as array<varchar>)")),
                                                                values("left_array_k1")))),
                                        values("right_k1"))));
    }

    @Test
    public void testArrayContainsWithCastBothRulesArrayRightSide()
    {
        tester().assertThat(
                ImmutableSet.of(
                        new PushDownFilterExpressionEvaluationThroughCrossJoin(getFunctionManager()),
                        new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager())))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_k1", VARCHAR);
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("contains(right_array_k1, CAST(left_k1 AS BIGINT))"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", VARCHAR)),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)))));
                })
                .matches(
                        project(
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("cast_l", "field")),
                                        project(
                                                ImmutableMap.of("cast_l", expression("CAST(left_k1 AS bigint)")),
                                                values("left_k1")),
                                        unnest(
                                                ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                                project(
                                                        ImmutableMap.of("array_distinct", expression("array_distinct(right_array_k1)")),
                                                        values("right_array_k1"))))));
    }

    @Test
    public void testArrayContainsWithCoalesceBothRules()
    {
        tester().assertThat(
                ImmutableSet.of(
                        new PushDownFilterExpressionEvaluationThroughCrossJoin(getFunctionManager()),
                        new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager())))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", VARCHAR);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, coalesce(CAST(right_k1 AS BIGINT), right_k2))"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT))),
                                    p.values(p.variable("right_k1", VARCHAR), p.variable("right_k2", BIGINT))));
                })
                .matches(
                        project(
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("field", "expr")),
                                        unnest(
                                                ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                                project(
                                                        ImmutableMap.of("array_distinct", expression("array_distinct(left_array_k1)")),
                                                        values("left_array_k1"))),
                                        project(
                                                ImmutableMap.of("expr", expression("COALESCE(CAST(right_k1 AS bigint), right_k2)")),
                                                values("right_k1", "right_k2")))));
    }

    @Test
    public void testArrayContainsWithCoalesceBothRulesArrayOnRight()
    {
        tester().assertThat(
                ImmutableSet.of(
                        new PushDownFilterExpressionEvaluationThroughCrossJoin(getFunctionManager()),
                        new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager())))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k1", VARCHAR);
                    p.variable("left_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("contains(right_array_k1, coalesce(CAST(left_k1 AS BIGINT), left_k2))"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", VARCHAR), p.variable("left_k2", BIGINT)),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)))));
                })
                .matches(
                        project(
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("expr", "field")),
                                        project(
                                                ImmutableMap.of("expr", expression("COALESCE(CAST(left_k1 AS bigint), left_k2)")),
                                                values("left_k1", "left_k2")),
                                        unnest(
                                                ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                                project(
                                                        ImmutableMap.of("array_distinct", expression("array_distinct(right_array_k1)")),
                                                        values("right_array_k1"))))));
    }

    @Test
    public void testConditionWithAnd()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, right_k1) and left_k2+right_k2 > 10"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                })
                .matches(
                        filter(
                                "left_k2+right_k2 > 10",
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("field", "right_k1")),
                                        unnest(
                                                ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                                project(
                                                        ImmutableMap.of("array_distinct", expression("array_distinct(left_array_k1)")),
                                                        values("left_array_k1", "left_k2"))),
                                        values("right_k1", "right_k2"))));
    }

    @Test
    public void testConditionWithAndArrayOnRight()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k2", BIGINT);
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("contains(right_array_k1, left_k1) and right_k2+left_k2 > 10"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)), p.variable("right_k2"))));
                })
                .matches(
                        filter(
                                "right_k2+left_k2 > 10",
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("left_k1", "field")),
                                        values("left_k1", "left_k2"),
                                        unnest(
                                                ImmutableMap.of("array_distinct", ImmutableList.of("field")),
                                                project(
                                                        ImmutableMap.of("array_distinct", expression("array_distinct(right_array_k1)")),
                                                        values("right_array_k1", "right_k2"))))));
    }

    @Test
    public void testNonMatchingCondition()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, right_k1) or left_k2+right_k2 > 10"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                }).doesNotFire();
    }

    @Test
    public void testNonMatchingCondition2()
    {
        tester().assertThat(new CrossJoinWithArrayContainsToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("contains(left_array_k1, left_k2)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                }).doesNotFire();
    }
}

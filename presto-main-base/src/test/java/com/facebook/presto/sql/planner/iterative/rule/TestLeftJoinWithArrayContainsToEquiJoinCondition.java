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
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.singletonList;

public class TestLeftJoinWithArrayContainsToEquiJoinCondition
        extends BaseRuleTest
{
    @BeforeClass
    @Override
    public void setUp()
    {
        tester = new RuleTester(singletonList(new SqlInvokedFunctionsPlugin()));
    }

    @Test
    public void testTriggerForBigIntArrayRightSide()
    {
        tester().assertThat(new LeftJoinWithArrayContainsToEquiJoinCondition(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    return
                            p.join(JoinType.LEFT,
                                    p.values(p.variable("left_k1")),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT))),
                                    p.rowExpression("contains(right_array_k1, left_k1)"));
                }).matches(
                        join(
                                JoinType.LEFT,
                                ImmutableList.of(equiJoinClause("left_k1", "unnest")),
                                values("left_k1"),
                                unnest(
                                        ImmutableMap.of("array_distinct", ImmutableList.of("unnest")),
                                        project(
                                                ImmutableMap.of("array_distinct", expression("remove_nulls(array_distinct(right_array_k1))")),
                                                values("right_array_k1")))));
    }

    @Test
    public void testNotTriggerForArrayOnLeftSide()
    {
        tester().assertThat(new LeftJoinWithArrayContainsToEquiJoinCondition(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return
                            p.join(JoinType.LEFT,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT))),
                                    p.values(p.variable("right_k1")),
                                    p.rowExpression("contains(left_array_k1, right_k1)"));
                }).doesNotFire();
    }

    @Test
    public void testMultipleArrayContainsConditions()
    {
        tester().assertThat(new LeftJoinWithArrayContainsToEquiJoinCondition(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_array_k2", new ArrayType(BIGINT));
                    return
                            p.join(JoinType.LEFT,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_array_k2", new ArrayType(BIGINT))),
                                    p.rowExpression("contains(left_array_k1, right_k1) and contains(right_array_k2, left_k2)"));
                }).matches(
                        join(
                                JoinType.LEFT,
                                ImmutableList.of(equiJoinClause("left_k2", "unnest")),
                                Optional.of("contains(left_array_k1, right_k1)"),
                                values("left_array_k1", "left_k2"),
                                unnest(
                                        ImmutableMap.of("array_distinct", ImmutableList.of("unnest")),
                                        project(
                                                ImmutableMap.of("array_distinct", expression("remove_nulls(array_distinct(right_array_k2))")),
                                                values("right_k1", "right_array_k2")))));
    }

    @Test
    public void testMultipleInvalidArrayContainsConditions()
    {
        tester().assertThat(new LeftJoinWithArrayContainsToEquiJoinCondition(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_array_k2", new ArrayType(BIGINT));
                    return
                            p.join(JoinType.LEFT,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_array_k2", new ArrayType(BIGINT))),
                                    p.rowExpression("contains(left_array_k1, right_k1) or contains(right_array_k2, left_k2)"));
                }).doesNotFire();
    }

    @Test
    public void testArrayContainsWithCast()
    {
        tester().assertThat(new LeftJoinWithArrayContainsToEquiJoinCondition(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .on(p ->
                {
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k1", VARCHAR);
                    return p.join(JoinType.LEFT,
                            p.values(p.variable("left_k1", VARCHAR)),
                            p.values(p.variable("right_array_k1", new ArrayType(BIGINT))),
                            p.rowExpression("contains(right_array_k1, CAST(left_k1 AS BIGINT))"));
                }).matches(
                        join(
                                JoinType.LEFT,
                                ImmutableList.of(equiJoinClause("cast_left", "unnest")),
                                project(
                                        ImmutableMap.of("cast_left", expression("CAST(left_k1 AS BIGINT)")),
                                        values("left_k1")),
                                unnest(
                                        ImmutableMap.of("array_distinct", ImmutableList.of("unnest")),
                                        project(
                                                ImmutableMap.of("array_distinct", expression("remove_nulls(array_distinct(right_array_k1))")),
                                                values("right_array_k1")))));
    }

    @Test
    public void testArrayContainsWithCast2()
    {
        tester().assertThat(new LeftJoinWithArrayContainsToEquiJoinCondition(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .on(p ->
                {
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k1", VARCHAR);
                    return p.join(JoinType.LEFT,
                            p.values(p.variable("left_k1", VARCHAR)),
                            p.values(p.variable("right_array_k1", new ArrayType(BIGINT))),
                            p.rowExpression("contains(CAST(right_array_k1 AS ARRAY<VARCHAR>), left_k1)"));
                }).matches(
                        join(
                                JoinType.LEFT,
                                ImmutableList.of(equiJoinClause("left_k1", "unnest")),
                                values("left_k1"),
                                unnest(
                                        ImmutableMap.of("array_distinct", ImmutableList.of("unnest")),
                                        project(
                                                ImmutableMap.of("array_distinct", expression("remove_nulls(array_distinct(CAST(right_array_k1 AS ARRAY<VARCHAR>)))")),
                                                values("right_array_k1")))));
    }

    @Test
    public void testArrayContainsWithCoalesce()
    {
        tester().assertThat(
                        ImmutableSet.of(
                                new LeftJoinWithArrayContainsToEquiJoinCondition(getFunctionManager())))
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .on(p ->
                {
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k1", VARCHAR);
                    p.variable("left_k2", BIGINT);
                    return
                            p.join(JoinType.LEFT,
                                    p.values(p.variable("left_k1", VARCHAR), p.variable("left_k2", BIGINT)),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT))),
                                    p.rowExpression("contains(right_array_k1, coalesce(CAST(left_k1 AS BIGINT), left_k2))"));
                }).matches(
                        join(
                                JoinType.LEFT,
                                ImmutableList.of(equiJoinClause("expr", "unnest")),
                                project(
                                        ImmutableMap.of("expr", expression("COALESCE(CAST(left_k1 AS bigint), left_k2)")),
                                        values("left_k1", "left_k2")),
                                unnest(
                                        ImmutableMap.of("array_distinct", ImmutableList.of("unnest")),
                                        project(
                                                ImmutableMap.of("array_distinct", expression("remove_nulls(array_distinct(right_array_k1))")),
                                                values("right_array_k1")))));
    }

    @Test
    public void testConditionWithAnd()
    {
        tester().assertThat(new LeftJoinWithArrayContainsToEquiJoinCondition(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .on(p ->
                {
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k2", BIGINT);
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    return
                            p.join(JoinType.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_array_k1", new ArrayType(BIGINT)), p.variable("right_k2")),
                                    p.rowExpression("contains(right_array_k1, left_k1) and right_k2+left_k2 > 10"));
                }).matches(
                        join(
                                JoinType.LEFT,
                                ImmutableList.of(equiJoinClause("left_k1", "unnest")),
                                Optional.of("right_k2+left_k2 > 10"),
                                values("left_k1", "left_k2"),
                                unnest(
                                        ImmutableMap.of("array_distinct", ImmutableList.of("unnest")),
                                        project(
                                                ImmutableMap.of("array_distinct", expression("remove_nulls(array_distinct(right_array_k1))")),
                                                values("right_array_k1", "right_k2")))));
    }
}

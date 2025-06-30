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
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static java.util.Collections.singletonList;

public class TestCrossJoinWithArrayNotContainsToAntiJoin
        extends BaseRuleTest
{
    @BeforeClass
    @Override
    public void setUp()
    {
        tester = new RuleTester(singletonList(new SqlInvokedFunctionsPlugin()));
    }

    @Test
    public void testTriggerForBigInt()
    {
        tester().assertThat(new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("not contains(left_array_k1, right_k1)"),
                            p.join(JoinType.INNER,
                                    p.enforceSingleRow(p.values(p.variable("left_array_k1", new ArrayType(BIGINT)))),
                                    p.values(p.variable("right_k1"))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(FilterNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(UnnestNode.class, node(ProjectNode.class,
                                                        node(EnforceSingleRowNode.class, (node(ValuesNode.class)))))))));
    }

    @Test
    public void testTriggerForBigIntArrayRightSide()
    {
        tester().assertThat(new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("right_array_k1", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("not contains(right_array_k1, left_k1)"),
                            p.join(JoinType.INNER,
                                    p.values(p.variable("left_k1")),
                                    p.enforceSingleRow(p.values(p.variable("right_array_k1", new ArrayType(BIGINT))))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(FilterNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(UnnestNode.class, node(ProjectNode.class, node(EnforceSingleRowNode.class, node(ValuesNode.class))))))));
    }

    @Test
    public void testMultipleConditions()
    {
        tester().assertThat(new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("not contains(left_array_k1, right_k1) and right_k2>1"),
                            p.join(JoinType.INNER,
                                    p.enforceSingleRow(p.values(p.variable("left_array_k1", new ArrayType(BIGINT)))),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(FilterNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(UnnestNode.class,
                                                        node(ProjectNode.class,
                                                                node(EnforceSingleRowNode.class, node(ValuesNode.class))))))));
    }

    @Test
    public void testComputedArrayExpression()
    {
        tester().assertThat(
                        ImmutableSet.of(
                                new PushDownFilterExpressionEvaluationThroughCrossJoin(getFunctionManager()),
                                new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager())))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("json_string", VARCHAR);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("not contains(cast(json_parse(json_string) as array<bigint>), right_k1)"),
                            p.join(JoinType.INNER,
                                    p.project(p.enforceSingleRow(p.values(p.variable("x", VARCHAR))),
                                            Assignments.builder()
                                                    .put(p.variable("json_string", VARCHAR), p.rowExpression("x"))
                                                    .build()),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(ProjectNode.class,
                                        node(FilterNode.class,
                                                node(JoinNode.class,
                                                        node(ValuesNode.class),
                                                        node(UnnestNode.class,
                                                                node(ProjectNode.class,
                                                                        node(ProjectNode.class,
                                                                                node(ProjectNode.class, node(EnforceSingleRowNode.class, node(ValuesNode.class)))))))))));
    }

    @Test
    public void testMultipleInvalidArrayNotContainsConditions()
    {
        tester().assertThat(new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_array_k2", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("not contains(left_array_k1, right_k1) or not contains(right_array_k2, left_k2)"),
                            p.join(JoinType.INNER,
                                    p.enforceSingleRow(p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2"))),
                                    p.values(p.variable("right_k1"), p.variable("right_array_k2", new ArrayType(BIGINT)))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerForDouble()
    {
        tester().assertThat(new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(DOUBLE));
                    p.variable("right_k1", DOUBLE);
                    return p.filter(
                            p.rowExpression("not contains(left_array_k1, right_k1)"),
                            p.join(JoinType.INNER,
                                    p.enforceSingleRow(p.values(p.variable("left_array_k1", new ArrayType(DOUBLE)))),
                                    p.values(p.variable("right_k1", DOUBLE))));
                }).doesNotFire();
    }

    @Test
    public void testArrayContainsWithCast()
    {
        tester().assertThat(new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", VARCHAR);
                    return p.filter(
                            p.rowExpression("not contains(left_array_k1, CAST(right_k1 AS BIGINT))"),
                            p.join(JoinType.INNER,
                                    p.enforceSingleRow(p.values(p.variable("left_k1", new ArrayType(BIGINT)))),
                                    p.values(p.variable("right_k1", VARCHAR))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerForMultiRow()
    {
        tester().assertThat(new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("not contains(left_array_k1, right_k1)"),
                            p.join(JoinType.INNER,
                                    p.values(ImmutableList.of(p.variable("left_array_k1", new ArrayType(BIGINT))),
                                            ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                    p.values(p.variable("right_k1"))));
                })
                .doesNotFire();
    }

    @Test
    public void testNotTriggerForMultipleColumnsOnArraySide()
    {
        tester().assertThat(new CrossJoinWithArrayNotContainsToAntiJoin(getMetadata(), getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(BIGINT));
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_array_k2", new ArrayType(BIGINT));
                    return p.filter(
                            p.rowExpression("not contains(left_array_k1, right_k1)"),
                            p.join(JoinType.INNER,
                                    p.values(p.variable("left_array_k1", new ArrayType(BIGINT)), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_array_k2", new ArrayType(BIGINT)))));
                })
                .doesNotFire();
    }

    @Test
    public void testNotTriggerForNonDeterministicArrayExpression()
    {
        // transform function considered non-deterministic, so
        tester().assertThat(new PushDownFilterExpressionEvaluationThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .on(p ->
                {
                    p.variable("left_array_k1", new ArrayType(VARCHAR));
                    p.variable("right_k1", VARCHAR);
                    return p.filter(
                            p.rowExpression("not contains(transform(left_array_k1, x->lower(x)), right_k1)"),
                            p.join(JoinType.INNER,
                                    p.enforceSingleRow(p.values(p.variable("left_array_k1", new ArrayType(VARCHAR)))),
                                    p.values(p.variable("right_k1", VARCHAR))));
                })
                .doesNotFire();
    }
}

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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.SystemSessionProperties.PULL_EXPRESSION_FROM_LAMBDA_ENABLED;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;

public class TestPullUpExpressionInLambdaRules
        extends BaseRuleTest
{
    private static final MethodHandle KEY_NATIVE_EQUALS = getOperatorMethodHandle(OperatorType.EQUAL, BIGINT, BIGINT);
    private static final MethodHandle KEY_BLOCK_EQUALS = compose(KEY_NATIVE_EQUALS, nativeValueGetter(BIGINT), nativeValueGetter(BIGINT));
    private static final MethodHandle KEY_NATIVE_HASH_CODE = getOperatorMethodHandle(OperatorType.HASH_CODE, BIGINT);
    private static final MethodHandle KEY_BLOCK_HASH_CODE = compose(KEY_NATIVE_HASH_CODE, nativeValueGetter(BIGINT));

    @Test
    public void testProjection()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    return p.project(
                            Assignments.builder().put(p.variable("expr"), p.rowExpression("map_filter(idmap, (k, v) -> array_position(array_sort(map_keys(idmap)), k) <= 200)")).build(),
                            p.values(p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE))));
                })
                .matches(
                        project(
                                ImmutableMap.of("expr", expression("map_filter(idmap, (k, v) -> (array_position(array_sort, k)) <= (INTEGER'200'))")),
                                project(ImmutableMap.of("array_sort", expression("array_sort(map_keys(idmap))")),
                                        values("idmap"))));
    }

    @Test
    public void testFilter()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).filterNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    return p.filter(
                            p.rowExpression("cardinality(map_filter(idmap, (k, v) -> array_position(array_sort(map_keys(idmap)), k) <= 200)) > 0"),
                            p.values(p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE))));
                })
                .matches(
                        project(
                                ImmutableMap.of("idmap", expression("idmap")),
                                filter(
                                        "(cardinality(map_filter(idmap, (k, v) -> (array_position(array_sort, k)) <= (INTEGER'200')))) > (INTEGER'0')",
                                        project(ImmutableMap.of("array_sort", expression("array_sort(map_keys(idmap))")),
                                                values("idmap")))));
    }

    @Test
    public void testNonDeterministicProjection()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    return p.project(
                            Assignments.builder().put(p.variable("expr"), p.rowExpression("map_filter(idmap, (k, v) -> array_position(array[random()], k) <= 200)")).build(),
                            p.values(p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE))));
                }).doesNotFire();
    }

    @Test
    public void testNonDeterministicFilter()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).filterNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    return p.filter(
                            p.rowExpression("cardinality(map_filter(idmap, (k, v) -> array_position(array_sort(array[random(), random()]), k) <= 200)) > 0"),
                            p.values(p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE))));
                }).doesNotFire();
    }

    @Test
    public void testNoValidProjection()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    return p.project(
                            Assignments.builder().put(p.variable("expr"), p.rowExpression("map_filter(idmap, (k, v) -> array_position(array_sort(array[v]), k) <= 200)")).build(),
                            p.values(p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE))));
                })
                .doesNotFire();
    }

    @Test
    public void testNoValidFilter()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).filterNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    return p.filter(
                            p.rowExpression("cardinality(map_filter(idmap, (k, v) -> array_position(array_sort(array[v, k]), k) <= 200)) > 0"),
                            p.values(p.variable("idmap", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE))));
                }).doesNotFire();
    }

    @Test
    public void testNestedLambdaInProjection()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("expr", new ArrayType(new ArrayType(BIGINT)));
                    p.variable("arr1", new ArrayType(BIGINT));
                    p.variable("arr2", new ArrayType(BIGINT));
                    return p.project(
                            Assignments.builder().put(p.variable("expr", new ArrayType(new ArrayType(BIGINT))), p.rowExpression("transform(arr1, x->transform(arr2, y->slice(arr2, 1, 10)))")).build(),
                            p.values(p.variable("arr1", new ArrayType(BIGINT)), p.variable("arr2", new ArrayType(BIGINT))));
                })
                .matches(
                        project(
                                ImmutableMap.of("expr", expression("transform(arr1, (x) -> transform(arr2, (y) -> slice))")),
                                project(

                                        ImmutableMap.of("slice", expression("slice(arr2, 1, 10)")),
                                        values("arr1", "arr2"))));
    }

    @Test
    public void testInvalidNestedLambdaInProjection()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("expr", new ArrayType(new ArrayType(BIGINT)));
                    p.variable("arr1", new ArrayType(BIGINT));
                    p.variable("arr2", new ArrayType(BIGINT));
                    return p.project(
                            Assignments.builder().put(p.variable("expr", new ArrayType(new ArrayType(BIGINT))), p.rowExpression("transform(arr1, x->transform(arr2, y->slice(arr2, 1, x)))")).build(),
                            p.values(p.variable("arr1", new ArrayType(BIGINT)), p.variable("arr2", new ArrayType(BIGINT))));
                }).doesNotFire();
    }

    @Test
    public void testSkipTryFunction()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("x");
                    return p.project(
                            Assignments.builder().put(p.variable("expr", VARCHAR), p.rowExpression("JSON_FORMAT(CAST(TRY(MAP(ARRAY[NULL], ARRAY[x])) AS JSON))")).build(),
                            p.values(p.variable("x")));
                }).doesNotFire();
    }
}

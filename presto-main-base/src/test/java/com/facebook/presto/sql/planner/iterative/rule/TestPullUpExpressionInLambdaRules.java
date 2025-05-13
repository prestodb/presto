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
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
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

    @Test
    public void testSwitchWhenExpression()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("arr", new ArrayType(VARCHAR));
                    p.variable("arr2", new ArrayType(VARCHAR));
                    return p.project(
                            Assignments.builder().put(p.variable("expr", VARCHAR), p.rowExpression(
                                    "transform(arr, x -> concat(case when arr2 is null then '*' when contains(arr2, x) then '+' else ' ' end, x))")).build(),
                            p.values(p.variable("arr", new ArrayType(VARCHAR)), p.variable("arr2", new ArrayType(VARCHAR))));
                }).matches(
                        project(
                                ImmutableMap.of("expr", expression("transform(arr, x -> concat(case when expr_0 then '*' when contains(arr2, x) then '+' else ' ' end, x))")),
                                project(ImmutableMap.of("expr_0", expression("arr2 is null")),
                                        values("arr", "arr2"))));
    }

    // Candidate expression for extract is the second when expression, hence skip
    @Test
    public void testInvalidSwitchWhenExpression()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("arr", new ArrayType(VARCHAR));
                    p.variable("arr2", new ArrayType(VARCHAR));
                    return p.project(
                            Assignments.builder().put(p.variable("expr", VARCHAR), p.rowExpression(
                                    "transform(arr, x -> concat(case when contains(arr2, x) then '*' when arr2 is null then '+' else ' ' end, x))")).build(),
                            p.values(p.variable("arr", new ArrayType(VARCHAR)), p.variable("arr2", new ArrayType(VARCHAR))));
                }).doesNotFire();
    }

    @Test
    public void testCaseWhenExpression()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("arr", new ArrayType(VARCHAR));
                    p.variable("arr2", new ArrayType(VARCHAR));
                    p.variable("col1");
                    return p.project(
                            Assignments.builder().put(p.variable("expr", VARCHAR), p.rowExpression(
                                    "transform(arr, x -> concat(case (col1 > 2) when arr2 is null then '*' when contains(arr2, x) then '+' else ' ' end, x))")).build(),
                            p.values(p.variable("arr", new ArrayType(VARCHAR)), p.variable("arr2", new ArrayType(VARCHAR)), p.variable("col1")));
                }).matches(
                        project(
                                ImmutableMap.of("expr", expression("transform(arr, x -> concat(case expr_1 when expr_0 then '*' when contains(arr2, x) then '+' else ' ' end, x))")),
                                project(ImmutableMap.of("expr_0", expression("arr2 is null"), "expr_1", expression("col1>2")),
                                        values("arr", "arr2", "col1"))));
    }

    @Test
    public void testConditionalExpression()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("col1", new ArrayType(BOOLEAN));
                    p.variable("col2", new ArrayType(BIGINT));
                    return p.project(
                            Assignments.builder().put(p.variable("expr", VARCHAR), p.rowExpression(
                                    "transform(col1, x -> if(x, col2[2], 0))")).build(),
                            p.values(p.variable("col1", new ArrayType(BOOLEAN)), p.variable("col2", new ArrayType(BIGINT))));
                }).doesNotFire();
    }

    @Test
    public void testIfExpressionOnCondition()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("col1", new ArrayType(BOOLEAN));
                    p.variable("col2", new ArrayType(BIGINT));
                    p.variable("col3");
                    return p.project(
                            Assignments.builder().put(p.variable("expr", VARCHAR), p.rowExpression(
                                    "transform(col1, x -> if(col3 > 2, col2[2], 0))")).build(),
                            p.values(p.variable("col1", new ArrayType(BOOLEAN)), p.variable("col2", new ArrayType(BIGINT)), p.variable("col3")));
                }).matches(
                        project(
                                ImmutableMap.of("expr", expression("transform(col1, x -> if(greater_than, col2[2], 0))")),
                                project(ImmutableMap.of("greater_than", expression("col3>2")),
                                        values("col1", "col2", "col3"))));
    }

    @Test
    public void testIfExpressionOnValue()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("col1", new ArrayType(BOOLEAN));
                    p.variable("col2", new ArrayType(BIGINT));
                    p.variable("col3");
                    return p.project(
                            Assignments.builder().put(p.variable("expr", VARCHAR), p.rowExpression(
                                    "transform(col1, x -> if(x, col3 - 2, 0))")).build(),
                            p.values(p.variable("col1", new ArrayType(BOOLEAN)), p.variable("col2", new ArrayType(BIGINT)), p.variable("col3")));
                }).doesNotFire();
    }

    @Test
    public void testSubscriptExpression()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("col1", new ArrayType(BOOLEAN));
                    p.variable("col2", new ArrayType(BIGINT));
                    p.variable("col3");
                    return p.project(
                            Assignments.builder().put(p.variable("expr", VARCHAR), p.rowExpression(
                                    "transform(col1, x -> col2[2])")).build(),
                            p.values(p.variable("col1", new ArrayType(BOOLEAN)), p.variable("col2", new ArrayType(BIGINT)), p.variable("col3")));
                }).doesNotFire();
    }

    @Test
    public void testLikeExpression()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("expr", new ArrayType(BOOLEAN));
                    p.variable("col", VARCHAR);
                    p.variable("arr1", new ArrayType(VARCHAR));
                    return p.project(
                            Assignments.builder().put(p.variable("expr", new ArrayType(BOOLEAN)), p.rowExpression("transform(arr1, x-> x like concat(col, 'a'))")).build(),
                            p.values(p.variable("arr1", new ArrayType(VARCHAR)), p.variable("col", VARCHAR)));
                })
                .matches(
                        project(
                                ImmutableMap.of("expr", expression("transform(arr1, x -> x like concat_1)")),
                                project(

                                        ImmutableMap.of("concat_1", expression("concat(col, 'a')")),
                                        values("arr1", "col"))));
    }

    @Test
    public void testRegexpLikeExpression()
    {
        tester().assertThat(new PullUpExpressionInLambdaRules(getFunctionManager()).projectNodeRule())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .on(p ->
                {
                    p.variable("expr", new ArrayType(BOOLEAN));
                    p.variable("col", VARCHAR);
                    p.variable("arr1", new ArrayType(VARCHAR));
                    return p.project(
                            Assignments.builder().put(p.variable("expr", new ArrayType(BOOLEAN)), p.rowExpression("transform(arr1, x-> regexp_like(x, concat(col, 'a')))")).build(),
                            p.values(p.variable("arr1", new ArrayType(VARCHAR)), p.variable("col", VARCHAR)));
                })
                .matches(
                        project(
                                ImmutableMap.of("expr", expression("transform(arr1, x -> regexp_like(x, concat_1))")),
                                project(

                                        ImmutableMap.of("concat_1", expression("concat(col, 'a')")),
                                        values("arr1", "col"))));
    }
}

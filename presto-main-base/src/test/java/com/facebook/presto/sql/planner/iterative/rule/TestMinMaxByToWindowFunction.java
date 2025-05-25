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
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.SystemSessionProperties.REWRITE_MIN_MAX_BY_TO_TOP_N;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topNRowNumber;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;

public class TestMinMaxByToWindowFunction
        extends BaseRuleTest
{
    private static final MethodHandle KEY_NATIVE_EQUALS = getOperatorMethodHandle(OperatorType.EQUAL, BIGINT, BIGINT);
    private static final MethodHandle KEY_BLOCK_EQUALS = compose(KEY_NATIVE_EQUALS, nativeValueGetter(BIGINT), nativeValueGetter(BIGINT));
    private static final MethodHandle KEY_NATIVE_HASH_CODE = getOperatorMethodHandle(OperatorType.HASH_CODE, BIGINT);
    private static final MethodHandle KEY_BLOCK_HASH_CODE = compose(KEY_NATIVE_HASH_CODE, nativeValueGetter(BIGINT));

    @Test
    public void testMaxByOnly()
    {
        tester().assertThat(new MinMaxByToWindowFunction(getFunctionManager()))
                .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression id = p.variable("id", BIGINT);
                    return p.aggregation(ap -> ap.singleGroupingSet(id).step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("expr"), p.rowExpression("max_by(a, ds)"))
                            .source(
                                    p.values(ds, a, id)));
                })
                .matches(
                        project(
                                filter(
                                        topNRowNumber(
                                                topNRowNumber -> topNRowNumber
                                                        .specification(
                                                                ImmutableList.of("id"),
                                                                ImmutableList.of("ds"),
                                                                ImmutableMap.of("ds", DESC_NULLS_LAST))
                                                        .partial(false),
                                                values("ds", "a", "id")))));
    }

    @Test
    public void testMaxAndMaxBy()
    {
        tester().assertThat(new MinMaxByToWindowFunction(getFunctionManager()))
                .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression id = p.variable("id", BIGINT);
                    return p.aggregation(ap -> ap.singleGroupingSet(id).step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("expr"), p.rowExpression("max_by(a, ds)"))
                            .addAggregation(p.variable("expr2"), p.rowExpression("max(ds)"))
                            .source(
                                    p.values(ds, a, id)));
                })
                .matches(
                        project(
                                filter(
                                        topNRowNumber(
                                                topNRowNumber -> topNRowNumber
                                                        .specification(
                                                                ImmutableList.of("id"),
                                                                ImmutableList.of("ds"),
                                                                ImmutableMap.of("ds", DESC_NULLS_LAST))
                                                        .partial(false),
                                                values("ds", "a", "id")))));
    }

    @Test
    public void testMinByOnly()
    {
        tester().assertThat(new MinMaxByToWindowFunction(getFunctionManager()))
                .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression id = p.variable("id", BIGINT);
                    return p.aggregation(ap -> ap.singleGroupingSet(id).step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("expr"), p.rowExpression("min_by(a, ds)"))
                            .source(
                                    p.values(ds, a, id)));
                })
                .matches(
                        project(
                                filter(
                                        topNRowNumber(
                                                topNRowNumber -> topNRowNumber
                                                        .specification(
                                                                ImmutableList.of("id"),
                                                                ImmutableList.of("ds"),
                                                                ImmutableMap.of("ds", ASC_NULLS_LAST))
                                                        .partial(false),
                                                values("ds", "a", "id")))));
    }

    @Test
    public void testMinAndMinBy()
    {
        tester().assertThat(new MinMaxByToWindowFunction(getFunctionManager()))
                .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression id = p.variable("id", BIGINT);
                    return p.aggregation(ap -> ap.singleGroupingSet(id).step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("expr"), p.rowExpression("min_by(a, ds)"))
                            .addAggregation(p.variable("expr2"), p.rowExpression("min(ds)"))
                            .source(
                                    p.values(ds, a, id)));
                })
                .matches(
                        project(
                                filter(
                                        topNRowNumber(
                                                topNRowNumber -> topNRowNumber
                                                        .specification(
                                                                ImmutableList.of("id"),
                                                                ImmutableList.of("ds"),
                                                                ImmutableMap.of("ds", ASC_NULLS_LAST))
                                                        .partial(false),
                                                values("ds", "a", "id")))));
    }

    @Test
    public void testMinAndMaxBy()
    {
        tester().assertThat(new MinMaxByToWindowFunction(getFunctionManager()))
                .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression id = p.variable("id", BIGINT);
                    return p.aggregation(ap -> ap.singleGroupingSet(id).step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("expr"), p.rowExpression("max_by(a, ds)"))
                            .addAggregation(p.variable("expr2"), p.rowExpression("min(ds)"))
                            .source(
                                    p.values(ds, a, id)));
                }).doesNotFire();
    }

    @Test
    public void testMaxByOnlyNotOnMap()
    {
        tester().assertThat(new MinMaxByToWindowFunction(getFunctionManager()))
                .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", VARCHAR);
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression id = p.variable("id", BIGINT);
                    return p.aggregation(ap -> ap.singleGroupingSet(id).step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("expr"), p.rowExpression("max_by(a, ds)"))
                            .source(
                                    p.values(ds, a, id)));
                }).doesNotFire();
    }
}

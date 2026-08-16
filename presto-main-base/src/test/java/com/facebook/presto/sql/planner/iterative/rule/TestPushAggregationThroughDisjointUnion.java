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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_DISJOINT_UNION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;

public class TestPushAggregationThroughDisjointUnion
        extends BaseRuleTest
{
    @Test
    public void testFiresWithTwoBranchDisjointConstants()
    {
        // SELECT count(1), x FROM (SELECT 1 x UNION ALL SELECT 2 x) GROUP BY x
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(
                                    p.variable("cnt", BIGINT),
                                    p.rowExpression("count(x)"))
                            .singleGroupingSet(x)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, a)
                                            .put(x, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(
                                                    ImmutableList.of(a),
                                                    ImmutableList.of(constantExpressions(BIGINT, 1L))),
                                            p.values(
                                                    ImmutableList.of(b),
                                                    ImmutableList.of(constantExpressions(BIGINT, 2L)))))));
                })
                .matches(
                        union(
                                aggregation(
                                        ImmutableMap.of("cnt_0", functionCall("count", ImmutableList.of("a"))),
                                        values(ImmutableList.of("a"))),
                                aggregation(
                                        ImmutableMap.of("cnt_1", functionCall("count", ImmutableList.of("b"))),
                                        values(ImmutableList.of("b")))));
    }

    @Test
    public void testFiresWithThreeBranchDisjointConstants()
    {
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression c = p.variable("c", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count(x)"))
                            .singleGroupingSet(x)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, a)
                                            .put(x, b)
                                            .put(x, c)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(ImmutableList.of(a), ImmutableList.of(constantExpressions(BIGINT, 10L))),
                                            p.values(ImmutableList.of(b), ImmutableList.of(constantExpressions(BIGINT, 20L))),
                                            p.values(ImmutableList.of(c), ImmutableList.of(constantExpressions(BIGINT, 30L)))))));
                })
                .matches(
                        union(
                                aggregation(
                                        ImmutableMap.of("cnt_0", functionCall("count", ImmutableList.of("a"))),
                                        values(ImmutableList.of("a"))),
                                aggregation(
                                        ImmutableMap.of("cnt_1", functionCall("count", ImmutableList.of("b"))),
                                        values(ImmutableList.of("b"))),
                                aggregation(
                                        ImmutableMap.of("cnt_2", functionCall("count", ImmutableList.of("c"))),
                                        values(ImmutableList.of("c")))));
    }

    @Test
    public void testFiresWithProjectConstantSource()
    {
        // Each branch is Project[x := constant] -> Values(input)
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression aIn = p.variable("a_in", BIGINT);
                    VariableReferenceExpression bIn = p.variable("b_in", BIGINT);
                    VariableReferenceExpression aOut = p.variable("a_out", BIGINT);
                    VariableReferenceExpression bOut = p.variable("b_out", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count(x)"))
                            .singleGroupingSet(x)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, aOut)
                                            .put(x, bOut)
                                            .build(),
                                    ImmutableList.of(
                                            p.project(
                                                    Assignments.builder().put(aOut, p.rowExpression("BIGINT '5'")).build(),
                                                    p.values(aIn)),
                                            p.project(
                                                    Assignments.builder().put(bOut, p.rowExpression("BIGINT '6'")).build(),
                                                    p.values(bIn))))));
                })
                // Verify only the structural shape: Union(Agg(Project(Values)), Agg(Project(Values))).
                // The aggregation argument variables come from the inner project, which the alias
                // resolver doesn't expose, so we don't pin function-call argument names here.
                .matches(
                        node(UnionNode.class,
                                node(AggregationNode.class, project(values(ImmutableList.of("a_in")))),
                                node(AggregationNode.class, project(values(ImmutableList.of("b_in"))))));
    }

    @Test
    public void testDoesNotFireWithOverlappingConstants()
    {
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count(x)"))
                            .singleGroupingSet(x)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, a)
                                            .put(x, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(ImmutableList.of(a), ImmutableList.of(constantExpressions(BIGINT, 1L))),
                                            p.values(ImmutableList.of(b), ImmutableList.of(constantExpressions(BIGINT, 1L)))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenBranchKeyIsNotConstant()
    {
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count(x)"))
                            .singleGroupingSet(x)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, a)
                                            .put(x, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(ImmutableList.of(a), ImmutableList.of(constantExpressions(BIGINT, 1L))),
                                            p.values(b)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForGlobalAggregation()
    {
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, a)
                                            .put(x, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(ImmutableList.of(a), ImmutableList.of(constantExpressions(BIGINT, 1L))),
                                            p.values(ImmutableList.of(b), ImmutableList.of(constantExpressions(BIGINT, 2L)))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNonDeterministicArgument()
    {
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count(random())"))
                            .singleGroupingSet(x)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, a)
                                            .put(x, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(ImmutableList.of(a), ImmutableList.of(constantExpressions(BIGINT, 1L))),
                                            p.values(ImmutableList.of(b), ImmutableList.of(constantExpressions(BIGINT, 2L)))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "false")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count(x)"))
                            .singleGroupingSet(x)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, a)
                                            .put(x, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(ImmutableList.of(a), ImmutableList.of(constantExpressions(BIGINT, 1L))),
                                            p.values(ImmutableList.of(b), ImmutableList.of(constantExpressions(BIGINT, 2L)))))));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresWithMultiKeyGroupingOneDisjoint()
    {
        // GROUP BY (x, y) where x is disjoint across branches (1 vs 2) and y is overlapping
        // (same constant 10 in both branches). Disjointness on a single grouping key is
        // sufficient: every group still lives in exactly one branch.
        //   SELECT count(z), x, y FROM
        //     (SELECT 1 x, 10 y, 100 z UNION ALL SELECT 2 x, 10 y, 200 z)
        //     GROUP BY x, y
        tester().assertThat(new PushAggregationThroughDisjointUnion(getFunctionManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_DISJOINT_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression aX = p.variable("a_x", BIGINT);
                    VariableReferenceExpression aY = p.variable("a_y", BIGINT);
                    VariableReferenceExpression aZ = p.variable("a_z", BIGINT);
                    VariableReferenceExpression bX = p.variable("b_x", BIGINT);
                    VariableReferenceExpression bY = p.variable("b_y", BIGINT);
                    VariableReferenceExpression bZ = p.variable("b_z", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression z = p.variable("z", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count(z)"))
                            .singleGroupingSet(x, y)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(x, aX).put(x, bX)
                                            .put(y, aY).put(y, bY)
                                            .put(z, aZ).put(z, bZ)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(
                                                    ImmutableList.of(aX, aY, aZ),
                                                    ImmutableList.of(constantExpressions(BIGINT, 1L, 10L, 100L))),
                                            p.values(
                                                    ImmutableList.of(bX, bY, bZ),
                                                    ImmutableList.of(constantExpressions(BIGINT, 2L, 10L, 200L)))))));
                })
                .matches(
                        union(
                                aggregation(
                                        ImmutableMap.of("cnt_0", functionCall("count", ImmutableList.of("a_z"))),
                                        values(ImmutableList.of("a_x", "a_y", "a_z"))),
                                aggregation(
                                        ImmutableMap.of("cnt_1", functionCall("count", ImmutableList.of("b_z"))),
                                        values(ImmutableList.of("b_x", "b_y", "b_z")))));
    }
}

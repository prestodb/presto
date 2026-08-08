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

import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class TestPushPartialAggregationThroughJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesPartialAggregationThroughJoin()
    {
        tester().assertThat(new PushPartialAggregationThroughJoinRuleSet().withoutProjectionRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.variable("LEFT_EQUI"), p.variable("LEFT_NON_EQUI"), p.variable("LEFT_GROUP_BY"), p.variable("LEFT_AGGR"), p.variable("LEFT_HASH")),
                                        p.values(p.variable("RIGHT_EQUI"), p.variable("RIGHT_NON_EQUI"), p.variable("RIGHT_GROUP_BY"), p.variable("RIGHT_HASH")),
                                        ImmutableList.of(new EquiJoinClause(p.variable("LEFT_EQUI"), p.variable("RIGHT_EQUI"))),
                                        ImmutableList.of(p.variable("LEFT_GROUP_BY"), p.variable("LEFT_AGGR"), p.variable("RIGHT_GROUP_BY")),
                                        Optional.of(p.rowExpression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")),
                                        Optional.of(p.variable("LEFT_HASH")),
                                        Optional.of(p.variable("RIGHT_HASH"))))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(LEFT_AGGR)"))
                        .singleGroupingSet(p.variable("LEFT_GROUP_BY"), p.variable("RIGHT_GROUP_BY"))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                        "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI <= RIGHT_NON_EQUI"),
                                aggregation(
                                        singleGroupingSet("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_HASH"),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_AGGR"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR", "LEFT_HASH")),
                                values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_HASH"))));
    }

    @Test
    public void testPushesPartialAggregationWithProjectThroughJoin()
    {
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        // The projection computes LEFT_AGGR_X2 = LEFT_AGGR * 2, which only references the left side.
        // We expect the rule to push the projection and the partial aggregation to the left child of the join.
        tester().assertThat(new PushPartialAggregationThroughJoinRuleSet().withProjectionRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftEqui = p.variable("LEFT_EQUI");
                    VariableReferenceExpression leftNonEqui = p.variable("LEFT_NON_EQUI");
                    VariableReferenceExpression leftGroupBy = p.variable("LEFT_GROUP_BY");
                    VariableReferenceExpression leftAggr = p.variable("LEFT_AGGR");
                    VariableReferenceExpression leftHash = p.variable("LEFT_HASH");
                    VariableReferenceExpression rightEqui = p.variable("RIGHT_EQUI");
                    VariableReferenceExpression rightNonEqui = p.variable("RIGHT_NON_EQUI");
                    VariableReferenceExpression rightGroupBy = p.variable("RIGHT_GROUP_BY");
                    VariableReferenceExpression rightHash = p.variable("RIGHT_HASH");
                    // LEFT_AGGR_X2 = LEFT_AGGR * 2 – only depends on the left side
                    VariableReferenceExpression leftAggrX2 = p.variable("LEFT_AGGR_X2", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.project(
                                            PlanBuilder.assignment(
                                                    leftAggrX2,
                                                    call("LEFT_AGGR * 2",
                                                            functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT),
                                                            BIGINT,
                                                            leftAggr,
                                                            constant(2L, BIGINT))),
                                            p.join(
                                                    INNER,
                                                    p.values(leftEqui, leftNonEqui, leftGroupBy, leftAggr, leftHash),
                                                    p.values(rightEqui, rightNonEqui, rightGroupBy, rightHash),
                                                    ImmutableList.of(new EquiJoinClause(leftEqui, rightEqui)),
                                                    ImmutableList.of(leftGroupBy, leftAggr, rightGroupBy),
                                                    Optional.of(p.rowExpression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")),
                                                    Optional.of(leftHash),
                                                    Optional.of(rightHash))))
                            .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(LEFT_AGGR_X2)"))
                            .singleGroupingSet(leftGroupBy, rightGroupBy)
                            .step(PARTIAL));
                })
                .matches(project(ImmutableMap.of(
                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                        "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI <= RIGHT_NON_EQUI"),
                                aggregation(
                                        singleGroupingSet("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_HASH"),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_AGGR_X2"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        project(
                                                ImmutableMap.of("LEFT_AGGR_X2", PlanMatchPattern.expression("LEFT_AGGR * 2")),
                                                values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR", "LEFT_HASH"))),
                                values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_HASH"))));
    }

    @Test
    public void testPushesPartialAggregationWithMixedProjectionThroughJoin()
    {
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        // The projection has BOTH a left-side assignment (LEFT_AGGR_X2 = LEFT_AGGR * 2) and a
        // right-side assignment (RIGHT_AGGR_X2 = RIGHT_EQUI * 2). The rule must split them:
        // LEFT_AGGR_X2 is pushed below the left join child and RIGHT_AGGR_X2 is pushed below the
        // right join child. The aggregation (which uses only LEFT_AGGR_X2) is then pushed to the
        // left child.
        tester().assertThat(new PushPartialAggregationThroughJoinRuleSet().withProjectionRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftEqui = p.variable("LEFT_EQUI");
                    VariableReferenceExpression leftNonEqui = p.variable("LEFT_NON_EQUI");
                    VariableReferenceExpression leftGroupBy = p.variable("LEFT_GROUP_BY");
                    VariableReferenceExpression leftAggr = p.variable("LEFT_AGGR");
                    VariableReferenceExpression leftHash = p.variable("LEFT_HASH");
                    VariableReferenceExpression rightEqui = p.variable("RIGHT_EQUI");
                    VariableReferenceExpression rightNonEqui = p.variable("RIGHT_NON_EQUI");
                    VariableReferenceExpression rightGroupBy = p.variable("RIGHT_GROUP_BY");
                    VariableReferenceExpression rightHash = p.variable("RIGHT_HASH");
                    VariableReferenceExpression leftAggrX2 = p.variable("LEFT_AGGR_X2", BIGINT);
                    VariableReferenceExpression rightAggrX2 = p.variable("RIGHT_AGGR_X2", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.project(
                                            PlanBuilder.assignment(
                                                    leftAggrX2,
                                                    call("LEFT_AGGR * 2",
                                                            functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT),
                                                            BIGINT,
                                                            leftAggr,
                                                            constant(2L, BIGINT)),
                                                    rightAggrX2,
                                                    call("RIGHT_EQUI * 2",
                                                            functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT),
                                                            BIGINT,
                                                            rightEqui,
                                                            constant(2L, BIGINT))),
                                            p.join(
                                                    INNER,
                                                    p.values(leftEqui, leftNonEqui, leftGroupBy, leftAggr, leftHash),
                                                    p.values(rightEqui, rightNonEqui, rightGroupBy, rightHash),
                                                    ImmutableList.of(new EquiJoinClause(leftEqui, rightEqui)),
                                                    ImmutableList.of(leftGroupBy, leftAggr, rightGroupBy, rightEqui),
                                                    Optional.of(p.rowExpression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")),
                                                    Optional.of(leftHash),
                                                    Optional.of(rightHash))))
                            .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(LEFT_AGGR_X2)"))
                            .singleGroupingSet(leftGroupBy, rightGroupBy)
                            .step(PARTIAL));
                })
                .matches(project(ImmutableMap.of(
                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                        "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI <= RIGHT_NON_EQUI"),
                                aggregation(
                                        singleGroupingSet("LEFT_GROUP_BY", "LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_HASH"),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_AGGR_X2"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        project(
                                                ImmutableMap.of("LEFT_AGGR_X2", PlanMatchPattern.expression("LEFT_AGGR * 2")),
                                                values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR", "LEFT_HASH"))),
                                project(
                                        ImmutableMap.of("RIGHT_AGGR_X2", PlanMatchPattern.expression("RIGHT_EQUI * 2")),
                                        values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_HASH")))));
    }

    @Test
    public void testDoesNotFireWhenProjectSpansBothSides()
    {
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        // The projection references both LEFT_AGGR (left side) and RIGHT_EQUI (right side) – rule must not fire.
        tester().assertThat(new PushPartialAggregationThroughJoinRuleSet().withProjectionRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftEqui = p.variable("LEFT_EQUI");
                    VariableReferenceExpression leftNonEqui = p.variable("LEFT_NON_EQUI");
                    VariableReferenceExpression leftGroupBy = p.variable("LEFT_GROUP_BY");
                    VariableReferenceExpression leftAggr = p.variable("LEFT_AGGR");
                    VariableReferenceExpression leftHash = p.variable("LEFT_HASH");
                    VariableReferenceExpression rightEqui = p.variable("RIGHT_EQUI");
                    VariableReferenceExpression rightNonEqui = p.variable("RIGHT_NON_EQUI");
                    VariableReferenceExpression rightGroupBy = p.variable("RIGHT_GROUP_BY");
                    VariableReferenceExpression rightHash = p.variable("RIGHT_HASH");
                    // spans_both = LEFT_AGGR * RIGHT_EQUI – references both sides
                    VariableReferenceExpression spansBoth = p.variable("SPANS_BOTH", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.project(
                                            PlanBuilder.assignment(
                                                    spansBoth,
                                                    call("LEFT_AGGR * RIGHT_EQUI",
                                                            functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT),
                                                            BIGINT,
                                                            leftAggr,
                                                            rightEqui)),
                                            p.join(
                                                    INNER,
                                                    p.values(leftEqui, leftNonEqui, leftGroupBy, leftAggr, leftHash),
                                                    p.values(rightEqui, rightNonEqui, rightGroupBy, rightHash),
                                                    ImmutableList.of(new EquiJoinClause(leftEqui, rightEqui)),
                                                    ImmutableList.of(leftGroupBy, rightGroupBy),
                                                    Optional.of(p.rowExpression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")),
                                                    Optional.of(leftHash),
                                                    Optional.of(rightHash))))
                            .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(SPANS_BOTH)"))
                            .singleGroupingSet(leftGroupBy, rightGroupBy)
                            .step(PARTIAL));
                })
                .doesNotFire();
    }
}

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

import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushAggregationThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesAggregationThroughLeftJoin()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin(getFunctionManager()))
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        JoinNode.Type.LEFT,
                                        p.values(ImmutableList.of(p.variable("COL1")), ImmutableList.of(constantExpressions(BIGINT, 10L))),
                                        p.values(p.variable("COL2")),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                        ImmutableList.of(p.variable("COL1"), p.variable("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL2)"))
                        .singleGroupingSet(p.variable("COL1"))))
                .matches(
                        project(ImmutableMap.of(
                                "COL1", expression("COL1"),
                                "COALESCE", expression("coalesce(AVG, AVG_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("COL1", "COL2")),
                                                values(ImmutableMap.of("COL1", 0)),
                                                aggregation(
                                                        singleGroupingSet("COL2"),
                                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("COL2"))),
                                                        ImmutableMap.of(),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        values(ImmutableMap.of("COL2", 0)))),
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(Optional.of("AVG_NULL"), functionCall("avg", ImmutableList.of("null_literal"))),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                values(ImmutableMap.of("null_literal", 0))))));
    }

    @Test
    public void testPushesAggregationThroughLeftJoinWithOrderByFromRightSideColumn()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin(getFunctionManager()))
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        JoinNode.Type.LEFT,
                                        p.values(
                                                ImmutableList.of(p.variable("COL1"), p.variable("COL3")),
                                                ImmutableList.of(constantExpressions(BIGINT, 10L, 20L))),
                                        p.values(p.variable("COL2"), p.variable("COL4")),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                        ImmutableList.of(p.variable("COL1"), p.variable("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(
                                p.variable("AVG", DOUBLE),
                                p.rowExpression("avg(COL2)"),
                                Optional.empty(),
                                Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(p.variable("COL4"), ASC_NULLS_LAST)))),
                                false,
                                Optional.empty())
                        .singleGroupingSet(p.variable("COL1"), p.variable("COL3"))))
                .matches(
                        project(ImmutableMap.of(
                                "COL1", expression("COL1"),
                                "COL3", expression("COL3"),
                                "COALESCE", expression("coalesce(AVG, AVG_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("COL1", "COL2")),
                                                values(ImmutableMap.of("COL1", 0, "COL3", 0)),
                                                aggregation(
                                                        singleGroupingSet("COL2"),
                                                        ImmutableMap.of(Optional.of("AVG"),
                                                                functionCall(
                                                                        "avg",
                                                                        ImmutableList.of("COL2"),
                                                                        ImmutableList.of(sort("COL4", ASCENDING, LAST)))),
                                                        ImmutableMap.of(),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        values(ImmutableList.of("COL2", "COL4")))),
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(Optional.of("AVG_NULL"),
                                                        functionCall(
                                                                "avg",
                                                                ImmutableList.of("null_literal"),
                                                                ImmutableList.of(sort("null_literal2", ASCENDING, LAST)))),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                values(ImmutableList.of("null_literal", "null_literal2"))))));
    }

    @Test
    public void testPushesAggregationThroughRightJoin()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin(getFunctionManager()))
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                JoinNode.Type.RIGHT,
                                p.values(p.variable("COL2")),
                                p.values(ImmutableList.of(p.variable("COL1")), ImmutableList.of(constantExpressions(BIGINT, 10L))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL2"), p.variable("COL1"))),
                                ImmutableList.of(p.variable("COL2"), p.variable("COL1")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL2)"))
                        .singleGroupingSet(p.variable("COL1"))))
                .matches(
                        project(ImmutableMap.of(
                                "COALESCE", expression("coalesce(AVG, AVG_NULL)"),
                                "COL1", expression("COL1")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.RIGHT, ImmutableList.of(equiJoinClause("COL2", "COL1")),
                                                aggregation(
                                                        singleGroupingSet("COL2"),
                                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("COL2"))),
                                                        ImmutableMap.of(),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        values(ImmutableMap.of("COL2", 0))),
                                                values(ImmutableMap.of("COL1", 0))),
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(
                                                        Optional.of("AVG_NULL"), functionCall("avg", ImmutableList.of("null_literal"))),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                values(ImmutableMap.of("null_literal", 0))))));
    }

    @Test
    public void testDoesNotFireWhenNotDistinct()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin(getFunctionManager()))
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                JoinNode.Type.LEFT,
                                p.values(
                                        ImmutableList.of(p.variable("COL1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(p.variable("COL2")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL1"), p.variable("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL2)"))
                        .singleGroupingSet(p.variable("COL1"))))
                .doesNotFire();

        // https://github.com/prestodb/presto/issues/10592
        tester().assertThat(new PushAggregationThroughOuterJoin(getFunctionManager()))
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        JoinNode.Type.LEFT,
                                        p.project(identityAssignmentsAsSymbolReferences(p.variable("COL1", BIGINT)),
                                                p.aggregation(builder ->
                                                        builder.singleGroupingSet(p.variable("COL1"), p.variable("unused"))
                                                                .source(
                                                                        p.values(
                                                                                ImmutableList.of(p.variable("COL1"), p.variable("unused")),
                                                                                ImmutableList.of(constantExpressions(BIGINT, 10L, 1L), constantExpressions(BIGINT, 10L, 2L)))))),
                                        p.values(p.variable("COL2")),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                        ImmutableList.of(p.variable("COL1"), p.variable("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(p.variable("AVG", DOUBLE), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.variable("COL1"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenGroupingOnInner()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin(getFunctionManager()))
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(JoinNode.Type.LEFT,
                                p.values(ImmutableList.of(p.variable("COL1")), ImmutableList.of(constantExpressions(BIGINT, 10L))),
                                p.values(p.variable("COL2"), p.variable("COL3")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL1"), p.variable("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("avg(COL2)"))
                        .singleGroupingSet(p.variable("COL1"), p.variable("COL3"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAggregationDoesNotHaveSymbols()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin(getFunctionManager()))
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                JoinNode.Type.LEFT,
                                p.values(ImmutableList.of(p.variable("COL1")), ImmutableList.of(constantExpressions(BIGINT, 10L))),
                                p.values(ImmutableList.of(p.variable("COL2")), ImmutableList.of(constantExpressions(BIGINT, 20L))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL1"), p.variable("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("sum(COL1)"))
                        .singleGroupingSet(p.variable("COL1"))))
                .doesNotFire();
    }
}

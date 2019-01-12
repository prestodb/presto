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

package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestPushAggregationThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesAggregationThroughLeftJoin()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        JoinNode.Type.LEFT,
                                        p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                        p.values(p.symbol("COL2")),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("COL1"), p.symbol("COL2"))),
                                        ImmutableList.of(p.symbol("COL1"), p.symbol("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("COL1"))))
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
    public void testPushesAggregationThroughRightJoin()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                JoinNode.Type.RIGHT,
                                p.values(p.symbol("COL2")),
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("COL2"), p.symbol("COL1"))),
                                ImmutableList.of(p.symbol("COL2"), p.symbol("COL1")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("COL1"))))
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
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                JoinNode.Type.LEFT,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(new Symbol("COL2")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(new Symbol("AVG"), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(new Symbol("COL1"))))
                .doesNotFire();

        // https://github.com/prestodb/presto/issues/10592
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        JoinNode.Type.LEFT,
                                        p.project(Assignments.builder()
                                                        .putIdentity(p.symbol("COL1", BIGINT))
                                                        .build(),
                                                p.aggregation(builder ->
                                                        builder.singleGroupingSet(p.symbol("COL1"), p.symbol("unused"))
                                                                .source(
                                                                        p.values(
                                                                                ImmutableList.of(p.symbol("COL1"), p.symbol("unused")),
                                                                                ImmutableList.of(expressions("10", "1"), expressions("10", "2")))))),
                                        p.values(p.symbol("COL2")),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("COL1"), p.symbol("COL2"))),
                                        ImmutableList.of(p.symbol("COL1"), p.symbol("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("COL1"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenGroupingOnInner()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(JoinNode.Type.LEFT,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                p.values(new Symbol("COL2"), new Symbol("COL3")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(new Symbol("AVG"), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(new Symbol("COL1"), new Symbol("COL3"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAggregationDoesNotHaveSymbols()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                JoinNode.Type.LEFT,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                p.values(ImmutableList.of(p.symbol("COL2")), ImmutableList.of(expressions("20"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(new Symbol("SUM"), PlanBuilder.expression("sum(COL1)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(new Symbol("COL1"))))
                .doesNotFire();
    }
}

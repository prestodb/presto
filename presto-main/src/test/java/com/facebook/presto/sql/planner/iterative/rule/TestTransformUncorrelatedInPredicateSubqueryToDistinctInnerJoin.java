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

import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.EXPLOIT_CONSTRAINTS;
import static com.facebook.presto.SystemSessionProperties.IN_PREDICATES_AS_INNER_JOINS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.relational.Expressions.inSubquery;
import static java.util.Collections.emptyList;

public class TestTransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin
        extends BaseRuleTest
{
    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(emptyList(),
                ImmutableMap.of(IN_PREDICATES_AS_INNER_JOINS_ENABLED, Boolean.toString(true),
                        EXPLOIT_CONSTRAINTS, Boolean.toString(true),
                        JOIN_REORDERING_STRATEGY, AUTOMATIC.name()),
                Optional.of(1),
                new TpchConnectorFactory(1));
    }

    @Test
    public void testDoesNotFireOnCorrelation()
    {
        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin())
                .on(p -> p.apply(
                        assignment(
                                p.variable("x"),
                                inSubquery(p.variable("y"), p.variable("z"))),
                        ImmutableList.of(p.variable("y")),
                        p.values(p.variable("y")),
                        p.values()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonInPredicateSubquery()
    {
        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin())
                .on(p -> p.apply(
                        assignment(p.variable("x"), new ExistsExpression(Optional.empty(), TRUE_CONSTANT)),
                        emptyList(),
                        p.values(),
                        p.values()))
                .doesNotFire();
    }

    @Test
    public void testFiresForInPredicate()
    {
        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin())
                .on(p -> p.apply(
                        assignment(
                                p.variable("x"),
                                inSubquery(p.variable("y"), p.variable("z"))),
                        emptyList(),
                        p.values(p.variable("y")),
                        p.values(p.variable("z"))))
                .matches(project(
                        aggregation(ImmutableMap.of(),
                                SINGLE,
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("y", "z")),
                                        assignUniqueId("unique", values("y")),
                                        values("z")))));
    }

    @Test
    public void testDoesNotFiresForInPredicateThatMayParticipateInAntiJoin()
    {
        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin())
                .on(p -> p.apply(
                        assignment(
                                p.variable("x"),
                                inSubquery(p.variable("y"), p.variable("z"))),
                        emptyList(),
                        p.values(p.variable("y")),
                        p.values(p.variable("z")),
                        true))
                .doesNotFire();
    }

    @Test
    public void testSimpleSemijoins()
    {
        tester().assertThat(ImmutableSet.of(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin()), new LogicalPropertiesProviderImpl(new FunctionResolution(getFunctionManager().getFunctionAndTypeResolver())))
                .on("SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region)")
                .matches(output(anyTree(
                        aggregation(
                                ImmutableMap.of(),
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("regionkey", "regionkey_1")),
                                        assignUniqueId("unique",
                                                tableScan("nation", ImmutableMap.of("regionkey", "regionkey", "nationkey", "nationkey", "name", "name", "comment", "comment"))),
                                        tableScan("region", ImmutableMap.of("regionkey_1", "regionkey")))))));

        tester().assertThat(ImmutableSet.of(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin()), new LogicalPropertiesProviderImpl(new FunctionResolution(getFunctionManager().getFunctionAndTypeResolver())))
                .on("SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region) AND name IN (SELECT name FROM region)")
                .matches(output(anyTree(
                        aggregation(
                                ImmutableMap.of(),
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("name", "name_12")),
                                        assignUniqueId("unique",
                                                (anyTree(
                                                        aggregation(
                                                                ImmutableMap.of(),
                                                                join(INNER,
                                                                        ImmutableList.of(equiJoinClause("regionkey", "regionkey_1")),
                                                                        assignUniqueId("unique_37",
                                                                                tableScan("nation", ImmutableMap.of("regionkey", "regionkey", "nationkey", "nationkey", "name", "name", "comment", "comment"))),
                                                                        tableScan("region", ImmutableMap.of("regionkey_1", "regionkey"))))))),
                                        tableScan("region", ImmutableMap.of("name_12", "name")))))));
    }

    @Test
    public void testFeatureDisabled()
    {
        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin())
                .setSystemProperty(IN_PREDICATES_AS_INNER_JOINS_ENABLED, "false")
                .on(p -> p.apply(
                        assignment(
                                p.variable("x"),
                                inSubquery(p.variable("y"), p.variable("z"))),
                        emptyList(),
                        p.values(p.variable("y")),
                        p.values(p.variable("z"))))
                .doesNotFire();

        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "false")
                .on(p -> p.apply(
                        assignment(
                                p.variable("x"),
                                inSubquery(p.variable("y"), p.variable("z"))),
                        emptyList(),
                        p.values(p.variable("y")),
                        p.values(p.variable("z"))))
                .doesNotFire();

        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .on(p -> p.apply(
                        assignment(
                                p.variable("x"),
                                inSubquery(p.variable("y"), p.variable("z"))),
                        emptyList(),
                        p.values(p.variable("y")),
                        p.values(p.variable("z"))))
                .doesNotFire();
    }
}

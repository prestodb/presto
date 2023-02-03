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

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.EXPLOIT_CONSTRAINTS;
import static com.facebook.presto.SystemSessionProperties.IN_PREDICATES_AS_INNER_JOINS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static java.util.Collections.emptyList;

public class TestTransformDistinctInnerJoinToRightEarlyOutJoin
        extends BaseRuleTest
{
    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(emptyList(),
                ImmutableMap.of(IN_PREDICATES_AS_INNER_JOINS_ENABLED, Boolean.toString(true),
                        EXPLOIT_CONSTRAINTS, Boolean.toString(true),
                        JOIN_REORDERING_STRATEGY, AUTOMATIC.name()));
    }

    @Test
    public void testAggregationPushedDown()
    {
        tester().assertThat(new TransformDistinctInnerJoinToRightEarlyOutJoin(), new LogicalPropertiesProviderImpl(new FunctionResolution(getFunctionManager().getFunctionAndTypeResolver())))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression unique = p.variable("unique", BIGINT);
                    return p.aggregation(agg -> agg
                                    .step(SINGLE)
                                    .singleGroupingSet(b)
                                    .source(p.join(
                                            INNER,
                                            p.values(new PlanNodeId("valuesB"), 100, b),
                                            p.assignUniqueId(unique,
                                                    p.values(new PlanNodeId("valuesA"), 1000, a)),
                                            new JoinNode.EquiJoinClause(b, a))));
                })
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .setConfident(true)
                        .addVariableStatistics(variable("a", BIGINT), new VariableStatsEstimate(0, 1000, 0, 8, 100))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .setConfident(true)
                        .addVariableStatistics(variable("b", BIGINT), new VariableStatsEstimate(0, 1000, 0, 8, 10))
                        .build())
                .matches(aggregation(ImmutableMap.of(),
                                SINGLE,
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("b", "a")),
                                        aggregation(ImmutableMap.of(),
                                                SINGLE,
                                                values("b")),
                                        assignUniqueId("unique",
                                                values("a")))));
    }

    @Test
    public void testFeatureDisabled()
    {
        Function<PlanBuilder, PlanNode> planProvider = p -> {
            VariableReferenceExpression a = p.variable("a", BIGINT);
            VariableReferenceExpression b = p.variable("b", BIGINT);
            VariableReferenceExpression unique = p.variable("unique", BIGINT);
            return p.project(
                    assignment(a, a),
                    p.aggregation(agg -> agg
                            .step(SINGLE)
                            .singleGroupingSet(unique, a)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("valuesB"), b),
                                    p.assignUniqueId(unique,
                                            p.values(new PlanNodeId("valuesA"), a)),
                                    new JoinNode.EquiJoinClause(b, a)))));
        };

        tester().assertThat(new TransformDistinctInnerJoinToRightEarlyOutJoin())
                .setSystemProperty(IN_PREDICATES_AS_INNER_JOINS_ENABLED, "false")
                .on(planProvider)
                .doesNotFire();

        tester().assertThat(new TransformDistinctInnerJoinToRightEarlyOutJoin())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "false")
                .on(planProvider)
                .doesNotFire();

        tester().assertThat(new TransformDistinctInnerJoinToRightEarlyOutJoin())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .on(planProvider)
                .doesNotFire();
    }
}

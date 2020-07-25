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

import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestPushPartialAggregationThroughJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesPartialAggregationThroughJoin()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin())
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
                        .addAggregation(p.variable("AVG", DOUBLE), expression("AVG(LEFT_AGGR)"), ImmutableList.of(DOUBLE))
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
}

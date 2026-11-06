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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_OUTER_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushPartialAggregationThroughJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesPartialAggregationThroughJoin()
    {
        assertPushesToLeftChild(INNER, "avg");
    }

    @Test
    public void testPushesPartialAggregationToRightChildOfInnerJoin()
    {
        assertPushesToRightChild(INNER, "avg");
    }

    @Test
    public void testPushesPartialAggregationToLeftChildOfLeftJoin()
    {
        assertPushesToLeftChild(LEFT, "avg");
    }

    @Test
    public void testPushesPartialAggregationToRightChildOfRightJoin()
    {
        assertPushesToRightChild(RIGHT, "avg");
    }

    @Test
    public void testPushesNullIgnoringAggregationToNullProducingSideOfLeftJoin()
    {
        assertPushesToRightChild(LEFT, "sum");
    }

    @Test
    public void testPushesNullIgnoringAggregationToNullProducingSideOfRightJoin()
    {
        assertPushesToLeftChild(RIGHT, "sum");
    }

    @Test
    public void testPushesNullIgnoringAggregationThroughFullJoin()
    {
        assertPushesToLeftChild(FULL, "min");
        assertPushesToRightChild(FULL, "max");
    }

    @Test
    public void testDoesNotPushNonNullIgnoringAggregationToNullProducingSide()
    {
        // The right side of a LEFT join (and the left side of a RIGHT join) is null-extended,
        // so array_agg (which aggregates nulls) cannot be pushed to that side.
        tester().assertThat(newRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_OUTER_JOIN, "true")
                .on(p -> buildAggregationOverJoin(p, LEFT, "ARRAY_AGG(RIGHT_AGGR)", new ArrayType(DOUBLE)))
                .doesNotFire();

        tester().assertThat(newRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_OUTER_JOIN, "true")
                .on(p -> buildAggregationOverJoin(p, RIGHT, "ARRAY_AGG(LEFT_AGGR)", new ArrayType(DOUBLE)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushCountStarToNullProducingSide()
    {
        // count(*) counts null-extended rows, so it can only be pushed to an exactly
        // preserved side; in a FULL join no side qualifies
        tester().assertThat(newRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_OUTER_JOIN, "true")
                .on(p -> buildAggregationOverJoin(p, FULL, "COUNT()", BIGINT))
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushAggregationOverExpressionToNullProducingSide()
    {
        // a non-column argument may evaluate to a non-null value on a null-extended row
        tester().assertThat(newRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_OUTER_JOIN, "true")
                .on(p -> buildAggregationOverJoin(p, LEFT, "SUM(RIGHT_AGGR + 1E0)", DOUBLE))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForOuterJoinWhenOuterJoinPushdownDisabled()
    {
        // push_partial_aggregation_through_outer_join defaults to false
        tester().assertThat(newRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> buildAggregationOverJoin(p, LEFT, "AVG(LEFT_AGGR)", DOUBLE))
                .doesNotFire();
    }

    private RuleAssert newRuleAssert(JoinType joinType)
    {
        RuleAssert ruleAssert = tester().assertThat(newRule())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true");
        if (joinType != INNER) {
            ruleAssert = ruleAssert.setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_OUTER_JOIN, "true");
        }
        return ruleAssert;
    }

    private PushPartialAggregationThroughJoin newRule()
    {
        return new PushPartialAggregationThroughJoin(tester().getMetadata().getFunctionAndTypeManager());
    }

    private void assertPushesToLeftChild(JoinType joinType, String function)
    {
        newRuleAssert(joinType)
                .on(p -> buildAggregationOverJoin(p, joinType, function.toUpperCase() + "(LEFT_AGGR)", DOUBLE))
                .matches(project(ImmutableMap.of(
                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                        "AGGR_OUT", PlanMatchPattern.expression("AGGR_OUT")),
                        join(joinType, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI <= RIGHT_NON_EQUI"),
                                aggregation(
                                        singleGroupingSet("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_HASH"),
                                        ImmutableMap.of(Optional.of("AGGR_OUT"), functionCall(function, ImmutableList.of("LEFT_AGGR"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR", "LEFT_HASH")),
                                values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_AGGR", "RIGHT_HASH"))));
    }

    private void assertPushesToRightChild(JoinType joinType, String function)
    {
        newRuleAssert(joinType)
                .on(p -> buildAggregationOverJoin(p, joinType, function.toUpperCase() + "(RIGHT_AGGR)", DOUBLE))
                .matches(project(ImmutableMap.of(
                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                        "AGGR_OUT", PlanMatchPattern.expression("AGGR_OUT")),
                        join(joinType, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI <= RIGHT_NON_EQUI"),
                                values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR", "LEFT_HASH"),
                                aggregation(
                                        singleGroupingSet("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_HASH"),
                                        ImmutableMap.of(Optional.of("AGGR_OUT"), functionCall(function, ImmutableList.of("RIGHT_AGGR"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_AGGR", "RIGHT_HASH")))));
    }

    private static PlanNode buildAggregationOverJoin(PlanBuilder p, JoinType joinType, String aggregationExpression, Type outputType)
    {
        return p.aggregation(ab -> ab
                .source(
                        p.join(
                                joinType,
                                p.values(p.variable("LEFT_EQUI"), p.variable("LEFT_NON_EQUI"), p.variable("LEFT_GROUP_BY"), p.variable("LEFT_AGGR", DOUBLE), p.variable("LEFT_HASH")),
                                p.values(p.variable("RIGHT_EQUI"), p.variable("RIGHT_NON_EQUI"), p.variable("RIGHT_GROUP_BY"), p.variable("RIGHT_AGGR", DOUBLE), p.variable("RIGHT_HASH")),
                                ImmutableList.of(new EquiJoinClause(p.variable("LEFT_EQUI"), p.variable("RIGHT_EQUI"))),
                                ImmutableList.of(p.variable("LEFT_GROUP_BY"), p.variable("LEFT_AGGR", DOUBLE), p.variable("RIGHT_GROUP_BY"), p.variable("RIGHT_AGGR", DOUBLE)),
                                Optional.of(p.rowExpression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")),
                                Optional.of(p.variable("LEFT_HASH")),
                                Optional.of(p.variable("RIGHT_HASH"))))
                .addAggregation(p.variable("AGGR_OUT", outputType), p.rowExpression(aggregationExpression))
                .singleGroupingSet(p.variable("LEFT_GROUP_BY"), p.variable("RIGHT_GROUP_BY"))
                .step(PARTIAL));
    }
}

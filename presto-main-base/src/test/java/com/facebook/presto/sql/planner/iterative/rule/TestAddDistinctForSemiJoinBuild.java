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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestAddDistinctForSemiJoinBuild
        extends BaseRuleTest
{
    @Test
    public void testTrigger()
    {
        tester().assertThat(new AddDistinctForSemiJoinBuild())
                .setSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, "true")
                .on(p ->
                {
                    VariableReferenceExpression sourceJoinVariable = p.variable("sourceJoinVariable");
                    VariableReferenceExpression filteringSourceJoinVariable = p.variable("filteringSourceJoinVariable");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput");
                    return p.semiJoin(
                            sourceJoinVariable,
                            filteringSourceJoinVariable,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.values(sourceJoinVariable),
                            p.values(filteringSourceJoinVariable));
                }).matches(
                        semiJoin(
                                "sourceJoinVariable",
                                "filteringSourceJoinVariable",
                                "semiJoinOutput",
                                values("sourceJoinVariable"),
                                aggregation(
                                        singleGroupingSet("filteringSourceJoinVariable"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        AggregationNode.Step.SINGLE,
                                        values("filteringSourceJoinVariable"))));
    }

    @Test
    public void testTriggerOverNonQualifiedDistinctAggregation()
    {
        tester().assertThat(new AddDistinctForSemiJoinBuild())
                .setSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, "true")
                .on(p ->
                {
                    VariableReferenceExpression sourceJoinVariable = p.variable("sourceJoinVariable");
                    VariableReferenceExpression filteringSourceJoinVariable = p.variable("filteringSourceJoinVariable");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput");
                    VariableReferenceExpression col1 = p.variable("col1");
                    return p.semiJoin(
                            sourceJoinVariable,
                            filteringSourceJoinVariable,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.values(sourceJoinVariable),
                            p.aggregation((a) -> a
                                    .singleGroupingSet(filteringSourceJoinVariable, col1)
                                    .step(AggregationNode.Step.SINGLE)
                                    .source(p.values(filteringSourceJoinVariable, col1))));
                }).matches(
                        semiJoin(
                                "sourceJoinVariable",
                                "filteringSourceJoinVariable",
                                "semiJoinOutput",
                                values("sourceJoinVariable"),
                                aggregation(
                                        singleGroupingSet("filteringSourceJoinVariable"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        AggregationNode.Step.SINGLE,
                                        aggregation(
                                                singleGroupingSet("filteringSourceJoinVariable", "col1"),
                                                ImmutableMap.of(),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                AggregationNode.Step.SINGLE,
                                                values("filteringSourceJoinVariable", "col1")))));
    }

    @Test
    public void testNotTriggerOverDistinct()
    {
        tester().assertThat(new AddDistinctForSemiJoinBuild())
                .setSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, "true")
                .on(p ->
                {
                    VariableReferenceExpression sourceJoinVariable = p.variable("sourceJoinVariable");
                    VariableReferenceExpression filteringSourceJoinVariable = p.variable("filteringSourceJoinVariable");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput");
                    return p.semiJoin(
                            sourceJoinVariable,
                            filteringSourceJoinVariable,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.values(sourceJoinVariable),
                            p.aggregation((a) -> a
                                    .singleGroupingSet(filteringSourceJoinVariable)
                                    .step(AggregationNode.Step.SINGLE)
                                    .source(p.values(filteringSourceJoinVariable))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerOverDistinctUnderProject()
    {
        tester().assertThat(new AddDistinctForSemiJoinBuild())
                .setSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, "true")
                .on(p ->
                {
                    VariableReferenceExpression sourceJoinVariable = p.variable("sourceJoinVariable");
                    VariableReferenceExpression filteringSourceJoinVariable = p.variable("filteringSourceJoinVariable");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput");
                    VariableReferenceExpression col1 = p.variable("col1");
                    return p.semiJoin(
                            sourceJoinVariable,
                            filteringSourceJoinVariable,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.values(sourceJoinVariable),
                            p.project(
                                    assignment(filteringSourceJoinVariable, p.rowExpression("col1")),
                                    p.aggregation((a) -> a
                                            .singleGroupingSet(col1)
                                            .step(AggregationNode.Step.SINGLE)
                                            .source(p.values(col1)))));
                }).doesNotFire();
    }
}

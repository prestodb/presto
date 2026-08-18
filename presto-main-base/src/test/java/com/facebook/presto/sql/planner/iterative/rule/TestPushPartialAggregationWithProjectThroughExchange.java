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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_STRATEGY;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class TestPushPartialAggregationWithProjectThroughExchange
        extends BaseRuleTest
{
    @Test
    public void testProjectPushedIntoExchangeSource()
    {
        FunctionResolution functionResolution = new FunctionResolution(
                tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        // Project computes c = a * 2; the partial aggregation uses SUM(c).
        // The rule strips the project (pushing it into the exchange source) AND pushes the
        // partial aggregation below the exchange in one shot, producing:
        //   Exchange -> Project(SUM->SUM) -> Agg(PARTIAL, SUM(c)) -> Project(c=a*2) -> Values(a)
        tester().assertThat(new PushPartialAggregationThroughExchangeRuleSet(getFunctionManager(), false).withProjectionRule())
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression c = p.variable("c", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.project(
                                            PlanBuilder.assignment(
                                                    c,
                                                    call("a * 2",
                                                            functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT),
                                                            BIGINT,
                                                            a,
                                                            constant(2L, BIGINT))),
                                            p.exchange(e -> e
                                                    .addSource(p.values(a))
                                                    .addInputsSet(a)
                                                    .singleDistributionPartitioningScheme(a))))
                            .addAggregation(p.variable("SUM", BIGINT), p.rowExpression("SUM(c)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .matches(exchange(
                        project(
                                aggregation(
                                        ImmutableMap.of("SUM", functionCall("sum", ImmutableList.of("c"))),
                                        PARTIAL,
                                        project(
                                                ImmutableMap.of("c", PlanMatchPattern.expression("a * 2")),
                                                values("a"))))));
    }

    @Test
    public void testProjectPushedIntoEachExchangeSourceBranchWithRemapping()
    {
        FunctionResolution functionResolution = new FunctionResolution(
                tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        // Exchange has two source branches with different underlying variable names (a0, a1),
        // both mapped to the single exchange output variable 'a'.
        // The rule must push the project into both branches and remap the expression:
        //   source 0: c = a0 * 2   (a -> a0)
        //   source 1: c = a1 * 2   (a -> a1)
        tester().assertThat(new PushPartialAggregationThroughExchangeRuleSet(getFunctionManager(), false).withProjectionRule())
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .on(p -> {
                    VariableReferenceExpression a0 = p.variable("a0", BIGINT);
                    VariableReferenceExpression a1 = p.variable("a1", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression c = p.variable("c", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.project(
                                            PlanBuilder.assignment(
                                                    c,
                                                    call("a * 2",
                                                            functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT),
                                                            BIGINT,
                                                            a,
                                                            constant(2L, BIGINT))),
                                            p.exchange(e -> e
                                                    .addSource(p.values(a0))
                                                    .addSource(p.values(a1))
                                                    .addInputsSet(a0)
                                                    .addInputsSet(a1)
                                                    .singleDistributionPartitioningScheme(a))))
                            .addAggregation(p.variable("SUM", BIGINT), p.rowExpression("SUM(c)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .matches(exchange(
                        project(
                                aggregation(
                                        ImmutableMap.of("SUM", functionCall("sum", ImmutableList.of("c"))),
                                        PARTIAL,
                                        project(
                                                ImmutableMap.of("c", PlanMatchPattern.expression("a0 * 2")),
                                                values("a0")))),
                        project(
                                aggregation(
                                        ImmutableMap.of("SUM", functionCall("sum", ImmutableList.of("c"))),
                                        PARTIAL,
                                        project(
                                                ImmutableMap.of("c", PlanMatchPattern.expression("a1 * 2")),
                                                values("a1"))))));
    }

    @Test
    public void testDoesNotFireWhenStrategyIsNever()
    {
        FunctionResolution functionResolution = new FunctionResolution(
                tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushPartialAggregationThroughExchangeRuleSet(getFunctionManager(), false).withProjectionRule())
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "NEVER")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression c = p.variable("c", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.project(
                                            PlanBuilder.assignment(
                                                    c,
                                                    call("a * 2",
                                                            functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT),
                                                            BIGINT,
                                                            a,
                                                            constant(2L, BIGINT))),
                                            p.exchange(e -> e
                                                    .addSource(p.values(a))
                                                    .addInputsSet(a)
                                                    .singleDistributionPartitioningScheme(a))))
                            .addAggregation(p.variable("SUM", BIGINT), p.rowExpression("SUM(c)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForIdentityProject()
    {
        // The project maps a -> a (a pure pass-through): the exchange output variable 'a' is
        // already present, so computedOutputs is empty and the rule should not fire.
        tester().assertThat(new PushPartialAggregationThroughExchangeRuleSet(getFunctionManager(), false).withProjectionRule())
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.project(
                                            PlanBuilder.assignment(a, a),
                                            p.exchange(e -> e
                                                    .addSource(p.values(a))
                                                    .addInputsSet(a)
                                                    .singleDistributionPartitioningScheme(a))))
                            .addAggregation(p.variable("SUM", BIGINT), p.rowExpression("SUM(a)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .doesNotFire();
    }
}
